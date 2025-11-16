from hashlib import md5
from logging import getLogger
from pathlib import Path
from typing import Generator, List, Tuple

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.storage import Bucket
from magic import from_buffer

from cif.clients import Clients
from cif.util import Fingerprint

logger = getLogger(__name__)


class Connector:
    """
    A source of artifacts that can be managed by the CIF. Each subclass has a
    new_instance() method that accepts a Clients object as its first argument, followed
    kwargs for any other parameters that are needed.
    """

    def list_artifacts(self, **kwargs) -> Generator[Tuple[str, Fingerprint]]:
        """
        Enumerate the objects managed by this source as (external_id, Fingerprint) tuples.
        """
        raise NotImplementedError("list_artifacts")

    def get_artifact(
        self, external_id: str, offset: int = None, length: int = None, **kwargs
    ) -> Tuple[Generator[bytes], Fingerprint]:
        """
        Return data and metadata for the specified artifact.
        """
        raise NotImplementedError("get_artifact")

    def get_artifact_chunk(self, external_id: str, start: int, end: int, **kwargs) -> bytes:
        """
        Return a range of bytes from the specified artifact between start and end,
        inclusive.
        """
        raise NotImplementedError("get_artifact_chunk")

    def calc_line_chunks(self, external_id: str, lines_per_chunk: str, **kwargs) -> Generator[Tuple[int, int]]:
        """
        Scan the specified artifact and compute chunks containing a certain number of
        lines. The returned tuples represent offsets into the file suitable for use as
        the 'start' and 'end' parameters of the get_artifact_chunk() method. The
        artifact should support being opened for reading in text mode.
        """
        raise NotImplementedError("calc_line_chunks")


class FilesystemConnector(Connector):
    """
    A Connector that reads files matching a glob pattern from the filesystem.
    """

    @classmethod
    def new_instance(cls, clients: Clients, root: str = None, glob_pattern: str = None) -> "FilesystemConnector":
        return FilesystemConnector(Path(root), glob_pattern)

    def __init__(self, root: Path, glob_pattern: str) -> None:
        self.root = root
        self.glob_pattern = glob_pattern

    def list_artifacts(self, **kwargs) -> Generator[Tuple[str, Fingerprint]]:
        logger.info("Reading artifacts from %s, matching on glob pattern %s", self.root, self.glob_pattern)
        for path in self.root.glob(self.glob_pattern):
            _, fingerprint = self.get_artifact(path)
            yield path.name, fingerprint

    def get_artifact(self, external_id: str, chunk_size: int = 8192, **kwargs) -> Tuple[Generator[bytes], Fingerprint]:
        content_type = None
        digest = md5()
        content_length = 0
        content = b""
        with open(external_id, "rb") as f:
            while True:
                # until content_type is determined only read in 1k chunks as libmagic1
                # slows down *significantly* on larger buffers
                chunk = f.read(chunk_size if content_type is not None else 1024)
                if not chunk:
                    break
                if content_type is None:
                    content_type = from_buffer(chunk, mime=True)
                digest.update(chunk)
                content_length += len(chunk)
                content += chunk
        return iter([content]), Fingerprint(content_type=content_type, content_length=content_length, version=digest.hexdigest())

    def get_artifact_chunk(self, external_id: str, start: int, end: int, version: str = None) -> bytes:
        with open(external_id, "rb") as f:
            f.seek(start)
            return f.read(1 + end - start)


class BucketConnector(Connector):
    """
    A Connector that reads files matching a glob pattern from a GCS bucket.
    """

    @classmethod
    def new_instance(cls, clients: Clients, bucket_name: str = None, glob_pattern: str = None) -> "BucketConnector":
        bucket = clients.storage.bucket(bucket_name)
        return BucketConnector(bucket, glob_pattern)

    def __init__(self, bucket: Bucket, glob_pattern: str) -> None:
        self.bucket = bucket
        self.glob_pattern = glob_pattern

    def calc_glob_pattern(self) -> str:
        return self.glob_pattern

    def list_artifacts(self, **kwargs) -> Generator[Tuple[str, Fingerprint]]:
        glob_pattern = self.calc_glob_pattern()
        logger.info("Reading artifacts from %s, matching on glob pattern %s", self.bucket.name, glob_pattern)
        for blob in self.bucket.list_blobs(
            fields="items(name,size,contentType,generation),nextPageToken", match_glob=glob_pattern
        ):
            yield blob.name, Fingerprint(content_type=blob.content_type, content_length=blob.size, version=blob.generation)

    def get_artifact(
        self, external_id: str, version: str = None, chunk_size: int = (1024 * 1024)
    ) -> Tuple[Generator[bytes], Fingerprint]:
        blob = self.bucket.get_blob(external_id, generation=version)
        if blob is not None:
            fingerprint = Fingerprint(content_type=blob.content_type, content_length=blob.size, version=blob.generation)

            def stream_blob() -> Generator[bytes]:
                with blob.open("rb") as f:
                    while chunk := f.read(chunk_size):
                        yield chunk

            return stream_blob(), fingerprint
        else:
            raise ValueError(f"No blob found for {external_id}:{version}")

    def get_artifact_chunk(self, external_id: str, start_bytes: int, end_bytes: int, version: str = None) -> bytes:
        blob = self.bucket.get_blob(external_id, generation=version)
        if blob is not None:
            return blob.download_as_bytes(start=start_bytes, end=end_bytes)
        else:
            raise ValueError(f"No blob found for {external_id}:{version}")

    def calc_line_chunks(self, external_id: str, lines_per_chunk: int, version: str = None) -> Generator[Tuple[int, int]]:
        blob = self.bucket.get_blob(external_id, generation=version)
        if blob is not None:

            def stream_blob() -> Generator[bytes]:
                with blob.open("rt") as f:
                    start = 0
                    buf = ""
                    for line_num, line in enumerate(f, start=1):
                        buf += line
                        if line_num % lines_per_chunk == 0:
                            end = start + len(buf.encode("utf-8"))
                            yield start, end - 1
                            start = end
                            buf = ""
                    if start < blob.size:
                        yield start, blob.size

            return stream_blob()
        else:
            raise ValueError(f"No blob found for {external_id}:{version}")


class DynamicPrefixBucketConnector(BucketConnector):
    """
    A BucketConnector that determines its glob pattern by prepending a prefix to
    a caller-supplied glob pattern. The prefix is computed by filtering all top-level
    bucket prefixes against a pattern, sorting the results and returning the last one.

    The intent is to match prefixes with a pattern like <source>_<yyyymmdd> (e.g.
    "epolicies_20250407/") and select the most recent one in the event that multiple
    prefixes match.
    """

    @classmethod
    def new_instance(
        cls, clients: Clients, bucket_name: str = None, artifact_glob_pattern: str = None, prefix: str = None
    ) -> "DynamicPrefixBucketConnector":
        bucket = clients.storage.bucket(bucket_name)
        return DynamicPrefixBucketConnector(bucket, artifact_glob_pattern, prefix)

    def __init__(self, bucket: Bucket, artifact_glob_pattern: str, prefix: str) -> None:
        super().__init__(bucket, artifact_glob_pattern)
        self.prefix = prefix

    def calc_glob_pattern(self) -> str:
        """
        Compute the glob pattern to filter artifacts as "<prefix>*/<glob_pattern>"
        """
        prefixes = []
        for page in self.bucket.list_blobs(prefix=self.prefix, delimiter="/").pages:
            page_prefixes = page.prefixes
            prefixes.extend([x for x in page_prefixes if x.startswith(self.prefix)])
        prefixes.sort()
        if len(prefixes) > 0:
            glob_pattern = f"{prefixes[-1]}{self.glob_pattern}"
            return glob_pattern
        else:
            raise ValueError(f"No prefixes found matching {self.prefix}")


class BigQueryConnector(Connector):
    """
    A connector that reads artifacts from a BigQuery result set. It is configured with
    a SQL query that generates the result set and a list of key columns that will be
    used to generate the artifact IDs for each row.
    """

    @classmethod
    def new_instance(cls, clients: Clients, sql: str = None, key_columns: List[str] = None) -> "BigQueryConnector":
        return BigQueryConnector(clients.bigquery, sql, key_columns)

    def __init__(self, client: BigQueryClient, sql: str, key_columns: List[str]) -> None:
        self.client = client
        self.sql = sql
        self.key_columns = key_columns
