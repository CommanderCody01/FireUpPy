from io import StringIO
from pathlib import Path
from typing import List
from unittest.mock import MagicMock

from pytest import raises

from cif.connector import (
    BigQueryConnector,
    BucketConnector,
    Connector,
    DynamicPrefixBucketConnector,
    FilesystemConnector,
)
from cif.util import Fingerprint


def test_connector_list_artifacts_not_implemented() -> None:
    with raises(NotImplementedError, match="list_artifacts"):
        Connector().list_artifacts()


def test_connector_get_artifact_not_implemented() -> None:
    with raises(NotImplementedError, match="get_artifact"):
        Connector().get_artifact("external_id")


def test_connector_get_artifact_chunk_not_implemented() -> None:
    with raises(NotImplementedError, match="get_artifact_chunk"):
        Connector().get_artifact_chunk("external_id", 0, 100)


def test_connector_calc_line_chunks_not_implemented() -> None:
    with raises(NotImplementedError, match="calc_line_chunks"):
        Connector().calc_line_chunks("external_id", 1000)


def test_filesystem_connector_list_artifacts() -> None:
    fsc = FilesystemConnector(Path(__file__).parent, "artifact.txt")
    count = 0
    for artifact_id, fingerprint in fsc.list_artifacts():
        count = count + 1
    assert count == 1


def test_filesystem_connector_new_instance() -> None:
    connector = FilesystemConnector.new_instance(MagicMock(), root="root", glob_pattern="glob_pattern")
    assert connector is not None


def test_filesystem_connector_get_artifact() -> None:
    path = Path(__file__).parent / "artifact.txt"
    fsc = FilesystemConnector(Path("."), "glob_pattern")
    content, fingerprint = fsc.get_artifact(path)
    assert fingerprint.content_type == "text/plain"
    assert fingerprint.content_length == 20480
    # computed with /usr/bin/md5sum tests/artifact.txt
    assert fingerprint.version == "de0cf4509ad7ba59f5f378f52587c081"
    assert content is not None


def test_filesystem_connector_get_artifact_chunk() -> None:
    path = Path(__file__).parent / "artifact.txt"
    fsc = FilesystemConnector(Path("."), "glob_pattern")
    chunk = fsc.get_artifact_chunk(path, 0, 10)
    assert chunk == b"MFhz8M,kIlZ"
    assert len(chunk) == 11


def test_bucket_connector_new_instance() -> None:
    clients = MagicMock()
    connector = BucketConnector.new_instance(clients, bucket_name="bucket_name", glob_pattern="glob_pattern")
    assert connector is not None
    clients.storage.bucket.assert_called_once_with("bucket_name")


def test_bucket_connector_list_artifacts() -> None:
    bucket = MagicMock()
    bucket.list_blobs.return_value = [MagicMock()]
    bc = BucketConnector(bucket, glob_pattern="*.txt")
    count = 0
    for artifact_id, fingerprint in bc.list_artifacts():
        count = count + 1
    assert bc.calc_glob_pattern() == "*.txt"
    assert count == 1


def test_bucket_connector_get_artifact_not_found() -> None:
    bucket = MagicMock()
    bucket.get_blob.return_value = None
    bc = BucketConnector(bucket, "glob_pattern")
    with raises(ValueError, match=r"No blob .*"):
        bc.get_artifact("external_id")
    bucket.get_blob.assert_called_once_with("external_id", generation=None)


def test_bucket_connector_get_artifact() -> None:
    bucket = MagicMock()
    bucket.get_blob.return_value.content_type = "text/plain"
    bucket.get_blob.return_value.size = 5
    bucket.get_blob.return_value.generation = "generation"
    bucket.get_blob.return_value.open.return_value.__enter__.return_value.read.side_effect = [b"hello", None]
    bc = BucketConnector(bucket, "glob_pattern")
    stream, fingerprint = bc.get_artifact("external_id")
    assert fingerprint == Fingerprint(content_type="text/plain", content_length=5, version="generation")
    for x in stream:
        assert x == b"hello"


def test_bucket_connector_get_artifact_chunk_not_found() -> None:
    bucket = MagicMock()
    bucket.get_blob.return_value = None
    bc = BucketConnector(bucket, "glob_pattern")
    with raises(ValueError, match=r"No blob .*"):
        bc.get_artifact_chunk("external_id", 0, 10)
    bucket.get_blob.assert_called_once_with("external_id", generation=None)


def test_bucket_connector_get_artifact_chunk() -> None:
    bucket = MagicMock()
    bc = BucketConnector(bucket, "glob_pattern")
    bc.get_artifact_chunk("external_id", 0, 10)
    bc.bucket.get_blob.assert_called_once_with("external_id", generation=None)
    bc.bucket.get_blob.return_value.download_as_bytes.assert_called_once_with(start=0, end=10)


def test_bucket_connector_calc_line_chunks_not_found() -> None:
    bucket = MagicMock()
    bucket.get_blob.return_value = None
    bc = BucketConnector(bucket, "glob_pattern")
    with raises(ValueError, match=r"No blob .*"):
        bc.calc_line_chunks("external_id", 1000)
    bucket.get_blob.assert_called_once_with("external_id", generation=None)


def do_test_bucket_connector_calc_line_chunks(lines: List[str]) -> None:
    bucket = MagicMock()
    bc = BucketConnector(bucket, "glob_pattern")
    bc.bucket.get_blob.return_value.open.return_value.__enter__.return_value = StringIO(lines)
    bc.bucket.get_blob.return_value.size = len(lines.encode("utf-8"))
    line_chunks = list(bc.calc_line_chunks("external_id", 2))
    bc.bucket.get_blob.assert_called_once_with("external_id", generation=None)
    return line_chunks


def test_bucket_connector_calc_line_chunks_even() -> None:
    lines = "foo,bar,baz\n1,2,3\n4,5,6\n7,8,9\n"
    line_chunks = do_test_bucket_connector_calc_line_chunks(lines)
    # line 0-1, lines 2-3
    assert line_chunks == [(0, 17), (18, 29)]


def test_bucket_connector_calc_line_chunks_odd() -> None:
    lines = "foo,bar,baz\n1,2,3\n4,5,6\n"
    line_chunks = do_test_bucket_connector_calc_line_chunks(lines)
    # line 0-1, line 2
    assert line_chunks == [(0, 17), (18, 24)]


def test_dynamic_prefix_bucket_connector_new_instance() -> None:
    clients = MagicMock()
    connector = DynamicPrefixBucketConnector.new_instance(
        clients,
        bucket_name="bucket_name",
        artifact_glob_pattern="artifact_glob_pattern",
        prefix="prefix",
    )
    assert connector is not None
    clients.storage.bucket.assert_called_once_with("bucket_name")


def test_dynamic_prefix_bucket_connector_calc_glob_pattern() -> None:
    bucket = MagicMock()
    page = MagicMock()
    page.prefixes = ("foo/", "bar_20250101/", "bar_20240223/", "bar_20250619/", "baz/")
    bucket.list_blobs.return_value.pages = [page]
    bc = DynamicPrefixBucketConnector(bucket, "*.txt", "bar_")
    assert bc.calc_glob_pattern() == "bar_20250619/*.txt"


def test_dynamic_prefix_bucket_connecter_calc_glob_pattern_no_prefixes() -> None:
    bucket = MagicMock()
    page = MagicMock()
    page.prefixes = ("foo/", "bar_20250101/", "bar_20240223/", "bar_20250619/", "baz/")
    bucket.list_blobs.return_value.pages = [page]
    bc = DynamicPrefixBucketConnector(bucket, "*.txt", "quux_")
    with raises(ValueError, match=r"No prefixes found matching .*"):
        bc.calc_glob_pattern()


def test_bigquery_connector_new_instance() -> None:
    clients = MagicMock()
    connector = BigQueryConnector.new_instance(clients, sql="sql")
    assert connector is not None
    assert connector.client == clients.bigquery
    assert connector.sql == "sql"
