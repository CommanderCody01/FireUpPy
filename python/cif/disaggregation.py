from concurrent.futures import ThreadPoolExecutor, wait
from logging import getLogger
from typing import List
from uuid import uuid4

from cif.catalog import Catalog
from cif.clients import PubSupport
from cif.connector import Connector
from cif.extractor import Extractor
from cif.persistence import (
    ArtifactModel,
    DeferredDisaggregationModel,
    DisaggregationMode,
    GenerationModel,
    SourceModel,
)
from cif.util import batcher, flatten

logger = getLogger(__name__)


class Disaggregation:
    def __init__(
        self, catalog: Catalog, connector: Connector, pub: PubSupport, source: SourceModel, extractors: List[Extractor]
    ) -> None:
        self.catalog = catalog
        self.connector = connector
        self.pub = pub
        self.source = source
        self.extractors = extractors

    def disaggregate(self, generation: GenerationModel, limit: int = 100) -> int:
        """
        Disaggregate new artifacts for the specified generation per its configured
        disaggregtation mode.
        """
        logger.info("Starting disaggregation of source %s, generation %d", self.source.source_id, generation.generation_id)
        offset = 0
        count = 0
        while offset is not None:
            artifacts, offset = self.catalog.get_new_artifacts(
                self.source.source_id, generation.generation_id, limit=limit, offset=offset
            )
            if self.source.disaggregation_mode == DisaggregationMode.DEFERRED:
                count += self.defer_all(generation, artifacts)
                logger.info(
                    "Deferred %d disaggregations from source %s, generation %d",
                    count,
                    self.source.source_id,
                    generation.generation_id,
                )
            elif self.source.disaggregation_mode == DisaggregationMode.DEFERRED_CHUNKED:
                count += self.defer_and_chunk_all(generation, artifacts)
                logger.info(
                    "Deferred %d chunked disaggregations from source %s, generation %d",
                    count,
                    self.source.source_id,
                    generation.generation_id,
                )
            elif self.source.disaggregation_mode == DisaggregationMode.IMMEDIATE_CHUNKED:
                count += self.disaggregate_and_chunk_all(generation, artifacts)
                logger.info(
                    "Processed %d chunked disaggregations from source %s, generation %d",
                    count,
                    self.source.source_id,
                    generation.generation_id,
                )
            else:  # DisaggregationMode.IMMEDIATE
                count += self.disaggregate_all(generation, artifacts)
                logger.info(
                    "Processed %d disaggregations from source %s, generation %d",
                    count,
                    self.source.source_id,
                    generation.generation_id,
                )
        logger.info("Finished disaggregation of source %s, generation %d", self.source.source_id, generation.generation_id)
        return count

    def disaggregate_all(self, generation: GenerationModel, artifacts: List[ArtifactModel]) -> int:
        """
        Runs all extractors on a set of artifacts serially. Returns the number of
        artifact/extractor combinations processed.
        """
        count = 0
        for artifact in artifacts:
            for extractor in self.extractors:
                count += self.disaggregate_one(generation, artifact, extractor)
        return count

    def disaggregate_one(
        self,
        generation: GenerationModel,
        artifact: ArtifactModel,
        extractor: Extractor,
        fragment_id: str = None,
        start_byte: int = None,
        end_byte: int = None,
        **kwargs
    ) -> int:
        """
        Runs the supplied extractor on an artifact, writing all Fragment objects
        to the database for each piece of extracted text. Returns 1 if the extractor
        ran successfully on the artifact, zero otherwise. Any keyword arguments are
        passed through to the calc_fragments() method of the supplied extractor.
        """
        fragments = extractor.calc_fragments(artifact, fragment_id=fragment_id, start_byte=start_byte, end_byte=end_byte)
        logger.info("Extracted %d fragments from artifact %s", len(fragments), artifact.artifact_id)
        num_batches = 0
        rows = 0
        for batch_fragments in batcher(fragments, batch_size=1000):
            rows += self.catalog.insert_fragments(batch_fragments)
            logger.debug(
                "Inserted %d rows for fragment %s seq_no %d-%d",
                len(batch_fragments),
                set(x.fragment_id for x in batch_fragments),
                min([x.seq_no for x in batch_fragments]),
                max([x.seq_no for x in batch_fragments]),
            )
            num_batches += 1
        logger.info("Inserted %d rows (%d batches) into fragment for source %s", rows, num_batches, self.source.source_id)

        fragment_keys = flatten([extractor.calc_fragment_keys(artifact, fragment) for fragment in fragments])
        logger.info("Extracted %d fragment keys from artifact %s", len(fragment_keys), artifact.artifact_id)
        rows = 0
        num_batches = 0
        for batch_fragment_keys in batcher(fragment_keys, batch_size=2000):
            rows += self.catalog.insert_fragment_keys(batch_fragment_keys)
            logger.debug(
                "Inserted %d rows for fragment_key %s seq_no %d-%d",
                len(batch_fragment_keys),
                set(x.fragment_id for x in batch_fragment_keys),
                min([x.seq_no for x in batch_fragment_keys]),
                max([x.seq_no for x in batch_fragment_keys]),
            )
            num_batches += 1
        logger.info("Inserted %d rows (%d batches) into fragment_key for source %s", rows, num_batches, self.source.source_id)

        return 1

    def disaggregate_and_chunk_all(
        self, generation: GenerationModel, artifacts: List[ArtifactModel], lines_per_chunk: int = 50000
    ) -> int:
        """ """
        with ThreadPoolExecutor(max_workers=3) as tpe:
            fs = []
            for artifact in artifacts:
                logger.info("Splitting large artifact %s (%d bytes) into chunks", artifact.artifact_id, artifact.content_length)
                for start_byte, end_byte in self.connector.calc_line_chunks(artifact.external_id, lines_per_chunk):
                    for extractor in self.extractors:
                        fragment_id = uuid4().hex
                        fs.append(
                            tpe.submit(self.disaggregate_one, generation, artifact, extractor, fragment_id, start_byte, end_byte)
                        )
            wait(fs)
            return sum([f.result() for f in fs])

    def defer_and_chunk_all(
        self, generation: GenerationModel, artifacts: List[ArtifactModel], lines_per_chunk: int = 50000
    ) -> int:
        """
        Schedules a set of artifacts for disaggregation. Each artifact will be split on
        line boundaries in chunks of the specified size. One message will be published
        for each artifact/chunk/extractor combination. Returns the number of messages
        sent.
        """
        msgs = []
        for artifact in artifacts:
            logger.info("Splitting large artifact %s (%d bytes) into chunks", artifact.artifact_id, artifact.content_length)
            for start_byte, end_byte in self.connector.calc_line_chunks(artifact.external_id, lines_per_chunk):
                for extractor in self.source.extractors:
                    fragment_id = uuid4().hex
                    msgs.append(
                        DeferredDisaggregationModel(
                            source_id=self.source.source_id,
                            generation_id=generation.generation_id,
                            artifact_id=artifact.artifact_id,
                            extractor_type=extractor.type,
                            created_on=generation.created_on,
                            start_byte=start_byte,
                            end_byte=end_byte,
                            fragment_id=fragment_id,
                        )
                    )
        self.do_publish_messages(msgs)
        return len(msgs)

    def defer_all(self, generation: GenerationModel, artifacts: List[ArtifactModel]) -> int:
        """
        Schedules a set of artifacts for disaggregation. One message will be published
        for each artifact/extractor combination. Returns the number of messages
        sent.
        """
        msgs = []
        for artifact in artifacts:
            # run each extractor on the whole artifact if it's small enough
            logger.info("Will disaggregate artifact %s in a single pass", artifact.artifact_id)
            for extractor in self.source.extractors:
                msgs.append(
                    DeferredDisaggregationModel(
                        source_id=self.source.source_id,
                        generation_id=generation.generation_id,
                        artifact_id=artifact.artifact_id,
                        extractor_type=extractor.type,
                        created_on=generation.created_on,
                    )
                )
        self.do_publish_messages(msgs)
        return len(msgs)

    def do_publish_messages(self, msgs: List[DeferredDisaggregationModel]) -> int:
        """
        Saves the supplied deferred disaggregations to the database and publishes
        them to the work topic for processing.
        """
        for batch_msgs in batcher(msgs, batch_size=5000):
            self.catalog.insert_deferred_disaggregations(batch_msgs)
        wait(self.pub.publish_messages([msg.model_dump_json().encode("utf-8") for msg in msgs]))
        return len(msgs)
