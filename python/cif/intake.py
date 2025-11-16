from datetime import datetime
from logging import getLogger
from typing import Optional, Tuple
from uuid import uuid4

from cif.catalog import Catalog
from cif.connector import Connector
from cif.persistence import GenerationModel, SourceModel
from cif.util import batcher, now

logger = getLogger(__name__)


class Intake:
    """
    Manages intake of artifacts into the catalog.
    """

    # spanner has mutation limit of 80k per transaction. the _create_new_generation
    # operation sets the upper bound for intake, modifying 11 columns + 3 indexes in
    # its current form, yielding the number below. if changing the batch size, validate
    # the number of mutations in current schema definition vs. the limits described
    # here: https://cloud.google.com/spanner/docs/limits

    BATCH_SIZE = 6153

    def __init__(self, catalog: Catalog, connector: Connector, source: SourceModel) -> None:
        self.catalog = catalog
        self.connector = connector
        self.source = source

    def intake(self, use_stage_id: str = None, use_created_on: datetime = None) -> Optional[GenerationModel]:
        """
        Performs intake of artifacts supplied by this instance's connector, possibly
        creating a new generation. If no changes exist between the latest generation for
        a source and the supplied artifacts, no changes will occur. Returns the
        created_on value of the new generation, or None if a new generation was not
        created.
        """
        stage_id = use_stage_id or uuid4().hex
        created_on = use_created_on or now()

        num_batches = self._stage(stage_id, created_on)
        if num_batches == 0:
            logger.info("No data staged for source %s", self.source.source_id)
            return

        latest = self.catalog.get_latest_generation(self.source.source_id)
        if latest is not None:
            num_inserted_updated = self.catalog.count_inserted_updated(stage_id, self.source.source_id, latest.generation_id)
            num_deleted = self.catalog.count_deleted(stage_id, self.source.source_id, latest.generation_id)
            if num_inserted_updated == 0 and num_deleted == 0:
                logger.info(
                    "No changes detected between generation %d of source %s and stage %s",
                    latest.generation_id,
                    self.source.source_id,
                    stage_id,
                )
                return None
            else:
                logger.info(
                    "Changes detected between generation %d of source %s and stage %s: +%d, -%d artifacts",
                    latest.generation_id,
                    self.source.source_id,
                    stage_id,
                    num_inserted_updated,
                    num_deleted,
                )

        logger.info("Will create new generation for source %s from stage %s", self.source.source_id, stage_id)
        counts = self._create_new_generation(stage_id, created_on, num_batches)
        generation = self.catalog.get_latest_generation(self.source.source_id)
        logger.info(
            "Created new generation %d for source %s from stage %s with %d artifacts",
            generation.generation_id,
            self.source.source_id,
            stage_id,
            counts[2],
        )
        return generation

    def _stage(self, stage_id: str, created_on: datetime) -> int:
        """
        Helper method to stage artifacts supplied by this instance's connector in batches
        of batch_size. Returns the number of batches staged.
        """
        logger.info("Starting stage %s", stage_id)
        num_rows = 0
        batch_id = 0
        artifacts = self.connector.list_artifacts()
        for batch_artifacts in batcher(artifacts, batch_size=self.BATCH_SIZE):
            rows = self.catalog.insert_stage_batch(stage_id, self.source.source_id, batch_id, batch_artifacts, created_on)
            num_rows += rows
            batch_id += 1
        num_batches = batch_id
        logger.info("Inserted %d rows (%d batches) into stage %s", num_rows, num_batches, stage_id)
        return num_batches

    def _create_new_generation(self, stage_id: str, created_on: datetime, num_batches: int) -> Tuple[int, int, int]:
        """
        Helper function to create artifacts and generation entries for staged artifacts.
        """
        counts = (0, 0, 0)
        for batch_id in range(num_batches):
            batch_counts = self.catalog.insert_artifact_generation_batch(stage_id, self.source.source_id, batch_id)
            counts = tuple((a + b for a, b in zip(counts, batch_counts)))
        return counts
