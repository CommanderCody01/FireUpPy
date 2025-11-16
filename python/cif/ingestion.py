from logging import getLogger

from cif.disaggregation import Disaggregation
from cif.intake import Intake
from cif.persistence import SourceModel

logger = getLogger(__name__)


class Ingestion:
    """
    Manages the full ingestion process for a source.
    """

    def __init__(self, source: SourceModel, intake: Intake, disaggregation: Disaggregation) -> None:
        self.source = source
        self.intake = intake
        self.disaggregation = disaggregation

    def ingest(self) -> None:
        try:
            logger.info("Starting ingestion for source %s", self.source.source_id)
            generation = self.intake.intake()
            if generation is not None:
                self.disaggregation.disaggregate(generation)
        finally:
            logger.info("Ingestion for source %s finished", self.source.source_id)
