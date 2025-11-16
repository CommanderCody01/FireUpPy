from unittest.mock import MagicMock

from cif.ingestion import Ingestion


def test_ingestion() -> None:
    ingestion = Ingestion(MagicMock(), MagicMock(), MagicMock())
    ingestion.intake.intake.return_value = 0
    ingestion.ingest()
    ingestion.intake.intake.assert_called_once_with()
    ingestion.disaggregation.disaggregate.assert_called_once_with(0)


def test_ingestion_no_changes() -> None:
    ingestion = Ingestion(MagicMock(), MagicMock(), MagicMock())
    ingestion.intake.intake.return_value = None
    ingestion.ingest()
    ingestion.intake.intake.assert_called_once_with()
    ingestion.disaggregation.disaggregate.assert_not_called()
