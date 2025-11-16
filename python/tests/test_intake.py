from datetime import timedelta
from unittest.mock import ANY, MagicMock, call

from pytest import fixture

from cif.intake import Intake
from cif.persistence import (
    DisaggregationMode,
    FilesystemConnectorModel,
    GenerationModel,
    SourceModel,
)
from cif.util import now


@fixture
def source() -> SourceModel:
    return SourceModel(
        source_id="source_id",
        external_id="external_id",
        category="category",
        created_on=now(),
        enabled=True,
        connector=FilesystemConnectorModel(root="../local/test_100/0", glob_pattern="*.txt"),
        extractors=[],
        disaggregation_mode=DisaggregationMode.IMMEDIATE,
        retain_generations=3,
    )


def test_intake_stage(source: SourceModel) -> None:
    stage_id = "stage_id"
    created_on = now()
    intake = Intake(MagicMock(), MagicMock(), source)
    intake.connector.list_artifacts.return_value = [MagicMock()] * 30
    intake.catalog.insert_stage_batch.return_value = 1
    num_batches = intake._stage(stage_id, created_on)
    assert num_batches == 1
    intake.catalog.insert_stage_batch.assert_has_calls(
        [call(stage_id, intake.source.source_id, batch_id, ANY, created_on) for batch_id in range(0, num_batches)]
    )


def test_intake_intake_no_data_staged(source: SourceModel) -> None:
    stage_id = "stage_id"
    created_on = now()
    num_batches = 0
    intake = Intake(MagicMock(), MagicMock(), source)
    intake._stage = MagicMock()
    intake._stage.return_value = num_batches
    intake._create_new_generation = MagicMock()
    intake.intake(use_stage_id=stage_id, use_created_on=created_on)
    intake._create_new_generation.assert_not_called()


def test_intake_intake_no_latest_generation(source: SourceModel) -> None:
    stage_id = "stage_id"
    created_on = now()
    num_batches = 15
    intake = Intake(MagicMock(), MagicMock(), source)
    intake._stage = MagicMock()
    intake._stage.return_value = num_batches
    intake._create_new_generation = MagicMock()
    intake.catalog.get_latest_generation.side_effect = [
        None,
        GenerationModel(source_id=source.source_id, generation_id=0, created_on=created_on),
    ]
    intake.intake(use_stage_id=stage_id, use_created_on=created_on)
    intake._stage.assert_called_once()
    intake._create_new_generation.assert_called_once_with(stage_id, created_on, num_batches)


def test_intake_intake_no_changes(source: SourceModel) -> None:
    stage_id = "stage_id"
    created_on = now()
    num_batches = 15
    intake = Intake(MagicMock(), MagicMock(), source)
    intake._stage = MagicMock()
    intake._stage.return_value = num_batches
    intake._create_new_generation = MagicMock()
    intake.catalog.get_latest_generation.return_value = GenerationModel(
        source_id=source.source_id, generation_id=0, created_on=now() - timedelta(hours=24)
    )
    intake.catalog.count_inserted_updated.return_value = 0
    intake.catalog.count_deleted.return_value = 0
    intake.intake(use_stage_id=stage_id, use_created_on=created_on)
    intake._stage.assert_called_once_with(stage_id, created_on)
    intake._create_new_generation.assert_not_called()


def test_intake_intake_changes(source: SourceModel) -> None:
    stage_id = "stage_id"
    created_on = now()
    num_batches = 15
    intake = Intake(MagicMock(), MagicMock(), source)
    intake._stage = MagicMock()
    intake._stage.return_value = num_batches
    intake._create_new_generation = MagicMock()
    intake.catalog.get_latest_generation.return_value = GenerationModel(
        source_id=source.source_id, generation_id=0, created_on=now() - timedelta(hours=24)
    )
    intake.catalog.count_inserted_updated.return_value = 3
    intake.catalog.count_deleted.return_value = 5
    intake.intake(use_stage_id=stage_id, use_created_on=created_on)
    intake._stage.assert_called_once_with(stage_id, created_on)
    intake._create_new_generation.assert_called_once_with(stage_id, created_on, num_batches)


def test_intake_create_new_generation(source: SourceModel) -> None:
    stage_id = "stage_id"
    created_on = now()
    num_batches = 3
    intake = Intake(MagicMock(), MagicMock(), source)
    intake.catalog.insert_artifact_generation_batch.side_effect = [(1, 0, 1), (0, 2, 2), (1, 1, 2)]
    counts = intake._create_new_generation(stage_id, created_on, num_batches)
    assert counts == (2, 3, 5)
    intake.catalog.insert_artifact_generation_batch.assert_has_calls(
        [call(stage_id, intake.source.source_id, batch_id) for batch_id in range(num_batches)]
    )
