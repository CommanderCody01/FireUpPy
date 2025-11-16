from unittest.mock import MagicMock

from pydantic import ValidationError
from pytest import fixture, raises

from cif.extractor import HTMLExtractor
from cif.persistence import (
    ArtifactModel,
    DeferredDisaggregationModel,
    DisaggregationMode,
    DisaggregationStatus,
    FilesystemConnectorModel,
    GenerationModel,
    HTMLExtractorModel,
    HTMLLinkExtractorModel,
    SourceModel,
)
from cif.util import now
from cif.worker import NotFoundError, Worker


@fixture
def source() -> SourceModel:
    return SourceModel(
        source_id="source_id",
        external_id="external_id",
        category="category",
        created_on=now(),
        enabled=True,
        connector=FilesystemConnectorModel(root="../local/test_100/0", glob_pattern="*.txt"),
        extractors=[HTMLExtractorModel(), HTMLLinkExtractorModel()],
        disaggregation_mode=DisaggregationMode.DEFERRED,
        retain_generations=3,
    )


@fixture
def generation() -> GenerationModel:
    return GenerationModel(source_id="source_id", generation_id=0, created_on=now())


@fixture
def artifact() -> ArtifactModel:
    return ArtifactModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        version="version",
        external_id="external_id",
        created_on=now(),
        content_length=0,
        content_type="text/html",
    )


def test_worker_parse_message() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    parsed_message = DeferredDisaggregationModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        extractor_type="HTMLExtractor",
        created_on=now(),
    )
    message = MagicMock()
    message.data = parsed_message.model_dump_json().encode("utf-8")
    assert worker.parse_message(message) == parsed_message


def test_worker_parse_message_invalid() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    message = MagicMock()
    message.data = b""
    with raises(ValidationError):
        worker.parse_message(message)


def test_worker_call(source: SourceModel, generation: GenerationModel, artifact: ArtifactModel) -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    mock_disaggregation_model = DeferredDisaggregationModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        extractor_type="HTMLExtractor",
        created_on=now(),
    )
    worker.parse_message = MagicMock(return_value=mock_disaggregation_model)
    worker.check_references = MagicMock(return_value=(source, generation, artifact))
    worker.check_extractor = MagicMock(return_value=HTMLExtractor(MagicMock()))
    worker.handle_disaggregation_done = MagicMock()
    worker.handle_disaggregation_failed_maybe_retry = MagicMock()
    worker.handle_disaggregation_failed_no_retry = MagicMock()
    message = MagicMock()
    worker(message)
    worker.parse_message.assert_called_once_with(message)
    worker.check_references.assert_called_once_with(worker.parse_message.return_value)
    worker.check_extractor.assert_called_once_with(
        worker.parse_message.return_value, worker.factory.new_disaggregation.return_value
    )
    worker.factory.new_disaggregation.return_value.disaggregate_one.assert_called_once_with(
        generation,
        artifact,
        worker.check_extractor.return_value,
        start_byte=worker.parse_message.return_value.start_byte,
        end_byte=worker.parse_message.return_value.end_byte,
        fragment_id=worker.parse_message.return_value.fragment_id,
    )
    worker.handle_disaggregation_done.assert_called_once_with(message, worker.parse_message.return_value)
    worker.handle_disaggregation_failed_maybe_retry.assert_not_called()
    worker.handle_disaggregation_failed_no_retry.assert_not_called()


def test_worker_call_validation_error() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    worker.parse_message = MagicMock(side_effect=ValidationError("some validation error", []))
    worker.check_references = MagicMock()
    worker.check_extractor = MagicMock()
    worker.handle_disaggregation_done = MagicMock()
    worker.handle_disaggregation_failed_maybe_retry = MagicMock()
    worker.handle_disaggregation_failed_no_retry = MagicMock()
    message = MagicMock()
    worker(message)
    worker.parse_message.assert_called_once_with(message)
    worker.check_references.assert_not_called()
    worker.check_extractor.assert_not_called()
    worker.factory.new_disaggregation.return_value.disaggregate_one.assert_not_called()
    worker.handle_disaggregation_failed_no_retry.assert_called_once_with(message, None)
    worker.handle_disaggregation_done.assert_not_called()
    worker.handle_disaggregation_failed_maybe_retry.assert_not_called()


def test_worker_call_not_found_error(source: SourceModel, generation: GenerationModel, artifact: ArtifactModel) -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    worker.parse_message = MagicMock()
    worker.check_references = MagicMock(side_effect=NotFoundError)
    worker.check_extractor = MagicMock()
    worker.handle_disaggregation_done = MagicMock()
    worker.handle_disaggregation_failed_maybe_retry = MagicMock()
    worker.handle_disaggregation_failed_no_retry = MagicMock()
    message = MagicMock()
    worker(message)
    worker.parse_message.assert_called_once_with(message)
    worker.check_references.assert_called_once_with(worker.parse_message.return_value)
    worker.check_extractor.assert_not_called()
    worker.factory.new_disaggregation.return_value.disaggregate_one.assert_not_called()
    worker.handle_disaggregation_failed_no_retry.assert_called_once_with(message, worker.parse_message.return_value)
    worker.handle_disaggregation_done.assert_not_called()
    worker.handle_disaggregation_failed_maybe_retry.assert_not_called()


def test_worker_call_other_error(source: SourceModel, generation: GenerationModel, artifact: ArtifactModel) -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    mock_disaggregation_model = DeferredDisaggregationModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        extractor_type="HTMLExtractor",
        created_on=now(),
    )
    worker.parse_message = MagicMock(return_value=mock_disaggregation_model)
    worker.check_references = MagicMock(return_value=(source, generation, artifact))
    worker.check_extractor = MagicMock(return_value=HTMLExtractor(MagicMock()))
    worker.factory.new_disaggregation.return_value.disaggregate_one.side_effect = ValueError("some other error")
    worker.handle_disaggregation_done = MagicMock()
    worker.handle_disaggregation_failed_maybe_retry = MagicMock()
    worker.handle_disaggregation_failed_no_retry = MagicMock()
    message = MagicMock()
    worker(message)
    worker.parse_message.assert_called_once_with(message)
    worker.check_references.assert_called_once_with(worker.parse_message.return_value)
    worker.check_extractor.assert_called_once_with(
        worker.parse_message.return_value, worker.factory.new_disaggregation.return_value
    )
    worker.factory.new_disaggregation.return_value.disaggregate_one.assert_called_once_with(
        generation,
        artifact,
        worker.check_extractor.return_value,
        start_byte=worker.parse_message.return_value.start_byte,
        end_byte=worker.parse_message.return_value.end_byte,
        fragment_id=worker.parse_message.return_value.fragment_id,
    )
    worker.handle_disaggregation_done.assert_not_called()
    worker.handle_disaggregation_failed_maybe_retry.assert_called_once_with(message, worker.parse_message.return_value)
    worker.handle_disaggregation_failed_no_retry.assert_not_called()


def test_worker_handle_disaggregation_done() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    message = MagicMock()
    message.delivery_attempt = 1
    parsed_message = MagicMock()
    worker.handle_disaggregation_done(message, parsed_message)
    assert parsed_message.delivery_attempt == message.delivery_attempt
    assert parsed_message.status == DisaggregationStatus.DONE
    worker.factory.catalog.insert_deferred_disaggregations.assert_called_once_with([parsed_message])
    message.ack.assert_called_once_with()
    message.nack.assert_not_called()


def test_worker_handle_disaggregation_failed_maybe_retry() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    message = MagicMock()
    message.delivery_attempt = 1
    parsed_message = MagicMock()
    worker.handle_disaggregation_failed_maybe_retry(message, parsed_message)
    assert parsed_message.delivery_attempt == message.delivery_attempt
    assert parsed_message.status == DisaggregationStatus.FAILED
    worker.factory.catalog.insert_deferred_disaggregations.assert_called_once_with([parsed_message])
    message.ack.assert_not_called()
    message.nack.assert_called_once_with()


def test_worker_handle_disaggregation_failed_no_retry() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    message = MagicMock()
    message.delivery_attempt = 1
    parsed_message = MagicMock()
    worker.handle_disaggregation_failed_no_retry(message, parsed_message)
    assert parsed_message.delivery_attempt == message.delivery_attempt
    assert parsed_message.status == DisaggregationStatus.FAILED
    worker.factory.catalog.insert_deferred_disaggregations.assert_called_once_with([parsed_message])
    message.ack.assert_called_once_with()
    message.nack.assert_not_called()


def test_worker_handle_disaggregation_failed_no_retry_unparseable() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    message = MagicMock()
    worker.handle_disaggregation_failed_no_retry(message, None)
    worker.factory.catalog.insert_deferred_disaggregations.assert_not_called()
    message.ack.assert_called_once_with()
    message.nack.assert_not_called()


def test_worker_check_references_not_found(source: SourceModel, generation: GenerationModel, artifact: ArtifactModel) -> None:
    factory = MagicMock()
    factory.catalog.get_source.return_value = source
    factory.catalog.get_generation.return_value = generation
    factory.catalog.get_artifact.return_value = None
    worker = Worker(factory, metrics=MagicMock())
    with raises(NotFoundError, match=r"artifact_id artifact_id not found"):
        parsed_message = DeferredDisaggregationModel(
            source_id=source.source_id,
            generation_id=generation.generation_id,
            artifact_id=artifact.artifact_id,
            extractor_type="HTMLExtractor",
            created_on=now(),
        )
        worker.check_references(parsed_message)


def test_worker_check_references(source: SourceModel, generation: GenerationModel, artifact: ArtifactModel) -> None:
    factory = MagicMock()
    factory.catalog.get_source.return_value = source
    factory.catalog.get_generation.return_value = generation
    factory.catalog.get_artifact.return_value = artifact
    worker = Worker(factory, metrics=MagicMock())
    parsed_message = DeferredDisaggregationModel(
        source_id=source.source_id,
        generation_id=generation.generation_id,
        artifact_id=artifact.artifact_id,
        extractor_type="HTMLExtractor",
        created_on=now(),
    )
    referenced_source, referenced_generation, referenced_artifact = worker.check_references(parsed_message)
    assert referenced_source == factory.catalog.get_source.return_value
    assert referenced_generation == factory.catalog.get_generation.return_value
    assert referenced_artifact == factory.catalog.get_artifact.return_value
    factory.catalog.get_source.assert_called_once_with(parsed_message.source_id)
    factory.catalog.get_generation.assert_called_once_with(parsed_message.source_id, parsed_message.generation_id)
    factory.catalog.get_artifact.assert_called_once_with(parsed_message.artifact_id)


def test_worker_check_extractor() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    parsed_message = DeferredDisaggregationModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        extractor_type="HTMLExtractor",
        created_on=now(),
    )
    disaggregation = MagicMock()
    disaggregation.extractors = [HTMLExtractor(MagicMock())]
    assert worker.check_extractor(parsed_message, disaggregation) == disaggregation.extractors[0]


def test_worker_check_extractor_not_found() -> None:
    factory = MagicMock()
    worker = Worker(factory, metrics=MagicMock())
    parsed_message = DeferredDisaggregationModel(
        source_id="source_id",
        generation_id=0,
        artifact_id="artifact_id",
        extractor_type="CSVRowExtractor",
        created_on=now(),
    )
    disaggregation = MagicMock()
    disaggregation.extractors = [HTMLExtractor(MagicMock())]
    with raises(NotFoundError, match=r"extractor_type CSVRowExtractor not found"):
        worker.check_extractor(parsed_message, disaggregation)
