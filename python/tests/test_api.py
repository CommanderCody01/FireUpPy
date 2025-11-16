from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from google.api_core.exceptions import DeadlineExceeded
from pytest import fixture, mark, raises

from cif.api import (
    app,
    check_artifact,
    check_source,
    check_source_and_generation,
    get_factory,
    timeout_exception_handler,
)
from cif.factory import Factory
from cif.persistence import (
    ArtifactModel,
    DisaggregationMode,
    FilesystemConnectorModel,
    GenerationModel,
    SourceModel,
)
from cif.util import now


@fixture
def test_client() -> TestClient:
    return TestClient(app)


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


@fixture
def generation() -> GenerationModel:
    return GenerationModel(source_id="source_id", generation_id=0, created_on=now())


@fixture
def artifact() -> ArtifactModel:
    return ArtifactModel(
        source_id="source_id",
        artifact_id="artifact_id",
        created_on=now(),
        external_id=str(Path(__file__).parent / "artifact.txt"),
        version="version",
        content_length=0,
        content_type="content_type",
    )


@fixture
def factory(source: SourceModel, generation: GenerationModel, artifact: ArtifactModel) -> Factory:
    factory = Factory(MagicMock())
    factory.catalog = MagicMock()
    factory.catalog.get_source.return_value = source
    factory.catalog.get_sources.return_value = ([source], None)
    factory.catalog.get_latest_generation.return_value = generation
    factory.catalog.get_generations.return_value = ([generation], None)
    factory.catalog.get_artifact.return_value = artifact
    factory.catalog.get_artifacts.return_value = ([artifact], None)
    factory.catalog.diff_generations.return_value = ([], None)
    factory.catalog.search_fragments.return_value = ([], None)
    factory.catalog.get_sources_by_external_id_like.return_value = ([], None)
    factory.catalog.search_fragments_json.return_value = ([], None)
    factory.catalog.search_fragments_key.return_value = ([], None)
    factory.catalog.search_fragments_ngram.return_value = ([], None)
    factory.catalog.get_deferred_disaggregations_by_date_range.return_value = ([], None)
    factory.catalog.get_deferred_disaggregation_summaries_by_date_range.return_value = ([], None)
    return factory


@patch("cif.api.get_clients")
@patch("cif.api.configure_logging")
def test_lifespan(mock_get_clients: MagicMock, mock_configure_logging: MagicMock) -> None:
    # lifespan is only called when TestClient is used as a context manager: see
    # https://fastapi.tiangolo.com/advanced/testing-events/
    with TestClient(app):
        mock_get_clients.assert_called_once()
        mock_configure_logging.assert_called_once()


@mark.asyncio
async def test_timeout_exception_handler() -> None:
    response = await timeout_exception_handler(MagicMock(), DeadlineExceeded("message"))
    assert response.body == "message".encode("utf-8")
    assert response.status_code == 504


@mark.asyncio
@patch("cif.api.get_clients")
async def test_get_factory(mock_get_clients: MagicMock) -> None:
    factory = await get_factory()
    assert factory is not None
    mock_get_clients.assert_called_once_with()


def test_ruok(test_client: TestClient) -> None:
    response = test_client.get("/1/ruok")
    assert response.status_code == 200
    assert response.text.startswith("OK ")


@mark.asyncio
async def test_check_source(factory: Factory) -> None:
    source = await check_source(factory.catalog, "source_id")
    assert source is not None
    factory.catalog.get_source.assert_called_once_with("source_id")


@mark.asyncio
async def test_check_source_not_found(factory: Factory) -> None:
    factory.catalog.get_source.return_value = None
    with raises(HTTPException, match="Source .* not found"):
        await check_source(factory.catalog, "source_id")
    factory.catalog.get_source.assert_called_once_with("source_id")


@mark.asyncio
async def test_check_source_and_generation(factory: Factory) -> None:
    source, generation = await check_source_and_generation(factory.catalog, "source_id", "generation_id")
    assert source is not None
    assert generation is not None
    factory.catalog.get_source.assert_called_once_with("source_id")
    factory.catalog.get_generation.assert_called_once_with("source_id", "generation_id")


@mark.asyncio
async def test_check_source_and_generation_not_found(factory: Factory) -> None:
    factory.catalog.get_generation.return_value = None
    with raises(HTTPException, match="Generation .* not found"):
        await check_source_and_generation(factory.catalog, "source_id", "generation_id")
    factory.catalog.get_source.assert_called_once_with("source_id")
    factory.catalog.get_generation.assert_called_once_with("source_id", "generation_id")


@mark.asyncio
async def test_check_artifact(factory: Factory) -> None:
    artifact = await check_artifact(factory.catalog, "artifact_id")
    assert artifact is not None
    factory.catalog.get_artifact.assert_called_once_with("artifact_id")


@mark.asyncio
async def test_check_artifact_not_found(factory: Factory) -> None:
    factory.catalog.get_artifact.return_value = None
    with raises(HTTPException, match="Artifact .* not found"):
        await check_artifact(factory.catalog, "artifact_id")
    factory.catalog.get_artifact.assert_called_once_with("artifact_id")


def test_get_sources(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.get("/1/sources")
        factory.catalog.get_sources.assert_called_once_with(limit=100, offset=0)
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_get_generations(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.get("/1/source/source_id/generations")
        assert response.status_code == 200
        factory.catalog.get_source.assert_called_once_with("source_id")
        factory.catalog.get_generations.assert_called_once_with("source_id", limit=100, offset=0)
    finally:
        test_client.app.dependency_overrides = {}


def test_get_latest_generation(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.get("/1/source/source_id/generation/latest")
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_get_latest_generation_not_found(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        factory.catalog.get_latest_generation.return_value = None
        response = test_client.get("/1/source/source_id/generation/latest")
        assert response.status_code == 404
        factory.catalog.get_latest_generation.assert_called_once()
    finally:
        test_client.app.dependency_overrides = {}


def test_get_artifacts(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.get("/1/source/source_id/generation/0/artifacts")
        assert response.status_code == 200
        factory.catalog.get_artifacts.assert_called_once_with("source_id", 0, limit=100, offset=0)
    finally:
        test_client.app.dependency_overrides = {}


def test_diff_generations(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.get("/1/source/source_id/generation/0/diff/1")
        assert response.status_code == 200
        factory.catalog.diff_generations.assert_called_once_with("source_id", 0, 1, limit=100, offset=0)
    finally:
        test_client.app.dependency_overrides = {}


def test_get_artifact(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.get("/1/artifact/artifact_id")
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_search_fragments(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post("/1/fragment/search", json={"query": "query", "source_id": "source_id"})
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_search_fragments_ngram(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post("/1/fragment/search/ngram", json={"query": "query", "source_id": "source_id"})
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_search_sources(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post("/1/source/search", json={"external_id_like": "epolicies:%"})
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_search_fragments_json(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post(
            "/1/fragment/search/json", json={"source_id": "source_id", "query": [{"json_path": "json_path", "values": ["value"]}]}
        )
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_search_fragments_key(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post(
            "/1/fragment/search/key", json={"source_id": "source_id", "query": [{"key": "key", "values": ["value"]}]}
        )
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_get_deferred_disaggregations_by_date_range(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post("/1/admin/deferred_disaggregation", json={})
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_get_deferred_disaggregation_summaries_by_date_range(test_client: TestClient, factory: Factory) -> None:
    try:
        test_client.app.dependency_overrides[get_factory] = lambda: factory
        response = test_client.post("/1/admin/deferred_disaggregation/summary", json={})
        assert response.status_code == 200
    finally:
        test_client.app.dependency_overrides = {}


def test_metrics_middleware_records_metrics(test_client: TestClient) -> None:
    """Tests if the metrics middleware calls the correct functions."""
    with patch("cif.middleware.metrics") as mocked_middleware_metrics:
        response = test_client.get("/1/ruok")
        assert response.status_code == 200
        assert mocked_middleware_metrics.increment_counter.call_count == 2
        assert mocked_middleware_metrics.record_histogram.call_count == 1
        assert mocked_middleware_metrics.set_gauge_value.call_count == 1

        req_params, res_params = mocked_middleware_metrics.increment_counter.call_args_list
        req_labels, res_labels = req_params[1]["labels"], res_params[1]["labels"]
        assert req_labels["method"] == "GET"
        assert req_labels["path"] == "/1/ruok"
        assert str(res_labels["status_code"]) == "200"


def test_metrics_middleware_records_metrics_during_exception(test_client: TestClient, factory: Factory) -> None:
    """Tests if the metrics middleware calls the correct functions when the endpoint raises an exception."""
    with patch("cif.middleware.metrics") as mocked_middleware_metrics:
        # make the endpoint raise and exception
        factory.catalog.get_sources.side_effect = Exception("test exc")
        test_client.app.dependency_overrides[get_factory] = lambda: factory

        try:
            _ = test_client.get("/1/sources")
        except Exception:
            # by default fastapi TestClient raises server exceptions
            # i.e. it does response.raise_for_status() by default
            pass

        assert mocked_middleware_metrics.increment_counter.call_count == 2
        assert mocked_middleware_metrics.record_histogram.call_count == 1
        assert mocked_middleware_metrics.set_gauge_value.call_count == 1

        req_params, res_params = mocked_middleware_metrics.increment_counter.call_args_list
        req_labels, res_labels = req_params[1]["labels"], res_params[1]["labels"]
        assert req_labels["method"] == "GET"
        assert req_labels["path"] == "/1/sources"
        assert str(res_labels["status_code"]) == "500"
