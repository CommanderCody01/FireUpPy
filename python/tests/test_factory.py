from typing import List
from unittest.mock import MagicMock

from pytest import fixture

from cif.factory import Factory
from cif.persistence import (
    BigQueryConnectorModel,
    BucketConnectorModel,
    ConnectorModel,
    DynamicPrefixBucketConnectorModel,
    ExtractorModel,
    FilesystemConnectorModel,
    HTMLExtractorModel,
    HTMLLinkExtractorModel,
    HTMLTitleExtractorModel,
    TextContentFilterModel,
)


@fixture
def connector_models() -> List[ConnectorModel]:
    return [
        BucketConnectorModel(bucket_name="bucket_name", glob_pattern="glob_pattern"),
        DynamicPrefixBucketConnectorModel(bucket_name="bucket_name", artifact_glob_pattern="glob_pattern", prefix="prefix"),
        FilesystemConnectorModel(root="root", glob_pattern="glob_pattern"),
        BigQueryConnectorModel(sql="sql", key_columns=["foo", "bar"]),
    ]


@fixture
def extractor_models() -> List[ExtractorModel]:
    return [HTMLExtractorModel(text_content_filter=TextContentFilterModel()), HTMLTitleExtractorModel(), HTMLLinkExtractorModel()]


def test_factory_new_connector_from_model(connector_models: List[ConnectorModel]) -> None:
    clients = MagicMock()
    config = Factory(clients)
    for connector_model in connector_models:
        connector = config.new_connector_from_model(connector_model)
        assert connector is not None


def test_factory_new_extractor_from_model(extractor_models: List[ExtractorModel]) -> None:
    clients = MagicMock()
    config = Factory(clients)
    connector = MagicMock()
    for extractor_model in extractor_models:
        extractor = config.new_extractor_from_model(connector, extractor_model)
        assert extractor is not None
        assert extractor.connector == connector
        if extractor_model.text_content_filter is not None:
            assert extractor.text_content_filter is not None
        else:
            assert extractor.text_content_filter is None


def test_factory_new_intake(connector_models: List[ConnectorModel]) -> None:
    clients = MagicMock()
    config = Factory(clients)
    source = MagicMock()
    source.connector = connector_models[0]
    config.new_connector_from_model = MagicMock()
    intake = config.new_intake(source)
    assert intake is not None
    config.new_connector_from_model.assert_called_once_with(connector_models[0])


def test_factory_new_disaggregation(connector_models: List[ConnectorModel]) -> None:
    clients = MagicMock()
    config = Factory(clients)
    source = MagicMock()
    source.connector = connector_models[0]
    config.new_connector_from_model = MagicMock()
    disaggregation = config.new_disaggregation(source)
    assert disaggregation is not None
    config.new_connector_from_model.assert_called_once_with(connector_models[0])


def test_factory_new_ingestion() -> None:
    clients = MagicMock()
    factory = Factory(clients)
    factory.new_intake = MagicMock()
    factory.new_disaggregation = MagicMock()
    source = MagicMock()
    ingestion = factory.new_ingestion(source)
    assert ingestion is not None
    factory.new_intake.assert_called_once_with(source)
    factory.new_disaggregation.assert_called_once_with(source)
