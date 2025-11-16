from cif.catalog import Catalog
from cif.clients import Clients
from cif.connector import Connector
from cif.disaggregation import Disaggregation
from cif.extractor import Extractor
from cif.ingestion import Ingestion
from cif.intake import Intake
from cif.persistence import (
    ConnectorModel,
    ExtractorModel,
    SourceModel,
)
from cif.util import calc_subclasses


class Factory:
    """
    Factory for creating objects based from configurations.
    """

    def __init__(self, clients: Clients) -> None:
        self.clients = clients
        self.catalog = Catalog(clients.spanner)
        self.connectors = {x.__name__: x for x in calc_subclasses(Connector)}
        self.extractors = {x.__name__: x for x in calc_subclasses(Extractor)}

    def new_connector_from_model(self, model: ConnectorModel) -> Connector:
        return self.connectors[model.type].new_instance(
            self.clients, **{k: v for k, v in model.model_dump().items() if k != "type"}
        )

    def new_extractor_from_model(self, connector: Connector, model: ExtractorModel) -> Extractor:
        return self.extractors[model.type].new_instance(connector, text_content_filter=model.text_content_filter)

    def new_intake(self, source: SourceModel) -> Intake:
        return Intake(self.catalog, self.new_connector_from_model(source.connector), source)

    def new_disaggregation(self, source: SourceModel) -> Disaggregation:
        connector = self.new_connector_from_model(source.connector)
        return Disaggregation(
            self.catalog,
            connector,
            self.clients.pub,
            source,
            [self.new_extractor_from_model(connector, x) for x in source.extractors],
        )

    def new_ingestion(self, source: SourceModel) -> Ingestion:
        return Ingestion(source, self.new_intake(source), self.new_disaggregation(source))
