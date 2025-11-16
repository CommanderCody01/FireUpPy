from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel


class DynamicPrefixBucketConnectorModel(BaseModel):
    type: Literal["DynamicPrefixBucketConnector"] = "DynamicPrefixBucketConnector"
    bucket_name: str
    artifact_glob_pattern: str
    prefix: str


class BucketConnectorModel(BaseModel):
    type: Literal["BucketConnector"] = "BucketConnector"
    bucket_name: str
    glob_pattern: str


class FilesystemConnectorModel(BaseModel):
    type: Literal["FilesystemConnector"] = "FilesystemConnector"
    root: str
    glob_pattern: str


class BigQueryConnectorModel(BaseModel):
    """
    Configuration model for BigQuery connectors.
    """

    type: Literal["BigQueryConnector"] = "BigQueryConnector"
    sql: str
    key_columns: List[str]


ConnectorModel = DynamicPrefixBucketConnectorModel | BucketConnectorModel | FilesystemConnectorModel | BigQueryConnectorModel


class TextContentFilterModel(BaseModel):
    include_base_stop_words: bool = True
    additional_stop_words: List[str] = []


class BaseExtractorModel(BaseModel):
    text_content_filter: Optional[TextContentFilterModel] = None


class HTMLExtractorModel(BaseExtractorModel):
    type: Literal["HTMLExtractor"] = "HTMLExtractor"


class HTMLLinkExtractorModel(BaseExtractorModel):
    type: Literal["HTMLLinkExtractor"] = "HTMLLinkExtractor"


class HTMLTitleExtractorModel(BaseExtractorModel):
    type: Literal["HTMLTitleExtractor"] = "HTMLTitleExtractor"


class CSVRowExtractorModel(BaseExtractorModel):
    type: Literal["CSVRowExtractor"] = "CSVRowExtractor"


ExtractorModel = HTMLExtractorModel | HTMLLinkExtractorModel | HTMLTitleExtractorModel | CSVRowExtractorModel


class DisaggregationMode(StrEnum):
    IMMEDIATE = "IMMEDIATE"
    IMMEDIATE_CHUNKED = "IMMEDIATE_CHUNKED"
    DEFERRED = "DEFERRED"
    DEFERRED_CHUNKED = "DEFERRED_CHUNKED"


class SourceModel(BaseModel):
    source_id: str
    created_on: datetime
    external_id: str
    category: str
    enabled: bool
    connector: ConnectorModel
    extractors: List[ExtractorModel]
    disaggregation_mode: DisaggregationMode
    retain_generations: int


class GenerationModel(BaseModel):
    source_id: str
    generation_id: int
    created_on: datetime


class ArtifactModel(BaseModel):
    source_id: str
    artifact_id: str
    created_on: datetime
    external_id: str
    version: str
    content_length: int
    content_type: str


class Change(StrEnum):
    INSERTED = "INSERTED"
    DELETED = "DELETED"
    UPDATED = "UPDATED"
    NONE = "NONE"


class ArtifactChangeModel(BaseModel):
    external_id: str
    artifact_id_a: Optional[str]
    artifact_id_b: Optional[str]
    change: Change


class AggregationLevel(StrEnum):
    DOCUMENT = "DOCUMENT"
    LINK = "LINK"
    TITLE = "TITLE"
    ROW = "ROW"


class FragmentModel(BaseModel):
    source_id: str
    artifact_id: str
    fragment_id: str
    aggregation_level: AggregationLevel
    seq_no: int
    text_content: str
    json_content: Optional[Dict[str, Any]] = None


class FragmentKeyModel(BaseModel):
    source_id: str
    artifact_id: str
    fragment_id: str
    seq_no: int
    key: str
    value: str


class FragmentView(FragmentModel):
    generation_id: int
    external_id: str
    relevance: Optional[float] = None


Persistent = SourceModel | GenerationModel | ArtifactModel | FragmentModel

# A general type for all paginated query operations. Returns row data as a list of
# Record offset for the next page (if any).

PaginatedQueryResults = Tuple[List[Persistent], Optional[int]]


class DisaggregationStatus(StrEnum):
    PENDING = "PENDING"
    DONE = "DONE"
    FAILED = "FAILED"


class DeferredDisaggregationModel(BaseModel):
    source_id: str
    generation_id: int
    artifact_id: str
    extractor_type: str
    fragment_id: Optional[str] = None
    start_byte: Optional[int] = None
    end_byte: Optional[int] = None
    created_on: datetime
    status: DisaggregationStatus = DisaggregationStatus.PENDING
    delivery_attempt: int = 0


class DeferredDisaggregationSummaryModel(BaseModel):
    source_id: str
    generation_id: int
    disaggregation_count: int
    artifact_count: int
    avg_delivery_attempt: float
    min_created_on: datetime
    max_created_on: datetime
    status: DisaggregationStatus


class JsonSearchTerm(BaseModel):
    json_path: str
    values: List[str]


class KeySearchTerm(BaseModel):
    key: str
    values: List[str]


class MetricName(StrEnum):
    APP_STARTUP_COUNT = "app_startup_count"
    REQUEST_COUNT = "request_count"
    RESPONSE_COUNT = "response_count"
    REQUEST_LATENCY = "request_latency"
    REQUEST_LATENCY_CURRENT = "request_latency_current"
    WORKER_CALLBACK = "worker_callback"
    WORKER_DISAGGREGATION = "worker_disaggregation"


class MetricLabels(BaseModel):
    method: str | None = None
    status_code: int | None = None
    path: str | None = None
    source_id: str | None = None
    generation_id: int | None = None
    artifact_id: str | None = None
    extractor_type: str | None = None
    # task name is used to name metrics/functions like worker and ingestion
    task_name: str | None = None
    task_success: bool | None = None

    def model_dump(self, **kwargs: Any) -> dict[str, str]:
        # makes exclude_none=True by default to ignore the None values so that we only
        # have the non-null values in the final dictionary of the metric labels.
        return super().model_dump(exclude_none=True, **kwargs)
