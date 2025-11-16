from contextlib import asynccontextmanager
from datetime import datetime
from functools import cache
from logging import getLogger
from mimetypes import guess_extension
from typing import Annotated, List, Optional, Tuple

from asyncer import asyncify
from common_metrics.metrics import metrics
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from google.api_core.exceptions import DeadlineExceeded
from pydantic import BaseModel

from cif.catalog import Catalog
from cif.clients import get_clients
from cif.factory import Factory
from cif.middleware import RequestMetricsMiddleware
from cif.persistence import (
    AggregationLevel,
    ArtifactChangeModel,
    ArtifactModel,
    DeferredDisaggregationModel,
    DeferredDisaggregationSummaryModel,
    FragmentView,
    GenerationModel,
    JsonSearchTerm,
    KeySearchTerm,
    MetricName,
    SourceModel,
)
from cif.util import calc_version, configure_logging

logger = getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    configure_logging()
    logger.info(f"Starting cif-api {cached_calc_version()}")
    metrics.init("csr-va-cif-api")
    get_clients()  # force initialization here
    metrics.increment_counter(MetricName.APP_STARTUP_COUNT, value=1)
    yield
    logger.info("Stopping")


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestMetricsMiddleware)


@app.exception_handler(DeadlineExceeded)
async def timeout_exception_handler(request: Request, exception: DeadlineExceeded) -> PlainTextResponse:
    return PlainTextResponse(exception.message, status_code=504)


class QueryResults(BaseModel):
    next_offset: Optional[int] = None
    records: List[
        SourceModel
        | GenerationModel
        | ArtifactModel
        | ArtifactChangeModel
        | FragmentView
        | DeferredDisaggregationModel
        | DeferredDisaggregationSummaryModel
    ] = []


async def get_factory() -> Factory:
    return Factory(get_clients())


async def check_source(catalog: Catalog, source_id: str) -> SourceModel:
    source = await asyncify(catalog.get_source)(source_id)
    if source is not None:
        return source
    else:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")


async def check_source_and_generation(
    catalog: Catalog, source_id: str, generation_id: str
) -> Tuple[SourceModel, GenerationModel]:
    source = await check_source(catalog, source_id)
    generation = await asyncify(catalog.get_generation)(source_id, generation_id)
    if generation is not None:
        return source, generation
    else:
        raise HTTPException(status_code=404, detail=f"Generation {generation_id} not found")


async def check_artifact(catalog: Catalog, artifact_id: str) -> ArtifactModel:
    artifact = await asyncify(catalog.get_artifact)(artifact_id)
    if artifact is not None:
        return artifact
    else:
        raise HTTPException(status_code=404, detail=f"Artifact {artifact_id} not found")


@cache
def cached_calc_version() -> str:
    # only compute this once to avoid re-reading the file on every healthcheck
    return calc_version()


@app.get(
    "/1/ruok",
    summary="Heartbeat",
    description="Health check",
    response_description="Returns the string 'OK' followed by the application version, indicating the system is alive",
    tags=["Miscellaneous"],
    response_class=PlainTextResponse,
)
async def ruok() -> str:
    return f"OK {cached_calc_version()}\n"


@app.get(
    "/1/sources",
    summary="Get Sources",
    description="Get all sources",
    tags=["v1", "Source"],
)
async def get_sources(factory: Annotated[Factory, Depends(get_factory)], limit: int = 100, offset: int = 0) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.get_sources)(limit=limit, offset=offset)
    return QueryResults(next_offset=next_offset, records=results)


class SourceSearchRequest(BaseModel):
    external_id_like: str
    limit: Optional[int] = 100
    offset: Optional[int] = 0


@app.post(
    "/1/source/search",
    summary="Search Sources",
    description="Search Sources",
    tags=["v1", "Source"],
)
async def search_sources(req: SourceSearchRequest, factory: Annotated[Factory, Depends(get_factory)]) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.get_sources_by_external_id_like)(
        req.external_id_like,
        limit=req.limit,
        offset=req.offset,
    )
    return QueryResults(next_offset=next_offset, records=results)


@app.get(
    "/1/source/{source_id}/generations",
    summary="Get Generations",
    description="Get all generations for a source",
    tags=["v1", "Source"],
)
async def get_generations(
    source_id: str, factory: Annotated[Factory, Depends(get_factory)], limit: int = 100, offset: int = 0
) -> QueryResults:
    catalog = factory.catalog
    await check_source(catalog, source_id)
    results, next_offset = await asyncify(catalog.get_generations)(source_id, limit=limit, offset=offset)
    return QueryResults(next_offset=next_offset, records=results)


@app.get(
    "/1/source/{source_id}/generation/latest",
    summary="Get Latest Generation",
    description="Get the latest generation for source",
    tags=["v1", "Source"],
)
async def get_latest_generation(source_id: str, factory: Annotated[Factory, Depends(get_factory)]) -> GenerationModel:
    catalog = factory.catalog
    generation = await asyncify(catalog.get_latest_generation)(source_id)
    if generation is not None:
        return generation
    else:
        raise HTTPException(status_code=404, detail=f"No generations found for source {source_id}")


@app.get(
    "/1/source/{source_id}/generation/{generation_id}/artifacts",
    summary="Get Artifacts",
    description="Get artifacts for a source and generation",
    tags=["v1", "Source"],
)
async def get_artifacts(
    source_id: str, generation_id: int, factory: Annotated[Factory, Depends(get_factory)], limit: int = 100, offset: int = 0
) -> QueryResults:
    catalog = factory.catalog
    source, generation = await check_source_and_generation(catalog, source_id, generation_id)
    results, next_offset = await asyncify(catalog.get_artifacts)(source_id, generation_id, limit=limit, offset=offset)
    return QueryResults(next_offset=next_offset, records=results)


@app.get(
    "/1/source/{source_id}/generation/{generation_id_a}/diff/{generation_id_b}",
    summary="Diff Generations",
    description="Compute differences between generations",
    tags=["v1", "Source"],
)
async def diff_generations(
    source_id: str,
    generation_id_a: int,
    generation_id_b: int,
    factory: Annotated[Factory, Depends(get_factory)],
    limit: int = 100,
    offset: int = 0,
) -> QueryResults:
    catalog = factory.catalog
    source, generation = await check_source_and_generation(catalog, source_id, generation_id_a)
    results, next_offset = await asyncify(catalog.diff_generations)(
        source_id, generation_id_a, generation_id_b, limit=limit, offset=offset
    )
    return QueryResults(next_offset=next_offset, records=results)


@app.get(
    "/1/artifact/{artifact_id}",
    summary="Get Artifact",
    description="Download artifact data",
    tags=["v1", "Artifact"],
)
async def get_artifact(artifact_id: str, factory: Annotated[Factory, Depends(get_factory)]) -> StreamingResponse:
    catalog = factory.catalog
    artifact = await check_artifact(catalog, artifact_id)
    source = await check_source(catalog, artifact.source_id)
    connector = factory.new_connector_from_model(source.connector)
    content, _ = await asyncify(connector.get_artifact)(artifact.external_id, version=artifact.version)
    return StreamingResponse(
        content,
        media_type=artifact.content_type,
        headers={"content-disposition": f"attachment; filename={artifact_id}{guess_extension(artifact.content_type)}"},
    )


class FragmentSearchRequest(BaseModel):
    source_id: str
    query: Optional[str] = None
    score_query: Optional[str] = None
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    aggregation_level: Optional[AggregationLevel] = None
    external_id: Optional[str] = None
    generation_id: Optional[int] = None


@app.post(
    "/1/fragment/search",
    summary="Search Fragments",
    description="Search Fragment text content",
    tags=["v1", "Fragment"],
)
async def search_fragments(req: FragmentSearchRequest, factory: Annotated[Factory, Depends(get_factory)]) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.search_fragments)(
        req.source_id,
        query=req.query,
        score_query=req.score_query,
        ngram=False,
        limit=req.limit,
        offset=req.offset,
        aggregation_level=req.aggregation_level,
        external_id=req.external_id,
        generation_id=req.generation_id,
    )
    return QueryResults(next_offset=next_offset, records=results)


class FragmentJsonSearchRequest(BaseModel):
    source_id: str
    query: List[JsonSearchTerm]
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    aggregation_level: Optional[AggregationLevel] = None
    external_id: Optional[str] = None
    generation_id: Optional[int] = None


@app.post(
    "/1/fragment/search/json", summary="Search Fragments", description="Search Fragment JSON content", tags=["v1", "Fragment"]
)
async def search_fragments_json(
    req: FragmentJsonSearchRequest, factory: Annotated[Factory, Depends(get_factory)]
) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.search_fragments_json)(
        req.source_id,
        req.query,
        limit=req.limit,
        offset=req.offset,
        aggregation_level=req.aggregation_level,
        external_id=req.external_id,
        generation_id=req.generation_id,
    )
    return QueryResults(next_offset=next_offset, records=results)


class FragmentKeySearchRequest(BaseModel):
    source_id: str
    query: List[KeySearchTerm]
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    aggregation_level: Optional[AggregationLevel] = None
    external_id: Optional[str] = None
    generation_id: Optional[int] = None


@app.post(
    "/1/fragment/search/key",
    summary="Search Fragments",
    description="Search Fragments by their associated keys",
    tags=["v1", "Fragment"],
)
async def search_fragments_key(req: FragmentKeySearchRequest, factory: Annotated[Factory, Depends(get_factory)]) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.search_fragments_key)(
        req.source_id,
        req.query,
        limit=req.limit,
        offset=req.offset,
        aggregation_level=req.aggregation_level,
        external_id=req.external_id,
        generation_id=req.generation_id,
    )
    return QueryResults(next_offset=next_offset, records=results)


@app.post(
    "/1/fragment/search/ngram",
    summary="Search Fragments",
    description="Search Fragments ranked by by their n-gram relevance",
    tags=["v1", "Fragment"],
)
async def search_fragments_ngram(req: FragmentSearchRequest, factory: Annotated[Factory, Depends(get_factory)]) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.search_fragments)(
        req.source_id,
        query=req.query,
        score_query=req.score_query,
        ngram=True,
        limit=req.limit,
        offset=req.offset,
        aggregation_level=req.aggregation_level,
        external_id=req.external_id,
        generation_id=req.generation_id,
    )
    return QueryResults(next_offset=next_offset, records=results)


class DeferredDisaggregationSearchRequest(BaseModel):
    source_id: Optional[str] = None
    created_on_start: Optional[datetime] = None
    created_on_end: Optional[datetime] = None
    limit: Optional[int] = 100
    offset: Optional[int] = 0


@app.post(
    "/1/admin/deferred_disaggregation",
    summary="Get Deferred Disaggregations",
    description="Get Deferred Disaggregations processed in a given date range. Defaults to last day if omitted",
    tags=["v1", "Admin"],
)
async def get_deferred_disaggregations_by_date_range(
    req: DeferredDisaggregationSearchRequest, factory: Annotated[Factory, Depends(get_factory)]
) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.get_deferred_disaggregations_by_date_range)(
        use_created_on_start=req.created_on_start,
        use_created_on_end=req.created_on_end,
        source_id=req.source_id,
        offset=req.offset,
        limit=req.limit,
    )
    return QueryResults(next_offset=next_offset, records=results)


@app.post(
    "/1/admin/deferred_disaggregation/summary",
    summary="Get Deferred Disaggregation Summaries",
    description="Summarize Deferred Disaggregations processed in a given date range. Defaults to last day if omitted",
    tags=["v1", "Admin"],
)
async def get_deferred_disaggregation_summaries_by_date_range(
    req: DeferredDisaggregationSearchRequest, factory: Annotated[Factory, Depends(get_factory)]
) -> QueryResults:
    catalog = factory.catalog
    results, next_offset = await asyncify(catalog.get_deferred_disaggregation_summaries_by_date_range)(
        use_created_on_start=req.created_on_start,
        use_created_on_end=req.created_on_end,
        source_id=req.source_id,
        offset=req.offset,
        limit=req.limit,
    )
    return QueryResults(next_offset=next_offset, records=results)
