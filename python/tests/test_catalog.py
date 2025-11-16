from unittest.mock import ANY, MagicMock, call

from pytest import raises

from cif.catalog import Catalog
from cif.persistence import (
    AggregationLevel,
    ArtifactChangeModel,
    ArtifactModel,
    DeferredDisaggregationModel,
    DeferredDisaggregationSummaryModel,
    FragmentKeyModel,
    FragmentModel,
    FragmentView,
    GenerationModel,
    JsonSearchTerm,
    KeySearchTerm,
    SourceModel,
)
from cif.util import now


def test_catalog_get_latest_generation() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_latest_generation("source_id")
    catalog.spanner.select_zero_or_one.assert_called_once_with(ANY, GenerationModel, source_id="source_id")


def test_catalog_count_inserted_updated() -> None:
    catalog = Catalog(MagicMock())
    catalog.count_inserted_updated("stage_id", "source_id", 0)
    catalog.spanner.select_count.assert_called_once_with(ANY, stage_id="stage_id", source_id="source_id", generation_id=0)


def test_catalog_count_deleted() -> None:
    catalog = Catalog(MagicMock())
    catalog.count_deleted("stage_id", "source_id", 0)
    catalog.spanner.select_count.assert_called_once_with(ANY, stage_id="stage_id", source_id="source_id", generation_id=0)


def test_catalog_insert_stage_batch() -> None:
    catalog = Catalog(MagicMock())
    batch_artifacts = [(str(i), MagicMock()) for i in range(10)]
    created_on = now()
    num_rows = catalog.insert_stage_batch("stage_id", "source_id", 0, batch_artifacts, created_on)
    assert num_rows == len(batch_artifacts)
    catalog.spanner.database.batch.return_value.__enter__.return_value.insert.assert_called_once()


def test_catalog_insert_artifact_generation_batch() -> None:
    catalog = Catalog(MagicMock())
    catalog._insert_artifact_generation_batch_helper = MagicMock()
    catalog.insert_artifact_generation_batch("stage_id", "source_id", 0)
    catalog.spanner.database.run_in_transaction.assert_called_once_with(
        catalog._insert_artifact_generation_batch_helper, "stage_id", "source_id", 0
    )


def test_catalog_insert_artifact_generation_batch_helper() -> None:
    transaction = MagicMock()
    transaction.execute_update.side_effect = [1, 2, 3]
    counts = Catalog._insert_artifact_generation_batch_helper(transaction, "stage_id", "source_id", 0)
    assert counts == (1, 2, 3)
    # three passes
    transaction.execute_update.assert_has_calls(
        [call(ANY, params={"stage_id": "stage_id", "source_id": "source_id", "batch_id": 0})] * 3
    )


def test_get_sources() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_sources(limit=50, offset=200)
    catalog.spanner.select_many.assert_called_once_with(ANY, SourceModel, limit=50, offset=200)


def test_get_source() -> None:
    catalog = Catalog(MagicMock())
    source = catalog.get_source("source_id")
    assert source is not None
    catalog.spanner.select_zero_or_one.assert_called_once_with(ANY, SourceModel, source_id="source_id")


def test_get_generations() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_generations("source_id", limit=50, offset=200)
    catalog.spanner.select_many.assert_called_once_with(ANY, GenerationModel, limit=50, offset=200, source_id="source_id")


def test_get_generation() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_generation("source_id", 0)
    catalog.spanner.select_zero_or_one.assert_called_once_with(ANY, GenerationModel, source_id="source_id", generation_id=0)


def test_get_artifacts() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_artifacts("source_id", 0, limit=50, offset=200)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, ArtifactModel, limit=50, offset=200, source_id="source_id", generation_id=0
    )


def test_get_artifact() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_artifact("artifact_id")
    catalog.spanner.select_zero_or_one.assert_called_once_with(ANY, ArtifactModel, artifact_id="artifact_id")


def test_diff_generations() -> None:
    catalog = Catalog(MagicMock())
    catalog.diff_generations("source_id", 0, 1, limit=50, offset=200)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, ArtifactChangeModel, limit=50, offset=200, source_id="source_id", generation_id_a=0, generation_id_b=1
    )


def test_get_new_artifacts() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_new_artifacts("source_id", 0, limit=50, offset=200)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, ArtifactModel, limit=50, offset=200, source_id="source_id", generation_id=0
    )


def test_insert_fragments() -> None:
    catalog = Catalog(MagicMock())
    fragments = [
        FragmentModel(
            source_id="source_id",
            artifact_id="artifact_id",
            fragment_id="fragment_id",
            aggregation_level=AggregationLevel.DOCUMENT,
            text_content="content",
            seq_no=0,
            logical_id="logical_id",
        )
    ]
    catalog.insert_fragments(fragments)
    catalog.spanner.insert_batch.assert_called_once_with("fragment", fragments)


def test_insert_fragment_keys() -> None:
    catalog = Catalog(MagicMock())
    fragment_keys = [
        FragmentKeyModel(
            source_id="source_id", artifact_id="artifact_id", fragment_id="fragment_id", seq_no=0, key="key", value="value"
        )
    ]
    catalog.insert_fragment_keys(fragment_keys)
    catalog.spanner.insert_batch.assert_called_once_with("fragment_key", fragment_keys)


def test_insert_deferred_disaggregations() -> None:
    catalog = Catalog(MagicMock())
    deferred_disaggregations = []
    catalog.insert_deferred_disaggregations(deferred_disaggregations)
    catalog.spanner.insert_batch.assert_called_once_with("deferred_disaggregation", deferred_disaggregations)


def test_validate_search_fragments_query_and_score_query_ngram_error() -> None:
    catalog = Catalog(MagicMock())
    with raises(ValueError, match=r"At least one of .*"):
        catalog.validate_search_fragments_query_and_score_query(None, None, True)


def test_validate_search_fragments_query_and_score_query_ngram() -> None:
    catalog = Catalog(MagicMock())
    catalog.validate_search_fragments_query_and_score_query("query", None, True)
    catalog.validate_search_fragments_query_and_score_query(None, "score_query", True)
    catalog.validate_search_fragments_query_and_score_query("query", "score_query", True)


def test_validate_search_fragments_query_and_score_query_error() -> None:
    catalog = Catalog(MagicMock())
    with raises(ValueError, match=r"A query"):
        catalog.validate_search_fragments_query_and_score_query(None, "score_query", False)


def test_validate_search_fragments_query_and_score_query() -> None:
    catalog = Catalog(MagicMock())
    catalog.validate_search_fragments_query_and_score_query("query", None, False)
    catalog.validate_search_fragments_query_and_score_query("query", "score_query", False)


def test_search_fragments_query() -> None:
    catalog = Catalog(MagicMock())
    source_id = "source_id"
    query = "phrase in text"
    catalog.search_fragments(source_id, query=query)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, FragmentView, limit=100, offset=0, source_id=source_id, query=query, score_query=query
    )


def test_search_fragments_score_query() -> None:
    catalog = Catalog(MagicMock())
    source_id = "source_id"
    query = "phrase in text"
    score_query = "a different phrase to score"
    catalog.search_fragments(source_id, query=query, score_query=score_query)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, FragmentView, limit=100, offset=0, source_id=source_id, query=query, score_query=score_query
    )


def test_search_fragments_ngram_score_query_only() -> None:
    catalog = Catalog(MagicMock())
    source_id = "source_id"
    score_query = "only score"
    catalog.search_fragments(source_id, score_query=score_query, ngram=True)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, FragmentView, limit=100, offset=0, source_id=source_id, score_query=score_query
    )


def test_calc_search_fragments_where_clause() -> None:
    catalog = Catalog(MagicMock())
    query = "phrase in text"
    where, params = catalog.calc_search_fragments_where_clause(
        "where XXXX",
        False,
        query=query,
        aggregation_level=AggregationLevel.DOCUMENT,
        generation_id=0,
        external_id="external_id",
    )
    assert "search(f.text_tokens, @query)" in where
    assert params["query"] == query
    assert "aggregation_level = @aggregation_level" in where
    assert params["aggregation_level"] == AggregationLevel.DOCUMENT.value
    assert "generation_id = @generation_id" in where
    assert params["generation_id"] == 0
    assert "external_id = @external_id" in where
    assert params["external_id"] == "external_id"


def test_calc_search_fragments_where_clause_ngram() -> None:
    catalog = Catalog(MagicMock())
    query = "phrase in text"
    where, params = catalog.calc_search_fragments_where_clause("where XXXX", True, query=query)
    assert "search_ngrams(f.ngram_tokens, @query)" in where
    assert params["query"] == query


def test_search_fragments_json() -> None:
    catalog = Catalog(MagicMock())
    source_id = "source_id"
    query = [JsonSearchTerm(json_path="$.ADA_CD", values=["D6194", "D6197"])]
    catalog.search_fragments_json("source_id", query)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, FragmentView, limit=100, offset=0, source_id=source_id, json_path0="$.ADA_CD", value0_0="D6194", value0_1="D6197"
    )


def test_calc_search_fragments_json_where_clause() -> None:
    catalog = Catalog(MagicMock())
    where, params = catalog.calc_search_fragments_json_where_clause(
        [JsonSearchTerm(json_path="$.ADA_CD", values=["D6194", "D6197"])]
    )
    assert params["json_path0"] == "$.ADA_CD"
    assert params["value0_0"] == "D6194"
    assert params["value0_1"] == "D6197"


def test_calc_search_fragments_where_clause_source_id_default_generation_id() -> None:
    catalog = Catalog(MagicMock())
    where, params = catalog.calc_search_fragments_where_clause("where XXXX", False)
    assert "AND g.generation_id = (select max(generation_id) from generation where source_id = @source_id)" in where


def test_get_sources_by_external_id_like() -> None:
    catalog = Catalog(MagicMock())
    catalog.get_sources_by_external_id_like("external_id")
    catalog.spanner.select_many.assert_called_once_with(ANY, SourceModel, limit=100, offset=0, external_id="external_id")


def test_search_fragments_key() -> None:
    catalog = Catalog(MagicMock())
    query = [KeySearchTerm(key="ADA_CODE", values=["D6194", "D6197"])]
    catalog.search_fragments_key("05814440726642c9b4f9f3f92aa9a5bf", query)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, FragmentView, limit=100, offset=0, key0="ADA_CODE", value0_0="D6194", value0_1="D6197"
    )


def test_search_fragments_key_invalid_source_id() -> None:
    catalog = Catalog(MagicMock())
    query = [KeySearchTerm(key="ADA_CODE", values=["D6194", "D6197"])]
    with raises(ValueError, match="Invalid source_id .*"):
        catalog.search_fragments_key("source_id", query)


def test_calc_search_fragments_key_where_clause() -> None:
    catalog = Catalog(MagicMock())
    where, params = catalog.calc_search_fragments_key_where_clause([KeySearchTerm(key="ADA_CODE", values=["D6194", "D6197"])])
    assert params["key0"] == "ADA_CODE"
    assert params["value0_0"] == "D6194"
    assert params["value0_1"] == "D6197"


def test_calc_get_deferred_disaggregations_where_clause() -> None:
    catalog = Catalog(MagicMock())
    created_on_start = now()
    created_on_end = now()
    where, params = catalog.calc_get_deferred_disaggregations_where_clause(
        created_on_start,
        created_on_end=created_on_end,
        source_id="source_id",
    )
    assert "where created_on >= @created_on_start" in where
    assert params["created_on_start"] == created_on_start.isoformat()
    assert "source_id = @source_id" in where
    assert params["source_id"] == "source_id"
    assert "created_on <= @created_on_end" in where
    assert params["created_on_end"] == created_on_end.isoformat()


def test_get_deferred_disaggregations_by_date_range() -> None:
    catalog = Catalog(MagicMock())
    created_on_start = now()
    catalog.get_deferred_disaggregations_by_date_range(created_on_start)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, DeferredDisaggregationModel, limit=100, offset=0, created_on_start=created_on_start.isoformat()
    )


def test_get_deferred_disaggregation_summaries_by_date_range() -> None:
    catalog = Catalog(MagicMock())
    created_on_start = now()
    catalog.get_deferred_disaggregation_summaries_by_date_range(created_on_start)
    catalog.spanner.select_many.assert_called_once_with(
        ANY, DeferredDisaggregationSummaryModel, limit=100, offset=0, created_on_start=created_on_start.isoformat()
    )
