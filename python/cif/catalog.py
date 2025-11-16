from datetime import datetime, timezone
from logging import getLogger
from re import match
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from google.cloud.spanner_v1.transaction import Transaction

from cif.clients import SpannerSupport
from cif.persistence import (
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
    PaginatedQueryResults,
    SourceModel,
)
from cif.util import Fingerprint, now

logger = getLogger(__name__)


class Catalog:
    """
    Data access methods for the CIF catalog.
    """

    def __init__(self, spanner: SpannerSupport) -> None:
        self.spanner = spanner

    def get_latest_generation(self, source_id: str) -> Optional[GenerationModel]:
        """
        Get the latest generation for the supplied source. Returns None if source has
        no generations.
        """
        return self.spanner.select_zero_or_one(
            "select * from generation where source_id = @source_id order by generation_id desc",
            GenerationModel,
            source_id=source_id,
        )

    def count_inserted_updated(self, stage_id: str, source_id: str, generation_id: int) -> Optional[int]:
        """
        Count records in stage for which no corresponding artifact exists in the
        supplied generation.
        """
        return self.spanner.select_count(
            """
            select count(*) from stage s
            where
            s.stage_id = @stage_id AND
            s.source_id = @source_id AND
            not exists (
            select * from generation g
            inner join artifact a on
            a.source_id = g.source_id and
            a.artifact_id = g.artifact_id
            WHERE
            g.generation_id = @generation_id AND
            g.source_id = s.source_id and
            a.external_id = s.external_id and
            a.version = s.version)
            """,
            stage_id=stage_id,
            source_id=source_id,
            generation_id=generation_id,
        )

    def count_deleted(self, stage_id: str, source_id: str, generation_id: int) -> Optional[int]:
        """
        Count artifacts in the supplied generation for which no corresponding
        record exists in stage.
        """
        return self.spanner.select_count(
            """
            select count(*) from generation g
            inner join artifact a on
            g.source_id = a.source_id AND
            g.artifact_id = a.artifact_id
            where
            g.source_id = @source_id and
            g.generation_id = @generation_id AND
            not exists (
            select * from stage s
            WHERE
            s.stage_id = @stage_id and
            s.source_id = @source_id and
            s.source_id = a.source_id and
            s.external_id = a.external_id AND
            s.version = a.version
            );
            """,
            stage_id=stage_id,
            source_id=source_id,
            generation_id=generation_id,
        )

    def insert_stage_batch(
        self, stage_id: str, source_id: str, batch_id: int, batch_artifacts: List[Tuple[str, Fingerprint]], created_on: int
    ) -> int:
        """
        Insert rows into stage as a batch. Returns the number of rows inserted.
        """
        columns = [
            "stage_id",
            "batch_id",
            "source_id",
            "external_id",
            "version",
            "content_length",
            "content_type",
            "created_on",
            "artifact_id",
        ]
        with self.spanner.database.batch() as batch:
            values = [
                (
                    stage_id,
                    batch_id,
                    source_id,
                    external_id,
                    fingerprint.version,
                    fingerprint.content_length,
                    fingerprint.content_type,
                    created_on,
                    uuid4().hex,
                )
                for external_id, fingerprint in batch_artifacts
            ]
            batch.insert(table="stage", columns=columns, values=values)
            num_rows = len(values)
            logger.debug("Staged batch %d (%d rows) for stage %s", batch_id, num_rows, stage_id)
            return num_rows

    def insert_artifact_generation_batch(self, stage_id: str, source_id: str, batch_id: int) -> Tuple[int, int, int]:
        """
        Create artifacts and generation entries from a batch of staged records. This will
        execute in a single transaction. Returns a triplet of counts from each phase of
        processing: number of unmodified artifacts in batch, number of new artifacts in
        batch, total number of artifacts in batch.
        """
        return self.spanner.database.run_in_transaction(
            self._insert_artifact_generation_batch_helper, stage_id, source_id, batch_id
        )

    @staticmethod
    def _insert_artifact_generation_batch_helper(
        transaction: Transaction, stage_id: str, source_id: str, batch_id: int
    ) -> Tuple[int, int, int]:

        # in the first pass set the artifact_id in stage to match existing artifact
        # ids where the source_id, external_id and version agree. if no such artifacts
        # exist, the provisional artifact_id assigned in insert_stage_batch() will
        # be retained

        pass_1 = transaction.execute_update(
            """
            update stage s
            set s.artifact_id = (select a0.artifact_id from artifact a0 where
            a0.source_id = s.source_id and
            a0.external_id = s.external_id and
            a0.version = s.version)
            where
            s.stage_id = @stage_id and
            s.batch_id = @batch_id and
            s.source_id = @source_id and
            exists (select * from artifact a1
            where
            a1.source_id = s.source_id and
            a1.external_id = s.external_id and
            a1.version = s.version
            )
            """,
            params={"stage_id": stage_id, "batch_id": batch_id, "source_id": source_id},
        )

        # in the second pass insert all new artifacts from this batch, ignoring
        # those that already exist

        pass_2 = transaction.execute_update(
            """
            insert or ignore into artifact
            (artifact_id, source_id, external_id, version, content_type, content_length, created_on)
            (select artifact_id, source_id, external_id, version, content_type, content_length, created_on
            from stage
            where stage_id = @stage_id and
            batch_id = @batch_id and
            source_id = @source_id);
            """,
            params={"stage_id": stage_id, "batch_id": batch_id, "source_id": source_id},
        )

        # in the final pass, insert generation entries for all artifacts in this batch

        pass_3 = transaction.execute_update(
            """
            insert into generation (source_id, generation_id, artifact_id, created_on)
            (select source_id, unix_micros(timestamp_trunc(created_on, MICROSECOND)), artifact_id, created_on from stage
            where stage_id = @stage_id and
            batch_id = @batch_id and
            source_id = @source_id)
            """,
            params={"stage_id": stage_id, "batch_id": batch_id, "source_id": source_id},
        )

        logger.debug("Processed batch %d for stage %s: %d, %d, %d,", batch_id, stage_id, pass_1, pass_2, pass_3)

        return pass_1, pass_2, pass_3

    def get_sources(self, limit: int = 100, offset: int = 0) -> PaginatedQueryResults:
        """
        List all sources.
        """
        return self.spanner.select_many("select * from source order by source_id", SourceModel, limit=limit, offset=offset)

    def get_source(self, source_id: str) -> Optional[SourceModel]:
        """
        Gets a single source by its ID.
        """
        return self.spanner.select_zero_or_one(
            "select * from source where source_id = @source_id", SourceModel, source_id=source_id
        )

    def get_generations(self, source_id: str, limit: int = 100, offset: int = 0) -> PaginatedQueryResults:
        """
        List all generations for the specified source.
        """
        return self.spanner.select_many(
            """
            select distinct source_id, generation_id, created_on from generation
            where source_id = @source_id order by generation_id desc
            """,
            GenerationModel,
            limit=limit,
            offset=offset,
            source_id=source_id,
        )

    def get_generation(self, source_id: str, generation_id: str) -> Optional[GenerationModel]:
        """
        Gets a single generation by its source and generation ID.
        """
        return self.spanner.select_zero_or_one(
            "select * from generation where source_id = @source_id and generation_id = @generation_id",
            GenerationModel,
            source_id=source_id,
            generation_id=generation_id,
        )

    def get_artifacts(
        self, source_id: str, generation_id: int = None, limit: int = 100, offset: int = 0
    ) -> PaginatedQueryResults:
        """
        List all artifacts for the supplied source and generation.
        """
        return self.spanner.select_many(
            """
            select a.source_id, a.artifact_id, a.created_on, external_id, version, content_length, content_type
            from artifact a
            where a.artifact_id in (
            select g.artifact_id from generation g
            where g.source_id = @source_id and g.generation_id = @generation_id
            )
            order by a.artifact_id
            """,
            ArtifactModel,
            limit=limit,
            offset=offset,
            source_id=source_id,
            generation_id=generation_id,
        )

    def get_artifact(self, artifact_id: str) -> Optional[ArtifactModel]:
        """
        Get an artifact given its ID.
        """
        return self.spanner.select_zero_or_one(
            "select * from artifact where artifact_id = @artifact_id", ArtifactModel, artifact_id=artifact_id
        )

    def diff_generations(
        self, source_id: str, generation_id_a: int, generation_id_b: int, limit: int = 100, offset: int = 0
    ) -> PaginatedQueryResults:
        """
        Computes the differences between two generations of a source. Returns a result
        set listing external_id, artifact_id, generation_id for each external_id in
        either a or b, and the difference expressed as:

            INSERTED: present in generation b, not in a
             DELETED: present in generation a, not in b
             UPDATED: present in both generations, but with different content
                NONE: present in both generations with identical content

        """
        return self.spanner.select_many(
            """
            select
            coalesce(a.external_id, b.external_id) as external_id,
            a.artifact_id as artifact_id_a,
            b.artifact_id as artifact_id_b,
            (case
            when a.external_id is null then 'INSERTED'
            when b.external_id is null then 'DELETED'
            when a.artifact_id != b.artifact_id then 'UPDATED'
            else 'NONE' END) as change
            from (
            select a0.external_id, a0.artifact_id, g.generation_id
            from generation g
            inner join artifact a0 on a0.artifact_id = g.artifact_id
            where g.source_id = @source_id
            and g.generation_id = @generation_id_a) as a
            full outer join (
            select a0.external_id, a0.artifact_id, g.generation_id
            from generation g
            inner join artifact a0 on a0.artifact_id = g.artifact_id
            where g.source_id = @source_id
            and g.generation_id = @generation_id_b) as b
            on a.external_id = b.external_id
            """,
            ArtifactChangeModel,
            limit=limit,
            offset=offset,
            source_id=source_id,
            generation_id_a=generation_id_a,
            generation_id_b=generation_id_b,
        )

    def get_new_artifacts(self, source_id: str, generation_id: int, limit: int = 100, offset: int = 0) -> PaginatedQueryResults:
        """
        Return all artifacts that were newly created as part of the supplied generation.
        """
        return self.spanner.select_many(
            """
            select a.source_id, a.artifact_id, a.created_on, external_id, version, content_length, content_type from artifact as a
            inner join generation as g on
            g.artifact_id = a.artifact_id and g.created_on = a.created_on
            where g.source_id = @source_id and g.generation_id = @generation_id
            """,
            ArtifactModel,
            limit=limit,
            offset=offset,
            source_id=source_id,
            generation_id=generation_id,
        )

    def insert_fragments(self, fragments: List[FragmentModel]) -> int:
        return self.spanner.insert_batch("fragment", fragments)

    def insert_fragment_keys(self, fragment_keys: List[FragmentKeyModel]) -> int:
        return self.spanner.insert_batch("fragment_key", fragment_keys)

    def insert_deferred_disaggregations(self, deferred_disaggregations: List[DeferredDisaggregationModel]) -> int:
        return self.spanner.insert_batch("deferred_disaggregation", deferred_disaggregations)

    def calc_search_fragments_where_clause(self, where: str, ngram: bool, **kwargs: Any) -> Tuple[str, Dict[str, Any]]:
        params = {}

        if (v := kwargs.get("aggregation_level")) is not None:
            where += " AND aggregation_level = @aggregation_level"
            params["aggregation_level"] = v.value

        if (v := kwargs.get("generation_id")) is not None:
            where += " AND g.generation_id = @generation_id"
            params["generation_id"] = v
        else:
            where += " AND g.generation_id = (select max(generation_id) from generation where source_id = @source_id)"

        if (v := kwargs.get("external_id")) is not None:
            where += " AND a.external_id = @external_id"
            params["external_id"] = v

        if (v := kwargs.get("query")) is not None:
            if ngram:
                where += " AND search_ngrams(f.ngram_tokens, @query)"
            else:
                where += " AND search(f.text_tokens, @query)"
            params["query"] = v

        return where, params

    def validate_search_fragments_query_and_score_query(
        self, query: Optional[str], score_query: Optional[str], ngram: bool
    ) -> None:
        if ngram:
            if query is None and score_query is None:
                raise ValueError("At least one of query or score_query must be supplied for ngram searches")
        else:
            if query is None:
                raise ValueError("A query is required for text searches")

    def calc_search_fragments_relevance_expression(self, ngram: bool) -> str:
        if ngram:
            return "score_ngrams(f.ngram_tokens, @score_query) as relevance"
        else:
            return "score(f.text_tokens, @score_query) as relevance"

    def search_fragments(
        self,
        source_id: str,
        query: Optional[str] = None,
        score_query: Optional[str] = None,
        ngram: bool = False,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> PaginatedQueryResults:
        """
        Search the text_content or ngram_content field of the Fragment table, ordering
        results by a computed relevance score.

        For text searches, a query is required and will be used to construct a search()
        expression in the where clause. The score_query parameter is optional and if
        omitted will default to query. It will be used to construct relevance via a
        score() expression in the selected columns.

        For ngram searches, at least one of query or score_query parameters must be
        supplied. The former will be used to construct a search_ngrams() expression in
        the where clause if present; the latter will be used to construct relevance via
        a score_ngrams() expression in the selected columns. If score_query is omitted,
        it will default to query.

        The mode of the search is selected via the ngram parameter (True for ngram,
        False for text).

        Both query and score_query should be formed using Spanner's rquery syntax, as
        described here:

        https://cloud.google.com/spanner/docs/reference/standard-sql/search_functions#rquery-syntax

        Additional kwargs can be supplied to filter results:

        - generation_id (int), will default to the latest generation if not supplied
        - external_id (str)
        - aggregation_level (AggregationLevel)

        """
        self.validate_search_fragments_query_and_score_query(query, score_query, ngram)

        relevance = self.calc_search_fragments_relevance_expression(ngram)

        where, params = self.calc_search_fragments_where_clause("where f.source_id = @source_id", ngram, query=query, **kwargs)

        sql = f"""
        select f.*, a.external_id, g.generation_id, {relevance} from fragment f
        inner join generation g on f.source_id = g.source_id and f.artifact_id = g.artifact_id
        inner join artifact a on f.source_id = a.source_id and a.artifact_id = f.artifact_id
        {where}
        order by relevance desc
        """
        return self.spanner.select_many(
            sql,
            FragmentView,
            limit=limit,
            offset=offset,
            source_id=source_id,
            score_query=score_query or query,
            **params,
        )

    def calc_search_fragments_json_where_clause(self, query: List[JsonSearchTerm]) -> Tuple[str, Dict[str, Any]]:
        params = {}
        terms = []
        for ix, term in enumerate(query):
            json_path_placeholder = f"@json_path{ix}"
            params[json_path_placeholder[1:]] = term.json_path

            value_placeholders = []
            for iy, value in enumerate(term.values):
                value_placeholder = f"@value{ix}_{iy}"
                params[value_placeholder[1:]] = value
                value_placeholders.append(value_placeholder)

            terms.append(f"json_value(f.json_content, {json_path_placeholder}) in ({', '.join(value_placeholders)})")

        return " and ".join(terms), params

    def search_fragments_json(
        self, source_id: str, query: List[JsonSearchTerm], limit: int = 100, offset: int = 0, **kwargs: Any
    ) -> PaginatedQueryResults:
        """
        Search the json_content field of the Fragment table. The supplied search terms
        will be used to form a where clause by joining each term with "AND" and creating
        an IN expression with the supplied values. The format of the json_path value for
        each term should conform to the format accepted by the Spanner JSON_VALUE
        function: https://cloud.google.com/spanner/docs/reference/standard-sql/json_functions#json_value
        """
        where0, params0 = self.calc_search_fragments_json_where_clause(query)
        where1, params1 = self.calc_search_fragments_where_clause(f"where f.source_id = @source_id and {where0}", False, **kwargs)
        sql = f"""
        select f.*, a.external_id, g.generation_id from fragment f
        inner join generation g on f.source_id = g.source_id and f.artifact_id = g.artifact_id
        inner join artifact a on f.source_id = a.source_id and a.artifact_id = f.artifact_id
        {where1}
        """
        return self.spanner.select_many(
            sql,
            FragmentView,
            limit=limit,
            offset=offset,
            source_id=source_id,
            **params0,
            **params1,
        )

    def calc_search_fragments_key_where_clause(self, query: List[KeySearchTerm]) -> Tuple[str, Dict[str, Any]]:
        params = {}
        terms = []
        for ix, term in enumerate(query):
            key_placeholder = f"@key{ix}"
            params[key_placeholder[1:]] = term.key

            value_placeholders = []
            for iy, value in enumerate(term.values):
                value_placeholder = f"@value{ix}_{iy}"
                params[value_placeholder[1:]] = value
                value_placeholders.append(value_placeholder)

            terms.append(f"(key = {key_placeholder} and value in ({', '.join(value_placeholders)}))")
        return f"where {' or '.join(terms)}", params

    def search_fragments_key(
        self, source_id: str, query: List[KeySearchTerm], limit: int = 100, offset: int = 0, **kwargs: Any
    ) -> PaginatedQueryResults:
        """
        Search Fragments by their associated key(s). The supplied search terms will be
        used to find Fragments with a set of keys matching the supplied values. All terms
        must match.
        """
        if not match(r"[0-9a-fA-F]{32}", source_id):
            # enforce the format of source_id as it is being directly written into the
            # query to avoid potention injection attacks
            raise ValueError(f"Invalid source_id '{source_id}'")
        where0, params0 = self.calc_search_fragments_key_where_clause(query)
        where1, params1 = self.calc_search_fragments_where_clause("where f.source_id = @source_id", False, **kwargs)
        sql = f"""
        SELECT f.*, a.external_id, g.generation_id from fragment f
        JOIN (
        SELECT source_id, artifact_id, fragment_id, seq_no
        FROM fragment_key
        {where0}
        and source_id = @source_id
        group by source_id, artifact_id, fragment_id, seq_no having count(distinct key) = {len(query)}) matches
        ON f.source_id = matches.source_id and f.artifact_id = matches.artifact_id and
        f.fragment_id = matches.fragment_id AND f.seq_no = matches.seq_no
        inner join artifact a on f.artifact_id = a.artifact_id and f.source_id = a.source_id
        inner join generation g on f.artifact_id = g.artifact_id and f.source_id = g.source_id
        {where1}
        """
        sql = sql.replace("@source_id", f"'{source_id}'")
        return self.spanner.select_many(
            sql,
            FragmentView,
            limit=limit,
            offset=offset,
            **params0,
            **params1,
        )

    def get_sources_by_external_id_like(self, external_id: str, limit: int = 100, offset: int = 0) -> PaginatedQueryResults:
        return self.spanner.select_many(
            "select * from source where external_id like @external_id",
            SourceModel,
            external_id=external_id,
            limit=limit,
            offset=offset,
        )

    def calc_get_deferred_disaggregations_where_clause(
        self, created_on_start: datetime, created_on_end: datetime = None, source_id: str = None
    ) -> Tuple[str, Dict[str, Any]]:
        params = {"created_on_start": created_on_start.isoformat()}
        where = "where created_on >= @created_on_start"

        if created_on_end is not None:
            params["created_on_end"] = created_on_end.isoformat()
            where += " AND created_on <= @created_on_end"

        if source_id is not None:
            params["source_id"] = source_id
            where += " AND source_id = @source_id"

        return where, params

    def get_deferred_disaggregations_by_date_range(
        self,
        use_created_on_start: datetime = None,
        use_created_on_end: datetime = None,
        source_id: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedQueryResults:
        """
        Query the deferred_disaggregation table by a date range. If not supplied,
        the start date will be midnight of the current day (in UTC) with no end date.
        Otherwise the supplied values will be used. The results may be filtered by a
        source_id as well.
        """
        created_on_start = use_created_on_start or now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        where, params = self.calc_get_deferred_disaggregations_where_clause(
            created_on_start, created_on_end=use_created_on_end, source_id=source_id
        )
        sql = f"""
        select * from deferred_disaggregation
        {where}
        order by source_id, created_on, status desc
        """
        return self.spanner.select_many(sql, DeferredDisaggregationModel, limit=limit, offset=offset, **params)

    def get_deferred_disaggregation_summaries_by_date_range(
        self,
        use_created_on_start: datetime = None,
        use_created_on_end: datetime = None,
        source_id: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedQueryResults:
        """
        Query the deferred_disaggregation table by a date range. If not supplied,
        the start date will be midnight of the current day (in UTC) with no end date.
        Otherwise the supplied values will be used. The results may be filtered by a
        source_id as well.
        """
        created_on_start = use_created_on_start or now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        where, params = self.calc_get_deferred_disaggregations_where_clause(
            created_on_start, created_on_end=use_created_on_end, source_id=source_id
        )
        sql = f"""select source_id, generation_id, status,
        min(created_on) as min_created_on, max(created_on) max_created_on,
        count(distinct artifact_id) as artifact_count, count(*) as disaggregation_count,
        avg(delivery_attempt) as avg_delivery_attempt
        from deferred_disaggregation
        {where}
        group by 1,2,3
        order by source_id, generation_id, min_created_on, status
        """
        return self.spanner.select_many(sql, DeferredDisaggregationSummaryModel, limit=limit, offset=offset, **params)
