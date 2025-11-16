from concurrent.futures import Future
from dataclasses import dataclass
from functools import cache
from json import dumps
from logging import getLogger
from os import environ
from typing import Any, Callable, List, Optional

from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.spanner_v1 import Client as SpannerClient
from google.cloud.spanner_v1 import Type as SpannerType
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.param_types import INT64, JSON, STRING
from google.cloud.storage import Client as StorageClient

from cif.persistence import PaginatedQueryResults, Persistent

logger = getLogger(__name__)


class SpannerSupport:

    @classmethod
    def new_instance(cls, project_id: str, instance_id: str, database_id: str) -> "SpannerSupport":
        client = SpannerClient(project=project_id)
        instance = client.instance(instance_id)
        database = instance.database(database_id)
        return cls(client, instance, database)

    def __init__(self, client: SpannerClient, instance: Instance, database: Database) -> None:
        self.client = client
        self.instance = instance
        self.database = database

    @classmethod
    def new_persistent(cls, result_dict: dict, result_class: Persistent) -> Persistent:
        return result_class(
            **{k: v._array_value if isinstance(v, JsonObject) and v._is_array else v for k, v in result_dict.items()}
        )

    def calc_spanner_type(self, v: Any) -> SpannerType:
        if type(v) is str:
            return STRING
        elif type(v) is int:
            return INT64
        elif type(v) is dict:
            return JSON
        else:
            raise ValueError(f"Can't map value '{v}' with type '{type(v)}' to a Spanner type")

    def calc_spanner_value(self, v: Any) -> Any:
        if type(v) is dict:
            return dumps(v)
        else:
            return v

    def select_many(
        self, sql: str, result_class: Persistent, limit: int = 100, offset: int = 0, timeout: int = 30, **params: Any
    ) -> PaginatedQueryResults:
        """
        Execute a query that returns multiple rows. Will apply a limit/offset clause to
        the end of the supplied SQL. Adjust limit/offset to page through a large result
        set.
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                sql + " limit @limit offset @offset",
                params={"limit": limit, "offset": offset, **{k: self.calc_spanner_value(v) for k, v in params.items()}},
                param_types={"limit": INT64, "offset": INT64, **{k: self.calc_spanner_type(v) for k, v in params.items()}},
                timeout=timeout,
            )
            results_dict_list = results.to_dict_list()
            results = [self.new_persistent(result_dict, result_class) for result_dict in results_dict_list]
            next_page_offset = offset + limit if len(results_dict_list) == limit else None
            return results, next_page_offset

    def select_zero_or_one(self, sql: str, result_class: Persistent, timeout: int = 30, **params: Any) -> Optional[Persistent]:
        """
        Execute a query that returns zero or one row. If the query returns multiple rows,
        only the first will be processed..
        """
        results, _ = self.select_many(sql, result_class, limit=1, offset=0, timeout=timeout, **params)
        if len(results) > 0:
            return results[0]
        else:
            return None

    def select_count(self, sql: str, timeout: int = 30, **params: Any) -> int:
        """
        Execute a query of the form "select count(*) from ...". The query should return
        a single column of type int64.
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                sql,
                params={**params},
                param_types={k: self.calc_spanner_type(v) for k, v in params.items()},
                timeout=timeout,
            )
            return results.one()[0]

    def insert_batch(self, table: str, xs: List[Persistent]) -> int:
        """
        Insert Persistent models into a table as a batch.
        """
        num_rows = 0
        if len(xs) > 0:
            columns = list(xs[0].model_dump().keys())
            with self.database.batch() as batch:
                values = [[self.calc_spanner_value(y) for y in x.model_dump().values()] for x in xs]
                batch.insert_or_update(table=table, columns=columns, values=values)
                num_rows += len(values)
        return num_rows


class PubSupport:

    @classmethod
    def new_instance(cls, project_id: str, topic_id: str, kms_key_name: str) -> "PubSupport":
        client = PublisherClient()
        topic_path = client.topic_path(project_id, topic_id)
        return PubSupport(client, topic_path, kms_key_name)

    def __init__(self, client: PublisherClient, topic_path: str, kms_key_name: str) -> None:
        self.client = client
        self.topic_path = topic_path
        self.kms_key_name = kms_key_name

    def publish_messages(self, messages: List[bytes], timeout_seconds: int = 30) -> List[Future]:
        fs = []
        for message in messages:
            f = self.client.publish(self.topic_path, message)
            fs.append(f)
        logger.info("Published %d messages to %s", len(messages), self.topic_path)
        return fs


MessageHandler = Callable[[Message], None]


class SubSupport:

    @classmethod
    def new_instance(cls, project_id: str, subscription_id: str) -> "SubSupport":
        client = SubscriberClient()
        subscription_path = client.subscription_path(project_id, subscription_id)
        return SubSupport(client, subscription_path)

    def __init__(self, client: SubscriberClient, subscription_path: str) -> None:
        self.client = client
        self.subscription_path = subscription_path
        self.f = None

    def start(self, message_handler: MessageHandler, max_messages: int = 1) -> None:
        """
        Starts a subscriber on this topic dispatching messages to this
        instance's handle_message method.
        """
        if self.f is not None:
            raise ValueError("Already started")
        logger.info("Starting subscriber for %s", self.subscription_path)
        self.f = self.client.subscribe(
            self.subscription_path, callback=message_handler, flow_control=FlowControl(max_messages=max_messages)
        )
        self.f.result()  # waits forever or until a non-recoverable error occurs

    def stop(self, timeout_seconds: int = 30) -> None:
        """
        Attempts to shut down the subscriber, blocking until shutdown has
        completed.
        """
        if self.f is None:
            raise ValueError("Not started")
        self.f.cancel()
        self.f.result(timeout_seconds)
        logger.info("Stopped subscriber for %s", self.subscription_path)
        self.f = None


@dataclass
class Clients:
    spanner: SpannerSupport
    storage: StorageClient
    pub: PubSupport
    sub: SubSupport
    bigquery: BigQueryClient


@cache
def get_clients(
    project_id: str = environ.get("GCP_PROJECT", "anbc-hcb-dev"),
    catalog_instance_id: str = environ.get("CIF_CATALOG_INSTANCE_ID", "csr-va"),
    catalog_database_id: str = environ.get("CIF_CATALOG_DATABASE_ID", "csr-va-cif"),
    work_topic_project_id: str = environ.get("CIF_WORK_TOPIC_PROJECT_ID", "anbc-dev-csr-va"),
    work_topic_id: str = environ.get("CIF_WORK_TOPIC_ID", "csr-va-cif-work-hcb-dev"),
    work_topic_subscription_id: str = environ.get("CIF_WORK_TOPIC_SUBSCRIPTION_ID", "csr-va-cif-work-hcb-dev-sub"),
    work_topic_kms_key_name: str = environ.get(
        "CIF_WORK_TOPIC_KMS_KEY_NAME",
        "projects/cvs-key-vault-nonprod/locations/us-east4/keyRings/gkr-nonprod-us-east4/cryptoKeys/gk-anbc-dev-csr-va-us-east4",
    ),
) -> Clients:
    logger.info("Project is %s", project_id)
    spanner = SpannerSupport.new_instance(project_id=project_id, instance_id=catalog_instance_id, database_id=catalog_database_id)
    logger.info("Catalog spanner instance is %s, database is %s", catalog_instance_id, catalog_database_id)
    pub = PubSupport.new_instance(work_topic_project_id, work_topic_id, work_topic_kms_key_name)
    logger.info("Work topic is %s", work_topic_id)
    sub = SubSupport.new_instance(work_topic_project_id, work_topic_subscription_id)
    logger.info("Work subscription is %s", work_topic_subscription_id)

    return Clients(
        spanner=spanner, storage=StorageClient(project=project_id), pub=pub, sub=sub, bigquery=BigQueryClient(project=project_id)
    )
