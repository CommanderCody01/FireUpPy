from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, call, patch

from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.spanner_v1.data_types import JsonObject
from pydantic import BaseModel
from pytest import raises

from cif.clients import PubSupport, SpannerSupport, SubSupport, get_clients


@patch("cif.clients.BigQueryClient")
@patch("cif.clients.SubscriberClient")
@patch("cif.clients.PublisherClient")
@patch("cif.clients.StorageClient")
@patch("cif.clients.SpannerClient")
def test_get_clients(
    mock_spanner_client: MagicMock,
    mock_storage_client: MagicMock,
    mock_publisher_client: MagicMock,
    mock_subscriber_client: MagicMock,
    mock_bigquery_client: MagicMock,
) -> None:
    clients = get_clients("project_id", "spanner_instance_id", "spanner_database_id", "topic_id", "kms_key_name")
    assert clients.spanner is not None
    assert clients.storage is not None
    assert clients.pub is not None
    assert clients.sub is not None
    assert clients.bigquery is not None
    mock_spanner_client.assert_called_once_with(project="project_id")
    mock_storage_client.assert_called_once_with(project="project_id")
    mock_publisher_client.assert_called_once_with()
    mock_subscriber_client.assert_called_once_with()
    mock_bigquery_client.assert_called_once_with(project="project_id")


@patch("cif.clients.PublisherClient")
def test_pub_support_new_instance(mock_publisher_client: MagicMock) -> None:
    pub = PubSupport.new_instance("project_id", "topic_id", "kms_key_name")
    assert pub.topic_path is not None
    assert pub.kms_key_name == "kms_key_name"
    mock_publisher_client.assert_called_once()
    mock_publisher_client().topic_path.assert_called_once_with("project_id", "topic_id")


def test_pub_support_publish_messages() -> None:
    pub = PubSupport(MagicMock(), "topic_id", "kms_key_name")
    messages = [b""]
    futures = pub.publish_messages(messages)
    assert len(futures) == len(messages)
    pub.client.publish.assert_has_calls([call(pub.topic_path, message) for message in messages])


@patch("cif.clients.SubscriberClient")
def test_sub_support_new_instance(mock_subscriber_client: MagicMock) -> None:
    sub = SubSupport.new_instance("project_id", "subscription_id")
    assert sub.subscription_path is not None
    mock_subscriber_client.assert_called_once()
    mock_subscriber_client().subscription_path.assert_called_once_with("project_id", "subscription_id")


def test_sub_support_start_running() -> None:
    message_handler = MagicMock()
    sub = SubSupport(MagicMock(), "subscription_path")
    sub.f = MagicMock()
    with raises(ValueError, match="Already started"):
        sub.start(message_handler)


def test_sub_support_stop_not_running() -> None:
    sub = SubSupport(MagicMock(), "subscription_path")
    with raises(ValueError, match="Not started"):
        sub.stop()


def test_sub_support_start() -> None:
    message_handler = MagicMock()
    sub = SubSupport(MagicMock(), "subscription_path")
    sub.stop = MagicMock()
    sub.client.subscribe.return_value.result = MagicMock()
    sub.start(message_handler)
    sub.client.subscribe.assert_called_once_with(
        sub.subscription_path, callback=message_handler, flow_control=FlowControl(max_messages=1)
    )


def test_sub_support_stop() -> None:
    sub = SubSupport(MagicMock(), "subscription_path")
    f = sub.f = MagicMock()
    sub.stop(timeout_seconds=1)
    assert sub.f is None
    f.cancel.assert_called_once()
    f.result.assert_called_once_with(1)


class SomeTestModel(BaseModel):
    a_list: Optional[List] = None
    a_dict: Optional[Dict[str, Any]] = None


def test_spanner_support_new_persistent() -> None:
    # ensure that a JsonObject returned by a query that is actually an array is
    # parsed properly
    test = SpannerSupport.new_persistent(
        {"a_list": JsonObject.from_str("[]"), "a_dict": JsonObject.from_str("{}")}, SomeTestModel
    )
    assert test.a_list == []
    assert test.a_dict == {}


def test_spanner_support_select_many() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    sql = "select foo from bar where param1 = @param1"
    limit = 50
    offset = 50
    results = MagicMock()
    results.stats.row_count_exact = 0
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.return_value = results
    records, next_offset = spanner.select_many(sql, SomeTestModel, limit=limit, offset=offset, param1="x")
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.assert_called_once_with(
        sql + " limit @limit offset @offset",
        params={"limit": limit, "offset": offset, "param1": "x"},
        param_types={
            "limit": spanner.calc_spanner_type(limit),
            "offset": spanner.calc_spanner_type(offset),
            "param1": spanner.calc_spanner_type("x"),
        },
        timeout=30,
    )


def test_spanner_support_select_zero_or_one_zero() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    sql = "select foo from bar where param1 = @param1"
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.return_value.to_dict_list.return_value = []
    result = spanner.select_zero_or_one(sql, SomeTestModel, param1="x")
    assert result is None
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.assert_called_once_with(
        sql + " limit @limit offset @offset",
        params={"limit": 1, "offset": 0, "param1": "x"},
        param_types={
            "limit": spanner.calc_spanner_type(1),
            "offset": spanner.calc_spanner_type(0),
            "param1": spanner.calc_spanner_type("x"),
        },
        timeout=30,
    )


def test_spanner_support_select_zero_or_one_one() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    sql = "select foo from bar where param1 = @param1"
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.return_value.to_dict_list.return_value = [{}]
    result = spanner.select_zero_or_one(sql, SomeTestModel, param1="x")
    assert result is not None
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.assert_called_once_with(
        sql + " limit @limit offset @offset",
        params={"limit": 1, "offset": 0, "param1": "x"},
        param_types={
            "limit": spanner.calc_spanner_type(1),
            "offset": spanner.calc_spanner_type(0),
            "param1": spanner.calc_spanner_type("x"),
        },
        timeout=30,
    )


def test_spanner_support_select_count() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    sql = "select count(*) from bar where param1 = @param1"
    spanner.select_count(sql, param1="x")
    spanner.database.snapshot.return_value.__enter__.return_value.execute_sql.assert_called_once_with(
        sql,
        params={"param1": "x"},
        param_types={"param1": spanner.calc_spanner_type("x")},
        timeout=30,
    )


def test_spanner_support_insert_batch() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    xs = [SomeTestModel(a_list=[], a_dict={})]
    num_rows = spanner.insert_batch("table", xs)
    assert num_rows == len(xs)
    spanner.database.batch.return_value.__enter__.return_value.insert_or_update.assert_called_once_with(
        table="table",
        columns=list(xs[0].model_dump().keys()),
        values=[[spanner.calc_spanner_value(y) for y in x.model_dump().values()] for x in xs],
    )


def test_spanner_support_insert_batch_empty() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    xs = []
    num_rows = spanner.insert_batch("table", xs)
    assert num_rows == len(xs)
    spanner.database.batch.return_value.__enter__.return_value.insert.assert_not_called()


def test_spanner_support_calc_spanner_type() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    for i in [1, "str", {}]:
        spanner.calc_spanner_type(i)


def test_spanner_support_calc_spanner_type_not_mapped() -> None:
    spanner = SpannerSupport(MagicMock(), MagicMock(), MagicMock())
    with raises(ValueError, match=r"Can't map value .*"):
        spanner.calc_spanner_type(1.2345)
