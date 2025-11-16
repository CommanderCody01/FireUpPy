from datetime import timezone
from logging import NullHandler, getLogger
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from cif.persistence import MetricLabels
from cif.util import (
    batcher,
    calc_subclasses,
    calc_version,
    configure_logging,
    flatten,
    metric_timer,
    now,
)


def test_now() -> None:
    ts = now()
    assert ts.tzinfo == timezone.utc


def test_now_tzinfo() -> None:
    ts = now(tzinfo=timezone.max)
    assert ts.tzinfo == timezone.max


def test_configure_logging() -> None:
    # these are only set up when running in a uvicorn process
    for i in ["uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi"]:
        logger = getLogger(i)
        logger.addHandler(NullHandler())
    configure_logging()


def source(n: int) -> Generator[int]:
    for i in range(n):
        yield i


def test_batcher_no_remainder() -> None:
    # source is evenly divided into batches of batch_size
    batches = []
    for batch in batcher(source(9), batch_size=3):
        batches.append(batch)
    assert batches[0] == [0, 1, 2]
    assert batches[1] == [3, 4, 5]
    assert batches[2] == [6, 7, 8]


def test_batcher_remainder() -> None:
    # source is not evenly divided into batches of batch_size
    batches = []
    for batch in batcher(source(10), batch_size=3):
        batches.append(batch)
    assert batches[0] == [0, 1, 2]
    assert batches[1] == [3, 4, 5]
    assert batches[2] == [6, 7, 8]
    assert batches[3] == [9]


def test_calc_subclasses() -> None:
    class Foo:
        pass

    class Bar(Foo):
        pass

    class Baz(Bar):
        pass

    assert set([Bar, Baz]) == calc_subclasses(Foo)


@patch("builtins.open")
def test_calc_version(mock_open: MagicMock) -> None:
    mock_open.return_value.__enter__.return_value.read.return_value = "x.y.z+deadbeef\n"
    assert calc_version() == "x.y.z+deadbeef"


@patch("builtins.open")
def test_calc_version_error(mock_open: MagicMock) -> None:
    mock_open.return_value.__enter__.side_effect = FileNotFoundError()
    assert calc_version() == "UNKNOWN"


def test_flatten() -> None:
    assert flatten([[1, 2], [3, 4], [5, 6]]) == [1, 2, 3, 4, 5, 6]


def test_metrics_context_manager() -> None:
    def hello() -> str:
        return "world"

    mocked_metrics = MagicMock()
    input_labels = MetricLabels(source_id="1234", task_success=True)
    with metric_timer(metrics=mocked_metrics, metrics_labels=input_labels):
        hello()

    assert mocked_metrics.increment_counter.call_count == 2
    assert mocked_metrics.record_histogram.call_count == 1
    assert mocked_metrics.set_gauge_value.call_count == 1

    increment_counter_params = mocked_metrics.increment_counter.call_args
    labels = increment_counter_params[1]["labels"]
    assert labels == input_labels.model_dump()


def test_metrics_context_manager_with_exception() -> None:
    def hello() -> None:
        raise Exception("world")

    mocked_metrics = MagicMock()
    input_labels = MetricLabels(source_id="1234", task_success=False)

    with pytest.raises(Exception):
        with metric_timer(metrics=mocked_metrics, metrics_labels=input_labels):
            hello()

    # since an exception was raised some of the metrics weren't recorded
    assert mocked_metrics.increment_counter.call_count == 2
    assert mocked_metrics.record_histogram.call_count == 1
    assert mocked_metrics.set_gauge_value.call_count == 1

    increment_counter_params = mocked_metrics.increment_counter.call_args
    labels = increment_counter_params[1]["labels"]
    assert labels == input_labels.model_dump()
