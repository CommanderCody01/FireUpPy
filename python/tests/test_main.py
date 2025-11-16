from unittest.mock import MagicMock, patch

from pytest import raises

from cif.main import do_check, do_ingestion, do_worker, parse_args


@patch("sys.argv", ["", "ingestion", "source_id"])
def test_parse_args_ingestion() -> None:
    args = parse_args()
    assert args.source_id == "source_id"
    assert args.name == "ingestion"


@patch("sys.argv", ["", "ingestion"])
def test_parse_args_ingestion_no_source_id() -> None:
    with raises(SystemExit):
        parse_args()


@patch("sys.argv", ["", "worker"])
def test_parse_args_worker_max_messages_default() -> None:
    args = parse_args()
    assert args.max_messages == 1
    assert args.name == "worker"


def test_do_ingestion() -> None:
    factory = MagicMock()
    args = MagicMock()
    args.name = "task name"
    args.source_id = "some source id"
    do_ingestion(args, factory)
    factory.new_ingestion.return_value.ingest.assert_called_once()


def test_do_worker() -> None:
    factory = MagicMock()
    args = MagicMock()
    args.name = "task name"
    args.source_id = "some source id"
    do_worker(args, factory)


def test_do_check() -> None:
    factory = MagicMock()
    args = MagicMock()
    do_check(args, factory)
