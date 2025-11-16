from argparse import ArgumentParser, Namespace
from logging import getLogger
from signal import SIGTERM, signal

from common_metrics.metrics import metrics

from cif.clients import get_clients
from cif.factory import Factory
from cif.persistence import MetricLabels, MetricName
from cif.util import calc_version, configure_logging, metric_timer
from cif.worker import Worker

logger = getLogger(__name__)


def do_ingestion(args: Namespace, factory: Factory) -> None:
    with metric_timer(metrics=metrics, metrics_labels=MetricLabels(task_name=args.name, source_id=args.source_id)):
        source = factory.catalog.get_source(args.source_id)
        ingestion = factory.new_ingestion(source)
        ingestion.ingest()


def do_worker(args: Namespace, factory: Factory) -> None:
    worker = Worker(factory, metrics=metrics)
    metrics.increment_counter(MetricName.REQUEST_COUNT, 1, labels=MetricLabels(task_name=args.name).model_dump())
    signal(SIGTERM, lambda signum, frame: clients.sub.stop())
    factory.clients.sub.start(worker, max_messages=args.max_messages)


def do_check(args: Namespace, factory: Factory) -> None:
    # no-op task
    logger.info(f"OK {calc_version()}")


def parse_args() -> Namespace:
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    ingestion = subparsers.add_parser("ingestion", description="Ingestion driver")
    ingestion.add_argument("source_id", help="Source to ingest")
    ingestion.set_defaults(func=do_ingestion, name="ingestion")

    worker = subparsers.add_parser("worker", description="Ingestion worker")
    worker.add_argument("-m", "--max-messages", type=int, nargs="?", default=1)
    worker.set_defaults(func=do_worker, name="worker")

    check = subparsers.add_parser("check", description="Check client configuration")
    check.set_defaults(func=do_check, name="check")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    configure_logging()
    logger.info("Starting cif-%s %s", args.name, calc_version())
    metrics.init("csr-va-cif-main")
    clients = get_clients()
    factory = Factory(clients)
    args.func(args, factory)
