from logging import getLogger
from typing import List, Optional

from common_metrics.metrics import Metrics
from google.cloud.pubsub_v1.subscriber.message import Message
from pydantic import ValidationError

from cif.disaggregation import Disaggregation
from cif.extractor import Extractor
from cif.factory import Factory
from cif.persistence import (
    DeferredDisaggregationModel,
    DisaggregationStatus,
    MetricLabels,
    MetricName,
)
from cif.util import metric_timer

logger = getLogger(__name__)


class NotFoundError(Exception):
    pass


class Worker:
    def __init__(self, factory: Factory, metrics: Metrics) -> None:
        self.factory = factory
        self.metrics = metrics

    def parse_message(self, message: Message) -> DeferredDisaggregationModel:
        return DeferredDisaggregationModel.model_validate_json(message.data.decode("utf-8"))

    def __call__(self, message: Message) -> None:
        parsed_message = None
        try:
            with metric_timer(self.metrics, metrics_labels=MetricLabels(task_name=MetricName.WORKER_CALLBACK)):
                parsed_message = self.parse_message(message)
                source, generation, artifact = self.check_references(parsed_message)
                disaggregation = self.factory.new_disaggregation(source)
                extractor = self.check_extractor(parsed_message, disaggregation)
                try:
                    with metric_timer(
                        self.metrics,
                        metrics_labels=MetricLabels(
                            source_id=parsed_message.source_id,
                            generation_id=parsed_message.generation_id,
                            artifact_id=parsed_message.artifact_id,
                            extractor_type=parsed_message.extractor_type,
                            task_name=MetricName.WORKER_DISAGGREGATION,
                        ),
                    ):
                        disaggregation.disaggregate_one(
                            generation,
                            artifact,
                            extractor,
                            fragment_id=parsed_message.fragment_id,
                            start_byte=parsed_message.start_byte,
                            end_byte=parsed_message.end_byte,
                        )
                        self.handle_disaggregation_done(message, parsed_message)
                        logger.info(
                            "Processed disaggregation for source %s, generation %d, artifact %s",
                            parsed_message.source_id,
                            parsed_message.generation_id,
                            parsed_message.artifact_id,
                        )
                except Exception:
                    self.handle_disaggregation_failed_maybe_retry(message, parsed_message)
                    logger.exception(
                        "Unhandled exception processing disaggregation for source %s, generation %d, artifact %s",
                        parsed_message.source_id,
                        parsed_message.generation_id,
                        parsed_message.artifact_id,
                    )
        except ValidationError:
            self.handle_disaggregation_failed_no_retry(message, None)
            logger.exception("Discarded unparseable message %s", message)
        except NotFoundError:
            self.handle_disaggregation_failed_no_retry(message, parsed_message)
            logger.exception(
                "Discarded disaggregation for source %s, generation %d, artifact %s",
                parsed_message.source_id,
                parsed_message.generation_id,
                parsed_message.artifact_id,
            )

    def check_references(self, parsed_message: DeferredDisaggregationModel) -> List:
        """
        Load all objects referenced in the message, or raise NotFoundError if any do not
        exist, calling to unloadable objects.
        """
        source = self.factory.catalog.get_source(parsed_message.source_id)
        generation = self.factory.catalog.get_generation(parsed_message.source_id, parsed_message.generation_id)
        artifact = self.factory.catalog.get_artifact(parsed_message.artifact_id)
        references = {
            "source_id": (parsed_message.source_id, source),
            "generation_id": (parsed_message.generation_id, generation),
            "artifact_id": (parsed_message.artifact_id, artifact),
        }
        if all(map(lambda kv: kv[1][1] is not None, references.items())):
            return source, generation, artifact
        else:
            raise NotFoundError(", ".join([f"{k} {v[0]} not found" for k, v in references.items() if v[1] is None]))

    def check_extractor(self, parsed_message: DeferredDisaggregationModel, disaggregation: Disaggregation) -> Extractor:
        extractor = next((x for x in disaggregation.extractors if x.__class__.__name__ == parsed_message.extractor_type), None)
        if extractor is None:
            raise NotFoundError(f"extractor_type {parsed_message.extractor_type} not found")
        return extractor

    def handle_disaggregation_failed_maybe_retry(self, message: Message, parsed_message: DeferredDisaggregationModel) -> None:
        """
        Set status to FAILED, but nack so it can be redelivered.
        """
        parsed_message.delivery_attempt = message.delivery_attempt or 1
        parsed_message.status = DisaggregationStatus.FAILED
        self.factory.catalog.insert_deferred_disaggregations([parsed_message])
        message.nack()

    def handle_disaggregation_failed_no_retry(
        self, message: Message, parsed_message: Optional[DeferredDisaggregationModel]
    ) -> None:
        """
        Set status to FAILED, and ack so it won't be redelivered.
        """
        if parsed_message is not None:
            parsed_message.delivery_attempt = message.delivery_attempt or 1
            parsed_message.status = DisaggregationStatus.FAILED
            self.factory.catalog.insert_deferred_disaggregations([parsed_message])
        message.ack()

    def handle_disaggregation_done(self, message: Message, parsed_message: DeferredDisaggregationModel) -> None:
        """
        Set status to DONE and ack message.
        """
        parsed_message.delivery_attempt = message.delivery_attempt or 1
        parsed_message.status = DisaggregationStatus.DONE
        self.factory.catalog.insert_deferred_disaggregations([parsed_message])
        message.ack()
