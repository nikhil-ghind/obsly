"""Kafka consumer: reads metric envelopes off every source topic and
persists them via MetricRepository.

Runs as a consumer group so multiple instances can be scaled out; each
instance subscribes to all metric topics and hands every decoded envelope
to the repository (SQL + Redis hot window).
"""

from __future__ import annotations

import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from obsly.config import get_settings
from obsly.kafka.topics import all_metric_topics
from obsly.models.metric import MetricEnvelope
from obsly.storage.repository import MetricRepository

logger = logging.getLogger(__name__)


class MetricConsumer:
    """Consumes metric envelopes from Kafka and writes them to storage."""

    def __init__(self, repository: MetricRepository | None = None) -> None:
        self._settings = get_settings()
        self._repository = repository or MetricRepository()
        self._consumer = KafkaConsumer(
            *all_metric_topics(),
            bootstrap_servers=self._settings.kafka.bootstrap_servers,
            group_id=self._settings.kafka.consumer_group,
            client_id=f"{self._settings.kafka.client_id}-consumer",
            value_deserializer=lambda v: v,
            key_deserializer=lambda k: k,
            enable_auto_commit=True,
            auto_offset_reset="latest",
            max_poll_records=500,
        )

    def run_forever(self) -> None:
        logger.info("metric consumer starting, subscribed to %s", all_metric_topics())
        try:
            for message in self._consumer:
                self._handle_message(message.value)
        except KafkaError:
            logger.exception("kafka consumer loop terminated with an error")
            raise
        finally:
            self._consumer.close()

    def _handle_message(self, raw_value: bytes) -> None:
        try:
            metric = MetricEnvelope.from_kafka_value(raw_value)
        except Exception:
            logger.exception("failed to decode metric envelope, dropping message")
            return

        try:
            self._repository.save(metric)
        except Exception:
            logger.exception(
                "failed to persist metric %s from %s", metric.metric_name, metric.host
            )
