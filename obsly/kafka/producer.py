"""Shared Kafka producer client used by every collector.

Wraps kafka-python's KafkaProducer with the serialization, batching, and
retry behavior all Obsly collectors want, so individual collectors only
need to worry about *what* metric to publish, not *how*.
"""

from __future__ import annotations

import logging
from typing import Iterable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from obsly.config import get_settings
from obsly.kafka.topics import metrics_topic
from obsly.models.metric import MetricEnvelope

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    """Thin wrapper around KafkaProducer tuned for metric envelopes."""

    def __init__(self) -> None:
        settings = get_settings()
        self._settings = settings
        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            client_id=settings.kafka.client_id,
            acks=settings.kafka.acks,
            linger_ms=settings.kafka.linger_ms,
            batch_size=settings.kafka.max_batch_size * 256,
            value_serializer=lambda v: v,
            key_serializer=lambda k: k,
            retries=5,
            retry_backoff_ms=250,
        )

    def send(self, metric: MetricEnvelope) -> None:
        topic = metrics_topic(metric.source)
        try:
            self._producer.send(
                topic,
                key=metric.to_kafka_key(),
                value=metric.to_kafka_value(),
            )
        except KafkaError:
            logger.exception("failed to enqueue metric on topic %s", topic)
            raise

    def send_batch(self, metrics: Iterable[MetricEnvelope]) -> int:
        count = 0
        for metric in metrics:
            self.send(metric)
            count += 1
        self.flush()
        return count

    def flush(self, timeout: float | None = None) -> None:
        self._producer.flush(timeout=timeout)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()

    def __enter__(self) -> "KafkaProducerClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
