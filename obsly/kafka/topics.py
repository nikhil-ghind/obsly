"""Kafka topic naming conventions for Obsly.

All producers and consumers derive topic names from here so the prefix and
per-source layout stay consistent (e.g. easy to namespace multiple fleets
sharing one Kafka cluster).
"""

from __future__ import annotations

from obsly.config import get_settings
from obsly.models.metric import MetricSource


def metrics_topic(source: MetricSource) -> str:
    settings = get_settings()
    return f"{settings.kafka.topic_prefix}.metrics.{source.value}"


def logs_topic() -> str:
    settings = get_settings()
    return f"{settings.kafka.topic_prefix}.logs"


def all_metric_topics() -> list[str]:
    return [metrics_topic(source) for source in MetricSource]


def dead_letter_topic(original_topic: str) -> str:
    return f"{original_topic}.dlq"
