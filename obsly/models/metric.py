"""Shared metric envelope schema used by every producer and consumer.

Every collector (Kubernetes, Redis, MySQL) publishes MetricEnvelope
instances onto Kafka, and the consumer/exporter deserialize the same shape
back out. Keeping one schema module avoids drift between producers and
consumers.
"""

from __future__ import annotations

import json
import time
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class MetricSource(str, Enum):
    KUBERNETES = "kubernetes"
    REDIS = "redis"
    MYSQL = "mysql"


class MetricType(str, Enum):
    GAUGE = "gauge"
    COUNTER = "counter"
    HISTOGRAM = "histogram"


class MetricEnvelope(BaseModel):
    """A single metric observation flowing through Kafka.

    Example: {"source": "redis", "metric_name": "redis_used_memory_bytes",
    "metric_type": "gauge", "value": 10485760, "fleet": "default-fleet",
    "host": "redis-0", "labels": {"role": "master"}, "timestamp": 1700000000.0}
    """

    source: MetricSource
    metric_name: str
    metric_type: MetricType = MetricType.GAUGE
    value: float
    fleet: str
    host: str
    labels: Dict[str, str] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=lambda: time.time())
    correlation_id: Optional[str] = None

    def to_kafka_key(self) -> bytes:
        return f"{self.source.value}:{self.host}:{self.metric_name}".encode("utf-8")

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_value(cls, raw: bytes) -> "MetricEnvelope":
        return cls.model_validate(json.loads(raw))

    def prometheus_labels(self) -> Dict[str, str]:
        """Labels to attach when exporting this metric to Prometheus."""

        base = {"fleet": self.fleet, "host": self.host, "source": self.source.value}
        base.update(self.labels)
        return base


class LogRecord(BaseModel):
    """Normalized log line emitted by the log aggregator."""

    source: str
    host: str
    fleet: str
    level: str = "INFO"
    message: str
    timestamp: float = Field(default_factory=lambda: time.time())
    fields: Dict[str, Any] = Field(default_factory=dict)

    def to_json_line(self) -> str:
        return self.model_dump_json()
