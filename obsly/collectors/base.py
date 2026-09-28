"""Base class every metric collector implements.

A collector's only job is to produce a list of MetricEnvelope objects for
one polling cycle; BaseCollector handles the run loop, timing, error
isolation, and publishing to Kafka via KafkaProducerClient.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import List

from obsly.config import get_settings
from obsly.kafka.producer import KafkaProducerClient
from obsly.models.metric import MetricEnvelope

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Common polling/publishing loop for all fleet metric collectors."""

    #: seconds between collection cycles; subclasses may override the default
    default_interval_seconds: int = 15

    def __init__(self, producer: KafkaProducerClient | None = None) -> None:
        self._settings = get_settings()
        self._producer = producer or KafkaProducerClient()
        self._owns_producer = producer is None

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier used in logs, e.g. 'k8s', 'redis', 'mysql'."""

    @abstractmethod
    def collect(self) -> List[MetricEnvelope]:
        """Collect one batch of metric readings for this cycle."""

    def run_once(self) -> int:
        """Run a single collection + publish cycle. Returns metrics published."""

        started = time.monotonic()
        try:
            metrics = self.collect()
        except Exception:
            logger.exception("collector %s failed to collect metrics", self.name)
            return 0

        published = self._producer.send_batch(metrics)
        duration = time.monotonic() - started
        logger.info(
            "collector cycle complete",
            extra={
                "extra_fields": {
                    "collector": self.name,
                    "metrics_published": published,
                    "duration_seconds": round(duration, 3),
                }
            },
        )
        return published

    def run_forever(self, interval_seconds: int | None = None) -> None:
        """Blocking loop: collect + publish on a fixed interval."""

        interval = interval_seconds or self.default_interval_seconds
        logger.info("starting collector loop for %s (interval=%ss)", self.name, interval)
        try:
            while True:
                cycle_start = time.monotonic()
                self.run_once()
                elapsed = time.monotonic() - cycle_start
                sleep_for = max(0.0, interval - elapsed)
                time.sleep(sleep_for)
        finally:
            if self._owns_producer:
                self._producer.close()
