"""Persistence layer: durable SQL history plus a Redis hot-window cache.

The consumer calls `MetricRepository.save` for every metric it processes.
Writes go to two places:
  - SQLAlchemy-backed SQL storage for durable historical querying
  - A Redis sorted-set "hot window" keyed by metric identity, which the
    Prometheus exporter reads on every scrape without touching SQL
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Iterable, List

import redis
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session, sessionmaker

from obsly.config import get_settings
from obsly.models.metric import MetricEnvelope
from obsly.storage.models import Base, MetricSample

logger = logging.getLogger(__name__)


def _hot_key(metric: MetricEnvelope) -> str:
    return f"obsly:hot:{metric.source.value}:{metric.host}:{metric.metric_name}"


class MetricRepository:
    """Durable + hot-window storage for ingested metrics."""

    def __init__(self) -> None:
        settings = get_settings()
        self._settings = settings
        self._engine = create_engine(settings.storage.database_url, future=True)
        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine, future=True)
        self._redis = redis.Redis.from_url(settings.redis.url, decode_responses=True)

    def save(self, metric: MetricEnvelope) -> None:
        self._save_sql(metric)
        self._save_hot_window(metric)

    def save_batch(self, metrics: Iterable[MetricEnvelope]) -> int:
        count = 0
        session: Session
        with self._session_factory() as session:
            for metric in metrics:
                session.add(self._to_row(metric))
                self._save_hot_window(metric)
                count += 1
            session.commit()
        return count

    def _to_row(self, metric: MetricEnvelope) -> MetricSample:
        return MetricSample(
            source=metric.source.value,
            metric_name=metric.metric_name,
            metric_type=metric.metric_type.value,
            value=metric.value,
            fleet=metric.fleet,
            host=metric.host,
            labels_json=json.dumps(metric.labels),
            recorded_at=datetime.utcfromtimestamp(metric.timestamp),
        )

    def _save_sql(self, metric: MetricEnvelope) -> None:
        with self._session_factory() as session:
            session.add(self._to_row(metric))
            session.commit()

    def _save_hot_window(self, metric: MetricEnvelope) -> None:
        key = _hot_key(metric)
        payload = json.dumps({"value": metric.value, "labels": metric.labels, "ts": metric.timestamp})
        try:
            self._redis.zadd(key, {payload: metric.timestamp})
            cutoff = metric.timestamp - self._settings.storage.hot_window_seconds
            self._redis.zremrangebyscore(key, 0, cutoff)
            self._redis.expire(key, self._settings.storage.hot_window_seconds * 2)
        except redis.RedisError:
            logger.exception("failed to update hot window cache for %s", key)

    def latest_hot_value(self, source: str, host: str, metric_name: str) -> dict | None:
        key = f"obsly:hot:{source}:{host}:{metric_name}"
        try:
            entries = self._redis.zrevrange(key, 0, 0)
        except redis.RedisError:
            logger.exception("failed to read hot window cache for %s", key)
            return None
        if not entries:
            return None
        return json.loads(entries[0])

    def distinct_hot_keys(self, pattern: str = "obsly:hot:*") -> List[str]:
        try:
            return list(self._redis.scan_iter(match=pattern, count=500))
        except redis.RedisError:
            logger.exception("failed to scan hot window keys")
            return []

    def purge_expired(self) -> int:
        """Delete SQL rows older than the configured retention window."""

        cutoff = datetime.utcnow() - timedelta(days=self._settings.storage.retention_days)
        with self._session_factory() as session:
            result = session.execute(delete(MetricSample).where(MetricSample.recorded_at < cutoff))
            session.commit()
            return result.rowcount or 0
