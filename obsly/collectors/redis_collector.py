"""Redis fleet metric collector.

Polls `INFO` on each configured Redis instance for memory, throughput,
client, keyspace, and replication metrics.
"""

from __future__ import annotations

import logging
from typing import List
from urllib.parse import urlparse

import redis

from obsly.collectors.base import BaseCollector
from obsly.models.metric import MetricEnvelope, MetricSource, MetricType

logger = logging.getLogger(__name__)

_GAUGE_INFO_FIELDS = {
    "used_memory": "redis_used_memory_bytes",
    "used_memory_rss": "redis_used_memory_rss_bytes",
    "maxmemory": "redis_maxmemory_bytes",
    "connected_clients": "redis_connected_clients",
    "blocked_clients": "redis_blocked_clients",
    "mem_fragmentation_ratio": "redis_mem_fragmentation_ratio",
    "master_repl_offset": "redis_master_repl_offset",
    "connected_slaves": "redis_connected_slaves",
    "rdb_changes_since_last_save": "redis_rdb_changes_since_last_save",
}

_COUNTER_INFO_FIELDS = {
    "total_commands_processed": "redis_commands_processed_total",
    "total_connections_received": "redis_connections_received_total",
    "expired_keys": "redis_expired_keys_total",
    "evicted_keys": "redis_evicted_keys_total",
    "keyspace_hits": "redis_keyspace_hits_total",
    "keyspace_misses": "redis_keyspace_misses_total",
}


class RedisCollector(BaseCollector):
    """Collects health/throughput metrics from one or more Redis instances."""

    default_interval_seconds = 15

    @property
    def name(self) -> str:
        return "redis"

    def collect(self) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        for url in self._settings.redis.monitored_urls:
            metrics.extend(self._collect_instance(url))
        return metrics

    def _host_label(self, url: str) -> str:
        parsed = urlparse(url)
        return parsed.hostname or url

    def _collect_instance(self, url: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        host = self._host_label(url)
        try:
            conn = redis.Redis.from_url(url, socket_timeout=5)
            info = conn.info()
        except redis.RedisError:
            logger.exception("failed to collect redis INFO from %s", host)
            return metrics

        role = info.get("role", "unknown")
        labels = {"role": role}

        for field, metric_name in _GAUGE_INFO_FIELDS.items():
            if field in info:
                metrics.append(self._envelope(metric_name, float(info[field]), host, labels))

        for field, metric_name in _COUNTER_INFO_FIELDS.items():
            if field in info:
                metrics.append(
                    self._envelope(
                        metric_name,
                        float(info[field]),
                        host,
                        labels,
                        metric_type=MetricType.COUNTER,
                    )
                )

        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        total = hits + misses
        hit_ratio = (hits / total) if total else 1.0
        metrics.append(self._envelope("redis_keyspace_hit_ratio", hit_ratio, host, labels))

        ops_per_sec = info.get("instantaneous_ops_per_sec")
        if ops_per_sec is not None:
            metrics.append(
                self._envelope("redis_instantaneous_ops_per_sec", float(ops_per_sec), host, labels)
            )

        keys_total = 0
        for db_name, db_stats in info.items():
            if isinstance(db_stats, dict) and db_name.startswith("db"):
                keys_total += db_stats.get("keys", 0)
        metrics.append(self._envelope("redis_keys_total", float(keys_total), host, labels))

        return metrics

    def _envelope(self, name: str, value: float, host: str, labels: dict, metric_type=MetricType.GAUGE) -> MetricEnvelope:
        return MetricEnvelope(
            source=MetricSource.REDIS,
            metric_name=name,
            metric_type=metric_type,
            value=value,
            fleet=self._settings.fleet_name,
            host=host,
            labels=labels,
        )
