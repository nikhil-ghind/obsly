"""Prometheus exporter: serves the hot-window metric cache as a scrape target.

Reads the Redis hot-window keys written by MetricRepository and republishes
them as native prometheus_client Gauges, refreshed on a background timer so
`/metrics` scrapes stay cheap.
"""

from __future__ import annotations

import json
import logging
import threading
import time

from prometheus_client import Gauge, start_http_server

from obsly.config import get_settings
from obsly.storage.repository import MetricRepository

logger = logging.getLogger(__name__)

# Cache of already-registered Gauge objects, keyed by metric name, so we
# don't re-register the same series (and its full label set) every refresh.
_gauge_registry: dict[str, Gauge] = {}
_registry_lock = threading.Lock()


def _get_or_create_gauge(metric_name: str, label_names: list[str]) -> Gauge:
    with _registry_lock:
        gauge = _gauge_registry.get(metric_name)
        if gauge is None:
            gauge = Gauge(metric_name, f"Obsly metric: {metric_name}", label_names)
            _gauge_registry[metric_name] = gauge
        return gauge


class PrometheusExporter:
    """Periodically republishes hot-window metrics as Prometheus gauges."""

    def __init__(self, repository: MetricRepository | None = None) -> None:
        self._settings = get_settings()
        self._repository = repository or MetricRepository()
        self._stop_event = threading.Event()

    def _refresh_once(self) -> int:
        updated = 0
        for key in self._repository.distinct_hot_keys():
            # key format: obsly:hot:<source>:<host>:<metric_name>
            try:
                _, _, source, host, metric_name = key.split(":", 4)
            except ValueError:
                continue

            latest = self._repository.latest_hot_value(source, host, metric_name)
            if latest is None:
                continue

            labels = dict(latest.get("labels", {}))
            labels.setdefault("fleet", self._settings.fleet_name)
            labels["host"] = host
            labels["source"] = source

            label_names = sorted(labels.keys())
            gauge = _get_or_create_gauge(metric_name, label_names)
            gauge.labels(**{k: labels[k] for k in label_names}).set(latest["value"])
            updated += 1
        return updated

    def _refresh_loop(self) -> None:
        interval = self._settings.exporter.refresh_interval_seconds
        while not self._stop_event.is_set():
            try:
                updated = self._refresh_once()
                logger.debug("exporter refreshed %d series", updated)
            except Exception:
                logger.exception("exporter refresh cycle failed")
            self._stop_event.wait(interval)

    def start(self) -> None:
        start_http_server(self._settings.exporter.port, addr=self._settings.exporter.host)
        logger.info(
            "prometheus exporter listening on %s:%s",
            self._settings.exporter.host,
            self._settings.exporter.port,
        )
        refresh_thread = threading.Thread(target=self._refresh_loop, daemon=True)
        refresh_thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self._stop_event.set()
