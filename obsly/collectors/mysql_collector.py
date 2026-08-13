"""MySQL fleet metric collector.

Queries `SHOW GLOBAL STATUS`, `SHOW GLOBAL VARIABLES`, and replication
status on each configured MySQL instance to surface connection, query,
buffer pool, and replication health metrics.
"""

from __future__ import annotations

import logging
from typing import List
from urllib.parse import urlparse

import pymysql
from sqlalchemy.engine import make_url

from obsly.collectors.base import BaseCollector
from obsly.models.metric import MetricEnvelope, MetricSource, MetricType

logger = logging.getLogger(__name__)

_GAUGE_STATUS_FIELDS = {
    "Threads_connected": "mysql_threads_connected",
    "Threads_running": "mysql_threads_running",
    "Innodb_buffer_pool_pages_free": "mysql_innodb_buffer_pool_pages_free",
    "Innodb_buffer_pool_pages_total": "mysql_innodb_buffer_pool_pages_total",
    "Innodb_row_lock_current_waits": "mysql_innodb_row_lock_current_waits",
    "Threads_cached": "mysql_threads_cached",
}

_COUNTER_STATUS_FIELDS = {
    "Questions": "mysql_questions_total",
    "Slow_queries": "mysql_slow_queries_total",
    "Connections": "mysql_connections_total",
    "Aborted_connects": "mysql_aborted_connects_total",
    "Com_select": "mysql_com_select_total",
    "Com_insert": "mysql_com_insert_total",
    "Com_update": "mysql_com_update_total",
    "Com_delete": "mysql_com_delete_total",
    "Innodb_buffer_pool_read_requests": "mysql_innodb_buffer_pool_read_requests_total",
    "Innodb_buffer_pool_reads": "mysql_innodb_buffer_pool_reads_total",
}


class MySQLCollector(BaseCollector):
    """Collects connection/query/replication health metrics from MySQL."""

    default_interval_seconds = 15

    @property
    def name(self) -> str:
        return "mysql"

    def collect(self) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        for dsn in self._settings.mysql.monitored_dsns:
            metrics.extend(self._collect_instance(dsn))
        return metrics

    def _connect(self, dsn: str) -> pymysql.connections.Connection:
        url = make_url(dsn)
        return pymysql.connect(
            host=url.host or "localhost",
            port=url.port or 3306,
            user=url.username,
            password=url.password or "",
            database=url.database,
            connect_timeout=5,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _host_label(self, dsn: str) -> str:
        url = make_url(dsn)
        return url.host or dsn

    def _collect_instance(self, dsn: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        host = self._host_label(dsn)
        try:
            conn = self._connect(dsn)
        except Exception:
            logger.exception("failed to connect to mysql instance %s", host)
            return metrics

        try:
            status = self._fetch_status(conn)
            metrics.extend(self._status_metrics(status, host))
            metrics.extend(self._buffer_pool_metrics(status, host))
            metrics.extend(self._replication_metrics(conn, host))
        finally:
            conn.close()
        return metrics

    def _fetch_status(self, conn: pymysql.connections.Connection) -> dict:
        with conn.cursor() as cursor:
            cursor.execute("SHOW GLOBAL STATUS")
            rows = cursor.fetchall()
        return {row["Variable_name"]: row["Value"] for row in rows}

    def _status_metrics(self, status: dict, host: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        for field, metric_name in _GAUGE_STATUS_FIELDS.items():
            if field in status:
                metrics.append(self._envelope(metric_name, float(status[field]), host, {}))
        for field, metric_name in _COUNTER_STATUS_FIELDS.items():
            if field in status:
                metrics.append(
                    self._envelope(
                        metric_name, float(status[field]), host, {}, metric_type=MetricType.COUNTER
                    )
                )
        return metrics

    def _buffer_pool_metrics(self, status: dict, host: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        free = float(status.get("Innodb_buffer_pool_pages_free", 0))
        total = float(status.get("Innodb_buffer_pool_pages_total", 0)) or 1.0
        utilization = 1.0 - (free / total)
        metrics.append(self._envelope("mysql_innodb_buffer_pool_utilization", utilization, host, {}))

        read_requests = float(status.get("Innodb_buffer_pool_read_requests", 0))
        disk_reads = float(status.get("Innodb_buffer_pool_reads", 0))
        hit_ratio = 1.0 - (disk_reads / read_requests) if read_requests else 1.0
        metrics.append(self._envelope("mysql_innodb_buffer_pool_hit_ratio", hit_ratio, host, {}))
        return metrics

    def _replication_metrics(self, conn: pymysql.connections.Connection, host: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW REPLICA STATUS")
                row = cursor.fetchone()
        except pymysql.MySQLError:
            row = None

        if row:
            lag = row.get("Seconds_Behind_Source") or row.get("Seconds_Behind_Master")
            io_running = row.get("Replica_IO_Running") or row.get("Slave_IO_Running")
            sql_running = row.get("Replica_SQL_Running") or row.get("Slave_SQL_Running")
            labels = {"role": "replica"}
            metrics.append(
                self._envelope(
                    "mysql_replication_lag_seconds", float(lag or 0), host, labels
                )
            )
            metrics.append(
                self._envelope(
                    "mysql_replication_io_running",
                    1.0 if io_running == "Yes" else 0.0,
                    host,
                    labels,
                )
            )
            metrics.append(
                self._envelope(
                    "mysql_replication_sql_running",
                    1.0 if sql_running == "Yes" else 0.0,
                    host,
                    labels,
                )
            )
        return metrics

    def _envelope(self, name: str, value: float, host: str, labels: dict, metric_type=MetricType.GAUGE) -> MetricEnvelope:
        return MetricEnvelope(
            source=MetricSource.MYSQL,
            metric_name=name,
            metric_type=metric_type,
            value=value,
            fleet=self._settings.fleet_name,
            host=host,
            labels=labels,
        )
