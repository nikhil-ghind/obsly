"""Generates Grafana dashboard JSON (dashboards-as-code).

Building panels in Python keeps the dashboards consistent with the metric
names collectors actually emit, and avoids hand-editing large JSON blobs.
Run `python -m obsly.provisioning.dashboard_gen` (or `obsly-provision
dashboards`) to regenerate `deploy/grafana/dashboards/*.json`.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS_DIR = REPO_ROOT / "deploy" / "grafana" / "dashboards"

_DATASOURCE = {"type": "prometheus", "uid": "obsly-prometheus"}


def _panel(
    panel_id: int,
    title: str,
    expr: str,
    x: int,
    y: int,
    w: int = 12,
    h: int = 8,
    unit: str = "short",
    panel_type: str = "timeseries",
) -> Dict[str, Any]:
    return {
        "id": panel_id,
        "type": panel_type,
        "title": title,
        "datasource": _DATASOURCE,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "targets": [{"expr": expr, "refId": "A", "datasource": _DATASOURCE}],
    }


def _dashboard(uid: str, title: str, tags: List[str], panels: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "tags": tags,
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "30s",
        "time": {"from": "now-3h", "to": "now"},
        "panels": panels,
    }


def fleet_overview_dashboard() -> Dict[str, Any]:
    panels = [
        _panel(1, "Nodes Ready (avg)", "fleet:k8s_node_ready:avg", 0, 0, unit="percentunit"),
        _panel(2, "Redis Hit Ratio (avg)", "fleet:redis_keyspace_hit_ratio:avg", 12, 0, unit="percentunit"),
        _panel(3, "MySQL Buffer Pool Hit Ratio (avg)", "fleet:mysql_innodb_buffer_pool_hit_ratio:avg", 0, 8, unit="percentunit"),
        _panel(4, "Active Alerts", "ALERTS{alertstate=\"firing\"}", 12, 8, panel_type="table", unit="short"),
    ]
    return _dashboard("obsly-fleet-overview", "Obsly / Fleet Overview", ["obsly", "fleet"], panels)


def kubernetes_dashboard() -> Dict[str, Any]:
    panels = [
        _panel(1, "Node Ready", "k8s_node_ready", 0, 0, unit="short"),
        _panel(2, "Node CPU Allocatable (cores)", "k8s_node_cpu_allocatable_cores", 12, 0, unit="short"),
        _panel(3, "Node Memory Allocatable (bytes)", "k8s_node_memory_allocatable_bytes", 0, 8, unit="bytes"),
        _panel(4, "Pod Restarts (rate 15m)", "increase(k8s_pod_restart_count[15m])", 12, 8, unit="short"),
        _panel(5, "Pods Pending", "sum(k8s_pod_pending) by (namespace)", 0, 16, unit="short"),
        _panel(6, "Deployment Unavailable Replicas", "k8s_deployment_replicas_unavailable", 12, 16, unit="short"),
    ]
    return _dashboard("obsly-kubernetes", "Obsly / Kubernetes", ["obsly", "kubernetes"], panels)


def redis_dashboard() -> Dict[str, Any]:
    panels = [
        _panel(1, "Used Memory (bytes)", "redis_used_memory_bytes", 0, 0, unit="bytes"),
        _panel(2, "Ops / sec", "redis_instantaneous_ops_per_sec", 12, 0, unit="ops"),
        _panel(3, "Keyspace Hit Ratio", "redis_keyspace_hit_ratio", 0, 8, unit="percentunit"),
        _panel(4, "Connected Clients", "redis_connected_clients", 12, 8, unit="short"),
        _panel(5, "Connected Slaves", "redis_connected_slaves", 0, 16, unit="short"),
        _panel(6, "Evicted Keys (rate)", "rate(redis_evicted_keys_total[5m])", 12, 16, unit="short"),
    ]
    return _dashboard("obsly-redis", "Obsly / Redis", ["obsly", "redis"], panels)


def mysql_dashboard() -> Dict[str, Any]:
    panels = [
        _panel(1, "Threads Connected", "mysql_threads_connected", 0, 0, unit="short"),
        _panel(2, "Slow Queries (rate)", "rate(mysql_slow_queries_total[5m])", 12, 0, unit="short"),
        _panel(3, "InnoDB Buffer Pool Utilization", "mysql_innodb_buffer_pool_utilization", 0, 8, unit="percentunit"),
        _panel(4, "InnoDB Buffer Pool Hit Ratio", "mysql_innodb_buffer_pool_hit_ratio", 12, 8, unit="percentunit"),
        _panel(5, "Replication Lag (seconds)", "mysql_replication_lag_seconds", 0, 16, unit="s"),
        _panel(6, "Queries / sec (rate)", "rate(mysql_questions_total[5m])", 12, 16, unit="qps"),
    ]
    return _dashboard("obsly-mysql", "Obsly / MySQL", ["obsly", "mysql"], panels)


def write_dashboard_files() -> List[Path]:
    DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, generator in (
        ("fleet_overview.json", fleet_overview_dashboard),
        ("kubernetes.json", kubernetes_dashboard),
        ("redis.json", redis_dashboard),
        ("mysql.json", mysql_dashboard),
    ):
        path = DASHBOARDS_DIR / filename
        with path.open("w", encoding="utf-8") as fh:
            json.dump(generator(), fh, indent=2)
            fh.write("\n")
        written.append(path)
        logger.info("wrote %s", path)
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    write_dashboard_files()
