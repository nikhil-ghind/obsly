"""Generates Prometheus recording + alerting rules as YAML.

Rules are defined here as Python data (not hand-written YAML) so alert
thresholds stay reviewable and DRY. Run `python -m obsly.provisioning.alert_rules`
(or `obsly-provision rules`) to regenerate `deploy/prometheus/rules/*.yml`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
RULES_DIR = REPO_ROOT / "deploy" / "prometheus" / "rules"


def _rule_group(name: str, interval: str, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"groups": [{"name": name, "interval": interval, "rules": rules}]}


def kubernetes_rules() -> Dict[str, Any]:
    rules = [
        {
            "record": "fleet:k8s_node_ready:avg",
            "expr": "avg(k8s_node_ready) by (fleet)",
        },
        {
            "alert": "KubernetesNodeNotReady",
            "expr": "k8s_node_ready == 0",
            "for": "5m",
            "labels": {"severity": "critical", "team": "platform"},
            "annotations": {
                "summary": "Node {{ $labels.host }} is not Ready",
                "description": "Node {{ $labels.host }} in fleet {{ $labels.fleet }} has been NotReady for 5 minutes.",
            },
        },
        {
            "alert": "KubernetesPodCrashLooping",
            "expr": "increase(k8s_pod_restart_count[15m]) > 3",
            "for": "5m",
            "labels": {"severity": "warning", "team": "platform"},
            "annotations": {
                "summary": "Pod {{ $labels.pod }} is restarting frequently",
                "description": "Pod {{ $labels.pod }} in namespace {{ $labels.namespace }} restarted more than 3 times in 15 minutes.",
            },
        },
        {
            "alert": "KubernetesDeploymentReplicasUnavailable",
            "expr": "k8s_deployment_replicas_unavailable > 0",
            "for": "10m",
            "labels": {"severity": "warning", "team": "platform"},
            "annotations": {
                "summary": "Deployment {{ $labels.deployment }} has unavailable replicas",
                "description": "Deployment {{ $labels.deployment }} in namespace {{ $labels.namespace }} has had unavailable replicas for 10 minutes.",
            },
        },
    ]
    return _rule_group("obsly.kubernetes", "30s", rules)


def redis_rules() -> Dict[str, Any]:
    rules = [
        {
            "record": "fleet:redis_keyspace_hit_ratio:avg",
            "expr": "avg(redis_keyspace_hit_ratio) by (fleet)",
        },
        {
            "alert": "RedisMemoryNearMax",
            "expr": "redis_maxmemory_bytes > 0 and (redis_used_memory_bytes / redis_maxmemory_bytes) > 0.9",
            "for": "5m",
            "labels": {"severity": "critical", "team": "platform"},
            "annotations": {
                "summary": "Redis {{ $labels.host }} memory usage above 90%",
                "description": "Redis instance {{ $labels.host }} is using over 90% of maxmemory.",
            },
        },
        {
            "alert": "RedisLowHitRatio",
            "expr": "redis_keyspace_hit_ratio < 0.8",
            "for": "15m",
            "labels": {"severity": "warning", "team": "platform"},
            "annotations": {
                "summary": "Redis {{ $labels.host }} keyspace hit ratio degraded",
                "description": "Redis instance {{ $labels.host }} hit ratio has been below 80% for 15 minutes.",
            },
        },
        {
            "alert": "RedisReplicationDown",
            "expr": 'redis_connected_slaves == 0 and redis_master_repl_offset > 0',
            "for": "5m",
            "labels": {"severity": "critical", "team": "platform"},
            "annotations": {
                "summary": "Redis {{ $labels.host }} has no connected replicas",
                "description": "Redis master {{ $labels.host }} reports zero connected replicas.",
            },
        },
    ]
    return _rule_group("obsly.redis", "30s", rules)


def mysql_rules() -> Dict[str, Any]:
    rules = [
        {
            "record": "fleet:mysql_innodb_buffer_pool_hit_ratio:avg",
            "expr": "avg(mysql_innodb_buffer_pool_hit_ratio) by (fleet)",
        },
        {
            "alert": "MySQLReplicationLagHigh",
            "expr": "mysql_replication_lag_seconds > 30",
            "for": "5m",
            "labels": {"severity": "critical", "team": "platform"},
            "annotations": {
                "summary": "MySQL replica {{ $labels.host }} lagging",
                "description": "Replica {{ $labels.host }} is more than 30s behind its source for 5 minutes.",
            },
        },
        {
            "alert": "MySQLReplicationBroken",
            "expr": "mysql_replication_io_running == 0 or mysql_replication_sql_running == 0",
            "for": "2m",
            "labels": {"severity": "critical", "team": "platform"},
            "annotations": {
                "summary": "MySQL replication broken on {{ $labels.host }}",
                "description": "Replica IO/SQL thread is not running on {{ $labels.host }}.",
            },
        },
        {
            "alert": "MySQLTooManyConnections",
            "expr": "mysql_threads_connected > 150",
            "for": "5m",
            "labels": {"severity": "warning", "team": "platform"},
            "annotations": {
                "summary": "MySQL {{ $labels.host }} connection count high",
                "description": "MySQL instance {{ $labels.host }} has more than 150 connected threads.",
            },
        },
        {
            "alert": "MySQLBufferPoolHitRatioLow",
            "expr": "mysql_innodb_buffer_pool_hit_ratio < 0.95",
            "for": "15m",
            "labels": {"severity": "warning", "team": "platform"},
            "annotations": {
                "summary": "MySQL {{ $labels.host }} InnoDB buffer pool hit ratio degraded",
                "description": "Buffer pool hit ratio on {{ $labels.host }} has been below 95% for 15 minutes.",
            },
        },
    ]
    return _rule_group("obsly.mysql", "30s", rules)


def write_rule_files() -> List[Path]:
    RULES_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, generator in (
        ("kubernetes.yml", kubernetes_rules),
        ("redis.yml", redis_rules),
        ("mysql.yml", mysql_rules),
    ):
        path = RULES_DIR / filename
        with path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(generator(), fh, sort_keys=False, default_flow_style=False)
        written.append(path)
        logger.info("wrote %s", path)
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    write_rule_files()
