"""Tests for the generated Prometheus alerting rules."""

from obsly.provisioning.alert_rules import kubernetes_rules, mysql_rules, redis_rules


def _alert_names(rule_group: dict) -> set[str]:
    names = set()
    for group in rule_group["groups"]:
        for rule in group["rules"]:
            if "alert" in rule:
                names.add(rule["alert"])
    return names


def test_kubernetes_rules_cover_node_and_pod_health():
    names = _alert_names(kubernetes_rules())
    assert "KubernetesNodeNotReady" in names
    assert "KubernetesPodCrashLooping" in names


def test_redis_rules_cover_memory_and_replication():
    names = _alert_names(redis_rules())
    assert "RedisMemoryNearMax" in names
    assert "RedisReplicationDown" in names


def test_mysql_rules_cover_replication_and_connections():
    names = _alert_names(mysql_rules())
    assert "MySQLReplicationLagHigh" in names
    assert "MySQLTooManyConnections" in names


def test_every_alert_rule_has_severity_label():
    for rule_group in (kubernetes_rules(), redis_rules(), mysql_rules()):
        for group in rule_group["groups"]:
            for rule in group["rules"]:
                if "alert" in rule:
                    assert "severity" in rule["labels"]
