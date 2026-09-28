"""Tests for Kafka topic naming conventions."""

from obsly.kafka.topics import all_metric_topics, dead_letter_topic, logs_topic, metrics_topic
from obsly.models.metric import MetricSource


def test_metrics_topic_uses_configured_prefix_and_source():
    topic = metrics_topic(MetricSource.KUBERNETES)
    assert topic.endswith(".metrics.kubernetes")
    assert topic.startswith("obsly")


def test_all_metric_topics_covers_every_source():
    topics = all_metric_topics()
    assert len(topics) == len(list(MetricSource))
    for source in MetricSource:
        assert metrics_topic(source) in topics


def test_logs_topic_is_distinct_from_metric_topics():
    assert logs_topic() not in all_metric_topics()


def test_dead_letter_topic_suffix():
    base = metrics_topic(MetricSource.MYSQL)
    assert dead_letter_topic(base) == f"{base}.dlq"
