"""Tests for the shared MetricEnvelope schema."""

from obsly.models.metric import MetricEnvelope, MetricSource, MetricType


def make_envelope(**overrides) -> MetricEnvelope:
    defaults = dict(
        source=MetricSource.REDIS,
        metric_name="redis_used_memory_bytes",
        metric_type=MetricType.GAUGE,
        value=1024.0,
        fleet="test-fleet",
        host="redis-0",
        labels={"role": "master"},
    )
    defaults.update(overrides)
    return MetricEnvelope(**defaults)


def test_kafka_key_encodes_source_host_and_metric_name():
    envelope = make_envelope()
    assert envelope.to_kafka_key() == b"redis:redis-0:redis_used_memory_bytes"


def test_round_trip_through_kafka_serialization():
    envelope = make_envelope()
    raw = envelope.to_kafka_value()
    restored = MetricEnvelope.from_kafka_value(raw)

    assert restored.source == envelope.source
    assert restored.metric_name == envelope.metric_name
    assert restored.value == envelope.value
    assert restored.labels == envelope.labels


def test_prometheus_labels_include_fleet_host_source_and_custom_labels():
    envelope = make_envelope()
    labels = envelope.prometheus_labels()

    assert labels["fleet"] == "test-fleet"
    assert labels["host"] == "redis-0"
    assert labels["source"] == "redis"
    assert labels["role"] == "master"
