"""Kafka admin bootstrap: creates the topics Obsly needs.

Run as `python -m obsly.kafka.admin` (or via the `obsly-bootstrap` console
script) once against a fresh Kafka cluster before starting collectors,
the consumer, or the exporter.
"""

from __future__ import annotations

import logging

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from obsly.config import get_settings
from obsly.kafka.topics import all_metric_topics, dead_letter_topic, logs_topic

logger = logging.getLogger(__name__)


def required_topics() -> list[str]:
    topics = all_metric_topics() + [logs_topic()]
    topics += [dead_letter_topic(t) for t in topics]
    return topics


def bootstrap_topics() -> None:
    settings = get_settings()
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        client_id=f"{settings.kafka.client_id}-admin",
    )
    try:
        new_topics = [
            NewTopic(
                name=name,
                num_partitions=settings.kafka.num_partitions,
                replication_factor=settings.kafka.replication_factor,
            )
            for name in required_topics()
        ]
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            logger.info("created topics: %s", [t.name for t in new_topics])
        except TopicAlreadyExistsError:
            logger.info("topics already exist, skipping creation")
    finally:
        admin.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bootstrap_topics()
