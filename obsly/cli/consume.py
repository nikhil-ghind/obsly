"""CLI entrypoint for the Kafka metric consumer.

Usage:
    python -m obsly.cli.consume
"""

from __future__ import annotations

from obsly.kafka.consumer import MetricConsumer
from obsly.logging_setup import configure_logging, new_correlation_id


def main() -> None:
    configure_logging("obsly.consume")
    new_correlation_id()
    consumer = MetricConsumer()
    consumer.run_forever()


if __name__ == "__main__":
    main()
