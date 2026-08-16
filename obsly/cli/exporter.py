"""CLI entrypoint for the Prometheus exporter HTTP server.

Usage:
    python -m obsly.cli.exporter
"""

from __future__ import annotations

from obsly.exporter.prometheus_exporter import PrometheusExporter
from obsly.logging_setup import configure_logging, new_correlation_id


def main() -> None:
    configure_logging("obsly.exporter")
    new_correlation_id()
    exporter = PrometheusExporter()
    exporter.start()


if __name__ == "__main__":
    main()
