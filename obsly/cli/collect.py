"""CLI entrypoint for running a single collector on a loop.

Usage:
    python -m obsly.cli.collect --source k8s
    python -m obsly.cli.collect --source redis --interval 30
    python -m obsly.cli.collect --source mysql --once
"""

from __future__ import annotations

import click

from obsly.collectors.k8s_collector import KubernetesCollector
from obsly.collectors.mysql_collector import MySQLCollector
from obsly.collectors.redis_collector import RedisCollector
from obsly.logging_setup import configure_logging, new_correlation_id

_COLLECTORS = {
    "k8s": KubernetesCollector,
    "redis": RedisCollector,
    "mysql": MySQLCollector,
}


@click.command()
@click.option(
    "--source",
    type=click.Choice(sorted(_COLLECTORS)),
    required=True,
    help="Which fleet source to collect metrics from.",
)
@click.option("--interval", type=int, default=None, help="Polling interval in seconds.")
@click.option("--once", is_flag=True, default=False, help="Run a single collection cycle and exit.")
def main(source: str, interval: int | None, once: bool) -> None:
    configure_logging(f"obsly.collect.{source}")
    new_correlation_id()

    collector_cls = _COLLECTORS[source]
    collector = collector_cls()

    if once:
        collector.run_once()
    else:
        collector.run_forever(interval_seconds=interval)


if __name__ == "__main__":
    main()
