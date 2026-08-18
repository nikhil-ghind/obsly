"""CLI entrypoint to regenerate Prometheus rules and Grafana dashboards.

Usage:
    python -m obsly.cli.provision rules
    python -m obsly.cli.provision dashboards
    python -m obsly.cli.provision all
"""

from __future__ import annotations

import click

from obsly.logging_setup import configure_logging
from obsly.provisioning.alert_rules import write_rule_files
from obsly.provisioning.dashboard_gen import write_dashboard_files


@click.command()
@click.argument("target", type=click.Choice(["rules", "dashboards", "all"]), default="all")
def main(target: str) -> None:
    configure_logging("obsly.provision")
    if target in ("rules", "all"):
        write_rule_files()
    if target in ("dashboards", "all"):
        write_dashboard_files()


if __name__ == "__main__":
    main()
