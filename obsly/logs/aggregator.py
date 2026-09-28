"""Log aggregation: tails and normalizes host/container logs onto Kafka.

Two collection modes:
  - `tail_file`: tails a plain-text log file (e.g. a container's stdout log
    on disk) and emits one LogRecord per line
  - `tail_docker`: streams logs from a running Docker container by name via
    the Docker SDK/CLI, for hosts running the app stack under Compose

Every LogRecord is published to the shared logs topic; downstream, an
external shipper (Promtail/Fluent Bit) or Obsly's own consumer can also
read the same normalized JSON lines from the log sink file for local
debugging without Kafka.
"""

from __future__ import annotations

import logging
import subprocess
import time
from pathlib import Path
from typing import Iterator, Optional

from kafka import KafkaProducer

from obsly.config import get_settings
from obsly.kafka.topics import logs_topic
from obsly.models.metric import LogRecord

logger = logging.getLogger(__name__)


class LogAggregator:
    """Normalizes and ships log lines from files or Docker containers."""

    def __init__(self, fleet: Optional[str] = None) -> None:
        self._settings = get_settings()
        self._fleet = fleet or self._settings.fleet_name
        self._producer = KafkaProducer(
            bootstrap_servers=self._settings.kafka.bootstrap_servers,
            client_id=f"{self._settings.kafka.client_id}-logs",
            value_serializer=lambda v: v.encode("utf-8"),
        )
        log_dir = Path(self._settings.logging.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        self._sink_path = log_dir / "obsly-aggregated.log"

    def _emit(self, record: LogRecord) -> None:
        line = record.to_json_line()
        try:
            self._producer.send(logs_topic(), value=line)
        except Exception:
            logger.exception("failed to publish log record to kafka")
        with self._sink_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def tail_file(self, path: str, host: str, source: str = "file", poll_interval: float = 1.0) -> None:
        """Tail a plain-text file forever, emitting a LogRecord per new line."""

        file_path = Path(path)
        logger.info("tailing log file %s", file_path)
        with file_path.open("r", encoding="utf-8", errors="replace") as fh:
            fh.seek(0, 2)  # seek to end
            while True:
                line = fh.readline()
                if not line:
                    time.sleep(poll_interval)
                    continue
                record = LogRecord(
                    source=source,
                    host=host,
                    fleet=self._fleet,
                    message=line.rstrip("\n"),
                )
                self._emit(record)

    def tail_docker(self, container_name: str, host: str) -> None:
        """Stream `docker logs -f <container_name>` and ship each line."""

        logger.info("tailing docker container logs for %s", container_name)
        process = subprocess.Popen(
            ["docker", "logs", "-f", "--tail", "0", container_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert process.stdout is not None
        for line in process.stdout:
            record = LogRecord(
                source=f"docker:{container_name}",
                host=host,
                fleet=self._fleet,
                message=line.rstrip("\n"),
            )
            self._emit(record)

    def iter_sink_lines(self) -> Iterator[str]:
        """Read back normalized log lines from the local JSON sink file."""

        if not self._sink_path.exists():
            return iter(())
        with self._sink_path.open("r", encoding="utf-8") as fh:
            return iter(fh.readlines())
