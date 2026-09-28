"""Structured logging setup shared by every Obsly process.

Emits JSON logs (or plain text in dev) with a correlation id so log lines
from collectors, the consumer, and the exporter can be aggregated and
correlated downstream (e.g. by Promtail/ELK).
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import time
import uuid
from typing import Any, Dict

from obsly.config import get_settings

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def new_correlation_id() -> str:
    cid = uuid.uuid4().hex[:16]
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> str:
    return _correlation_id.get()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_correlation_id(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        return json.dumps(payload)


def configure_logging(service_name: str) -> logging.Logger:
    """Configure root logging for a service and return its logger.

    Every entrypoint (collector CLI, consumer, exporter) calls this once at
    startup so log formatting/level are consistent across the fleet.
    """

    settings = get_settings()
    root = logging.getLogger()
    root.setLevel(settings.logging.level.upper())

    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    if settings.logging.json_output:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
    root.addHandler(handler)

    logger = logging.getLogger(service_name)
    logger.info("logging configured", extra={"extra_fields": {"service": service_name}})
    return logger
