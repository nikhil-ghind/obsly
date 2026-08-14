"""SQLAlchemy models for historical metric storage.

The consumer writes every MetricEnvelope it processes into `metric_samples`
for long-term/queryable history; the Prometheus exporter reads the most
recent hot-window samples (cached in Redis, see storage/repository.py) to
serve scrapes without hammering the database.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MetricSample(Base):
    """One historical metric observation."""

    __tablename__ = "metric_samples"
    __table_args__ = (
        Index("ix_metric_samples_source_name_time", "source", "metric_name", "recorded_at"),
        Index("ix_metric_samples_fleet_host", "fleet", "host"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(128), nullable=False)
    metric_type: Mapped[str] = mapped_column(String(16), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    fleet: Mapped[str] = mapped_column(String(64), nullable=False)
    host: Mapped[str] = mapped_column(String(128), nullable=False)
    labels_json: Mapped[str] = mapped_column(String(1024), default="{}")
    recorded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
