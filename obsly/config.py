"""Central configuration for Obsly, loaded from environment variables.

All services (collectors, consumer, exporter, provisioning scripts) import
their settings from here instead of reading os.environ directly, so the
whole fleet stays consistent and testable.
"""

from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_KAFKA_")

    bootstrap_servers: str = Field(default="localhost:9092")
    client_id: str = Field(default="obsly")
    consumer_group: str = Field(default="obsly-consumers")
    topic_prefix: str = Field(default="obsly")
    num_partitions: int = Field(default=3)
    replication_factor: int = Field(default=1)
    acks: str = Field(default="all")
    linger_ms: int = Field(default=50)
    max_batch_size: int = Field(default=500)


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_REDIS_")

    url: str = Field(default="redis://localhost:6379/0")
    monitored_urls: List[str] = Field(default_factory=lambda: ["redis://localhost:6379/0"])
    collect_interval_seconds: int = Field(default=15)


class MySQLSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_MYSQL_")

    dsn: str = Field(default="mysql+pymysql://obsly:obsly@localhost:3306/obsly")
    monitored_dsns: List[str] = Field(
        default_factory=lambda: ["mysql+pymysql://obsly:obsly@localhost:3306/obsly"]
    )
    collect_interval_seconds: int = Field(default=15)
    slow_query_threshold_seconds: float = Field(default=1.0)


class KubernetesSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_K8S_")

    in_cluster: bool = Field(default=False)
    kubeconfig_path: str | None = Field(default=None)
    context: str | None = Field(default=None)
    namespaces: List[str] = Field(default_factory=lambda: ["default"])
    collect_interval_seconds: int = Field(default=15)


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_STORAGE_")

    database_url: str = Field(default="sqlite:///./obsly.db")
    retention_days: int = Field(default=30)
    hot_window_seconds: int = Field(default=900)


class ExporterSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_EXPORTER_")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=9464)
    refresh_interval_seconds: int = Field(default=10)


class LoggingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_LOG_")

    level: str = Field(default="INFO")
    json_output: bool = Field(default=True)
    log_dir: str = Field(default="./logs")


class AlertSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBSLY_ALERT_")

    slack_webhook_url: str | None = Field(default=None)
    pagerduty_routing_key: str | None = Field(default=None)


class Settings(BaseSettings):
    """Top-level settings aggregating every subsystem's configuration."""

    model_config = SettingsConfigDict(env_prefix="OBSLY_", env_nested_delimiter="__")

    environment: str = Field(default="development")
    fleet_name: str = Field(default="default-fleet")
    aws_region: str = Field(default="us-east-1")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    mysql: MySQLSettings = Field(default_factory=MySQLSettings)
    kubernetes: KubernetesSettings = Field(default_factory=KubernetesSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    exporter: ExporterSettings = Field(default_factory=ExporterSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    alerts: AlertSettings = Field(default_factory=AlertSettings)


@lru_cache
def get_settings() -> Settings:
    """Return process-wide cached Settings instance."""

    return Settings()
