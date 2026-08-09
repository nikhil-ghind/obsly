# Obsly - Fleet Observability Platform — Implementation Plan

## Overview

Obsly is a Python platform that ingests metrics from Kubernetes clusters, Redis,
and MySQL via Kafka streams for fleet management. It automates Grafana/Prometheus
dashboard and alert provisioning so reliable services can run unattended, and is
deployed with Docker for log aggregation and real-time monitoring across fleets
of Linux machines on AWS.

Tech: Python, Kafka, Kubernetes, Redis, MySQL, Prometheus, Grafana, Docker, AWS

## Phases

### Phase 1 — Scaffolding & shared core
Coherent, minimal skeleton that later phases build on.

Deliverables:
- Project layout (`obsly/` package), `pyproject.toml`/`requirements.txt`, `setup.cfg`
- Config loading (env-driven settings via pydantic-settings) for Kafka brokers,
  DB DSNs, Redis URL, K8s context, Prometheus pushgateway/exporter ports
- Shared data models (metric envelope schema) used across producers/consumers
- Structured logging setup (JSON logs, correlation IDs) used fleet-wide
- Kafka topic naming/config module and admin bootstrap script (topic creation)

Files touched:
- `obsly/__init__.py`, `obsly/config.py`, `obsly/logging_setup.py`
- `obsly/models/metric.py`, `obsly/kafka/topics.py`, `obsly/kafka/admin.py`
- `pyproject.toml`, `requirements.txt`, `.env.example`

### Phase 2 — Metric collectors & Kafka producers
Real collection agents for each source, publishing onto Kafka.

Deliverables:
- Kubernetes collector (via `kubernetes` client): node/pod resource usage,
  restarts, pending pods, deployment health
- Redis collector: `INFO` stats (memory, ops/sec, connected clients, keyspace,
  replication lag)
- MySQL collector: `SHOW GLOBAL STATUS`/`SHOW ENGINE INNODB STATUS` derived
  metrics (connections, slow queries, replication lag, buffer pool hit ratio)
- A common `BaseCollector`/`KafkaProducerClient` wrapper (serialization,
  retries, batching) all three collectors use
- CLI entrypoint to run a given collector on an interval

Files touched:
- `obsly/collectors/base.py`, `obsly/collectors/k8s_collector.py`,
  `obsly/collectors/redis_collector.py`, `obsly/collectors/mysql_collector.py`
- `obsly/kafka/producer.py`, `obsly/cli/collect.py`

### Phase 3 — Consumer, storage & Prometheus exporter
Consumes the Kafka streams, persists/aggregates, and exposes metrics to Prometheus.

Deliverables:
- Kafka consumer group that fans metric envelopes out by source type
- Storage layer: time-series-friendly persistence (SQLite/Postgres via
  SQLAlchemy) for historical query + Redis-backed hot-window cache
- Prometheus exporter (`prometheus_client`) exposing gauges/counters/histograms
  per fleet/host/service, refreshed from the consumer pipeline
- Log aggregation module: tails/normalizes container & host logs, ships to a
  central sink (stdout JSON + optional file sink consumable by Promtail/ELK)

Files touched:
- `obsly/kafka/consumer.py`, `obsly/storage/models.py`, `obsly/storage/repository.py`
- `obsly/exporter/prometheus_exporter.py`, `obsly/logs/aggregator.py`
- `obsly/cli/consume.py`, `obsly/cli/exporter.py`

### Phase 4 — Dashboards, alerting & orchestration as code
Automates Grafana/Prometheus so operators don't hand-build dashboards.

Deliverables:
- Prometheus scrape config + alerting rules (recording rules, alert rules for
  K8s/Redis/MySQL thresholds) generated/templated from Python
  (`obsly/provisioning/alert_rules.py` renders YAML)
- Grafana dashboards-as-code: JSON dashboard definitions (fleet overview,
  Kubernetes, Redis, MySQL) plus Grafana provisioning config (datasources,
  dashboard providers) so dashboards load automatically on container start
- Alertmanager config (routing to Slack/webhook receiver) generated the same way
- Docker Compose stack wiring Kafka (+ Zookeeper/KRaft), Prometheus, Grafana,
  Alertmanager, MySQL, Redis, and the Obsly app services together

Files touched:
- `deploy/docker-compose.yml`, `deploy/prometheus/prometheus.yml`,
  `deploy/prometheus/rules/*.yml`, `deploy/alertmanager/alertmanager.yml`
- `deploy/grafana/provisioning/datasources/*.yml`,
  `deploy/grafana/provisioning/dashboards/*.yml`,
  `deploy/grafana/dashboards/*.json`
- `obsly/provisioning/alert_rules.py`, `obsly/provisioning/dashboard_gen.py`
- `Dockerfile`

### Phase 5 — Polish: docs, gitignore, packaging
Final portfolio polish.

Deliverables:
- `.gitignore` for Python/Docker
- Real `README.md` with architecture diagram (Mermaid), run instructions, test
  instructions
- Minor cleanups: `Makefile` for common commands, unit test stubs under `tests/`
  demonstrating structure (no execution required)

Files touched:
- `.gitignore`, `README.md`, `Makefile`, `tests/`
