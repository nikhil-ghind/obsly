"""Kubernetes fleet metric collector.

Polls the Kubernetes API (via the official client) for node resource
pressure, pod restarts/pending state, and deployment health across the
configured namespaces, and turns those into MetricEnvelope readings.
"""

from __future__ import annotations

import logging
from typing import List

from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException

from obsly.collectors.base import BaseCollector
from obsly.models.metric import MetricEnvelope, MetricSource, MetricType

logger = logging.getLogger(__name__)


def _quantity_to_float(quantity: str) -> float:
    """Parse a Kubernetes resource quantity (e.g. '250m', '2Gi') to a float."""

    if quantity is None:
        return 0.0
    suffixes = {
        "m": 1e-3,
        "Ki": 2**10,
        "Mi": 2**20,
        "Gi": 2**30,
        "Ti": 2**40,
        "K": 1e3,
        "M": 1e6,
        "G": 1e9,
    }
    for suffix, multiplier in suffixes.items():
        if quantity.endswith(suffix):
            try:
                return float(quantity[: -len(suffix)]) * multiplier
            except ValueError:
                return 0.0
    try:
        return float(quantity)
    except ValueError:
        return 0.0


class KubernetesCollector(BaseCollector):
    """Collects node and workload health metrics from a Kubernetes cluster."""

    default_interval_seconds = 15

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._load_kube_config()
        self._core_v1 = client.CoreV1Api()
        self._apps_v1 = client.AppsV1Api()

    def _load_kube_config(self) -> None:
        k8s_settings = self._settings.kubernetes
        if k8s_settings.in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config(
                config_file=k8s_settings.kubeconfig_path,
                context=k8s_settings.context,
            )

    @property
    def name(self) -> str:
        return "k8s"

    def collect(self) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        metrics.extend(self._collect_node_metrics())
        for namespace in self._settings.kubernetes.namespaces:
            metrics.extend(self._collect_pod_metrics(namespace))
            metrics.extend(self._collect_deployment_metrics(namespace))
        return metrics

    def _envelope(self, name: str, value: float, host: str, labels: dict, metric_type=MetricType.GAUGE) -> MetricEnvelope:
        return MetricEnvelope(
            source=MetricSource.KUBERNETES,
            metric_name=name,
            metric_type=metric_type,
            value=value,
            fleet=self._settings.fleet_name,
            host=host,
            labels=labels,
        )

    def _collect_node_metrics(self) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        try:
            nodes = self._core_v1.list_node()
        except ApiException:
            logger.exception("failed to list nodes")
            return metrics

        for node in nodes.items:
            host = node.metadata.name
            allocatable = node.status.allocatable or {}
            capacity = node.status.capacity or {}
            metrics.append(
                self._envelope(
                    "k8s_node_cpu_allocatable_cores",
                    _quantity_to_float(allocatable.get("cpu", "0")),
                    host,
                    {},
                )
            )
            metrics.append(
                self._envelope(
                    "k8s_node_memory_allocatable_bytes",
                    _quantity_to_float(allocatable.get("memory", "0")),
                    host,
                    {},
                )
            )
            metrics.append(
                self._envelope(
                    "k8s_node_memory_capacity_bytes",
                    _quantity_to_float(capacity.get("memory", "0")),
                    host,
                    {},
                )
            )
            ready = 0.0
            for condition in node.status.conditions or []:
                if condition.type == "Ready" and condition.status == "True":
                    ready = 1.0
            metrics.append(self._envelope("k8s_node_ready", ready, host, {}))
        return metrics

    def _collect_pod_metrics(self, namespace: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        try:
            pods = self._core_v1.list_namespaced_pod(namespace=namespace)
        except ApiException:
            logger.exception("failed to list pods in namespace %s", namespace)
            return metrics

        for pod in pods.items:
            host = pod.spec.node_name or "unscheduled"
            labels = {"namespace": namespace, "pod": pod.metadata.name}
            phase_pending = 1.0 if pod.status.phase == "Pending" else 0.0
            metrics.append(self._envelope("k8s_pod_pending", phase_pending, host, labels))

            restart_count = 0
            for status in pod.status.container_statuses or []:
                restart_count += status.restart_count or 0
            metrics.append(
                self._envelope(
                    "k8s_pod_restart_count",
                    float(restart_count),
                    host,
                    labels,
                    metric_type=MetricType.COUNTER,
                )
            )
        return metrics

    def _collect_deployment_metrics(self, namespace: str) -> List[MetricEnvelope]:
        metrics: List[MetricEnvelope] = []
        try:
            deployments = self._apps_v1.list_namespaced_deployment(namespace=namespace)
        except ApiException:
            logger.exception("failed to list deployments in namespace %s", namespace)
            return metrics

        for deployment in deployments.items:
            labels = {"namespace": namespace, "deployment": deployment.metadata.name}
            desired = deployment.spec.replicas or 0
            available = deployment.status.available_replicas or 0
            metrics.append(
                self._envelope(
                    "k8s_deployment_replicas_desired",
                    float(desired),
                    "cluster",
                    labels,
                )
            )
            metrics.append(
                self._envelope(
                    "k8s_deployment_replicas_available",
                    float(available),
                    "cluster",
                    labels,
                )
            )
            unavailable = max(0, desired - available)
            metrics.append(
                self._envelope(
                    "k8s_deployment_replicas_unavailable",
                    float(unavailable),
                    "cluster",
                    labels,
                )
            )
        return metrics
