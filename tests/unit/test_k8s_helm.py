"""Unit tests for SentinelStream Milestone 11 — Kubernetes Manifests & Helm Charts."""

from pathlib import Path
import pytest


def test_k8s_manifests_exist():
    """Test that all required Kubernetes manifests exist in deploy/kubernetes."""
    root = Path(__file__).parent.parent.parent
    k8s_dir = root / "deploy" / "kubernetes"

    assert (k8s_dir / "deployment.yaml").exists()
    assert (k8s_dir / "service.yaml").exists()
    assert (k8s_dir / "configmap.yaml").exists()
    assert (k8s_dir / "secret.yaml").exists()
    assert (k8s_dir / "hpa.yaml").exists()


def test_k8s_deployment_specification():
    """Test deployment manifest for resource limits, probes, and replica count."""
    root = Path(__file__).parent.parent.parent
    deploy_file = root / "deploy" / "kubernetes" / "deployment.yaml"
    content = deploy_file.read_text(encoding="utf-8")

    assert "replicas: 2" in content
    assert "cpu: 250m" in content
    assert "memory: 512Mi" in content
    assert "livenessProbe:" in content
    assert "readinessProbe:" in content
    assert "containerPort: 8000" in content


def test_helm_chart_structure():
    """Test Helm Chart.yaml and values.yaml definitions."""
    root = Path(__file__).parent.parent.parent
    helm_dir = root / "deploy" / "helm" / "sentinelstream"

    chart_file = helm_dir / "Chart.yaml"
    values_file = helm_dir / "values.yaml"

    assert chart_file.exists()
    assert values_file.exists()

    chart_content = chart_file.read_text(encoding="utf-8")
    values_content = values_file.read_text(encoding="utf-8")

    assert "name: sentinelstream" in chart_content
    assert "version: 0.1.0" in chart_content
    assert "replicaCount: 2" in values_content
    assert "kafkaBootstrapServers:" in values_content
