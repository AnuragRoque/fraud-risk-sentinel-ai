"""Unit tests for SentinelStream Milestone 9 — Docker Containerization & Profiles."""

from pathlib import Path
import pytest


def test_dockerfile_structure():
    """Test Dockerfile existence and multi-stage build target definition."""
    root = Path(__file__).parent.parent.parent
    dockerfile = root / "Dockerfile"
    assert dockerfile.exists()

    content = dockerfile.read_text(encoding="utf-8")
    assert "FROM python:3.10-slim AS builder" in content
    assert "FROM python:3.10-slim AS runner" in content
    assert "EXPOSE 8000" in content


def test_env_example_configuration():
    """Test .env.example configuration key definitions."""
    root = Path(__file__).parent.parent.parent
    env_file = root / ".env.example"
    assert env_file.exists()

    content = env_file.read_text(encoding="utf-8")
    assert "KAFKA_BOOTSTRAP_SERVERS" in content
    assert "KAFKA_RAW_TOPIC" in content
    assert "MODEL_PATH" in content
    assert "RISK_HIGH_THRESHOLD" in content
    assert "PROMETHEUS_PORT" in content


def test_docker_compose_structure():
    """Test docker-compose.yml service definitions and profiles."""
    root = Path(__file__).parent.parent.parent
    compose_file = root / "docker-compose.yml"
    assert compose_file.exists()

    content = compose_file.read_text(encoding="utf-8")
    assert "zookeeper" in content
    assert "kafka" in content
    assert "prometheus" in content
    assert "grafana" in content

    # Verify profiles
    assert 'profiles: ["kafka", "streaming", "full"]' in content or 'profiles:' in content
    assert 'sentinel-network' in content
