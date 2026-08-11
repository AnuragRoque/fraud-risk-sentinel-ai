"""Unit tests for SentinelStream Milestone 14 — Portfolio Hardening & Final Quality Gate."""

from pathlib import Path
import pytest


def test_interview_guide_exists_and_complete():
    """Test that docs/interview.md exists and covers key interview topics."""
    root = Path(__file__).parent.parent.parent
    guide_file = root / "docs" / "interview.md"
    assert guide_file.exists()

    content = guide_file.read_text(encoding="utf-8")
    assert "Technical Interview Guide" in content
    assert "user_id" in content
    assert "Watermarking" in content
    assert "Isolation Forest" in content
    assert "Hybrid Risk Engine" in content


def test_final_quality_gate_checklist():
    """Verify final quality gate checklist items from Section 3165."""
    root = Path(__file__).parent.parent.parent

    # 1. Codebase & Modules
    assert (root / "producer" / "generator.py").exists()
    assert (root / "producer" / "scenarios.py").exists()
    assert (root / "producer" / "schemas.py").exists()
    assert (root / "producer" / "publisher.py").exists()

    assert (root / "streaming" / "pipeline.py").exists()
    assert (root / "streaming" / "consumer.py").exists()
    assert (root / "streaming" / "windows.py").exists()
    assert (root / "streaming" / "features.py").exists()

    assert (root / "fraud" / "rules.py").exists()
    assert (root / "fraud" / "scorer.py").exists()
    assert (root / "fraud" / "risk_engine.py").exists()
    assert (root / "fraud" / "explanations.py").exists()

    assert (root / "ml" / "features.py").exists()
    assert (root / "ml" / "train.py").exists()
    assert (root / "ml" / "evaluate.py").exists()
    assert (root / "ml" / "inference.py").exists()
    assert (root / "ml" / "registry.py").exists()

    assert (root / "warehouse" / "schema.sql").exists()
    assert (root / "warehouse" / "marts.sql").exists()
    assert (root / "warehouse" / "loader.py").exists()

    assert (root / "airflow" / "dags" / "daily_metrics.py").exists()
    assert (root / "airflow" / "dags" / "training.py").exists()
    assert (root / "airflow" / "dags" / "data_quality.py").exists()
    assert (root / "airflow" / "dags" / "backfill.py").exists()

    assert (root / "monitoring" / "metrics.py").exists()
    assert (root / "monitoring" / "prometheus" / "prometheus.yml").exists()

    # 2. Deployment & Infra
    assert (root / "Dockerfile").exists()
    assert (root / "docker-compose.yml").exists()
    assert (root / ".env.example").exists()
    assert (root / "deploy" / "kubernetes" / "deployment.yaml").exists()
    assert (root / "deploy" / "helm" / "sentinelstream" / "Chart.yaml").exists()
    assert (root / "infrastructure" / "terraform" / "main.tf").exists()
    assert (root / ".github" / "workflows" / "ci.yml").exists()

    # 3. Documentation & Status
    assert (root / "README.md").exists()
    assert (root / "docs" / "project_status.md").exists()
    assert (root / "docs" / "architecture.md").exists()
    assert (root / "docs" / "decisions.md").exists()
    assert (root / "docs" / "failure-modes.md").exists()
    assert (root / "docs" / "benchmarks" / "results.md").exists()
    assert (root / "benchmark.py").exists()
