"""Unit test verifying Milestone 0 foundation configuration and directory structure."""

import os
from pathlib import Path


def test_project_structure_and_docs():
    root = Path(__file__).parent.parent.parent
    
    # Required core documentation files
    assert (root / "SentinelStream_Master_Build_Spec_and_Agent_Loop.md").exists()
    assert (root / "README.md").exists()
    assert (root / "pyproject.toml").exists()
    assert (root / "docs" / "project_status.md").exists()
    assert (root / "docs" / "architecture.md").exists()
    assert (root / "docs" / "decisions.md").exists()
    
    # Required M0 research documents
    assert (root / "docs" / "research" / "kafka_fundamentals.md").exists()
    assert (root / "docs" / "research" / "spark_streaming_fundamentals.md").exists()
    assert (root / "docs" / "research" / "isolation_forest_anomaly_detection.md").exists()


def test_project_status_content():
    root = Path(__file__).parent.parent.parent
    status_file = root / "docs" / "project_status.md"
    content = status_file.read_text(encoding="utf-8")
    
    assert "M0" in content
    assert "SentinelStream Project Status" in content
