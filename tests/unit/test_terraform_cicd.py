"""Unit tests for SentinelStream Milestone 12 — Terraform IaC & GitHub Actions CI/CD."""

from pathlib import Path
import pytest


def test_terraform_files_exist():
    """Test Terraform configuration file existence."""
    root = Path(__file__).parent.parent.parent
    tf_dir = root / "infrastructure" / "terraform"

    assert (tf_dir / "main.tf").exists()
    assert (tf_dir / "variables.tf").exists()
    assert (tf_dir / "outputs.tf").exists()
    assert (tf_dir / "providers.tf").exists()


def test_terraform_variable_definitions():
    """Test Terraform variables specification."""
    root = Path(__file__).parent.parent.parent
    var_file = root / "infrastructure" / "terraform" / "variables.tf"
    content = var_file.read_text(encoding="utf-8")

    assert 'variable "environment"' in content
    assert 'variable "cluster_node_count"' in content
    assert 'default     = 3' in content


def test_github_actions_ci_workflow():
    """Test GitHub Actions CI workflow specification."""
    root = Path(__file__).parent.parent.parent
    ci_file = root / ".github" / "workflows" / "ci.yml"
    assert ci_file.exists()

    content = ci_file.read_text(encoding="utf-8")
    assert "name: SentinelStream Continuous Integration" in content
    assert "actions/setup-python" in content
    assert "python -m pytest" in content
    assert "docker build" in content
