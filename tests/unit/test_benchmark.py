"""Unit tests for SentinelStream Milestone 13 — Benchmarking & Metric Reporting."""

import tempfile
from pathlib import Path
import pytest

from benchmark import PipelineBenchmarker, write_benchmark_report
from ml.inference import FraudModelPredictor
from ml.train import train_isolation_forest


def test_benchmark_batch_execution():
    """Test PipelineBenchmarker execution on small sample batch."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        predictor, _ = train_isolation_forest(
            num_train_events=150,
            num_eval_events=50,
            n_estimators=10,
            max_samples=64,
            model_version="bench_test_model",
            output_dir=tmp_dir,
        )

        benchmarker = PipelineBenchmarker(seed=42)
        res = benchmarker.benchmark_batch(batch_size=20, predictor=predictor)

        assert res["batch_size"] == 20
        assert res["throughput_events_per_sec"] >= 0.0
        assert "p50_latency_ms" in res
        assert "p95_latency_ms" in res
        assert "p99_latency_ms" in res
        assert "ram_used_mb" in res


def test_benchmark_report_generation():
    """Test generating Markdown benchmark report file."""
    sample_results = [
        {
            "batch_size": 100,
            "total_duration_sec": 1.5,
            "throughput_events_per_sec": 66.67,
            "p50_latency_ms": 12.0,
            "p95_latency_ms": 25.0,
            "p99_latency_ms": 40.0,
            "warehouse_write_duration_sec": 0.1,
            "ram_used_mb": 150.0,
        }
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        out_file = Path(tmp_dir) / "results.md"
        write_benchmark_report(sample_results, output_file=str(out_file))

        assert out_file.exists()
        content = out_file.read_text(encoding="utf-8")
        assert "SentinelStream Empirical Performance Benchmarks" in content
        assert "66.67" in content
        assert "25.000" in content
