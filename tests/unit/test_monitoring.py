"""Unit tests for SentinelStream Milestone 8 — Observability & Prometheus Metrics."""

import pytest
from monitoring.metrics import MetricsCollector


def test_metrics_collector_counters():
    """Test metrics collector counter increments."""
    metrics = MetricsCollector()
    metrics.inc_events_produced(10)
    metrics.inc_events_consumed(8)
    metrics.inc_failed_events(2)
    metrics.inc_records_processed(8)

    assert metrics.events_produced_total == 10
    assert metrics.events_consumed_total == 8
    assert metrics.failed_events_total == 2
    assert metrics.records_processed_total == 8

    metrics.inc_risk_prediction("HIGH")
    metrics.inc_risk_prediction("HIGH")
    metrics.inc_risk_prediction("MEDIUM")
    metrics.inc_risk_prediction("LOW")

    assert metrics.high_risk_predictions_total == 2
    assert metrics.medium_risk_predictions_total == 1
    assert metrics.low_risk_predictions_total == 1


def test_latency_observation():
    """Test processing latency observation and average calculation."""
    metrics = MetricsCollector()
    assert metrics.get_avg_processing_latency_ms() == 0.0

    metrics.observe_processing_latency(10.0)
    metrics.observe_processing_latency(20.0)
    metrics.observe_processing_latency(30.0)

    assert metrics.get_avg_processing_latency_ms() == 20.0


def test_prometheus_text_format_output():
    """Test Prometheus exposition text format rendering."""
    metrics = MetricsCollector()
    metrics.inc_events_produced(15)
    metrics.inc_risk_prediction("HIGH")
    metrics.observe_processing_latency(12.5)

    prom_text = metrics.to_prometheus_text()

    assert "sentinelstream_events_produced_total 15" in prom_text
    assert "sentinelstream_processing_latency_ms 12.50" in prom_text
    assert 'sentinelstream_risk_predictions_total{level="HIGH"} 1' in prom_text
    assert "# HELP" in prom_text
    assert "# TYPE" in prom_text
