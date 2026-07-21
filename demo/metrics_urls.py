from __future__ import annotations
import os

"""Observability endpoint URLs for collecting metrics after an experiment."""
DEFAULT_COORDINATOR_METRICS_URL = "http://localhost:8000/metrics"
DEFAULT_PROMETHEUS_URL = "http://localhost:9090"

def coordinator_metrics_url() -> str:
    return os.getenv("COORDINATOR_METRICS_URL", DEFAULT_COORDINATOR_METRICS_URL)

def prometheus_url() -> str:
    return os.getenv("PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL)
