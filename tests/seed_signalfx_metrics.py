#!/usr/bin/env python3
"""Seed SignalFx with all metrics used in DashForge archetypes + keyword map.

Extracts every metric name from archetypes.yaml PromQL expressions and the
KEYWORD_METRIC_MAP, then ingests realistic dummy datapoints so you can test
the full DashForge → SignalFx pipeline with real prompts.

Usage:
    SIGNALFX_INGEST_TOKEN=<token> python scripts/seed_signalfx_metrics.py

Requires SIGNALFX_API_TOKEN in config for realm lookup, and
SIGNALFX_INGEST_TOKEN env var for ingestion.
"""

from __future__ import annotations

import math
import os
import random
import re
import sys
import time
from importlib.resources import files

import httpx
import yaml

sys.path.insert(0, ".")

from dashforge.config import settings
from dashforge.grafana.adapters.signalfx import KEYWORD_METRIC_MAP

# ── Helpers ──────────────────────────────────────────────────────────────────


def _ok(msg: str) -> None:
    print(f"  \033[92m✔\033[0m {msg}")


def _fail(msg: str) -> None:
    print(f"  \033[91m✘\033[0m {msg}")


def _info(msg: str) -> None:
    print(f"  \033[90m→\033[0m {msg}")


def _header(msg: str) -> None:
    print(f"\n\033[1;34m{'─' * 60}\033[0m")
    print(f"\033[1;34m  {msg}\033[0m")
    print(f"\033[1;34m{'─' * 60}\033[0m")


# ── Extract metric names from archetypes ─────────────────────────────────────

_PROMQL_METRIC_RE = re.compile(
    r"""(?:^|[(\s,])                    # start of string or opening paren/whitespace
        ([a-zA-Z_:][a-zA-Z0-9_:]+)     # metric name
        \s*(?:\{|\[|$)                  # followed by { or [ or end
    """,
    re.VERBOSE,
)

# PromQL functions/keywords to exclude from metric extraction
_PROMQL_FUNCS = {
    "sum",
    "rate",
    "increase",
    "histogram_quantile",
    "count",
    "avg",
    "min",
    "max",
    "topk",
    "bottomk",
    "by",
    "without",
    "offset",
    "on",
    "ignoring",
    "group_left",
    "group_right",
    "le",
    "time",
    "job",
    "status",
    "method",
    "path",
    "container",
    "pod",
    "namespace",
    "reason",
    "phase",
    "type",
    "condition",
    "node",
    "rcode",
    "state",
    "resource",
    "cpu",
    "fstype",
    "mountpoint",
    "gpu",
    "device",
    "common_name",
}


def extract_metrics_from_archetypes() -> set[str]:
    """Parse archetypes.yaml and extract all metric names from PromQL expressions."""
    resource = files("dashforge.data").joinpath("archetypes.yaml")
    with resource.open() as f:
        data = yaml.safe_load(f) or {}

    metrics: set[str] = set()
    for arch in data.get("archetypes", []):
        # required_metrics
        for m in arch.get("required_metrics", []):
            metrics.add(m)
        # panel query expressions
        for panel in arch.get("panels", []):
            for q in panel.get("queries", []):
                expr = q.get("expr", "")
                # Remove template placeholders
                expr = re.sub(r"\{[^}]*\}", "", expr)
                for match in _PROMQL_METRIC_RE.findall(expr):
                    if match.lower() not in _PROMQL_FUNCS and not match.startswith("0"):
                        metrics.add(match)
    return metrics


def extract_metrics_from_keyword_map() -> set[str]:
    """Get all metric prefixes from KEYWORD_METRIC_MAP."""
    metrics: set[str] = set()
    for prefixes in KEYWORD_METRIC_MAP.values():
        for p in prefixes:
            # Some entries are prefixes with dots (e.g. "redis." "messaging.")
            # Make them concrete metric names
            name = p.rstrip(".")
            if name:
                metrics.add(name)
    return metrics


# ── Dimension + value templates for realistic data ───────────────────────────

SERVICES = ["checkout", "payment", "catalog", "gateway", "auth", "user-service"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
STATUSES = ["200", "201", "400", "404", "500", "502", "503"]
PODS = ["checkout-7f8b9-abc12", "payment-6d4e2-def34", "catalog-9a1c3-ghi56"]
NODES = ["node-01", "node-02", "node-03"]
CONTAINERS = ["checkout", "payment", "catalog", "gateway"]
ENDPOINTS = ["/api/orders", "/api/payments", "/api/products", "/api/auth", "/health"]
QUEUES = ["order-processing", "payment-events", "email-notifications"]
DEVICES = ["sda", "sdb", "nvme0n1"]
GPUS = ["gpu-0", "gpu-1"]


def _dims_for_metric(metric: str) -> list[dict[str, str]]:
    """Generate 2-4 dimension combos for a metric based on its name."""
    dims_list = []
    base = {"service": random.choice(SERVICES)}

    if any(k in metric for k in ["http_request", "http_server", "request_duration", "request_count"]):
        for svc in SERVICES[:3]:
            for method in METHODS[:2]:
                dims_list.append({"service": svc, "method": method, "endpoint": random.choice(ENDPOINTS)})
    elif any(k in metric for k in ["container_", "kube_pod"]):
        for pod in PODS:
            dims_list.append({"pod": pod, "container": pod.split("-")[0], "namespace": "production"})
    elif any(k in metric for k in ["node_", "kube_node"]):
        for node in NODES:
            dims_list.append({"node": node})
    elif "redis" in metric:
        dims_list.append({"service": "redis-primary", "instance": "redis-001"})
        dims_list.append({"service": "redis-replica", "instance": "redis-002"})
    elif any(k in metric for k in ["kafka", "queue", "messaging"]):
        for q in QUEUES:
            dims_list.append({"queue": q, "service": random.choice(SERVICES[:3]), "consumer_group": f"cg-{q}"})
    elif "db_" in metric:
        dims_list.append({"service": "checkout", "db": "postgres-primary"})
        dims_list.append({"service": "payment", "db": "postgres-primary"})
    elif "gpu" in metric or "inference" in metric:
        for g in GPUS:
            dims_list.append({"gpu": g, "service": "ml-inference", "model": "recommendation-v2"})
    elif "dns" in metric or "coredns" in metric:
        dims_list.append({"type": "A", "service": "coredns"})
        dims_list.append({"type": "AAAA", "service": "coredns"})
    elif "istio" in metric:
        for svc in SERVICES[:3]:
            dims_list.append({"service": svc, "destination_service": random.choice(SERVICES)})
    elif "nginx" in metric or "ingress" in metric:
        for status in ["200", "404", "502"]:
            dims_list.append({"status": status, "ingress": "main-ingress"})
    elif "tls" in metric or "certificate" in metric:
        dims_list.append({"common_name": "*.example.com", "service": "gateway"})
    elif "process_" in metric or "thread" in metric:
        for svc in SERVICES[:3]:
            dims_list.append({"service": svc})
    else:
        # Generic: 2 service variants
        for svc in SERVICES[:2]:
            dims_list.append({"service": svc})

    return dims_list if dims_list else [base]


def _value_for_metric(metric: str, t_offset: int) -> float:
    """Generate a realistic-ish value based on metric name."""
    # Add some time-based variance
    wave = math.sin(t_offset / 5.0) * 0.15 + 1.0
    noise = random.uniform(0.9, 1.1)

    if "duration" in metric or "latency" in metric:
        return round(random.uniform(0.01, 0.5) * wave * noise, 4)
    elif "bytes" in metric or "memory" in metric:
        return round(random.uniform(500_000_000, 4_000_000_000) * noise)
    elif "percent" in metric or "utilization" in metric:
        return round(random.uniform(20, 85) * wave * noise, 1)
    elif "total" in metric or "count" in metric:
        return round(random.uniform(100, 50000) * noise)
    elif "ratio" in metric or "rate" in metric:
        return round(random.uniform(0.001, 0.08) * wave * noise, 4)
    elif "depth" in metric or "queue" in metric or "flight" in metric:
        return round(random.uniform(0, 200) * wave * noise)
    elif "thread" in metric or "client" in metric or "connection" in metric:
        return round(random.uniform(5, 500) * noise)
    elif "replica" in metric:
        return round(random.uniform(2, 10))
    elif "restart" in metric:
        return round(random.uniform(0, 3) * noise)
    elif "expiry" in metric or "timestamp" in metric:
        return time.time() + random.uniform(86400, 2592000)  # 1-30 days out
    elif "gpu" in metric:
        return round(random.uniform(30, 95) * wave * noise, 1)
    else:
        return round(random.uniform(1, 1000) * noise, 2)


# ── Ingest ───────────────────────────────────────────────────────────────────


def build_payload(metrics: set[str], num_points: int = 20) -> dict:
    """Build a SignalFx ingest payload with gauge datapoints for all metrics."""
    gauges = []
    now_ms = int(time.time() * 1000)

    for metric in sorted(metrics):
        dim_combos = _dims_for_metric(metric)
        for dims in dim_combos:
            for i in range(num_points):
                ts = now_ms - (num_points - i) * 10_000  # 10s apart
                gauges.append(
                    {
                        "metric": metric,
                        "value": _value_for_metric(metric, i),
                        "dimensions": dims,
                        "timestamp": ts,
                    }
                )

    return {"gauge": gauges}


def ingest_batch(realm: str, token: str, payload: dict, batch_label: str) -> bool:
    """Send a batch of datapoints to the ingest endpoint."""
    url = f"https://ingest.{realm}.signalfx.com/v2/datapoint"
    headers = {"X-SF-TOKEN": token, "Content-Type": "application/json"}

    resp = httpx.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code == 200:
        _ok(f"{batch_label}: {len(payload.get('gauge', []))} datapoints ingested")
        return True
    else:
        _fail(f"{batch_label}: HTTP {resp.status_code} — {resp.text[:200]}")
        return False


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    realm = settings.signalfx_realm
    ingest_token = os.environ.get("SIGNALFX_INGEST_TOKEN", "").strip()

    if not ingest_token:
        _fail("SIGNALFX_INGEST_TOKEN not set")
        _info("Create an ingest token: Splunk Observability → Settings → Access Tokens")
        sys.exit(1)

    _header("DashForge → SignalFx Metric Seeder")
    _info(f"Realm: {realm}")
    _info(f"Ingest token: {ingest_token[:8]}...{ingest_token[-4:]}")

    # 1. Extract metrics
    _header("Step 1: Extract Metrics")

    arch_metrics = extract_metrics_from_archetypes()
    _ok(f"Archetypes: {len(arch_metrics)} unique metrics")

    kw_metrics = extract_metrics_from_keyword_map()
    _ok(f"Keyword map: {len(kw_metrics)} metric prefixes")

    all_metrics = arch_metrics | kw_metrics
    _ok(f"Total unique: {len(all_metrics)} metrics")

    # Print them grouped
    for m in sorted(all_metrics):
        _info(f"  {m}")

    # 2. Build payloads
    _header("Step 2: Build Datapoints")
    full_payload = build_payload(all_metrics, num_points=20)
    total_dps = len(full_payload.get("gauge", []))
    _ok(f"Generated {total_dps} datapoints ({len(all_metrics)} metrics × dimensions × 20 points)")

    # 3. Ingest in batches (SignalFx has a ~100K dp limit per request)
    _header("Step 3: Ingest")
    gauges = full_payload["gauge"]
    batch_size = 10_000
    batches = [gauges[i : i + batch_size] for i in range(0, len(gauges), batch_size)]

    success_count = 0
    for idx, batch in enumerate(batches):
        ok = ingest_batch(realm, ingest_token, {"gauge": batch}, f"Batch {idx + 1}/{len(batches)}")
        if ok:
            success_count += 1

    # 4. Summary
    _header("Summary")
    _ok(f"Metrics: {len(all_metrics)}")
    _ok(f"Datapoints: {total_dps}")
    _ok(f"Batches: {success_count}/{len(batches)} succeeded")

    if success_count == len(batches):
        _ok("All ingested! Metrics should be searchable in ~30-60 seconds.")
        _info("Run the integration test: python tests/live/signalfx_integration.py")
        _info("Or try a prompt: 'high latency on the checkout service'")
    else:
        _fail(f"{len(batches) - success_count} batches failed")

    print()


if __name__ == "__main__":
    main()
