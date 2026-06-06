"""End-to-end SignalFx integration test.

Usage:
    # Ensure SIGNALFX_API_TOKEN is set and signalfx_realm is configured.
    # For metric ingestion, also set SIGNALFX_INGEST_TOKEN (a separate
    # token with ingest scope from Settings → Access Tokens).
    python tests/live/signalfx_integration.py

Steps:
1. Ingest dummy metrics via the SignalFx ingest API
2. Wait for metrics to be indexed
3. Test discovery (search + dimension fetch)
4. Test publishing (chart + dashboard creation)
5. Clean up created resources
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx
import pytest
import structlog

# Ensure dashforge is importable
sys.path.insert(0, ".")

from dashforge.config import settings
from dashforge.models.schemas import DashboardSpec, PanelQuery, PanelSpec
from dashforge.signalfx.client import SignalFxClient
from dashforge.signalfx.discovery import discover_metrics
from dashforge.signalfx.publisher import publish_dashboard

logger = structlog.get_logger()


@pytest.fixture
async def client():
    """Provide a SignalFxClient for integration tests, skip if not configured."""
    token = settings.signalfx_api_token
    if not token:
        pytest.skip("SIGNALFX_API_TOKEN not set — skipping SignalFx integration tests")
    c = SignalFxClient()
    yield c
    await c.close()


# ── Dummy metrics to ingest ──────────────────────────────────────────────────

DUMMY_METRICS = {
    "dashforge.test.http_request_duration_seconds": {
        "type": "gauge",
        "dimensions": {"service": "checkout", "method": "GET", "endpoint": "/api/orders"},
        "values": [0.045, 0.052, 0.048, 0.12, 0.063, 0.055],
    },
    "dashforge.test.http_requests_total": {
        "type": "counter",
        "dimensions": {"service": "checkout", "method": "GET", "status_code": "200"},
        "values": [100, 150, 200, 250, 300, 350],
    },
    "dashforge.test.cpu_utilization_percent": {
        "type": "gauge",
        "dimensions": {"host": "web-01", "service": "checkout"},
        "values": [45.2, 52.1, 48.7, 67.3, 55.0, 61.2],
    },
    "dashforge.test.memory_used_bytes": {
        "type": "gauge",
        "dimensions": {"host": "web-01", "service": "checkout"},
        "values": [2_147_483_648, 2_200_000_000, 2_300_000_000, 2_250_000_000],
    },
    "dashforge.test.error_rate": {
        "type": "gauge",
        "dimensions": {"service": "checkout", "error_type": "timeout"},
        "values": [0.02, 0.03, 0.05, 0.08, 0.04, 0.03],
    },
    "dashforge.test.queue_depth": {
        "type": "gauge",
        "dimensions": {"queue": "order-processing", "service": "checkout"},
        "values": [12, 18, 25, 42, 30, 15],
    },
}


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


# ── Step 1: Ingest dummy metrics ─────────────────────────────────────────────


def ingest_dummy_metrics(realm: str, token: str) -> bool:
    """Send dummy datapoints to the SignalFx ingest endpoint."""
    ingest_url = f"https://ingest.{realm}.signalfx.com/v2/datapoint"
    headers = {"X-SF-TOKEN": token, "Content-Type": "application/json"}

    # Build payload — SignalFx expects {"gauge": [...], "counter": [...]}
    payload: dict[str, list] = {"gauge": [], "counter": []}

    now_ms = int(time.time() * 1000)

    for metric_name, spec in DUMMY_METRICS.items():
        metric_type = spec["type"]
        dims = spec["dimensions"]
        values = spec["values"]

        for i, val in enumerate(values):
            dp = {
                "metric": metric_name,
                "value": val,
                "dimensions": dims,
                "timestamp": now_ms - (len(values) - i) * 10_000,  # 10s apart
            }
            payload[metric_type].append(dp)

    # Remove empty keys
    payload = {k: v for k, v in payload.items() if v}

    total_dps = sum(len(v) for v in payload.values())
    _info(f"Sending {total_dps} datapoints for {len(DUMMY_METRICS)} metrics...")

    resp = httpx.post(ingest_url, headers=headers, json=payload, timeout=15)
    if resp.status_code == 200:
        _ok(f"Ingested {total_dps} datapoints (HTTP {resp.status_code})")
        return True
    else:
        _fail(f"Ingest failed: HTTP {resp.status_code} — {resp.text[:200]}")
        return False


# ── Step 2: Wait for indexing ────────────────────────────────────────────────


def wait_for_indexing(realm: str, token: str, max_wait: int = 60) -> bool:
    """Poll the metric API until our test metrics are discoverable."""
    api_url = f"https://api.{realm}.signalfx.com/v2/metric"
    headers = {"X-SF-TOKEN": token}
    target = "dashforge.test.http_request_duration_seconds"

    _info(f"Waiting for metrics to be indexed (up to {max_wait}s)...")
    start = time.time()
    while time.time() - start < max_wait:
        resp = httpx.get(
            api_url,
            headers=headers,
            params={"query": f"name:{target}", "limit": 1},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            count = data.get("count", 0)
            if count > 0:
                elapsed = round(time.time() - start, 1)
                _ok(f"Metric '{target}' indexed after {elapsed}s")
                return True
        time.sleep(5)

    _fail(f"Metrics not indexed after {max_wait}s — they may appear later")
    return False


# ── Step 3: Test discovery ───────────────────────────────────────────────────


async def test_discovery(client: SignalFxClient) -> list:
    """Test direct metric discovery using our test keywords."""
    keywords = ["http", "cpu", "error", "queue", "dashforge.test"]
    entries = await discover_metrics(client, keywords)

    test_metrics = [e for e in entries if e.name.startswith("dashforge.test.")]

    if test_metrics:
        _ok(f"Discovery found {len(entries)} total metrics, {len(test_metrics)} test metrics:")
        for m in test_metrics:
            dims_str = f" dims=[{', '.join(m.dimensions[:3])}]" if m.dimensions else ""
            _info(f"  {m.name}{dims_str}")
    else:
        _fail(f"Discovery found {len(entries)} metrics but none matching 'dashforge.test.*'")

    return entries


# ── Step 4: Test publishing ──────────────────────────────────────────────────


async def test_publishing(client: SignalFxClient) -> tuple[str, str]:
    """Test chart + dashboard creation with a real DashboardSpec."""
    spec = DashboardSpec(
        title="DashForge Integration Test",
        description="Auto-generated test dashboard for SignalFx integration verification",
        timerange="1h",
        tags=["dashforge-test", "integration-test"],
        panels=[
            PanelSpec(
                title="Request Duration (P99)",
                description="HTTP request latency",
                panel_type="timeseries",
                unit="s",
                queries=[
                    PanelQuery(
                        expr="data('dashforge.test.http_request_duration_seconds', "
                        "filter=filter('service', 'checkout')).percentile(pct=99).publish(label='P99')",
                        legend_format="P99",
                        datasource_uid="signalfx-direct",
                        datasource_type="signalfx",
                    )
                ],
                row="Latency",
            ),
            PanelSpec(
                title="Request Rate",
                description="HTTP requests per second",
                panel_type="timeseries",
                unit="reqps",
                queries=[
                    PanelQuery(
                        expr="data('dashforge.test.http_requests_total', "
                        "filter=filter('service', 'checkout')).sum().publish(label='RPS')",
                        legend_format="RPS",
                        datasource_uid="signalfx-direct",
                        datasource_type="signalfx",
                    )
                ],
                row="Latency",
            ),
            PanelSpec(
                title="CPU Utilization",
                description="Host CPU usage percentage",
                panel_type="timeseries",
                unit="percent",
                queries=[
                    PanelQuery(
                        expr="data('dashforge.test.cpu_utilization_percent').mean().publish(label='CPU %')",
                        legend_format="CPU %",
                        datasource_uid="signalfx-direct",
                        datasource_type="signalfx",
                    )
                ],
                row="Resources",
            ),
            PanelSpec(
                title="Error Rate",
                description="Error rate by service",
                panel_type="stat",
                unit="percentunit",
                queries=[
                    PanelQuery(
                        expr="data('dashforge.test.error_rate', "
                        "filter=filter('service', 'checkout')).mean().publish(label='Error Rate')",
                        legend_format="Error Rate",
                        datasource_uid="signalfx-direct",
                        datasource_type="signalfx",
                    )
                ],
                row="Resources",
            ),
        ],
    )

    url, dashboard_id = await publish_dashboard(client, spec, group_name="DashForge Test")

    if url and dashboard_id:
        _ok(f"Dashboard created: {dashboard_id}")
        _ok(f"URL: {url}")
    else:
        _fail("Dashboard creation failed — check logs above")

    return url, dashboard_id


# ── Step 5: Cleanup ──────────────────────────────────────────────────────────


async def cleanup(client: SignalFxClient, dashboard_id: str) -> None:
    """Delete the test dashboard (charts remain for inspection)."""
    if not dashboard_id:
        return

    try:
        resp = await client._client.delete(f"/v2/dashboard/{dashboard_id}")
        if resp.status_code in (200, 204):
            _ok(f"Deleted test dashboard {dashboard_id}")
        else:
            _fail(f"Cleanup: HTTP {resp.status_code}")
    except Exception as e:
        _fail(f"Cleanup failed: {e}")


# ── Main ─────────────────────────────────────────────────────────────────────


async def main():
    api_token = settings.signalfx_api_token
    ingest_token = os.environ.get("SIGNALFX_INGEST_TOKEN", "").strip()
    realm = settings.signalfx_realm

    if not api_token:
        _fail("SIGNALFX_API_TOKEN not set. Configure it in ~/.dashforge/.env")
        sys.exit(1)

    _header("SignalFx Integration Test")
    _info(f"Realm: {realm}")
    _info(f"API token: {api_token[:8]}...{api_token[-4:]}")
    if ingest_token:
        _info(f"Ingest token: {ingest_token[:8]}...{ingest_token[-4:]}")
    else:
        _info("No SIGNALFX_INGEST_TOKEN set — will try API token for ingest")

    # Step 1: Ingest
    _header("Step 1: Ingest Dummy Metrics")
    effective_ingest_token = ingest_token or api_token
    if not ingest_dummy_metrics(realm, effective_ingest_token):
        if not ingest_token:
            _info("Hint: set SIGNALFX_INGEST_TOKEN with a token that has ingest scope")
        _fail("Aborting — ingest failed")
        sys.exit(1)

    # Step 2: Wait for indexing
    _header("Step 2: Wait for Metric Indexing")
    indexed = wait_for_indexing(realm, api_token, max_wait=90)

    # Step 3: Discovery
    _header("Step 3: Test Discovery")
    client = SignalFxClient()
    try:
        if indexed:
            entries = await test_discovery(client)
        else:
            _info("Skipping discovery — metrics not yet indexed (try again in a minute)")
            entries = []

        # Step 4: Publishing (works regardless of metric indexing)
        _header("Step 4: Test Dashboard Publishing")
        url, dashboard_id = await test_publishing(client)

        # Step 5: Ask about cleanup
        _header("Step 5: Cleanup")
        if dashboard_id:
            _info(f"Test dashboard: {url}")
            answer = input("  Delete the test dashboard? [y/N] ").strip().lower()
            if answer == "y":
                await cleanup(client, dashboard_id)
            else:
                _info("Keeping test dashboard for inspection")
    finally:
        await client.close()

    # Summary
    _header("Summary")
    _ok("Ingest: OK")
    _ok(f"Indexing: {'OK' if indexed else 'pending (retry in ~60s)'}")
    if entries:
        _ok(f"Discovery: {len(entries)} metrics found")
    if url:
        _ok(f"Publishing: {url}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
