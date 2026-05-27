"""SignalFx backend adapter — wraps existing SignalFx client and helpers."""
from __future__ import annotations

import structlog

from dashforge.backends.base import DashboardBackend, PublishResult
from dashforge.models.schemas import DashboardSpec, Intent, MetricEntry
from dashforge.signalfx.client import SignalFxClient
from dashforge.signalfx.discovery import discover_metrics as sfx_discover
from dashforge.signalfx.publisher import publish_dashboard as sfx_publish
from dashforge.validation import validate_signalflow_queries

logger = structlog.get_logger()


class SignalFxBackend:
    """Dashboard backend that talks to Splunk Observability Cloud (SignalFx)."""

    def __init__(self, client: SignalFxClient | None = None):
        self._client = client or SignalFxClient()

    # ── Protocol properties ───────────────────────────────────────────

    @property
    def name(self) -> str:
        return "signalfx"

    @property
    def query_language(self) -> str:
        return "signalflow"

    # ── Discovery ─────────────────────────────────────────────────────

    async def discover_metrics(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        try:
            return await sfx_discover(self._client, keywords)
        except Exception:
            logger.error("signalfx_discover_failed", exc_info=True)
            return []

    # ── Validation ────────────────────────────────────────────────────

    async def validate_queries(
        self,
        spec: DashboardSpec,
    ) -> tuple[DashboardSpec, list[str]]:
        return await validate_signalflow_queries(self._client, spec)

    # ── Publish ───────────────────────────────────────────────────────

    async def publish(
        self,
        spec: DashboardSpec,
    ) -> PublishResult:
        url, uid = await sfx_publish(self._client, spec)
        return PublishResult(url=url, uid=uid, backend_name="signalfx")

    # ── Cleanup ───────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._client.close()
