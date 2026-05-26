"""Async HTTP client for the Splunk SignalFx (Observability Cloud) REST API.

Base URL: https://api.{realm}.signalfx.com
Auth: X-SF-TOKEN header

Docs: https://dev.splunk.com/observability/reference
"""
from __future__ import annotations

from typing import Any

import httpx
import structlog

from dashforge.config import settings

logger = structlog.get_logger()


class SignalFxClient:
    """Thin async wrapper around the SignalFx v2 REST API."""

    def __init__(
        self,
        api_token: str | None = None,
        realm: str | None = None,
    ):
        self.api_token = api_token or settings.signalfx_api_token
        self.realm = realm or settings.signalfx_realm
        base_url = f"https://api.{self.realm}.signalfx.com"

        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={
                "X-SF-TOKEN": self.api_token,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    # ── Low-level helpers ────────────────────────────────────────────────

    async def _get(self, path: str, params: dict | None = None) -> dict | list:
        resp = await self._client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, json: dict | list | None = None) -> dict:
        resp = await self._client.post(path, json=json)
        if resp.status_code >= 400:
            logger.error("signalfx_api_error", method="POST", path=path,
                         status=resp.status_code, body=resp.text[:500])
        resp.raise_for_status()
        return resp.json()

    async def _put(self, path: str, json: dict | None = None) -> dict:
        resp = await self._client.put(path, json=json)
        if resp.status_code >= 400:
            logger.error("signalfx_api_error", method="PUT", path=path,
                         status=resp.status_code, body=resp.text[:500])
        resp.raise_for_status()
        return resp.json()

    async def _delete(self, path: str) -> None:
        resp = await self._client.delete(path)
        resp.raise_for_status()

    # ── Metrics ──────────────────────────────────────────────────────────

    async def search_metrics(
        self,
        query: str = "*",
        limit: int = 200,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Search metrics. Returns {count, results: [{name, description, type, ...}]}."""
        return await self._get("/v2/metric", params={
            "query": query,
            "limit": limit,
            "offset": offset,
        })

    async def get_metric(self, metric_name: str) -> dict[str, Any]:
        """Get metadata for a single metric."""
        return await self._get(f"/v2/metric/{metric_name}")

    async def search_metric_timeseries(
        self,
        query: str,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Search metric time series (MTS) for dimension discovery."""
        return await self._get("/v2/metrictimeseries", params={
            "query": query,
            "limit": limit,
        })

    async def get_dimensions(
        self,
        query: str = "*",
        limit: int = 100,
    ) -> dict[str, Any]:
        """Search dimensions."""
        return await self._get("/v2/dimension", params={
            "query": query,
            "limit": limit,
        })

    # ── Charts ───────────────────────────────────────────────────────────

    async def create_chart(self, chart_json: dict[str, Any]) -> dict[str, Any]:
        """Create a chart. Returns the chart object with id."""
        return await self._post("/v2/chart", json=chart_json)

    async def update_chart(self, chart_id: str, chart_json: dict[str, Any]) -> dict[str, Any]:
        """Update an existing chart."""
        return await self._put(f"/v2/chart/{chart_id}", json=chart_json)

    async def delete_chart(self, chart_id: str) -> None:
        """Delete a chart."""
        await self._delete(f"/v2/chart/{chart_id}")

    # ── Dashboards ───────────────────────────────────────────────────────

    async def create_dashboard(self, dashboard_json: dict[str, Any]) -> dict[str, Any]:
        """Create a dashboard. Returns the dashboard object with id."""
        return await self._post("/v2/dashboard", json=dashboard_json)

    async def update_dashboard(self, dashboard_id: str, dashboard_json: dict[str, Any]) -> dict[str, Any]:
        """Update an existing dashboard."""
        return await self._put(f"/v2/dashboard/{dashboard_id}", json=dashboard_json)

    # ── Dashboard Groups ─────────────────────────────────────────────────

    async def list_dashboard_groups(self, limit: int = 100) -> dict[str, Any]:
        """List dashboard groups."""
        return await self._get("/v2/dashboardgroup", params={"limit": limit})

    async def create_dashboard_group(self, group_json: dict[str, Any]) -> dict[str, Any]:
        """Create a dashboard group."""
        return await self._post("/v2/dashboardgroup", json=group_json)

    async def get_or_create_dashboard_group(self, name: str) -> dict[str, Any]:
        """Find existing dashboard group by name, or create one."""
        data = await self.list_dashboard_groups(limit=200)
        results = data.get("results", []) if isinstance(data, dict) else data
        for group in results:
            if group.get("name") == name:
                return group
        return await self.create_dashboard_group({
            "name": name,
            "description": "Auto-generated by DashForge",
        })

    # ── Health ───────────────────────────────────────────────────────────

    async def check_connection(self) -> bool:
        """Verify API token and connectivity."""
        try:
            await self._get("/v2/metric", params={"query": "*", "limit": 1})
            return True
        except Exception:
            return False

    async def close(self):
        await self._client.aclose()
