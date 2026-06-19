from __future__ import annotations

from typing import cast

import httpx
import structlog

from dashforge.config import settings

logger = structlog.get_logger()


class GrafanaClient:
    """Thin async wrapper around the Grafana HTTP API."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        org_id: int | None = None,
    ):
        self.base_url = (base_url or settings.grafana_url).rstrip("/")
        self.api_key = api_key or settings.grafana_api_key
        self.org_id = org_id or settings.grafana_org_id
        headers = {
            "Content-Type": "application/json",
            "X-Grafana-Org-Id": str(self.org_id),
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=30.0,
        )

    # ── Low-level helpers ────────────────────────────────────────────────

    async def _get(self, path: str, **kwargs) -> dict | list:
        resp = await self._client.get(path, **kwargs)
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, json: dict | list | None = None, **kwargs) -> dict:
        resp = await self._client.post(path, json=json, **kwargs)
        resp.raise_for_status()
        return resp.json()

    # ── Datasources ──────────────────────────────────────────────────────

    async def list_datasources(self) -> list[dict]:
        return cast(list[dict], await self._get("/api/datasources"))

    async def datasource_proxy_get(self, datasource_uid: str, path: str) -> dict | list:
        """Proxy a GET request through the Grafana datasource proxy (by UID)."""
        return await self._get(f"/api/datasources/proxy/uid/{datasource_uid}/{path}")

    async def datasource_proxy_post(self, datasource_uid: str, path: str, json: dict | None = None) -> dict | list:
        """Proxy a POST request through the Grafana datasource proxy (by UID)."""
        return await self._post(f"/api/datasources/proxy/uid/{datasource_uid}/{path}", json=json)

    async def datasource_resource(
        self,
        datasource_uid: str,
        resource_path: str,
        body: dict | None = None,
    ) -> dict | list:
        """Call a Grafana datasource plugin resource endpoint (POST).

        Used by CloudWatch, Azure Monitor, etc. that expose custom resource APIs.
        """
        try:
            return await self._post(
                f"/api/datasources/uid/{datasource_uid}/resources/{resource_path}",
                json=body or {},
            )
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "datasource_resource_failed",
                uid=datasource_uid,
                path=resource_path,
                status=exc.response.status_code,
            )
            return {}

    # ── Dashboards ───────────────────────────────────────────────────────

    async def get_or_create_folder(self, title: str) -> dict:
        """Return an existing folder or create a new one."""
        folders = await self._get("/api/folders")
        for f in folders:
            if f.get("title") == title:
                return f
        return await self._post("/api/folders", json={"title": title})

    async def create_dashboard(self, dashboard_json: dict, folder_uid: str) -> dict:
        payload = {
            "dashboard": dashboard_json,
            "folderUid": folder_uid,
            "overwrite": True,
        }
        return await self._post("/api/dashboards/db", json=payload)

    async def close(self):
        await self._client.aclose()
