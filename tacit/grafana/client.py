from __future__ import annotations

from typing import cast

import httpx
import structlog

from tacit.cache import make_cache_key
from tacit.config import Settings, settings
from tacit.runtime_ownership import (
    RuntimeOwnershipDescriptor,
    RuntimeRemoteIdentity,
    canonical_remote_endpoint,
    copy_runtime_settings,
    credential_fingerprint,
    runtime_descriptor_for_remote,
    snapshot_runtime_settings,
)

logger = structlog.get_logger()


class GrafanaClient:
    """Thin async wrapper around the Grafana HTTP API."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        org_id: int | None = None,
        runtime_settings: Settings | None = None,
    ):
        configured_settings = snapshot_runtime_settings(runtime_settings or settings)
        effective_base_url = configured_settings.grafana_url if base_url is None else base_url
        self._base_url = canonical_remote_endpoint(effective_base_url)
        self._api_key = api_key if api_key is not None else configured_settings.grafana_api_key
        self._org_id = org_id if org_id is not None else configured_settings.grafana_org_id
        self._runtime_settings = snapshot_runtime_settings(
            configured_settings.model_copy(
                deep=True,
                update={
                    "grafana_url": self._base_url,
                    "grafana_api_key": self._api_key,
                    "grafana_org_id": self._org_id,
                },
            )
        )
        self.cache_namespace = make_cache_key(
            "grafana",
            self.base_url,
            str(self.org_id),
            self.api_key,
        )
        headers = {
            "Content-Type": "application/json",
            "X-Grafana-Org-Id": str(self.org_id),
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        self._runtime_ownership = (
            runtime_descriptor_for_remote(
                component="grafana_client",
                runtime_settings=self._runtime_settings,
                remote=RuntimeRemoteIdentity(
                    provider="grafana",
                    endpoint=self.base_url,
                    account=str(self.org_id),
                    credential_fingerprint=credential_fingerprint(self.api_key),
                ),
            )
            if isinstance(self._runtime_settings, Settings)
            else None
        )
        self._headers = headers
        self._http_client: httpx.AsyncClient | None = None
        if base_url is None and api_key is None and org_id is None:
            self._http_client = self._new_http_client()

    @property
    def runtime_settings(self) -> Settings:
        """Return a detached copy of the client's settings snapshot."""
        return copy_runtime_settings(self._runtime_settings)

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def org_id(self) -> int:
        return self._org_id

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return the effective Grafana identity without exposing credentials."""
        if self._runtime_ownership is None:
            raise TypeError("Grafana runtime settings must be a Settings instance")
        return self._runtime_ownership

    @property
    def _client(self) -> httpx.AsyncClient:
        """Create network state only after composition has accepted this owner."""
        if self._http_client is None:
            self._http_client = self._new_http_client()
        return self._http_client

    def _new_http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers,
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
        client = self._http_client
        if client is not None:
            self._http_client = None
            await client.aclose()
