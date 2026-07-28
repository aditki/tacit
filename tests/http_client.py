from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx
from anyio.from_thread import start_blocking_portal
from fastapi import FastAPI


class TestClient:
    """Sync-style API test client backed by httpx ASGI transport."""

    __test__ = False

    def __init__(
        self,
        app: FastAPI,
        *,
        base_url: str = "http://testserver",
        follow_redirects: bool = True,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.app = app
        self.base_url = base_url
        self.follow_redirects = follow_redirects
        self._default_headers = dict(headers or {})
        self._portal_cm = start_blocking_portal()
        self._portal = self._portal_cm.__enter__()
        self._client: httpx.AsyncClient | None = None
        self._lifespan_cm: Any = None
        self._closed = False
        self._portal.call(self._startup)

    async def _startup(self) -> None:
        self._lifespan_cm = self.app.router.lifespan_context(self.app)
        await self._lifespan_cm.__aenter__()
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url=self.base_url,
            follow_redirects=self.follow_redirects,
            headers=self._default_headers,
        )

    async def _shutdown(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._lifespan_cm is not None:
            await self._lifespan_cm.__aexit__(None, None, None)
            self._lifespan_cm = None

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        assert self._client is not None
        return await self._client.request(method, url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        if self._closed:
            raise RuntimeError("TestClient is already closed")
        return self._portal.call(lambda: self._request(method, url, **kwargs))

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)

    def options(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("OPTIONS", url, **kwargs)

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._portal.call(self._shutdown)
        finally:
            self._closed = True
            self._portal_cm.__exit__(None, None, None)

    def __enter__(self) -> TestClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - best-effort cleanup
        try:
            self.close()
        except Exception:
            pass
