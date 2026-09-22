from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import anyio
import pytest
from fastapi import FastAPI, Request, Response

from tests.http_client import TestClient


def test_context_manager_runs_asgi_lifespan_once() -> None:
    events: list[str] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[dict[str, bool]]:
        events.append("startup")
        app.state.ready = True
        yield {"lifespan_ready": True}
        events.append("shutdown")

    app = FastAPI(lifespan=lifespan)

    @app.get("/ready")
    async def ready(request: Request) -> dict[str, bool]:
        return {
            "ready": bool(getattr(request.app.state, "ready", False)),
            "lifespan_ready": bool(getattr(request.state, "lifespan_ready", False)),
        }

    assert events == []
    with TestClient(app) as client:
        assert events == ["startup"]
        assert client.get("/ready").json() == {"ready": True, "lifespan_ready": True}
        assert events == ["startup"]
    assert events == ["startup", "shutdown"]


def test_cookie_jar_persists_and_honors_server_deletion() -> None:
    app = FastAPI()

    @app.post("/session")
    async def create_session(response: Response) -> None:
        response.set_cookie("session", "secret")

    @app.delete("/session")
    async def delete_session(response: Response) -> None:
        response.delete_cookie("session")

    @app.get("/session")
    async def read_session(request: Request) -> dict[str, str | None]:
        return {"session": request.cookies.get("session")}

    client = TestClient(app)
    assert client.post("/session").status_code == 200
    assert client.get("/session").json() == {"session": "secret"}
    assert client.delete("/session").status_code == 200
    assert client.get("/session").json() == {"session": None}


def test_one_shot_request_does_not_require_a_lifespan_context() -> None:
    events: list[str] = []

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        events.append("startup")
        yield
        events.append("shutdown")

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    assert TestClient(app).get("/health").json() == {"status": "ok"}
    assert events == []


def test_startup_failed_preserves_the_application_message() -> None:
    events: list[str] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        assert scope["type"] == "lifespan"
        assert await receive() == {"type": "lifespan.startup"}
        try:
            await send(
                {
                    "type": "lifespan.startup.failed",
                    "message": "startup sentinel",
                }
            )
            await anyio.sleep_forever()
        finally:
            events.append("startup cleaned")

    with pytest.raises(RuntimeError, match="^startup sentinel$"):
        with TestClient(app):
            pass
    assert events == ["startup cleaned"]


def test_shutdown_failed_preserves_the_application_message() -> None:
    events: list[str] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        assert scope["type"] == "lifespan"
        assert await receive() == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})
        assert await receive() == {"type": "lifespan.shutdown"}
        try:
            await send(
                {
                    "type": "lifespan.shutdown.failed",
                    "message": "shutdown sentinel",
                }
            )
            await anyio.sleep_forever()
        finally:
            events.append("shutdown cleaned")

    with pytest.raises(RuntimeError, match="^shutdown sentinel$"):
        with TestClient(app):
            pass
    assert events == ["shutdown cleaned"]


@pytest.mark.parametrize("transition", ["startup", "shutdown"])
def test_lifespan_failed_preserves_the_original_exception_identity(transition: str) -> None:
    failure = LookupError(f"{transition} identity sentinel")

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        assert scope["type"] == "lifespan"
        assert await receive() == {"type": "lifespan.startup"}
        if transition == "shutdown":
            await send({"type": "lifespan.startup.complete"})
            assert await receive() == {"type": "lifespan.shutdown"}
        await send(
            {
                "type": f"lifespan.{transition}.failed",
                "message": f"{transition} protocol sentinel",
            }
        )
        raise failure

    with pytest.raises(LookupError) as exc_info:
        with TestClient(app):
            pass
    assert exc_info.value is failure


def test_request_exception_behavior_is_unchanged() -> None:
    failure = ValueError("request sentinel")
    app = FastAPI()

    @app.get("/failure")
    async def fail() -> None:
        raise failure

    with TestClient(app) as client:
        with pytest.raises(ValueError) as exc_info:
            client.get("/failure")
    assert exc_info.value is failure

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/failure")
    assert response.status_code == 500
