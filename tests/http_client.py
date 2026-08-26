"""Small synchronous ASGI client for Tacit's HTTP tests."""

from __future__ import annotations

import math
from concurrent.futures import Future
from contextlib import ExitStack
from typing import Any

import anyio
import httpx
from anyio.from_thread import BlockingPortal, start_blocking_portal
from anyio.streams.stapled import StapledObjectStream


class TestClient:
    """Drive an ASGI app without Starlette's deprecated httpx adapter."""

    __test__ = False

    def __init__(
        self,
        app: Any,
        base_url: str = "http://testserver",
        raise_server_exceptions: bool = True,
        root_path: str = "",
        backend: str = "asyncio",
        backend_options: dict[str, Any] | None = None,
        cookies: httpx._types.CookieTypes | None = None,
        headers: dict[str, str] | None = None,
        follow_redirects: bool = True,
        client: tuple[str, int] = ("testclient", 50000),
    ) -> None:
        self.app = app
        self.base_url = base_url
        self.raise_server_exceptions = raise_server_exceptions
        self.root_path = root_path
        self.backend = backend
        self.backend_options = backend_options or {}
        self.follow_redirects = follow_redirects
        self.client = client
        self.cookies = httpx.Cookies(cookies)
        self.headers = httpx.Headers(headers or {})
        self.headers.setdefault("user-agent", "testclient")
        self._app_state: dict[str, Any] = {}
        self._portal: BlockingPortal | None = None
        self._exit_stack: ExitStack | None = None
        self._lifespan_receive: StapledObjectStream[dict[str, Any]] | None = None
        self._lifespan_send: StapledObjectStream[dict[str, Any] | None] | None = None
        self._lifespan_task: Future[Any] | None = None
        self._lifespan_error: BaseException | None = None

    def request(self, method: str, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        async def send() -> httpx.Response:
            transport = httpx.ASGITransport(
                app=self._request_app,
                raise_app_exceptions=self.raise_server_exceptions,
                root_path=self.root_path,
                client=self.client,
            )
            async with httpx.AsyncClient(
                transport=transport,
                base_url=self.base_url,
                headers=self.headers,
                cookies=self.cookies,
                follow_redirects=self.follow_redirects,
            ) as async_client:
                response = await async_client.request(method, url, **kwargs)
                await response.aread()
                self.cookies = httpx.Cookies(async_client.cookies)
                return response

        if self._portal is not None:
            return self._portal.call(send)
        with start_blocking_portal(
            backend=self.backend,
            backend_options=self.backend_options,
        ) as portal:
            return portal.call(send)

    async def _request_app(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        request_scope = dict(scope)
        request_scope["state"] = self._app_state.copy()
        await self.app(request_scope, receive, send)

    async def _lifespan(self) -> None:
        assert self._lifespan_receive is not None
        assert self._lifespan_send is not None
        scope = {"type": "lifespan", "state": self._app_state}
        try:
            await self.app(
                scope,
                self._lifespan_receive.receive,
                self._lifespan_send.send,
            )
        except BaseException as exc:
            self._lifespan_error = exc
            raise
        finally:
            await self._lifespan_send.send(None)

    @staticmethod
    def _observe_lifespan_task(task: Future[Any]) -> None:
        if task.cancelled():
            return
        try:
            task.exception()
        except BaseException:
            pass

    async def _raise_lifespan_failure(self, message: object, default: str) -> None:
        assert self._lifespan_task is not None
        failure = RuntimeError(str(message or default))

        # Let an app that sent *.failed and immediately re-raised expose the
        # original exception. Never wait for another ASGI message: a failed app
        # is not required to send one and could otherwise deadlock this client.
        await anyio.lowlevel.checkpoint()
        original_error = self._lifespan_error
        self._lifespan_task.add_done_callback(self._observe_lifespan_task)
        if not self._lifespan_task.done():
            self._lifespan_task.cancel()
        if original_error is not None:
            raise original_error
        raise failure

    async def _receive_lifespan_message(self) -> dict[str, Any]:
        assert self._lifespan_send is not None
        message = await self._lifespan_send.receive()
        if message is None:
            assert self._lifespan_task is not None
            self._lifespan_task.result()
            raise RuntimeError("ASGI lifespan ended before completing the transition")
        return message

    async def _wait_startup(self) -> None:
        assert self._lifespan_receive is not None
        await self._lifespan_receive.send({"type": "lifespan.startup"})
        message = await self._receive_lifespan_message()
        if message["type"] not in {"lifespan.startup.complete", "lifespan.startup.failed"}:
            raise RuntimeError("ASGI app returned an invalid lifespan startup response")
        if message["type"] == "lifespan.startup.failed":
            await self._raise_lifespan_failure(
                message.get("message"),
                "ASGI lifespan startup failed",
            )

    async def _wait_shutdown(self) -> None:
        assert self._lifespan_receive is not None
        await self._lifespan_receive.send({"type": "lifespan.shutdown"})
        message = await self._receive_lifespan_message()
        if message["type"] not in {"lifespan.shutdown.complete", "lifespan.shutdown.failed"}:
            raise RuntimeError("ASGI app returned an invalid lifespan shutdown response")
        if message["type"] == "lifespan.shutdown.failed":
            await self._raise_lifespan_failure(
                message.get("message"),
                "ASGI lifespan shutdown failed",
            )

    def get(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def options(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("OPTIONS", url, **kwargs)

    def head(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("HEAD", url, **kwargs)

    def post(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def put(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: httpx._types.URLTypes, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)

    def close(self) -> None:
        if self._exit_stack is None:
            return
        exit_stack = self._exit_stack
        self._exit_stack = None
        exit_stack.close()

    def __enter__(self) -> TestClient:
        if self._exit_stack is not None:
            raise RuntimeError("TestClient context is already active")
        stack = ExitStack()
        try:
            portal = stack.enter_context(
                start_blocking_portal(
                    backend=self.backend,
                    backend_options=self.backend_options,
                )
            )
            self._portal = portal
            stack.callback(setattr, self, "_portal", None)

            send_streams = anyio.create_memory_object_stream[dict[str, Any] | None](math.inf)
            receive_streams = anyio.create_memory_object_stream[dict[str, Any]](math.inf)
            for channel in (*send_streams, *receive_streams):
                stack.callback(channel.close)
            self._lifespan_send = StapledObjectStream(*send_streams)
            self._lifespan_receive = StapledObjectStream(*receive_streams)
            self._lifespan_error = None
            self._lifespan_task = portal.start_task_soon(self._lifespan)
            portal.call(self._wait_startup)
            stack.callback(portal.call, self._wait_shutdown)
        except BaseException:
            stack.close()
            self._portal = None
            raise
        self._exit_stack = stack.pop_all()
        return self

    def __exit__(self, *args: Any) -> bool | None:
        if self._exit_stack is None:
            return None
        exit_stack = self._exit_stack
        self._exit_stack = None
        return exit_stack.__exit__(*args)
