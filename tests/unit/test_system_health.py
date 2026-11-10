"""Disclosure-safe process admission health contracts."""

from __future__ import annotations

import asyncio
import json
import socket
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

import tacit.api.routes.system as system_routes
import tacit.container_healthcheck as container_healthcheck_module
import tacit.pipeline_admission as pipeline_admission_module
from tacit.api.app import create_app
from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline_admission import runtime_admission_health_snapshot
from tests.http_client import TestClient


@pytest.fixture(autouse=True)
def _isolated_runtime_fatal_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pipeline_admission_module,
        "_PROCESS_RUNTIME_FATAL_REGISTRY",
        pipeline_admission_module._ProcessRuntimeFatalRegistry(limit=1024),
    )


def _settings(tmp_path, *, suffix: str, pipeline_max_concurrent: int = 1) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        pipeline_max_concurrent=pipeline_max_concurrent,
        pipeline_max_queued=1,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
    )


def test_health_probe_does_not_create_pipeline_runtime_state(tmp_path) -> None:
    app = create_app(runtime_settings=_settings(tmp_path, suffix="cold"), lifespan=None)
    runtime_identity = app.state.runtime_stores.runtime_ownership.admission_namespace
    assert runtime_identity is not None
    assert runtime_admission_health_snapshot(runtime_identity) is None

    response = TestClient(app).get("/healthz")

    assert response.status_code == 200
    assert "pipeline_admission" not in response.json()
    assert runtime_admission_health_snapshot(runtime_identity) is None


def test_health_schema_documents_fatal_readiness_response(tmp_path) -> None:
    app = create_app(runtime_settings=_settings(tmp_path, suffix="schema"), lifespan=None)

    responses = app.openapi()["paths"]["/healthz"]["get"]["responses"]

    assert responses["503"]["description"] == "Runtime is fatally fenced and requires restart"


def test_health_reports_bounded_aggregate_pipeline_and_cleanup_saturation(tmp_path) -> None:
    app = create_app(runtime_settings=_settings(tmp_path, suffix="busy"), lifespan=None)
    controller = app.state.runtime_stores.pipeline_admission()
    client = TestClient(app)

    async def scenario() -> tuple[int, dict[str, object]]:
        cleanup_permits = ()
        async with controller.slot(partition_key="tenant-secret-alpha"):
            cleanup_permits = await controller.acquire_cleanup_permits(1)

        waiter = asyncio.create_task(controller.acquire(partition_key="tenant-secret-beta"))
        try:
            for _ in range(100):
                snapshot = runtime_admission_health_snapshot(controller.runtime_identity)
                if snapshot is not None and snapshot.queued == 1:
                    break
                await asyncio.sleep(0.001)
            else:
                raise AssertionError("pipeline waiter was not queued")

            response = client.get("/healthz")
            return response.status_code, response.json()
        finally:
            controller.release_blocking_permit(cleanup_permits[0])
            lease = await asyncio.wait_for(waiter, timeout=1)
            controller.release(lease)

    status_code, payload = asyncio.run(scenario())

    assert status_code == 200
    assert payload["degraded"] is True
    assert payload["pipeline_admission"] == {
        "capacity": 1,
        "active": 1,
        "queued": 1,
        "retained": 1,
        "blocking_in_flight": 1,
        "cleanup_in_flight": 1,
        "service_owner_in_flight": 0,
        "saturated": True,
        "fatal": False,
        "degraded": True,
    }
    encoded = json.dumps(payload)
    assert "tenant-secret-alpha" not in encoded
    assert "tenant-secret-beta" not in encoded
    assert controller.runtime_identity not in encoded


def test_health_reports_only_bounded_fatal_metadata(tmp_path) -> None:
    app = create_app(runtime_settings=_settings(tmp_path, suffix="fatal"), lifespan=None)
    controller = app.state.runtime_stores.pipeline_admission()
    failure = RuntimeOwnershipError("provider failed with secret-token-value")
    setattr(failure, "cleanup_reason_code", "runtime_cleanup_failed")
    controller.fence_runtime_fatal(failure)

    response = TestClient(app).get("/healthz")
    payload = response.json()

    assert response.status_code == 503
    assert payload["status"] == "failed"
    assert payload["degraded"] is True
    assert payload["pipeline_admission"] == {
        "capacity": 1,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking_in_flight": 0,
        "cleanup_in_flight": 0,
        "service_owner_in_flight": 0,
        "saturated": False,
        "fatal": True,
        "degraded": True,
        "reason_code": "runtime_cleanup_failed",
    }
    encoded = json.dumps(payload)
    assert "secret-token-value" not in encoded
    assert controller.runtime_identity not in encoded


def test_health_head_matches_fatal_get_status_without_a_body(tmp_path) -> None:
    app = create_app(runtime_settings=_settings(tmp_path, suffix="fatal-head"), lifespan=None)
    controller = app.state.runtime_stores.pipeline_admission()
    controller.fence_runtime_fatal(RuntimeOwnershipError("synthetic fatal state"))

    with TestClient(app) as client:
        get_response = client.get("/healthz")
        head_response = client.head("/healthz")

    assert head_response.status_code == get_response.status_code == 503
    assert head_response.content == b""


def test_optional_integration_degradation_remains_ready(tmp_path) -> None:
    app = create_app(runtime_settings=_settings(tmp_path, suffix="optional"), lifespan=None)
    app.state.optional_integration_readiness = {
        "slack": {
            "status": "failed",
            "reason_code": "slack_connection_failed",
        }
    }

    response = TestClient(app).get("/healthz")
    payload = response.json()

    assert response.status_code == 200
    assert payload["status"] == "ok"
    assert payload["degraded"] is True
    assert payload["optional_integrations"] == {
        "slack": {
            "status": "failed",
            "reason_code": "slack_connection_failed",
        }
    }


def _handler(
    *,
    status: int = 200,
    body: bytes = b'{"status":"ok"}',
    headers: dict[str, str] | None = None,
    hits: list[dict[str, str]] | None = None,
) -> type[BaseHTTPRequestHandler]:
    response_body = body
    response_headers = headers or {"Content-Type": "application/json"}
    request_hits = hits if hits is not None else []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            request_hits.append({"path": self.path, "host": self.headers.get("Host", "")})
            self.send_response(status)
            for name, value in response_headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

        def log_message(self, _format: str, *args: object) -> None:
            return

    return Handler


@contextmanager
def _serve(handler: type[BaseHTTPRequestHandler]) -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def _probe_settings(allowed_hosts: str) -> SimpleNamespace:
    return SimpleNamespace(api_allowed_hosts=allowed_hosts)


@pytest.mark.parametrize(
    ("allowed_hosts", "expected_host"),
    [
        ("TACIT.Example.COM", "tacit.example.com"),
        ("*.INTERNAL.Example", "health.internal.example"),
    ],
)
def test_container_healthcheck_uses_canonical_host_while_connecting_only_to_loopback(
    monkeypatch: pytest.MonkeyPatch,
    allowed_hosts: str,
    expected_host: str,
) -> None:
    hits: list[dict[str, str]] = []
    with _serve(_handler(hits=hits)) as server:
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", server.server_port)
        system_routes.container_healthcheck(runtime_settings=_probe_settings(allowed_hosts))

    assert hits == [{"path": "/healthz", "host": expected_host}]


def test_lightweight_container_healthcheck_uses_materialized_validated_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    host_path = tmp_path / "health-host"
    monkeypatch.setattr(container_healthcheck_module, "_CONTAINER_HEALTH_HOST_PATH", host_path)
    hits: list[dict[str, str]] = []
    with _serve(_handler(hits=hits)) as server:
        container_healthcheck_module.write_container_health_host("tacit.example.com")
        container_healthcheck_module.probe_container_health(
            port=server.server_port,
            deadline_seconds=1.0,
        )

    assert hits == [{"path": "/healthz", "host": "tacit.example.com"}]
    assert stat.S_IMODE(host_path.stat().st_mode) == 0o400


def test_container_health_host_publication_retries_after_stale_same_pid_temporary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    host_path = tmp_path / "health-host"
    stale = tmp_path / f".{host_path.name}.{container_healthcheck_module.os.getpid()}.tmp"
    stale.write_text("stale\n", encoding="ascii")
    monkeypatch.setattr(container_healthcheck_module, "_CONTAINER_HEALTH_HOST_PATH", host_path)

    container_healthcheck_module.write_container_health_host("tacit.example.com")

    assert host_path.read_text(encoding="ascii") == "tacit.example.com\n"


def test_container_health_host_publication_completes_short_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    host_path = tmp_path / "health-host"
    monkeypatch.setattr(container_healthcheck_module, "_CONTAINER_HEALTH_HOST_PATH", host_path)
    original_write = container_healthcheck_module.os.write
    writes = 0

    def short_first_write(descriptor: int, payload: bytes) -> int:
        nonlocal writes
        writes += 1
        if writes == 1:
            return original_write(descriptor, payload[:1])
        return original_write(descriptor, payload)

    monkeypatch.setattr(container_healthcheck_module.os, "write", short_first_write)

    container_healthcheck_module.write_container_health_host("tacit.example.com")

    assert writes >= 2
    assert host_path.read_text(encoding="ascii") == "tacit.example.com\n"


@pytest.mark.parametrize("failure_phase", ["write", "fsync", "replace"])
def test_container_health_host_publication_failure_preserves_previous_value_and_allows_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    failure_phase: str,
) -> None:
    host_path = tmp_path / "health-host"
    host_path.write_text("previous.example.com\n", encoding="ascii")
    host_path.chmod(0o400)
    monkeypatch.setattr(container_healthcheck_module, "_CONTAINER_HEALTH_HOST_PATH", host_path)

    with monkeypatch.context() as failure_patch:
        if failure_phase == "write":
            failure_patch.setattr(
                container_healthcheck_module.os,
                "write",
                lambda _descriptor, _payload: (_ for _ in ()).throw(OSError("write failed")),
            )
        elif failure_phase == "fsync":
            failure_patch.setattr(
                container_healthcheck_module.os,
                "fsync",
                lambda _descriptor: (_ for _ in ()).throw(OSError("fsync failed")),
            )
        else:
            failure_patch.setattr(
                container_healthcheck_module.os,
                "replace",
                lambda _source, _target: (_ for _ in ()).throw(OSError("replace failed")),
            )

        with pytest.raises(OSError, match=f"{failure_phase} failed"):
            container_healthcheck_module.write_container_health_host("new.example.com")

    assert host_path.read_text(encoding="ascii") == "previous.example.com\n"
    assert not tuple(tmp_path.glob(f".{host_path.name}.*.tmp"))

    container_healthcheck_module.write_container_health_host("new.example.com")
    assert host_path.read_text(encoding="ascii") == "new.example.com\n"


def test_container_healthcheck_loads_tacit_config_yaml_with_server_precedence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    config_path = tmp_path / "tacit-health.yaml"
    config_path.write_text("api:\n  allowed_hosts: '*.YAML.Internal.Example'\n")
    hits: list[dict[str, str]] = []

    monkeypatch.setenv("TACIT_CONFIG", str(config_path))
    monkeypatch.delenv("API_ALLOWED_HOSTS", raising=False)
    with _serve(_handler(hits=hits)) as server:
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", server.server_port)
        system_routes.container_healthcheck()

    assert hits == [{"path": "/healthz", "host": "health.yaml.internal.example"}]


def test_container_healthcheck_environment_host_overrides_tacit_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    config_path = tmp_path / "tacit-health.yaml"
    config_path.write_text("api:\n  allowed_hosts: yaml.example.com\n")
    hits: list[dict[str, str]] = []

    monkeypatch.setenv("TACIT_CONFIG", str(config_path))
    monkeypatch.setenv("API_ALLOWED_HOSTS", "*.ENV.Internal.Example")
    with _serve(_handler(hits=hits)) as server:
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", server.server_port)
        system_routes.container_healthcheck()

    assert hits == [{"path": "/healthz", "host": "health.env.internal.example"}]


def test_container_healthcheck_ignores_all_proxy_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health_hits: list[dict[str, str]] = []
    proxy_hits: list[dict[str, str]] = []
    with _serve(_handler(hits=health_hits)) as health_server, _serve(_handler(hits=proxy_hits)) as proxy_server:
        proxy_url = f"http://127.0.0.1:{proxy_server.server_port}"
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            monkeypatch.setenv(name, proxy_url)
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", health_server.server_port)

        system_routes.container_healthcheck(runtime_settings=_probe_settings("tacit.example.com"))

    assert health_hits == [{"path": "/healthz", "host": "tacit.example.com"}]
    assert proxy_hits == []


def test_container_healthcheck_rejects_redirect_without_contacting_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redirect_hits: list[dict[str, str]] = []
    target_hits: list[dict[str, str]] = []
    with _serve(_handler(hits=target_hits)) as target_server:
        location = f"http://127.0.0.1:{target_server.server_port}/redirect-target"
        with _serve(_handler(status=302, headers={"Location": location}, hits=redirect_hits)) as redirect_server:
            monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", redirect_server.server_port)
            with pytest.raises(RuntimeError, match="HTTP status"):
                system_routes.container_healthcheck(runtime_settings=_probe_settings("localhost"))

    assert redirect_hits == [{"path": "/healthz", "host": "localhost"}]
    assert target_hits == []


@pytest.mark.parametrize(
    ("status", "body", "message"),
    [
        (503, b'{"status":"failed"}', "HTTP status"),
        (200, b"not-json", "JSON"),
        (200, b"[]", "JSON object"),
        (200, b'{"status":"failed"}', "status"),
        (200, b'{"healthy":true}', "status"),
    ],
)
def test_container_healthcheck_requires_success_status_and_expected_json(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    body: bytes,
    message: str,
) -> None:
    with _serve(_handler(status=status, body=body)) as server:
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", server.server_port)
        with pytest.raises(RuntimeError, match=message):
            system_routes.container_healthcheck(runtime_settings=_probe_settings("localhost"))


def test_container_healthcheck_rejects_non_json_content_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _serve(_handler(headers={"Content-Type": "text/plain"})) as server:
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", server.server_port)
        with pytest.raises(RuntimeError, match="JSON content"):
            system_routes.container_healthcheck(runtime_settings=_probe_settings("localhost"))


def test_container_healthcheck_rejects_oversized_response_before_buffering_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b'{"status":"ok","padding":"' + (b"x" * 128) + b'"}'
    with _serve(_handler(body=body)) as server:
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_PORT", server.server_port)
        monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_MAX_BODY_BYTES", 32)
        with pytest.raises(RuntimeError, match="size limit"):
            system_routes.container_healthcheck(runtime_settings=_probe_settings("localhost"))


def test_container_healthcheck_enforces_one_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: 15\r\n"
        b"Connection: close\r\n\r\n"
        b'{"status":"ok"}'
    )
    timeouts: list[float] = []
    recv_calls = 0

    class SlowSocket:
        def __enter__(self) -> SlowSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, timeout: float) -> None:
            timeouts.append(timeout)

        def send(self, payload: bytes) -> int:
            return len(payload)

        def recv(self, _size: int) -> bytes:
            nonlocal recv_calls
            value = response[recv_calls : recv_calls + 1]
            recv_calls += 1
            return value

    clock = 100.0

    def monotonic() -> float:
        nonlocal clock
        current = clock
        clock += 0.01
        return current

    monkeypatch.setattr(socket, "create_connection", lambda *_args, **_kwargs: SlowSocket())
    monkeypatch.setattr(container_healthcheck_module.time, "monotonic", monotonic)
    monkeypatch.setattr(system_routes, "_CONTAINER_HEALTH_DEADLINE_SECONDS", 0.05)

    with pytest.raises(TimeoutError, match="deadline"):
        system_routes.container_healthcheck(runtime_settings=_probe_settings("localhost"))

    assert recv_calls < len(response)
    assert timeouts
    assert all(0 < timeout <= 0.05 for timeout in timeouts)
