from __future__ import annotations

import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue
from subprocess import CompletedProcess
from threading import Thread
from urllib.parse import urlsplit

import httpx
import pytest
from click.testing import CliRunner

from tacit import demo_flow
from tacit.cli import cli


class _RecordingHttpServer:
    def __init__(self, *, status_code: int, location: str | None = None) -> None:
        self.requests: list[tuple[str, str, httpx.Headers]] = []
        requests = self.requests

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
                self._respond()

            def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
                content_length = int(self.headers.get("Content-Length", "0"))
                if content_length:
                    self.rfile.read(content_length)
                self._respond()

            def _respond(self) -> None:
                requests.append((self.command, self.path, httpx.Headers(dict(self.headers.items()))))
                self.send_response(status_code)
                if location is not None:
                    self.send_header("Location", location)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, _format: str, *_args: object) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def __enter__(self) -> _RecordingHttpServer:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)


def _invoke_credential_workflow(workflow: str, api_url: str, tmp_path: Path) -> None:
    if workflow == "learning":
        dashboard_path = tmp_path / "dashboard.json"
        dashboard_path.write_text("{}")
        with pytest.raises(demo_flow.DemoError):
            demo_flow.run_learning_flow(api_url, dashboard_path, echo=lambda _message: None)
        return
    if workflow == "generation":
        with pytest.raises(demo_flow.DemoError):
            demo_flow.run_generation(api_url, "checkout latency", echo=lambda _message: None)
        return
    demo_flow.record_demo_feedback(api_url, "dashboard-1")


@pytest.mark.parametrize("workflow", ["learning", "generation", "feedback"])
def test_credential_workflows_bypass_proxy_and_reject_redirects(
    workflow: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_AUTH_KEY", "demo-workflow-key")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)

    with (
        _RecordingHttpServer(status_code=204) as redirect_target,
        _RecordingHttpServer(status_code=307, location=f"{redirect_target.url}/collect") as origin,
        _RecordingHttpServer(status_code=502) as proxy,
    ):
        monkeypatch.setenv("HTTP_PROXY", proxy.url)
        monkeypatch.setenv("HTTPS_PROXY", proxy.url)
        monkeypatch.setenv("ALL_PROXY", proxy.url)

        _invoke_credential_workflow(workflow, origin.url, tmp_path)

    assert proxy.requests == []
    assert redirect_target.requests == []
    assert len(origin.requests) == 1
    assert origin.requests[0][2]["X-API-Key"] == "demo-workflow-key"


def test_load_demo_env_supplies_api_auth_header(tmp_path, monkeypatch):
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    (tmp_path / ".env").write_text("API_AUTH_KEY=demo-secret\n")

    demo_flow.load_demo_env(tmp_path)

    assert demo_flow._auth_headers() == {"X-API-Key": "demo-secret"}


def test_load_demo_env_does_not_override_process_env(tmp_path, monkeypatch):
    monkeypatch.setenv("API_AUTH_KEY", "shell-secret")
    (tmp_path / ".env").write_text("API_AUTH_KEY=demo-secret\n")

    demo_flow.load_demo_env(tmp_path)

    assert demo_flow._auth_headers() == {"X-API-Key": "shell-secret"}


def test_load_demo_env_generates_an_ephemeral_container_key_when_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    monkeypatch.setattr("secrets.token_urlsafe", lambda _bytes: "generated-demo-key")

    demo_flow.load_demo_env(tmp_path)

    assert demo_flow._auth_headers() == {"X-API-Key": "generated-demo-key"}


def test_compose_down_is_credential_independent_and_idempotent(tmp_path, monkeypatch):
    calls: list[tuple[list[str], Path, dict[str, str]]] = []
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    monkeypatch.setattr(
        demo_flow.secrets,
        "token_urlsafe",
        lambda _bytes: pytest.fail("teardown must not generate a credential"),
    )

    def run(command, *, cwd, env, check=False):
        calls.append((command, cwd, env))
        return CompletedProcess(command, 0)

    monkeypatch.setattr(demo_flow.subprocess, "run", run)

    demo_flow.compose_down(tmp_path, echo=lambda _message: None)
    demo_flow.compose_down(tmp_path, echo=lambda _message: None)

    assert len(calls) == 2
    assert all(command[-1] == "down" for command, _cwd, _env in calls)
    assert all(cwd == tmp_path for _command, cwd, _env in calls)
    assert all(env["API_AUTH_KEY"] == demo_flow.DEMO_TEARDOWN_INTERPOLATION_KEY for _command, _cwd, env in calls)
    assert "API_AUTH_KEY" not in calls[0][0]


def test_compose_down_does_not_forward_an_existing_runtime_credential(tmp_path, monkeypatch):
    observed_env: dict[str, str] = {}
    monkeypatch.setenv("API_AUTH_KEY", "runtime-secret-must-not-reach-compose-down")

    def run(command, *, cwd, env, check=False):
        observed_env.update(env)
        return CompletedProcess(command, 0)

    monkeypatch.setattr(demo_flow.subprocess, "run", run)

    demo_flow.compose_down(tmp_path, echo=lambda _message: None)

    assert observed_env["API_AUTH_KEY"] == demo_flow.DEMO_TEARDOWN_INTERPOLATION_KEY
    assert "runtime-secret-must-not-reach-compose-down" not in observed_env.values()


def test_demo_down_propagates_compose_failure_without_false_success(tmp_path, monkeypatch):
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    monkeypatch.setattr(demo_flow, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        demo_flow.subprocess,
        "run",
        lambda command, **_kwargs: CompletedProcess(command, 17),
    )

    result = CliRunner().invoke(cli, ["demo", "--down"])

    assert result.exit_code == 1
    assert "Demo stack stopped" not in result.output
    assert "API_AUTH_KEY" not in result.output
    assert "docker compose down failed" in result.output


def test_demo_down_reports_success_only_after_compose_succeeds(tmp_path, monkeypatch):
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    monkeypatch.setattr(demo_flow, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        demo_flow.subprocess,
        "run",
        lambda command, **_kwargs: CompletedProcess(command, 0),
    )

    result = CliRunner().invoke(cli, ["demo", "--down"])

    assert result.exit_code == 0
    assert "Demo stack stopped" in result.output
    assert "API_AUTH_KEY" not in result.output


def test_demo_skip_generation_opens_authenticated_ui_without_printing_key(tmp_path, monkeypatch):
    generated_key = "generated-demo-key-must-not-be-printed"
    opened: list[tuple[str, str]] = []
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    monkeypatch.setattr(demo_flow, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(demo_flow.secrets, "token_urlsafe", lambda _bytes: generated_key)
    monkeypatch.setattr(demo_flow, "compose_up", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(demo_flow, "wait_for_http", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(demo_flow, "run_learning_flow", lambda *_args, **_kwargs: "checkout")
    monkeypatch.setattr(
        demo_flow,
        "open_authenticated_demo_ui",
        lambda api_url, *, api_key: opened.append((api_url, api_key)),
    )

    result = CliRunner().invoke(cli, ["demo", "--skip-generate"])

    assert result.exit_code == 0
    assert opened == [(demo_flow.DEFAULT_API_URL, generated_key)]
    assert generated_key not in result.output


def test_demo_generation_opens_authenticated_ui_and_dashboard_without_printing_key(tmp_path, monkeypatch):
    generated_key = "generated-demo-key-must-stay-in-browser-handoff"
    dashboard_url = "http://localhost:3000/d/demo-dashboard"
    opened_ui: list[tuple[str, str]] = []
    opened_dashboards: list[str] = []
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    monkeypatch.setattr(demo_flow, "find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(demo_flow.secrets, "token_urlsafe", lambda _bytes: generated_key)
    monkeypatch.setattr(demo_flow, "compose_up", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(demo_flow, "wait_for_http", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(demo_flow, "run_learning_flow", lambda *_args, **_kwargs: "checkout")
    monkeypatch.setattr(
        demo_flow,
        "run_generation",
        lambda *_args, **_kwargs: {
            "dashboard_url": dashboard_url,
            "dashboard_uid": "demo-dashboard",
            "panel_count": 3,
        },
    )
    monkeypatch.setattr(demo_flow, "record_demo_feedback", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        demo_flow,
        "open_authenticated_demo_ui",
        lambda api_url, *, api_key: opened_ui.append((api_url, api_key)),
    )
    monkeypatch.setattr("tacit.cli.webbrowser.open", lambda url: opened_dashboards.append(url))

    result = CliRunner().invoke(cli, ["demo"])

    assert result.exit_code == 0
    assert opened_ui == [(demo_flow.DEFAULT_API_URL, generated_key)]
    assert opened_dashboards == [dashboard_url]
    assert generated_key not in result.output


@pytest.mark.parametrize(
    "api_url",
    [
        "http://tacit.example:8000",
        "http://localhost:8000/path",
        "http://user@localhost:8000",
        "http://localhost:8000/?key=not-allowed",
    ],
)
def test_demo_ui_handoff_rejects_nonlocal_or_decorated_targets_before_browser_open(api_url):
    opened: list[str] = []

    with pytest.raises(demo_flow.DemoError, match="only for an explicit loopback URL"):
        demo_flow.open_authenticated_demo_ui(
            api_url,
            api_key="demo-key",
            open_browser=lambda url, **_kwargs: opened.append(url) or True,
        )

    assert opened == []


@pytest.mark.parametrize(
    ("api_url", "expected_target", "expected_bootstrap_host"),
    [
        ("http://LOCALHOST:80", "http://localhost/", "localhost"),
        ("https://localhost:443/", "https://localhost/", "localhost"),
        ("http://[::1]:8000", "http://[::1]:8000/", "127.0.0.1"),
    ],
)
def test_demo_ui_handoff_canonicalizes_exact_loopback_origins(
    api_url: str,
    expected_target: str,
    expected_bootstrap_host: str,
) -> None:
    assert demo_flow._validated_demo_ui_url(api_url) == (expected_target, expected_bootstrap_host)


def test_demo_ui_handoff_redeems_key_once_and_requires_browser_completion(monkeypatch) -> None:
    api_key = "one-shot-demo-key"
    tokens = iter(("b" * 32, "f" * 32, "r" * 32, "c" * 32, "x" * 32, "h" * 24, "i" * 24))
    monkeypatch.setattr(demo_flow.secrets, "token_urlsafe", lambda _bytes: next(tokens))
    original_end_headers = demo_flow.BaseHTTPRequestHandler.end_headers
    completion_responses = 0

    def fail_final_response(handler: BaseHTTPRequestHandler) -> None:
        nonlocal completion_responses
        status_line = handler._headers_buffer[0] if handler._headers_buffer else b""
        if handler.path == f"/{'c' * 32}" and b" 204 " in status_line:
            completion_responses += 1
            if completion_responses == 3:
                raise OSError("injected final response write failure")
        original_end_headers(handler)

    monkeypatch.setattr(demo_flow.BaseHTTPRequestHandler, "end_headers", fail_final_response)
    browser_errors: list[BaseException] = []
    browser_finished = Thread()
    observed_document = ""

    def open_browser(url: str, **_kwargs) -> bool:
        origin = url.rsplit("/", 1)[0]

        def request_handoff() -> None:
            nonlocal observed_document
            try:
                with httpx.Client(trust_env=False, timeout=2.0) as client:
                    assert client.get(f"{origin}/unrelated-probe").status_code == 404
                    bootstrap = client.get(url)
                    assert bootstrap.status_code == 200
                    observed_document = bootstrap.text
                    assert bootstrap.headers["cache-control"] == "no-store, max-age=0"
                    assert bootstrap.headers["x-frame-options"] == "DENY"
                    assert client.get(url).status_code == 410
                    assert (
                        client.get(f"{origin}/{'f' * 32}", headers={"Referer": "http://127.0.0.1:9000/"}).status_code
                        == 403
                    )
                    frame = client.get(f"{origin}/{'f' * 32}", headers={"Referer": "http://127.0.0.1:8000/"})
                    assert frame.status_code == 200
                    assert api_key not in frame.text
                    assert frame.headers["cache-control"] == "no-store, max-age=0"
                    assert frame.headers.get("x-frame-options") is None
                    assert "frame-ancestors http://127.0.0.1:8000" in frame.headers["content-security-policy"]
                    assert (
                        client.get(
                            f"{origin}/{'f' * 32}",
                            headers={"Referer": "http://127.0.0.1:8000/"},
                        ).status_code
                        == 410
                    )
                    nonce = "n" * 48
                    headers = {"Origin": origin}
                    redeem_message = {"type": demo_flow._DEMO_AUTH_REDEEM_TYPE, "nonce": nonce}
                    assert client.post(f"{origin}/{'r' * 32}", json=redeem_message).status_code == 403
                    assert client.post(f"{origin}/{'r' * 32}", headers=headers, content=b"not-json").status_code == 400
                    redeemed = client.post(f"{origin}/{'r' * 32}", headers=headers, json=redeem_message)
                    assert redeemed.status_code == 200
                    assert redeemed.headers["cache-control"] == "no-store, max-age=0"
                    redemption = redeemed.json()
                    assert redemption["type"] == demo_flow._DEMO_AUTH_DELIVERY_TYPE
                    assert redemption["nonce"] == nonce
                    assert redemption["api_key"] == api_key
                    now_ms = int(time.time() * 1000)
                    assert now_ms < redemption["expires_at_ms"] <= now_ms + 2000
                    assert f"const serverDeadlineMs = {redemption['expires_at_ms']};" in observed_document
                    assert client.post(f"{origin}/{'r' * 32}", headers=headers, json=redeem_message).status_code == 410
                    assert (
                        client.post(
                            f"{origin}/{'c' * 32}",
                            headers=headers,
                            json={"type": demo_flow._DEMO_AUTH_ACCEPTED_TYPE, "nonce": "z" * 48},
                        ).status_code
                        == 409
                    )
                    completed = client.post(
                        f"{origin}/{'c' * 32}",
                        headers=headers,
                        json={"type": demo_flow._DEMO_AUTH_ACCEPTED_TYPE, "nonce": nonce},
                    )
                    assert completed.status_code == 204
                    assert completed.headers["cache-control"] == "no-store, max-age=0"
                    assert (
                        client.post(
                            f"{origin}/{'c' * 32}",
                            headers=headers,
                            json={"type": demo_flow._DEMO_AUTH_ACTIVATED_TYPE, "nonce": "z" * 48},
                        ).status_code
                        == 409
                    )
                    activated = client.post(
                        f"{origin}/{'c' * 32}",
                        headers=headers,
                        json={"type": demo_flow._DEMO_AUTH_ACTIVATED_TYPE, "nonce": nonce},
                    )
                    assert activated.status_code == 204
                    assert activated.headers["cache-control"] == "no-store, max-age=0"
                    assert (
                        client.post(
                            f"{origin}/{'c' * 32}",
                            headers=headers,
                            json={"type": demo_flow._DEMO_AUTH_FINALIZED_TYPE, "nonce": "z" * 48},
                        ).status_code
                        == 409
                    )
                    with pytest.raises(httpx.TransportError):
                        client.post(
                            f"{origin}/{'c' * 32}",
                            headers=headers,
                            json={"type": demo_flow._DEMO_AUTH_FINALIZED_TYPE, "nonce": nonce},
                        )
            except BaseException as exc:  # pragma: no cover - asserted in caller
                browser_errors.append(exc)

        nonlocal browser_finished
        browser_finished = Thread(target=request_handoff)
        browser_finished.start()
        return True

    demo_flow.open_authenticated_demo_ui(
        "http://127.0.0.1:8000",
        api_key=api_key,
        open_browser=open_browser,
        timeout_s=2.0,
    )
    browser_finished.join(timeout=2.0)

    assert not browser_finished.is_alive()
    assert browser_errors == []
    assert completion_responses == 3
    assert api_key not in observed_document
    assert "encodeURIComponent(JSON.stringify" in observed_document
    assert "destination.hash" in observed_document
    assert "window.open" not in observed_document
    assert "window.location.replace(destination.href)" in observed_document
    assert "http://127.0.0.1:8000" in observed_document


def test_demo_ui_handoff_document_uses_only_the_server_authored_absolute_deadline() -> None:
    expires_at_ms = 2_345_678_901_234

    document = demo_flow._demo_ui_handoff_document(
        "http://127.0.0.1:8000/",
        frame_path="/frame",
        csp_nonce="test-nonce",
        expires_at_ms=expires_at_ms,
    ).decode()
    frame_document = demo_flow._demo_ui_handoff_frame_document(
        "http://127.0.0.1:8000",
        redeem_path="/redeem",
        complete_path="/complete",
        failure_path="/failure",
        csp_nonce="frame-nonce",
        expires_at_ms=expires_at_ms,
    ).decode()

    assert f"const serverDeadlineMs = {expires_at_ms};" in document
    assert f"const serverDeadlineMs = {expires_at_ms};" in frame_document
    assert "payload.expires_at_ms !== serverDeadlineMs" in frame_document
    assert "monotonicEpochMs() >= serverDeadlineMs" in document
    assert "timeout_s" not in document
    assert "* 900" not in document
    assert "window.open" not in document
    assert "window.location.replace(destination.href)" in document
    assert "window.open" not in frame_document


def test_demo_ui_target_keeps_delivered_keys_pending_until_server_commit() -> None:
    document = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text(encoding="utf-8")

    delivery = document.index("sessionStorage.setItem(DEMO_AUTH_PENDING_SESSION_KEY")
    accepted = document.index("type: DEMO_AUTH_ACCEPTED_TYPE", delivery)
    committed = document.index("message.type === DEMO_AUTH_COMMITTED_TYPE")
    promotion = document.index("sessionStorage.setItem(API_KEY_SESSION_KEY", committed)
    activation = document.index("type: DEMO_AUTH_ACTIVATED_TYPE", promotion)
    confirmed = document.index("message.type === DEMO_AUTH_CONFIRMED_TYPE", activation)
    pending_removal = document.index("sessionStorage.removeItem(DEMO_AUTH_PENDING_SESSION_KEY);", confirmed)
    finalization = document.index("type: DEMO_AUTH_FINALIZED_TYPE", pending_removal)

    assert delivery < accepted
    assert committed < promotion < activation < confirmed < pending_removal < finalization
    assert "sessionStorage.removeItem(DEMO_AUTH_PENDING_SESSION_KEY);" in document
    assert "sessionStorage.getItem(DEMO_AUTH_PENDING_SESSION_KEY)" in document
    assert "apiKey = activeDemoApiKeyFromSession();" in document
    assert "window.addEventListener('pagehide', handlePageExit);" in document


def test_demo_ui_handoff_rejects_completion_before_redemption_and_times_out(monkeypatch) -> None:
    tokens = iter(("b" * 32, "f" * 32, "r" * 32, "c" * 32, "x" * 32, "h" * 24, "i" * 24))
    monkeypatch.setattr(demo_flow.secrets, "token_urlsafe", lambda _bytes: next(tokens))
    browser_finished = Thread()
    statuses: list[int] = []

    def open_browser(url: str, **_kwargs) -> bool:
        origin = url.rsplit("/", 1)[0]

        def malformed_completion() -> None:
            with httpx.Client(trust_env=False, timeout=2.0) as client:
                assert client.get(url).status_code == 200
                assert (
                    client.get(f"{origin}/{'f' * 32}", headers={"Referer": "http://127.0.0.1:8000/"}).status_code == 200
                )
                statuses.append(
                    client.post(
                        f"{origin}/{'c' * 32}",
                        headers={"Origin": origin},
                        json={"type": demo_flow._DEMO_AUTH_ACCEPTED_TYPE, "nonce": "n" * 48},
                    ).status_code
                )

        nonlocal browser_finished
        browser_finished = Thread(target=malformed_completion)
        browser_finished.start()
        return True

    with pytest.raises(demo_flow.DemoError, match="Timed out"):
        demo_flow.open_authenticated_demo_ui(
            "http://127.0.0.1:8000",
            api_key="demo-key",
            open_browser=open_browser,
            timeout_s=0.1,
        )

    browser_finished.join(timeout=2.0)
    assert statuses == [409]


def test_demo_ui_handoff_partial_headers_cannot_outlive_absolute_deadline() -> None:
    bootstrap_urls: Queue[str] = Queue(maxsize=1)
    handoff_errors: list[BaseException] = []

    def run_handoff() -> None:
        try:
            demo_flow.open_authenticated_demo_ui(
                "http://127.0.0.1:8000",
                api_key="slowloris-protected-demo-key",
                open_browser=lambda url, **_kwargs: bootstrap_urls.put(url) or True,
                timeout_s=0.15,
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            handoff_errors.append(exc)

    handoff_thread = Thread(target=run_handoff, daemon=True)
    handoff_thread.start()
    bootstrap_url = bootstrap_urls.get(timeout=2.0)
    parsed = urlsplit(bootstrap_url)
    assert parsed.hostname is not None
    assert parsed.port is not None

    connection = socket.create_connection((parsed.hostname, parsed.port), timeout=1.0)
    try:
        connection.sendall(f"GET {parsed.path} HTTP/1.1\r\nHost: 127.0.0.1".encode())
        began = time.monotonic()
        handoff_thread.join(timeout=0.6)
        completed_within_bound = not handoff_thread.is_alive()
        elapsed = time.monotonic() - began
    finally:
        connection.close()
        handoff_thread.join(timeout=2.0)

    assert completed_within_bound
    assert elapsed < 0.6
    assert not handoff_thread.is_alive()
    assert len(handoff_errors) == 1
    assert isinstance(handoff_errors[0], demo_flow.DemoError)
    assert "Timed out" in str(handoff_errors[0])


def test_demo_ui_handoff_reports_browser_protocol_failure(monkeypatch) -> None:
    tokens = iter(("b" * 32, "f" * 32, "r" * 32, "c" * 32, "x" * 32, "h" * 24, "i" * 24))
    monkeypatch.setattr(demo_flow.secrets, "token_urlsafe", lambda _bytes: next(tokens))
    browser_finished = Thread()

    def open_browser(url: str, **_kwargs) -> bool:
        origin = url.rsplit("/", 1)[0]

        def report_failure() -> None:
            with httpx.Client(trust_env=False, timeout=2.0) as client:
                assert client.get(url).status_code == 200
                assert (
                    client.get(f"{origin}/{'f' * 32}", headers={"Referer": "http://127.0.0.1:8000/"}).status_code == 200
                )
                assert (
                    client.post(
                        f"{origin}/{'x' * 32}",
                        headers={"Origin": origin},
                        json={"type": demo_flow._DEMO_AUTH_FAILED_TYPE, "nonce": "n" * 48},
                    ).status_code
                    == 204
                )

        nonlocal browser_finished
        browser_finished = Thread(target=report_failure)
        browser_finished.start()
        return True

    with pytest.raises(demo_flow.DemoError, match="browser could not open"):
        demo_flow.open_authenticated_demo_ui(
            "http://127.0.0.1:8000",
            api_key="demo-key",
            open_browser=open_browser,
            timeout_s=2.0,
        )

    browser_finished.join(timeout=2.0)
    assert not browser_finished.is_alive()


def test_demo_ui_handoff_rejects_browser_launcher_failure() -> None:
    with pytest.raises(demo_flow.DemoError, match="Could not open"):
        demo_flow.open_authenticated_demo_ui(
            "http://127.0.0.1:8000",
            api_key="demo-key",
            open_browser=lambda _url, **_kwargs: False,
            timeout_s=0.1,
        )


def test_wait_for_http_is_anonymous_and_ignores_ambient_proxies(monkeypatch):
    observed_client_options = []
    observed_requests: list[httpx.Request] = []
    real_client = httpx.Client
    monkeypatch.setenv("API_AUTH_KEY", "demo-health-key")
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")

    def handler(request: httpx.Request) -> httpx.Response:
        observed_requests.append(request)
        return httpx.Response(204, request=request)

    def client_factory(**kwargs):
        observed_client_options.append(kwargs)
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(demo_flow.httpx, "Client", client_factory)

    demo_flow.wait_for_http("http://grafana.test/api/health", timeout_s=0.1, echo=lambda _message: None)

    assert observed_client_options == [{"timeout": 5.0, "follow_redirects": False, "trust_env": False}]
    assert len(observed_requests) == 1
    assert observed_requests[0].url == httpx.URL("http://grafana.test/api/health")
    assert "x-api-key" not in observed_requests[0].headers
    assert "authorization" not in observed_requests[0].headers


def test_wait_for_http_rejects_cross_origin_redirect_without_disclosing_credentials(monkeypatch):
    observed_requests: list[httpx.Request] = []
    real_client = httpx.Client
    monotonic_values = iter((0.0, 0.0, 1.0))
    monkeypatch.setenv("API_AUTH_KEY", "demo-health-key")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.example:8080")

    def handler(request: httpx.Request) -> httpx.Response:
        observed_requests.append(request)
        if request.url.host == "grafana.test":
            return httpx.Response(
                302,
                headers={"Location": "http://attacker.test/collect"},
                request=request,
            )
        return httpx.Response(204, request=request)

    def client_factory(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(demo_flow.httpx, "Client", client_factory)
    monkeypatch.setattr(demo_flow.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(demo_flow.time, "sleep", lambda _seconds: None)

    with pytest.raises(demo_flow.DemoError, match=r"HTTP 302 redirect rejected"):
        demo_flow.wait_for_http("http://grafana.test/api/health", timeout_s=0.1, echo=lambda _message: None)

    assert [request.url.host for request in observed_requests] == ["grafana.test"]
    assert "x-api-key" not in observed_requests[0].headers
    assert "authorization" not in observed_requests[0].headers


def test_wait_for_http_rejects_same_origin_redirect(monkeypatch):
    observed_requests: list[httpx.Request] = []
    real_client = httpx.Client
    monotonic_values = iter((0.0, 0.0, 1.0))

    def handler(request: httpx.Request) -> httpx.Response:
        observed_requests.append(request)
        return httpx.Response(
            307,
            headers={"Location": "/login"},
            request=request,
        )

    def client_factory(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(demo_flow.httpx, "Client", client_factory)
    monkeypatch.setattr(demo_flow.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(demo_flow.time, "sleep", lambda _seconds: None)

    with pytest.raises(demo_flow.DemoError, match=r"HTTP 307 redirect rejected"):
        demo_flow.wait_for_http("http://grafana.test/api/health", timeout_s=0.1, echo=lambda _message: None)

    assert [request.url for request in observed_requests] == [httpx.URL("http://grafana.test/api/health")]


def test_wait_for_http_retries_transient_failure_then_succeeds(monkeypatch):
    observed_requests: list[httpx.Request] = []
    sleep_calls: list[float] = []
    real_client = httpx.Client
    statuses = iter((503, 204))
    monotonic_values = iter((0.0, 0.0, 0.01))

    def handler(request: httpx.Request) -> httpx.Response:
        observed_requests.append(request)
        return httpx.Response(next(statuses), request=request)

    def client_factory(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(demo_flow.httpx, "Client", client_factory)
    monkeypatch.setattr(demo_flow.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(demo_flow.time, "sleep", sleep_calls.append)

    demo_flow.wait_for_http("http://grafana.test/api/health", timeout_s=0.1, echo=lambda _message: None)

    assert [request.url for request in observed_requests] == [
        httpx.URL("http://grafana.test/api/health"),
        httpx.URL("http://grafana.test/api/health"),
    ]
    assert sleep_calls == [2.0]


def test_run_generation_keeps_tacit_workflow_authentication(monkeypatch):
    observed_client_options = []

    class Response:
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {"dashboard_uid": "demo"}

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def request(self, *_args, **_kwargs):
            return Response()

    def client_factory(**kwargs):
        observed_client_options.append(kwargs)
        return Client()

    monkeypatch.setenv("API_AUTH_KEY", "demo-workflow-key")
    monkeypatch.setattr(demo_flow.httpx, "Client", client_factory)

    demo_flow.run_generation("http://tacit.test", "checkout latency", echo=lambda _message: None)

    assert observed_client_options == [
        {
            "base_url": "http://tacit.test",
            "timeout": 180.0,
            "headers": {"X-API-Key": "demo-workflow-key"},
            "trust_env": False,
            "follow_redirects": False,
        }
    ]


def test_compose_up_wraps_missing_docker(tmp_path, monkeypatch):
    def missing_docker(*args, **kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(demo_flow.subprocess, "run", missing_docker)

    with pytest.raises(demo_flow.DemoError, match="Docker installed and on PATH"):
        demo_flow.compose_up(tmp_path, echo=lambda _msg: None)


def test_request_wraps_http_status_error_with_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, request=request, text="missing api key")

    transport = httpx.MockTransport(handler)
    with httpx.Client(base_url="http://demo.test", transport=transport) as client:
        with pytest.raises(demo_flow.DemoError, match="POST http://demo.test/api/v1/chart returned HTTP 401"):
            demo_flow._request(client, "POST", "/api/v1/chart", {"prompt": "checkout"})
