"""One-command demo orchestration for `tacit demo`.

Boots the local dev compose stack (Grafana + Prometheus + fake checkout
metrics + Tacit), waits for health, runs the learning flow against the bundled
checkout-incident dashboard, and generates a fresh investigation dashboard
from a plain-English prompt.

Zero-key friendly: when no LLM API key is configured, the server falls back to
deterministic intent classification and the archetype engine compiles the
dashboard without any LLM calls (see ``tacit.agents.intent_fallback``).
"""

from __future__ import annotations

import json
import os
import re
import secrets
import socket
import subprocess
import threading
import time
import webbrowser
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx

DEMO_PROMPT = (
    "checkout-service is in an incident: p95 latency is spiking after deploy, "
    "5xx errors are rising on payment routes, and requests are piling up. "
    "Build the dashboard before creating anything noisy."
)

DEFAULT_API_URL = "http://localhost:8000"
DEFAULT_GRAFANA_URL = "http://localhost:3000"
DEMO_TEARDOWN_INTERPOLATION_KEY = "unused-demo-teardown-key"
_DEMO_UI_HANDOFF_MAX_KEY_BYTES = 8192
_DEMO_UI_HANDOFF_MAX_MESSAGE_BYTES = 1024
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_DEMO_AUTH_BOOTSTRAP_TYPE = "tacit-demo-auth-bootstrap-v3"
_DEMO_AUTH_READY_TYPE = "tacit-demo-auth-ready-v3"
_DEMO_AUTH_REDEEM_TYPE = "tacit-demo-auth-redeem-v3"
_DEMO_AUTH_DELIVERY_TYPE = "tacit-demo-auth-deliver-v3"
_DEMO_AUTH_ACCEPTED_TYPE = "tacit-demo-auth-accepted-v3"
_DEMO_AUTH_COMMITTED_TYPE = "tacit-demo-auth-committed-v3"
_DEMO_AUTH_ACTIVATED_TYPE = "tacit-demo-auth-activated-v3"
_DEMO_AUTH_CONFIRMED_TYPE = "tacit-demo-auth-confirmed-v3"
_DEMO_AUTH_FINALIZED_TYPE = "tacit-demo-auth-finalized-v3"
_DEMO_AUTH_FAILED_TYPE = "tacit-demo-auth-failed-v3"
_DEMO_AUTH_NONCE_RE = re.compile(r"[A-Za-z0-9_-]{32,128}\Z")

Echo = Callable[[str], None]
BrowserOpen = Callable[..., bool]


class DemoError(RuntimeError):
    """Raised when a demo step fails with a user-facing message."""


class DemoHandoffIndeterminateError(DemoError):
    """Raised when browser authority may be active but final receipt is unknown."""


def find_repo_root(start: Path | None = None) -> Path | None:
    """Locate a Tacit checkout containing the demo stack.

    Checks TACIT_REPO, then the current directory and its parents, then the
    package's own parent (editable installs run from the checkout).
    """
    candidates: list[Path] = []
    env_root = os.environ.get("TACIT_REPO")
    if env_root:
        candidates.append(Path(env_root))
    base = (start or Path.cwd()).resolve()
    candidates.extend([base, *base.parents])
    candidates.append(Path(__file__).resolve().parent.parent)

    for candidate in candidates:
        if (candidate / "docker-compose.dev.yml").is_file() and (candidate / "demo").is_dir():
            return candidate
    return None


def _compose_command(root: Path) -> list[str]:
    return ["docker", "compose", "-f", str(root / "docker-compose.dev.yml")]


def load_demo_env(root: Path) -> None:
    """Load repo-local demo secrets so CLI requests match compose env_file."""
    env_file = root / ".env"
    if env_file.exists():
        from dotenv import load_dotenv

        load_dotenv(env_file, override=False)
    if not os.environ.get("API_AUTH_KEY"):
        os.environ["API_AUTH_KEY"] = secrets.token_urlsafe(32)


def _validated_demo_ui_url(api_url: str) -> tuple[str, str]:
    try:
        parsed = urlsplit(api_url)
        port = parsed.port
    except ValueError as exc:
        raise DemoError("The demo Web UI URL is invalid.") from exc
    hostname = (parsed.hostname or "").casefold()
    if (
        parsed.scheme not in {"http", "https"}
        or hostname not in _LOOPBACK_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or port is None
    ):
        raise DemoError("The authenticated demo Web UI handoff is available only for an explicit loopback URL.")
    canonical_host = f"[{hostname}]" if ":" in hostname else hostname
    scheme = parsed.scheme.casefold()
    canonical_netloc = (
        canonical_host if (scheme, port) in {("http", 80), ("https", 443)} else f"{canonical_host}:{port}"
    )
    target = urlunsplit((scheme, canonical_netloc, "/", "", ""))
    bootstrap_host = "localhost" if hostname == "localhost" else "127.0.0.1"
    return target, bootstrap_host


def _demo_ui_handoff_document(
    api_url: str,
    *,
    frame_path: str,
    csp_nonce: str,
    expires_at_ms: int,
) -> bytes:
    script = f"""
      (() => {{
        'use strict';
        const targetUrl = {json.dumps(api_url)};
        const targetOrigin = new URL(targetUrl).origin;
        const bootstrapOrigin = window.location.origin;
        const framePath = {json.dumps(frame_path)};
        const serverDeadlineMs = {expires_at_ms};
        const monotonicEpochMs = () => window.performance.timeOrigin + window.performance.now();
        if (monotonicEpochMs() >= serverDeadlineMs) return;
        const descriptor = encodeURIComponent(JSON.stringify({{
          type: {_DEMO_AUTH_BOOTSTRAP_TYPE!r},
          bootstrapOrigin,
          targetOrigin,
          frameUrl: new URL(framePath, bootstrapOrigin).href,
          expiresAtMs: serverDeadlineMs,
        }}));
        const destination = new URL(targetUrl);
        destination.hash = `tacit-demo-auth-v3=${{descriptor}}`;
        window.location.replace(destination.href);
      }})();
    """
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="referrer" content="origin">'
        "<title>Opening Tacit</title></head><body>"
        f'<script nonce="{csp_nonce}">{script}</script>'
        "</body></html>"
    ).encode()


def _demo_ui_handoff_frame_document(
    target_origin: str,
    *,
    redeem_path: str,
    complete_path: str,
    failure_path: str,
    csp_nonce: str,
    expires_at_ms: int,
) -> bytes:
    script = f"""
      (() => {{
        'use strict';
        const targetOrigin = {json.dumps(target_origin)};
        const redeemPath = {json.dumps(redeem_path)};
        const completePath = {json.dumps(complete_path)};
        const failurePath = {json.dumps(failure_path)};
        const serverDeadlineMs = {expires_at_ms};
        const monotonicEpochMs = () => window.performance.timeOrigin + window.performance.now();
        let handoffNonce = null;
        let transientApiKey = null;
        let settled = false;
        let state = 'waiting';

        const validNonce = value => (
          typeof value === 'string' && /^[A-Za-z0-9_-]{{32,128}}$/.test(value)
        );
        const postHandoff = (path, type) => fetch(path, {{
          method: 'POST',
          body: JSON.stringify({{type, nonce: handoffNonce}}),
          cache: 'no-store',
          credentials: 'omit',
          redirect: 'error',
          referrerPolicy: 'no-referrer',
        }});
        const failClosed = async () => {{
          if (settled) return;
          settled = true;
          transientApiKey = null;
          try {{
            if (handoffNonce !== null) await postHandoff(failurePath, {_DEMO_AUTH_FAILED_TYPE!r});
          }} catch (_error) {{
            // The Python-side deadline remains the final failure authority.
          }}
          window.parent.postMessage(
            {{type: {_DEMO_AUTH_FAILED_TYPE!r}, nonce: handoffNonce}},
            targetOrigin,
          );
          window.removeEventListener('message', receiveMessage);
        }};

        async function receiveMessage(event) {{
          if (settled || event.source !== window.parent || event.origin !== targetOrigin) return;
          const message = event.data;
          if (!message || typeof message !== 'object') return;
          if (message.type === {_DEMO_AUTH_FAILED_TYPE!r}) {{
            if (handoffNonce !== null && message.nonce !== handoffNonce) return;
            if (handoffNonce === null && !validNonce(message.nonce)) return;
            handoffNonce = message.nonce;
            await failClosed();
            return;
          }}
          if (message.type === {_DEMO_AUTH_ACCEPTED_TYPE!r}) {{
            if (state !== 'delivered' || handoffNonce === null || message.nonce !== handoffNonce) return;
            if (monotonicEpochMs() >= serverDeadlineMs) {{
              await failClosed();
              return;
            }}
            state = 'preparing';
            try {{
              const response = await postHandoff(completePath, {_DEMO_AUTH_ACCEPTED_TYPE!r});
              if (response.status !== 204 || response.url !== new URL(completePath, window.location.origin).href) {{
                throw new Error('handoff preparation rejected');
              }}
              if (monotonicEpochMs() >= serverDeadlineMs) throw new Error('expired handoff');
              state = 'prepared';
              window.parent.postMessage(
                {{type: {_DEMO_AUTH_COMMITTED_TYPE!r}, nonce: handoffNonce}},
                targetOrigin,
              );
            }} catch (_error) {{
              await failClosed();
            }}
            return;
          }}
          if (message.type === {_DEMO_AUTH_ACTIVATED_TYPE!r}) {{
            if (state !== 'prepared' || handoffNonce === null || message.nonce !== handoffNonce) return;
            if (monotonicEpochMs() >= serverDeadlineMs) {{
              await failClosed();
              return;
            }}
            state = 'activating';
            try {{
              const response = await postHandoff(completePath, {_DEMO_AUTH_ACTIVATED_TYPE!r});
              if (response.status !== 204 || response.url !== new URL(completePath, window.location.origin).href) {{
                throw new Error('handoff activation rejected');
              }}
              if (monotonicEpochMs() >= serverDeadlineMs) throw new Error('expired handoff');
              state = 'confirming';
              transientApiKey = null;
              window.parent.postMessage(
                {{type: {_DEMO_AUTH_CONFIRMED_TYPE!r}, nonce: handoffNonce}},
                targetOrigin,
              );
            }} catch (_error) {{
              await failClosed();
            }}
            return;
          }}
          if (message.type === {_DEMO_AUTH_FINALIZED_TYPE!r}) {{
            if (state !== 'confirming' || handoffNonce === null || message.nonce !== handoffNonce) return;
            if (monotonicEpochMs() >= serverDeadlineMs) {{
              await failClosed();
              return;
            }}
            state = 'finalizing';
            try {{
              const response = await postHandoff(completePath, {_DEMO_AUTH_FINALIZED_TYPE!r});
              if (response.status !== 204 || response.url !== new URL(completePath, window.location.origin).href) {{
                throw new Error('handoff finalization rejected');
              }}
              settled = true;
              state = 'finalized';
              window.parent.postMessage(
                {{type: {_DEMO_AUTH_FINALIZED_TYPE!r}, nonce: handoffNonce}},
                targetOrigin,
              );
              window.removeEventListener('message', receiveMessage);
            }} catch (_error) {{
              await failClosed();
            }}
            return;
          }}
          if (message.type !== {_DEMO_AUTH_REDEEM_TYPE!r} || handoffNonce !== null || state !== 'waiting') return;
          if (!validNonce(message.nonce) || monotonicEpochMs() >= serverDeadlineMs) {{
            await failClosed();
            return;
          }}
          handoffNonce = message.nonce;
          state = 'redeeming';
          try {{
            const response = await postHandoff(redeemPath, {_DEMO_AUTH_REDEEM_TYPE!r});
            if (!response.ok || response.url !== new URL(redeemPath, window.location.origin).href) {{
              throw new Error('handoff redemption rejected');
            }}
            const payload = await response.json();
            if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {{
              throw new Error('invalid handoff payload');
            }}
            const payloadKeys = Object.keys(payload).sort().join(',');
            if (payloadKeys !== 'api_key,expires_at_ms,nonce,type') {{
              throw new Error('invalid handoff payload');
            }}
            if (payload.type !== {_DEMO_AUTH_DELIVERY_TYPE!r} || payload.nonce !== handoffNonce) {{
              throw new Error('invalid handoff identity');
            }}
            if (!Number.isSafeInteger(payload.expires_at_ms) || payload.expires_at_ms !== serverDeadlineMs) {{
              throw new Error('invalid handoff expiry');
            }}
            if (monotonicEpochMs() >= serverDeadlineMs) throw new Error('expired handoff');
            if (
              typeof payload.api_key !== 'string' || !payload.api_key ||
              /[\\u0000-\\u001f\\u007f]/.test(payload.api_key) ||
              new TextEncoder().encode(payload.api_key).byteLength > {_DEMO_UI_HANDOFF_MAX_KEY_BYTES}
            ) {{
              throw new Error('invalid handoff key');
            }}
            transientApiKey = payload.api_key;
            state = 'delivered';
            window.parent.postMessage(
              {{
                type: {_DEMO_AUTH_DELIVERY_TYPE!r},
                nonce: handoffNonce,
                apiKey: transientApiKey,
                expiresAtMs: serverDeadlineMs,
              }},
              targetOrigin,
            );
            transientApiKey = null;
          }} catch (_error) {{
            await failClosed();
          }}
        }}

        if (window.parent === window || monotonicEpochMs() >= serverDeadlineMs) return;
        window.addEventListener('message', receiveMessage);
        window.parent.postMessage({{type: {_DEMO_AUTH_READY_TYPE!r}}}, targetOrigin);
      }})();
    """
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="referrer" content="no-referrer">'
        "<title>Tacit authentication handoff</title></head><body>"
        f'<script nonce="{csp_nonce}">{script}</script>'
        "</body></html>"
    ).encode()


def open_authenticated_demo_ui(
    api_url: str,
    *,
    api_key: str,
    open_browser: BrowserOpen = webbrowser.open,
    timeout_s: float = 15.0,
) -> None:
    """Open the loopback UI with a one-shot, URL-free credential handoff."""
    target_url, bootstrap_host = _validated_demo_ui_url(api_url)
    key_size = len(api_key.encode("utf-8"))
    if not api_key or key_size > _DEMO_UI_HANDOFF_MAX_KEY_BYTES:
        raise DemoError("The demo API key cannot be handed to the Web UI.")
    if timeout_s <= 0:
        raise DemoError("The demo Web UI handoff timeout must be positive.")

    handoff_path = f"/{secrets.token_urlsafe(24)}"
    frame_path = f"/{secrets.token_urlsafe(24)}"
    redeem_path = f"/{secrets.token_urlsafe(24)}"
    complete_path = f"/{secrets.token_urlsafe(24)}"
    failure_path = f"/{secrets.token_urlsafe(24)}"
    handoff_csp_nonce = secrets.token_urlsafe(18)
    frame_csp_nonce = secrets.token_urlsafe(18)
    deadline = time.monotonic() + timeout_s
    expires_at_ms = int((time.time() + max(deadline - time.monotonic(), 0.0)) * 1000)
    document = _demo_ui_handoff_document(
        target_url,
        frame_path=frame_path,
        csp_nonce=handoff_csp_nonce,
        expires_at_ms=expires_at_ms,
    )
    target_origin = target_url.removesuffix("/")
    frame_document = _demo_ui_handoff_frame_document(
        target_origin,
        redeem_path=redeem_path,
        complete_path=complete_path,
        failure_path=failure_path,
        csp_nonce=frame_csp_nonce,
        expires_at_ms=expires_at_ms,
    )
    served = False
    frame_served = False
    redeemed = False
    redeemed_nonce: str | None = None
    prepared = False
    activated = False
    outcome: str | None = None
    bootstrap_origin: str | None = None

    class HandoffHandler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            remaining = max(deadline - time.monotonic(), 0.0)
            self.connection.settimeout(max(remaining, 0.001))
            self._deadline_timer = threading.Timer(remaining, self._expire_connection)
            self._deadline_timer.daemon = True
            self._deadline_timer.start()

        def finish(self) -> None:
            self._deadline_timer.cancel()
            super().finish()

        def _expire_connection(self) -> None:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            nonlocal frame_served, served
            if time.monotonic() >= deadline:
                self.close_connection = True
                self._send_empty(410)
                return
            if self.path == frame_path:
                if not served or frame_served:
                    self._send_empty(410)
                    return
                if self.headers.get("Referer") != target_url:
                    self._send_empty(403)
                    return
                frame_served = self._send_document(
                    frame_document,
                    csp=(
                        f"default-src 'none'; connect-src 'self'; script-src 'nonce-{frame_csp_nonce}'; "
                        f"base-uri 'none'; frame-ancestors {target_origin}"
                    ),
                    referrer_policy="no-referrer",
                    frame_options=None,
                )
                return
            if self.path != handoff_path:
                self._send_empty(404)
                return
            if served:
                self._send_empty(410)
                return
            served = self._send_document(
                document,
                csp=(
                    f"default-src 'none'; script-src 'nonce-{handoff_csp_nonce}'; "
                    "base-uri 'none'; frame-ancestors 'none'"
                ),
                referrer_policy="origin",
                frame_options="DENY",
            )

        def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
            nonlocal activated, outcome, prepared, redeemed, redeemed_nonce
            if time.monotonic() >= deadline:
                self.close_connection = True
                self._send_empty(410)
                return
            if bootstrap_origin is None or self.headers.get("Origin") != bootstrap_origin:
                self._send_empty(403)
                return
            message = self._read_message()
            if message is None:
                return

            if self.path == redeem_path:
                if not frame_served:
                    self._send_empty(409)
                    return
                if redeemed or outcome is not None:
                    self._send_empty(410)
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._send_empty(410)
                    return
                nonce = self._message_nonce(message, _DEMO_AUTH_REDEEM_TYPE)
                if nonce is None:
                    self._send_empty(400)
                    return
                redeemed = True
                redeemed_nonce = nonce
                payload = json.dumps(
                    {
                        "type": _DEMO_AUTH_DELIVERY_TYPE,
                        "nonce": nonce,
                        "api_key": api_key,
                        "expires_at_ms": expires_at_ms,
                    },
                    separators=(",", ":"),
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                try:
                    self.wfile.write(payload)
                    self.wfile.flush()
                except OSError:
                    return
                return

            if self.path == complete_path:
                if not redeemed:
                    self._send_empty(409)
                    return
                if outcome is not None:
                    self._send_empty(410)
                    return
                if message.get("type") == _DEMO_AUTH_ACCEPTED_TYPE:
                    if prepared:
                        self._send_empty(410)
                        return
                    nonce = self._message_nonce(message, _DEMO_AUTH_ACCEPTED_TYPE)
                    if nonce is None or nonce != redeemed_nonce:
                        self._send_empty(409)
                        return
                    if self._send_empty(204):
                        prepared = True
                    return
                if message.get("type") == _DEMO_AUTH_ACTIVATED_TYPE:
                    if not prepared or activated:
                        self._send_empty(409)
                        return
                    nonce = self._message_nonce(message, _DEMO_AUTH_ACTIVATED_TYPE)
                    if nonce is None or nonce != redeemed_nonce:
                        self._send_empty(409)
                        return
                    if self._send_empty(204):
                        activated = True
                    return
                if message.get("type") == _DEMO_AUTH_FINALIZED_TYPE:
                    if not activated:
                        self._send_empty(409)
                        return
                    nonce = self._message_nonce(message, _DEMO_AUTH_FINALIZED_TYPE)
                    if nonce is None or nonce != redeemed_nonce:
                        self._send_empty(409)
                        return
                    outcome = "complete"
                    self._send_empty(204)
                    return
                self._send_empty(400)
                return

            if self.path == failure_path:
                if outcome is not None:
                    self._send_empty(410)
                    return
                nonce = self._message_nonce(message, _DEMO_AUTH_FAILED_TYPE)
                if nonce is None:
                    self._send_empty(400)
                    return
                if redeemed_nonce is not None and nonce != redeemed_nonce:
                    self._send_empty(409)
                    return
                if self._send_empty(204):
                    outcome = "indeterminate" if activated else "browser-failed"
                return

            self._send_empty(404)

        def _read_message(self) -> dict[str, object] | None:
            try:
                content_length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                self._send_empty(400)
                return None
            if (
                content_length <= 0
                or content_length > _DEMO_UI_HANDOFF_MAX_MESSAGE_BYTES
                or self.headers.get("Transfer-Encoding") is not None
            ):
                self.close_connection = True
                self._send_empty(400)
                return None
            try:
                body = self.rfile.read(content_length)
                if len(body) != content_length:
                    raise ValueError
                value = json.loads(body.decode("utf-8", errors="strict"))
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                self._send_empty(400)
                return None
            if not isinstance(value, dict):
                self._send_empty(400)
                return None
            return value

        @staticmethod
        def _message_nonce(message: dict[str, object], expected_type: str) -> str | None:
            if set(message) != {"type", "nonce"} or message.get("type") != expected_type:
                return None
            nonce = message.get("nonce")
            if not isinstance(nonce, str) or _DEMO_AUTH_NONCE_RE.fullmatch(nonce) is None:
                return None
            return nonce

        def _send_document(
            self,
            body: bytes,
            *,
            csp: str,
            referrer_policy: str,
            frame_options: str | None,
        ) -> bool:
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Referrer-Policy", referrer_policy)
                if frame_options is not None:
                    self.send_header("X-Frame-Options", frame_options)
                self.send_header("Content-Security-Policy", csp)
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
            except OSError:
                return False
            return True

        def _send_empty(self, status: int) -> bool:
            try:
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.flush()
            except OSError:
                return False
            return True

        def log_message(self, _format: str, *_args: object) -> None:
            return

    with HTTPServer(("127.0.0.1", 0), HandoffHandler) as server:
        bootstrap_origin = f"http://{bootstrap_host}:{server.server_port}"
        bootstrap_url = f"http://{bootstrap_host}:{server.server_port}{handoff_path}"
        try:
            opened = open_browser(bootstrap_url, new=2)
        except Exception as exc:  # noqa: BLE001 - browser launchers vary by platform
            raise DemoError(f"Could not open the demo Web UI ({type(exc).__name__}).") from exc
        if not opened:
            raise DemoError("Could not open the demo Web UI.")
        while outcome is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            server.timeout = remaining
            server.handle_request()

    if outcome == "browser-failed":
        raise DemoError("The browser could not open or complete the authenticated demo Web UI handoff.")
    if outcome == "indeterminate" or (outcome is None and activated):
        raise DemoHandoffIndeterminateError(
            "The browser may have activated the demo credential, but final handoff receipt could not be confirmed."
        )
    if outcome != "complete":
        raise DemoError("Timed out handing the demo credential to the Web UI.")


def compose_up(root: Path, *, echo: Echo, build: bool = True) -> None:
    cmd = [*_compose_command(root), "up", "-d"]
    if build:
        cmd.append("--build")
    echo(f"$ {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, cwd=root)
    except FileNotFoundError as exc:
        raise DemoError(
            "docker compose failed. Is Docker installed and on PATH? "
            "Install Docker Desktop or the docker CLI, then re-run `tacit demo`."
        ) from exc
    if result.returncode != 0:
        raise DemoError(
            "docker compose failed. Is Docker running? "
            "Install Docker Desktop or the docker CLI, then re-run `tacit demo`."
        )


def compose_down(root: Path, *, echo: Echo) -> None:
    cmd = [*_compose_command(root), "down"]
    echo(f"$ {' '.join(cmd)}")
    child_env = os.environ.copy()
    child_env["API_AUTH_KEY"] = DEMO_TEARDOWN_INTERPOLATION_KEY
    try:
        result = subprocess.run(cmd, cwd=root, env=child_env, check=False)
    except FileNotFoundError as exc:
        raise DemoError("docker compose down failed because Docker is not installed or not on PATH.") from exc
    if result.returncode != 0:
        raise DemoError("docker compose down failed. The demo stack may still be running.")


def wait_for_http(url: str, *, timeout_s: float = 240.0, echo: Echo) -> None:
    """Poll *url* anonymously until it returns 2xx or *timeout_s* elapses."""
    deadline = time.monotonic() + timeout_s
    last_error = ""
    with httpx.Client(timeout=5.0, follow_redirects=False, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get(url)
                if 200 <= response.status_code < 300:
                    return
                if response.is_redirect:
                    last_error = f"HTTP {response.status_code} redirect rejected"
                else:
                    last_error = f"HTTP {response.status_code}"
            except Exception as exc:  # noqa: BLE001 — report any connection failure
                last_error = type(exc).__name__
            time.sleep(2.0)
    raise DemoError(f"Timed out waiting for {url} ({last_error}). Check `docker compose logs`.")


def _auth_headers() -> dict[str, str]:
    api_key = os.environ.get("API_AUTH_KEY", "")
    return {"X-API-Key": api_key} if api_key else {}


def _authenticated_client(api_url: str, *, timeout_s: float) -> httpx.Client:
    return httpx.Client(
        base_url=api_url,
        timeout=timeout_s,
        headers=_auth_headers(),
        trust_env=False,
        follow_redirects=False,
    )


def _raise_demo_http_error(exc: httpx.HTTPStatusError) -> None:
    response = exc.response
    method = response.request.method
    url = response.request.url
    body = response.text.strip()
    detail = body[:500] if body else response.reason_phrase
    raise DemoError(f"Demo API request failed: {method} {url} returned HTTP {response.status_code}: {detail}") from exc


def _request(client: httpx.Client, method: str, path: str, payload: dict | None = None) -> dict:
    response = client.request(method, path, json=payload)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_demo_http_error(exc)
    return response.json() if response.content else {}


def run_learning_flow(api_url: str, dashboard_json: Path, *, echo: Echo) -> str:
    """Upload + approve the known-good incident dashboard. Returns its UID."""
    dashboard = json.loads(dashboard_json.read_text())
    with _authenticated_client(api_url, timeout_s=60.0) as client:
        _request(client, "GET", "/healthz")
        echo("Health check passed")

        upload = _request(
            client,
            "POST",
            "/api/v1/learn/dashboard/json",
            {
                "vendor": "grafana",
                "source_name": dashboard_json.name,
                "auto_approve": False,
                "dashboard": dashboard,
            },
        )
        uid = upload.get("dashboard_uid", "")
        if not uid:
            raise DemoError(f"Dashboard upload returned no UID: {upload}")
        echo(f"Uploaded learning dashboard (uid={uid})")

        _request(client, "POST", f"/api/v1/learn/dashboards/{uid}/approve?backend=grafana_json")
        echo("Approved inferred signal mappings")
        return uid


def run_generation(api_url: str, prompt: str, *, echo: Echo) -> dict:
    """Generate the investigation dashboard from *prompt*."""
    with _authenticated_client(api_url, timeout_s=180.0) as client:
        echo("Generating investigation dashboard (this can take 15-60s)...")
        return _request(
            client,
            "POST",
            "/api/v1/chart",
            {"prompt": prompt, "user_id": "demo", "channel_id": "tacit-demo"},
        )


def record_demo_feedback(api_url: str, dashboard_uid: str) -> None:
    """Best-effort demo feedback so the improvement loop shows up in history."""
    try:
        with _authenticated_client(api_url, timeout_s=30.0) as client:
            _request(
                client,
                "POST",
                "/api/v1/feedback",
                {
                    "dashboard_uid": dashboard_uid,
                    "symptom_visibility": 5,
                    "root_cause_support": 4,
                    "noise_level": 4,
                    "investigation_speed": 5,
                    "overall_useful": True,
                    "comment": "Demo review: useful incident surface.",
                    "reviewer": "demo",
                },
            )
    except Exception:
        pass  # feedback is decorative in the demo
