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
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

import httpx

DEMO_PROMPT = (
    "checkout-service is in an incident: p95 latency is spiking after deploy, "
    "5xx errors are rising on payment routes, and requests are piling up. "
    "Build the dashboard before creating anything noisy."
)

DEFAULT_API_URL = "http://localhost:8000"
DEFAULT_GRAFANA_URL = "http://localhost:3000"

Echo = Callable[[str], None]


class DemoError(RuntimeError):
    """Raised when a demo step fails with a user-facing message."""


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


def compose_up(root: Path, *, echo: Echo, build: bool = True) -> None:
    cmd = [*_compose_command(root), "up", "-d"]
    if build:
        cmd.append("--build")
    echo(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=root)
    if result.returncode != 0:
        raise DemoError(
            "docker compose failed. Is Docker running? "
            "Install Docker Desktop or the docker CLI, then re-run `tacit demo`."
        )


def compose_down(root: Path, *, echo: Echo) -> None:
    cmd = [*_compose_command(root), "down"]
    echo(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=root, check=False)


def wait_for_http(url: str, *, timeout_s: float = 240.0, echo: Echo) -> None:
    """Poll *url* until it returns a 2xx/3xx response or *timeout_s* elapses."""
    deadline = time.monotonic() + timeout_s
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=5.0, follow_redirects=True)
            if response.status_code < 400:
                return
            last_error = f"HTTP {response.status_code}"
        except Exception as exc:  # noqa: BLE001 — report any connection failure
            last_error = type(exc).__name__
        time.sleep(2.0)
    raise DemoError(f"Timed out waiting for {url} ({last_error}). Check `docker compose logs`.")


def _auth_headers() -> dict[str, str]:
    api_key = os.environ.get("API_AUTH_KEY", "")
    return {"X-API-Key": api_key} if api_key else {}


def _request(client: httpx.Client, method: str, path: str, payload: dict | None = None) -> dict:
    response = client.request(method, path, json=payload)
    response.raise_for_status()
    return response.json() if response.content else {}


def run_learning_flow(api_url: str, dashboard_json: Path, *, echo: Echo) -> str:
    """Upload + approve the known-good incident dashboard. Returns its UID."""
    dashboard = json.loads(dashboard_json.read_text())
    with httpx.Client(base_url=api_url, timeout=60.0, headers=_auth_headers()) as client:
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
    with httpx.Client(base_url=api_url, timeout=180.0, headers=_auth_headers()) as client:
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
        with httpx.Client(base_url=api_url, timeout=30.0, headers=_auth_headers()) as client:
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
