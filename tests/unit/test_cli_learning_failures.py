from __future__ import annotations

from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tacit.cli import cli
from tacit.config import Settings


class _Stores:
    settings = Settings(_env_file=None)

    def signals(self):
        return SimpleNamespace()


@pytest.mark.parametrize(
    ("command", "patch_target", "expected"),
    [
        (
            ["learn", "runbooks", "--file", "{artifact}"],
            "tacit.artifact_learning.learn_runbook_file",
            "Runbook learning failed",
        ),
        (
            ["learn", "incidents", "--file", "{artifact}"],
            "tacit.artifact_learning.learn_incident_file",
            "Incident learning failed",
        ),
    ],
)
def test_artifact_learning_cli_failures_return_nonzero(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
    patch_target: str,
    expected: str,
):
    artifact = tmp_path / "artifact.md"
    artifact.write_text("# Artifact")

    def fail(*_args, **_kwargs):
        raise RuntimeError("injected learning failure")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: _Stores())
    monkeypatch.setattr(patch_target, fail)
    result = CliRunner().invoke(cli, [part.format(artifact=artifact) for part in command])

    assert result.exit_code != 0
    assert expected in result.output
    assert "injected learning failure" in result.output


@pytest.mark.parametrize(
    ("command", "patch_target", "expected"),
    [
        (
            ["learn", "alerts", "--from", "grafana"],
            "tacit.alert_ingest.learn_backend_alerts",
            "Alert ingestion failed",
        ),
        (
            ["learn", "grafana"],
            "tacit.dashboard_ingest.learn_backend_dashboards",
            "Bulk learning failed",
        ),
    ],
)
def test_backend_learning_cli_failures_return_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
    patch_target: str,
    expected: str,
):
    async def fail(*_args, **_kwargs):
        raise RuntimeError("injected backend failure")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: _Stores())
    monkeypatch.setattr(patch_target, fail)
    result = CliRunner().invoke(cli, command)

    assert result.exit_code != 0
    assert expected in result.output
    assert "injected backend failure" in result.output


def test_single_dashboard_learning_failure_is_nonzero_and_payload_safe(
    monkeypatch: pytest.MonkeyPatch,
):
    sensitive = "secret=" + ("x" * 10_000)

    async def fail(*_args, **_kwargs):
        raise RuntimeError(sensitive)

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: _Stores())
    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", fail)

    result = CliRunner().invoke(cli, ["learn", "dashboard", "dash-1"])

    assert result.exit_code != 0
    assert "Dashboard ingestion failed" in result.output
    assert "RuntimeError" in result.output
    assert "reference=" in result.output
    assert sensitive not in result.output
    assert len(result.output) < 1_000


@pytest.mark.parametrize("artifact_kind", ["runbooks", "incidents"])
def test_artifact_learning_cli_requires_exactly_one_input(artifact_kind: str):
    result = CliRunner().invoke(cli, ["learn", artifact_kind])

    assert result.exit_code != 0
    assert "Pass exactly one of --file or --dir" in result.output
