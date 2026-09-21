"""Repository-level CI security invariants."""

from __future__ import annotations

import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_gitleaks_scans_full_history_and_gitless_current_tree() -> None:
    workflow = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    secret_scan = workflow.split("  secret-scan:\n", 1)[1].split("\n  fresh-install:\n", 1)[0]

    assert "fetch-depth: 0" in secret_scan
    assert "persist-credentials: false" in secret_scan
    assert "git archive --format=tar" in secret_scan
    assert "GIT_CONFIG_KEY_0: safe.directory" in secret_scan
    assert "GIT_CONFIG_VALUE_0: /github/workspace" in secret_scan
    assert "--log-opts=--all" in secret_scan
    assert "--source /github/workspace/.gitleaks-worktree --no-git" in secret_scan
    assert "zricethezav/gitleaks@sha256:" in secret_scan


def test_gitleaks_ignores_are_exact_historical_fingerprints() -> None:
    entries = [
        line
        for line in (REPOSITORY_ROOT / ".gitleaksignore").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]

    assert entries
    assert len(entries) == len(set(entries))
    assert all(re.fullmatch(r"[0-9a-f]{40}:[^:]+:generic-api-key:[1-9][0-9]*", entry) for entry in entries)
