#!/usr/bin/env python3
"""Run and record the two release-quality evaluations without hidden spend."""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import http.client
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlsplit

from release_quality_contract import (
    ADMITTED_STATE_MANIFEST,
    ARCHIVED_STATE_MANIFEST,
    LONG_LIVED_STATE_DATABASES,
    READ_CHUNK_BYTES,
    STATE_SNAPSHOT_LIMITS,
    ReleaseQualityContractError,
    StateSnapshotLimits,
    build_admitted_state_manifest,
    logical_snapshot_fingerprint,
    validate_concrete_tenant,
    validate_snapshot_database_sizes,
    validate_source_database_sizes,
)

SPEND_ACKNOWLEDGEMENT = "AUTHORIZE UP TO 500 LLM REQUESTS"
MIN_LLM_REQUESTS = 400
MAX_LLM_REQUESTS = 500
MAX_LLM_REQUEST_BYTES = 2 * 1024 * 1024
MAX_LLM_RESPONSE_BYTES = 20 * 1024 * 1024
SHA_PATTERN = re.compile(r"[0-9a-f]{40}\Z")
REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
WORKFLOW_PATH = ".github/workflows/release-quality.yml"
CORPUS_PATH = "tests/tacit_validation_prompts.csv"
MAX_CORPUS_BYTES = 1024 * 1024


class ReleaseQualityError(RuntimeError):
    """The quality run cannot produce trustworthy release evidence."""


class LLMRequestBudgetExceeded(ReleaseQualityError):
    """The provider request boundary refused spend beyond the approved limit."""


class AdmittedLongLivedState(NamedTuple):
    """The single disposable logical generation consumed by evaluation."""

    directory: Path
    manifest_path: Path
    fingerprint: str
    tenant_id: str
    limits: StateSnapshotLimits


class LLMRequestBudget:
    """Atomically reserve each outbound provider request before forwarding it."""

    def __init__(self, *, max_requests: int) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        self._max_requests = max_requests
        self._attempted_requests = 0
        self._forwarded_requests = 0
        self._rejected_requests = 0
        self._lock = threading.Lock()

    def reserve(self) -> None:
        with self._lock:
            self._attempted_requests += 1
            if self._forwarded_requests >= self._max_requests:
                self._rejected_requests += 1
                raise LLMRequestBudgetExceeded("hard LLM request budget exhausted before provider request")
            self._forwarded_requests += 1

    def snapshot(self) -> dict[str, int | bool]:
        with self._lock:
            return {
                "max_requests": self._max_requests,
                "attempted_requests": self._attempted_requests,
                "forwarded_requests": self._forwarded_requests,
                "rejected_requests": self._rejected_requests,
                "exhausted": self._rejected_requests > 0,
            }


def _send_proxy_error(handler: BaseHTTPRequestHandler, status: int, message: str) -> None:
    payload = json.dumps({"error": message}, sort_keys=True).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.wfile.write(payload)


@contextlib.contextmanager
def _budgeted_llm_proxy(upstream_url: str, budget: LLMRequestBudget) -> Iterator[str]:
    upstream = urlsplit(upstream_url)
    upstream_hostname = upstream.hostname or ""
    if not upstream_hostname:
        raise ReleaseQualityError("release-quality LLM URL has no hostname")
    connection_type = http.client.HTTPSConnection if upstream.scheme == "https" else http.client.HTTPConnection
    upstream_port = upstream.port or (443 if upstream.scheme == "https" else 80)

    class BudgetedProviderHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            if self.path != "/api/chat" or self.headers.get("Transfer-Encoding") is not None:
                _send_proxy_error(self, 400, "only bounded POST /api/chat requests are allowed")
                return
            try:
                content_length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                _send_proxy_error(self, 400, "provider request Content-Length is invalid")
                return
            if content_length < 1 or content_length > MAX_LLM_REQUEST_BYTES:
                _send_proxy_error(self, 413, "provider request exceeds the size limit")
                return
            request_payload = self.rfile.read(content_length)
            if len(request_payload) != content_length:
                _send_proxy_error(self, 400, "provider request body is incomplete")
                return
            try:
                budget.reserve()
            except LLMRequestBudgetExceeded as exc:
                _send_proxy_error(self, 429, str(exc))
                return

            connection = connection_type(upstream_hostname, upstream_port, timeout=180)
            try:
                connection.request(
                    "POST",
                    "/api/chat",
                    body=request_payload,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                )
                response = connection.getresponse()
                declared = response.getheader("Content-Length")
                if declared is not None and (int(declared) < 0 or int(declared) > MAX_LLM_RESPONSE_BYTES):
                    raise ReleaseQualityError("provider response exceeds the size limit")
                response_payload = response.read(MAX_LLM_RESPONSE_BYTES + 1)
                if len(response_payload) > MAX_LLM_RESPONSE_BYTES:
                    raise ReleaseQualityError("provider response exceeds the size limit")
                self.send_response(response.status)
                self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(response_payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(response_payload)
            except (OSError, http.client.HTTPException, ReleaseQualityError, ValueError) as exc:
                _send_proxy_error(self, 502, f"provider request failed: {type(exc).__name__}")
            finally:
                connection.close()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), BudgetedProviderHandler)
    thread = threading.Thread(target=server.serve_forever, name="release-quality-llm-budget")
    try:
        thread.start()
    except RuntimeError as exc:
        server.server_close()
        raise ReleaseQualityError("could not start the LLM request budget owner") from exc
    try:
        port = server.server_address[1]
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(READ_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_loopback_endpoint(value: str, label: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ReleaseQualityError(f"{label} is invalid") from exc
    hostname = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or hostname not in {"localhost", "127.0.0.1", "::1"}
        or parsed.username is not None
        or parsed.password is not None
        or (port is not None and not 1 <= port <= 65535)
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ReleaseQualityError(f"{label} must be an explicit loopback HTTP(S) endpoint")
    return value


def _validate_corpus(path: Path, expected_prompt_count: int) -> list[str]:
    if path.is_symlink() or not path.is_file():
        raise ReleaseQualityError("release-quality corpus must be a regular file")
    if not 1 <= path.stat().st_size <= MAX_CORPUS_BYTES:
        raise ReleaseQualityError("release-quality corpus exceeds the size limit")
    try:
        with path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            required = {
                "prompt_id",
                "prompt",
                "expected_archetype",
                "expected_metrics",
                "expected_datasources",
                "difficulty",
                "validation_goal",
                "critical_metrics",
            }
            if not required.issubset(reader.fieldnames or []):
                raise ReleaseQualityError("release-quality corpus is missing required columns")
            rows = list(reader)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ReleaseQualityError("release-quality corpus could not be parsed") from exc
    if any(None in row for row in rows):
        raise ReleaseQualityError("release-quality corpus contains extra columns")
    prompt_ids = [value.strip() if isinstance(value := row.get("prompt_id"), str) else "" for row in rows]
    if len(prompt_ids) != expected_prompt_count:
        raise ReleaseQualityError(f"release-quality corpus must contain exactly {expected_prompt_count} prompts")
    if any(not prompt_id for prompt_id in prompt_ids) or len(set(prompt_ids)) != len(prompt_ids):
        raise ReleaseQualityError("release-quality corpus prompt IDs must be nonempty and unique")
    return prompt_ids


def _validate_tenant(value: str) -> str:
    try:
        return validate_concrete_tenant(value)
    except ReleaseQualityContractError as exc:
        raise ReleaseQualityError(str(exc)) from None


def _long_lived_state_paths(directory: Path) -> tuple[Path, ...]:
    if directory.is_symlink() or not directory.is_dir():
        raise ReleaseQualityError("long-lived state directory must be a real directory")
    paths: list[Path] = []
    for name in LONG_LIVED_STATE_DATABASES:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ReleaseQualityError(f"long-lived state is missing regular database {name}")
        paths.append(path)
    return tuple(paths)


def _validate_state_database_sizes(
    paths: tuple[Path, ...],
    *,
    limits: StateSnapshotLimits = STATE_SNAPSHOT_LIMITS,
) -> int:
    try:
        return validate_snapshot_database_sizes(paths, limits=limits)
    except ReleaseQualityContractError as exc:
        raise ReleaseQualityError(str(exc)) from None


def _logical_snapshot_fingerprint(directory: Path) -> str:
    """Return the fingerprint of the one disposable generation actually evaluated."""
    paths = _long_lived_state_paths(directory)
    _validate_state_database_sizes(paths)
    try:
        return logical_snapshot_fingerprint(paths)
    except OSError as exc:
        raise ReleaseQualityError("long-lived state snapshot could not be fingerprinted") from exc


@contextlib.contextmanager
def _admitted_long_lived_state(
    source_directory: Path,
    *,
    tenant_id: str,
    limits: StateSnapshotLimits = STATE_SNAPSHOT_LIMITS,
) -> Iterator[AdmittedLongLivedState]:
    """Create and retain one disposable SQLite generation for the full evaluation."""
    from tacit.sqlite_identity import snapshot_sqlite_database_set

    concrete_tenant = _validate_tenant(tenant_id)
    source_paths = _long_lived_state_paths(source_directory)
    try:
        validate_source_database_sizes(source_paths, limits=limits)
    except ReleaseQualityContractError as exc:
        raise ReleaseQualityError(str(exc)) from None
    with tempfile.TemporaryDirectory(prefix="tacit-release-quality-state-") as temporary:
        snapshot_directory = Path(temporary)
        try:
            snapshot_sqlite_database_set(
                source_paths,
                snapshot_directory,
                timeout_ms=300_000,
                snapshot_max_bytes=limits.max_database_bytes,
                snapshot_total_max_bytes=limits.max_total_bytes,
            )
        except Exception as exc:
            raise ReleaseQualityError(
                f"long-lived state could not be admitted as one SQLite generation: {type(exc).__name__}"
            ) from exc
        snapshot_paths = _long_lived_state_paths(snapshot_directory)
        _validate_state_database_sizes(snapshot_paths, limits=limits)
        fingerprint = _logical_snapshot_fingerprint(snapshot_directory)
        manifest_path = snapshot_directory / ADMITTED_STATE_MANIFEST
        manifest_path.write_text(
            json.dumps(
                build_admitted_state_manifest(
                    fingerprint=fingerprint,
                    tenant_id=concrete_tenant,
                    limits=limits,
                ),
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        yield AdmittedLongLivedState(
            directory=snapshot_directory,
            manifest_path=manifest_path,
            fingerprint=fingerprint,
            tenant_id=concrete_tenant,
            limits=limits,
        )


def _minimal_child_environment() -> dict[str, str]:
    environment = {
        "HOME": os.environ.get("HOME", ""),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "PATH": os.environ.get("PATH", ""),
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
    }
    for name in ("LC_ALL", "SSL_CERT_FILE", "TMPDIR"):
        if value := os.environ.get(name):
            environment[name] = value
    return environment


def _load_report(path: Path, *, state: str, prompt_ids: list[str], tenant_id: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ReleaseQualityError(f"{state} evaluation did not produce a regular report file")
    try:
        loaded = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseQualityError(f"{state} evaluation did not produce a valid JSON report") from exc
    if not isinstance(loaded, dict):
        raise ReleaseQualityError(f"{state} evaluation report is not an object")
    if loaded.get("prompt_count") != len(prompt_ids) or loaded.get("mode") != "all":
        raise ReleaseQualityError(f"{state} evaluation report does not cover the full corpus")
    selected_state = loaded.get("state")
    if (
        not isinstance(selected_state, dict)
        or selected_state.get("mode") != state
        or selected_state.get("tenant") != tenant_id
    ):
        raise ReleaseQualityError(f"{state} evaluation report has the wrong state identity")
    gate = loaded.get("gate")
    if not isinstance(gate, dict) or gate.get("passed") is not True:
        raise ReleaseQualityError(f"{state} evaluation quality gate did not pass")
    for section_name in ("archetype", "pipeline"):
        section = loaded.get(section_name)
        if not isinstance(section, dict) or section.get("total") != len(prompt_ids):
            raise ReleaseQualityError(f"{state} evaluation report has incomplete {section_name} results")
        details = section.get("details")
        if (
            not isinstance(details, list)
            or [item.get("prompt_id") if isinstance(item, dict) else None for item in details] != prompt_ids
        ):
            raise ReleaseQualityError(f"{state} evaluation report changed corpus ordering")
    return loaded


def _run_validation(
    *,
    repository: Path,
    corpus: Path,
    output: Path,
    state: str,
    state_directory: Path,
    tenant_id: str,
    admitted_state_manifest: Path | None,
    llm_url: str,
    llm_model: str,
    grafana_url: str,
    timeout_seconds: int,
) -> int:
    command = [
        sys.executable,
        "tests/validate.py",
        corpus.relative_to(repository).as_posix(),
        "--mode",
        "all",
        "--state",
        state,
        "--llm-url",
        llm_url,
        "--llm-model",
        llm_model,
        "--grafana-url",
        grafana_url,
        "--tenant",
        tenant_id,
        "--output",
        str(output),
    ]
    if state == "long-lived":
        command.extend(("--state-dir", str(state_directory)))
        if admitted_state_manifest is None:
            raise ReleaseQualityError("long-lived release evaluation requires an admitted-state manifest")
        command.extend(("--admitted-state-manifest", str(admitted_state_manifest)))
    try:
        completed = subprocess.run(
            command,
            cwd=repository,
            env=_minimal_child_environment(),
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        print(f"{state} release-quality evaluation failed to complete: {type(exc).__name__}", file=sys.stderr)
        return 2
    return completed.returncode


def _write_failure_summary(
    output_directory: Path,
    outcomes: dict[str, int],
    reason: str,
    llm_budget: dict[str, int | bool] | None = None,
) -> None:
    summary = {"schema_version": 1, "outcomes": outcomes, "result": "failed", "reason": reason}
    if llm_budget is not None:
        summary["llm_budget"] = llm_budget
    (output_directory / "failure-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--repository-slug", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--workflow-run-id", required=True, type=int)
    parser.add_argument("--workflow-run-attempt", required=True, type=int)
    parser.add_argument("--credit-spend-acknowledgement", required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--expected-prompt-count", type=int, default=100)
    parser.add_argument("--max-llm-requests", type=int, default=MAX_LLM_REQUESTS)
    parser.add_argument("--long-lived-state-dir", type=Path, required=True)
    parser.add_argument("--llm-url", required=True)
    parser.add_argument("--llm-model", required=True)
    parser.add_argument("--grafana-url", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--evaluation-timeout-seconds", type=int, default=6300)
    args = parser.parse_args(argv)

    output_directory = args.output_directory.resolve()
    outcomes: dict[str, int] = {}
    budget: LLMRequestBudget | None = None
    try:
        if args.credit_spend_acknowledgement != SPEND_ACKNOWLEDGEMENT:
            raise ReleaseQualityError("explicit external credit-spend acknowledgement is required")
        if SHA_PATTERN.fullmatch(args.expected_sha) is None:
            raise ReleaseQualityError("expected SHA must be a full lowercase commit SHA")
        if REPOSITORY_PATTERN.fullmatch(args.repository_slug) is None:
            raise ReleaseQualityError("repository slug is invalid")
        tenant_id = _validate_tenant(args.tenant)
        if args.workflow_run_id <= 0 or args.workflow_run_attempt <= 0:
            raise ReleaseQualityError("workflow run identity must be positive")
        if args.expected_prompt_count != 100:
            raise ReleaseQualityError("release quality requires exactly 100 prompts per state")
        if args.max_llm_requests != MAX_LLM_REQUESTS:
            raise ReleaseQualityError(f"release quality requires the fixed {MAX_LLM_REQUESTS}-request hard budget")
        if not args.llm_model.strip() or len(args.llm_model) > 200:
            raise ReleaseQualityError("release-quality model identity is invalid")
        if args.evaluation_timeout_seconds < 60 or args.evaluation_timeout_seconds > 10800:
            raise ReleaseQualityError("evaluation timeout must be between 60 and 10800 seconds")
        llm_url = _validate_loopback_endpoint(args.llm_url, "release-quality LLM URL")
        grafana_url = _validate_loopback_endpoint(args.grafana_url, "release-quality Grafana URL")

        repository = args.repository.resolve()
        head_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )
        if head_result.returncode != 0:
            raise ReleaseQualityError("could not resolve the checked-out release-quality commit")
        head = head_result.stdout.strip()
        if head != args.expected_sha:
            raise ReleaseQualityError("checked-out commit does not match the requested evidence SHA")
        status_result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )
        if status_result.returncode != 0 or status_result.stdout:
            raise ReleaseQualityError("release-quality checkout is not an exact clean worktree")

        if output_directory.exists() and any(output_directory.iterdir()):
            raise ReleaseQualityError("release-quality output directory must start empty")
        output_directory.mkdir(parents=True, exist_ok=True)
        supplied_corpus = args.corpus if args.corpus.is_absolute() else repository / args.corpus
        corpus = supplied_corpus.resolve()
        if corpus != (repository / CORPUS_PATH).resolve():
            raise ReleaseQualityError("release quality requires the canonical repository corpus")
        prompt_ids = _validate_corpus(corpus, args.expected_prompt_count)
        corpus_digest = _sha256(corpus)
        with _admitted_long_lived_state(
            args.long_lived_state_dir,
            tenant_id=tenant_id,
        ) as admitted_state:
            archived_state_manifest = output_directory / ARCHIVED_STATE_MANIFEST
            shutil.copyfile(admitted_state.manifest_path, archived_state_manifest, follow_symlinks=False)
            if _sha256(archived_state_manifest) != _sha256(admitted_state.manifest_path):
                raise ReleaseQualityError("archived long-lived state manifest digest does not match")

            report_paths = {
                "clean": output_directory / "clean-report.json",
                "long-lived": output_directory / "long-lived-report.json",
            }
            budget = LLMRequestBudget(max_requests=args.max_llm_requests)
            with _budgeted_llm_proxy(llm_url, budget) as metered_llm_url:
                for state in ("clean", "long-lived"):
                    outcomes[state] = _run_validation(
                        repository=repository,
                        corpus=corpus,
                        output=report_paths[state],
                        state=state,
                        state_directory=admitted_state.directory,
                        tenant_id=tenant_id,
                        admitted_state_manifest=(admitted_state.manifest_path if state == "long-lived" else None),
                        llm_url=metered_llm_url,
                        llm_model=args.llm_model,
                        grafana_url=grafana_url,
                        timeout_seconds=args.evaluation_timeout_seconds,
                    )
            budget_snapshot = budget.snapshot()
            if budget_snapshot["exhausted"]:
                raise ReleaseQualityError("hard LLM request budget was exhausted")
            if any(code != 0 for code in outcomes.values()):
                raise ReleaseQualityError("one or more release-quality evaluations failed")
            if int(budget_snapshot["forwarded_requests"]) < MIN_LLM_REQUESTS:
                raise ReleaseQualityError("release-quality evaluation made fewer provider requests than required")

            reports = {
                state: _load_report(path, state=state, prompt_ids=prompt_ids, tenant_id=tenant_id)
                for state, path in report_paths.items()
            }
            if _sha256(corpus) != corpus_digest:
                raise ReleaseQualityError("release-quality corpus changed during evaluation")
            if (
                reports["clean"]["state"]["fingerprint"]
                != hashlib.sha256(b"tacit-validation-state-v1:clean").hexdigest()
            ):
                raise ReleaseQualityError("clean report fingerprint is invalid")
            if reports["long-lived"]["state"]["fingerprint"] != admitted_state.fingerprint:
                raise ReleaseQualityError("long-lived report fingerprint differs from the admitted snapshot")

            archived_corpus = output_directory / CORPUS_PATH
            archived_corpus.parent.mkdir(parents=True)
            shutil.copyfile(corpus, archived_corpus, follow_symlinks=False)
            if _sha256(archived_corpus) != corpus_digest:
                raise ReleaseQualityError("archived release-quality corpus digest does not match")
            evidence = {
                "schema_version": 3,
                "repository": args.repository_slug,
                "commit_sha": args.expected_sha,
                "workflow": WORKFLOW_PATH,
                "workflow_run_id": args.workflow_run_id,
                "workflow_run_attempt": args.workflow_run_attempt,
                "evaluation": {
                    "mode": "all",
                    "model": args.llm_model,
                    "llm_endpoint": llm_url,
                    "grafana_endpoint": grafana_url,
                    "tenant": tenant_id,
                },
                "corpus": {
                    "path": CORPUS_PATH,
                    "sha256": corpus_digest,
                    "prompt_count": len(prompt_ids),
                },
                "long_lived_state": {
                    "fingerprint": admitted_state.fingerprint,
                    "tenant": tenant_id,
                    "limits": admitted_state.limits.as_dict(),
                    "manifest": {
                        "path": ARCHIVED_STATE_MANIFEST,
                        "sha256": _sha256(archived_state_manifest),
                    },
                },
                "llm_budget": budget_snapshot,
                "reports": {
                    state: {"path": path.name, "sha256": _sha256(path)} for state, path in report_paths.items()
                },
            }
            (output_directory / "release-quality-evidence.json").write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
    except (OSError, ReleaseQualityError, subprocess.CalledProcessError) as exc:
        output_directory.mkdir(parents=True, exist_ok=True)
        _write_failure_summary(output_directory, outcomes, str(exc), budget.snapshot() if budget is not None else None)
        print(f"Release-quality evaluation failed: {exc}", file=sys.stderr)
        return 1

    print("Release-quality evidence recorded for the exact main commit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
