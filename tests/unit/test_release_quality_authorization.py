from __future__ import annotations

import contextlib
import csv
import hashlib
import importlib.util
import io
import json
import sqlite3
import sys
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from urllib.error import URLError
from urllib.request import Request

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).parents[2]
AUTHORIZATION_SCRIPT = REPOSITORY_ROOT / ".github" / "scripts" / "authorize_release_publication.py"
QUALITY_WORKFLOW = REPOSITORY_ROOT / ".github" / "workflows" / "release-quality.yml"
QUALITY_WORKFLOW_PATH = ".github/workflows/release-quality.yml"
QUALITY_WORKFLOW_RUN_PATH = f"{QUALITY_WORKFLOW_PATH}@main"
CORPUS_PATH = "tests/tacit_validation_prompts.csv"
STATE_MANIFEST_PATH = "long-lived-state-manifest.json"


def _load_script_module(path: Path) -> ModuleType:
    inserted = str(path.parent) not in sys.path
    if inserted:
        sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(f"quality_test_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        if inserted:
            sys.path.remove(str(path.parent))


def _corpus_bytes(prompt_count: int = 100) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream,
        fieldnames=(
            "prompt_id",
            "prompt",
            "expected_archetype",
            "expected_metrics",
            "expected_datasources",
            "difficulty",
            "validation_goal",
            "critical_metrics",
        ),
    )
    writer.writeheader()
    for index in range(prompt_count):
        writer.writerow(
            {
                "prompt_id": f"RQ-{index:03d}",
                "prompt": f"Investigate service {index}",
                "expected_archetype": "general",
                "expected_metrics": "up",
                "expected_datasources": "Prometheus",
                "difficulty": "medium",
                "validation_goal": "release quality",
                "critical_metrics": "up",
            }
        )
    return stream.getvalue().encode()


def _report(
    state: str,
    prompt_ids: list[str],
    *,
    passed: bool = True,
    fingerprint: str | None = None,
    tenant: str = "tenant-a",
) -> dict[str, Any]:
    archetype_details = []
    pipeline_details = []
    for prompt_id in prompt_ids:
        archetype_details.append(
            {
                "prompt_id": prompt_id,
                "expected": "general",
                "actual": "general" if passed else "error_spike",
                "passed": passed,
                "any_match": passed,
                "top_confidence": 1.0,
                "archetypes": [{"type": "general" if passed else "error_spike", "confidence": 1.0}],
                "latency_ms": 1.0,
                "error": "" if passed else "provider failure",
            }
        )
        pipeline_details.append(
            {
                "prompt_id": prompt_id,
                "metric_recall": 1.0 if passed else 0.0,
                "critical_recall": 1.0 if passed else 0.0,
                "weighted_recall": 1.0 if passed else 0.0,
                "signal_to_noise": 1.0 if passed else 0.0,
                "found_metrics": ["up"] if passed else [],
                "missing_metrics": [] if passed else ["up"],
                "extra_metrics": [],
                "critical_metrics_expected": ["up"],
                "critical_metrics_found": ["up"] if passed else [],
                "critical_metrics_missing": [] if passed else ["up"],
                "panel_count": 1 if passed else 0,
                "dashboard_url": "http://127.0.0.1:3000/d/release-quality" if passed else "",
                "latency_ms": 1.0,
                "error": "" if passed else "provider failure",
            }
        )
    successful = len(prompt_ids) if passed else 0
    errors = 0 if passed else len(prompt_ids)
    return {
        "dataset": CORPUS_PATH,
        "prompt_count": len(prompt_ids),
        "mode": "all",
        "state": {
            "mode": state,
            "fingerprint": fingerprint or hashlib.sha256(state.encode()).hexdigest(),
            "tenant": tenant,
        },
        "archetype": {
            "strict_accuracy": 1.0 if passed else 0.0,
            "soft_accuracy": 1.0 if passed else 0.0,
            "total": len(prompt_ids),
            "strict_passed": successful,
            "soft_passed": successful,
            "failed": len(prompt_ids) - successful,
            "errors": errors,
            "avg_latency_ms": 1.0,
            "avg_top_confidence": 1.0,
            "details": archetype_details,
        },
        "pipeline": {
            "avg_metric_recall": 1.0 if passed else 0.0,
            "avg_critical_recall": 1.0 if passed else 0.0,
            "avg_weighted_recall": 1.0 if passed else 0.0,
            "avg_signal_to_noise": 1.0 if passed else 0.0,
            "total": len(prompt_ids),
            "critical_cases": len(prompt_ids),
            "succeeded": successful,
            "errors": errors,
            "avg_latency_ms": 1.0,
            "details": pipeline_details,
        },
        "gate": {
            "passed": passed,
            "failures": [] if passed else ["quality floor missed"],
            "thresholds": {
                "min_archetype_accuracy": 0.85,
                "min_archetype_soft_accuracy": 0.9,
                "min_metric_recall": 0.75,
                "min_critical_recall": 0.8,
                "min_weighted_recall": 0.78,
                "min_signal_to_noise": 0.65,
                "max_errors": 0,
                "max_error_rate": 0.0,
            },
        },
    }


def _quality_archive(
    *,
    sha: str = "a" * 40,
    repository: str = "aditki/tacit",
    run_id: int = 42,
    run_attempt: int = 1,
    prompt_count: int = 100,
    long_lived_passed: bool = True,
    evidence_sha: str | None = None,
    extra_member: tuple[str, bytes] | None = None,
    include_expected_directory_entry: bool = False,
    tenant: str = "tenant-a",
) -> bytes:
    corpus = _corpus_bytes(prompt_count)
    with io.StringIO(corpus.decode(), newline="") as stream:
        prompt_ids = [row["prompt_id"] for row in csv.DictReader(stream)]
    clean_fingerprint = hashlib.sha256(b"tacit-validation-state-v1:clean").hexdigest()
    long_lived_fingerprint = hashlib.sha256(b"representative-long-lived-state").hexdigest()
    state_limits = {
        "max_database_bytes": 2 * 1024 * 1024 * 1024,
        "max_total_bytes": 4 * 1024 * 1024 * 1024,
    }
    state_manifest = {
        "schema_version": 1,
        "tenant": tenant,
        "fingerprint": long_lived_fingerprint,
        "databases": ["signals.db", "history.db", "feedback.db"],
        "limits": state_limits,
    }
    state_manifest_payload = json.dumps(state_manifest, sort_keys=True).encode()
    clean = json.dumps(
        _report("clean", prompt_ids, fingerprint=clean_fingerprint, tenant=tenant),
        sort_keys=True,
    ).encode()
    long_lived = json.dumps(
        _report(
            "long-lived",
            prompt_ids,
            passed=long_lived_passed,
            fingerprint=long_lived_fingerprint,
            tenant=tenant,
        ),
        sort_keys=True,
    ).encode()
    evidence = {
        "schema_version": 3,
        "repository": repository,
        "commit_sha": evidence_sha or sha,
        "workflow": QUALITY_WORKFLOW_PATH,
        "workflow_run_id": run_id,
        "workflow_run_attempt": run_attempt,
        "evaluation": {
            "mode": "all",
            "model": "release-model-v1",
            "llm_endpoint": "http://127.0.0.1:11434",
            "grafana_endpoint": "http://127.0.0.1:3000",
            "tenant": tenant,
        },
        "corpus": {
            "path": CORPUS_PATH,
            "sha256": hashlib.sha256(corpus).hexdigest(),
            "prompt_count": prompt_count,
        },
        "long_lived_state": {
            "fingerprint": long_lived_fingerprint,
            "tenant": tenant,
            "limits": state_limits,
            "manifest": {
                "path": STATE_MANIFEST_PATH,
                "sha256": hashlib.sha256(state_manifest_payload).hexdigest(),
            },
        },
        "llm_budget": {
            "max_requests": 500,
            "attempted_requests": 400,
            "forwarded_requests": 400,
            "rejected_requests": 0,
            "exhausted": False,
        },
        "reports": {
            "clean": {
                "path": "clean-report.json",
                "sha256": hashlib.sha256(clean).hexdigest(),
            },
            "long-lived": {
                "path": "long-lived-report.json",
                "sha256": hashlib.sha256(long_lived).hexdigest(),
            },
        },
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("release-quality-evidence.json", json.dumps(evidence, sort_keys=True))
        archive.writestr("clean-report.json", clean)
        archive.writestr("long-lived-report.json", long_lived)
        archive.writestr(STATE_MANIFEST_PATH, state_manifest_payload)
        if include_expected_directory_entry:
            archive.writestr("tests/", b"")
        archive.writestr(CORPUS_PATH, corpus)
        if extra_member is not None:
            archive.writestr(*extra_member)
    return output.getvalue()


def _mutate_quality_archive(
    archive_payload: bytes,
    mutate: Callable[[dict[str, Any], dict[str, Any]], None],
) -> bytes:
    with zipfile.ZipFile(io.BytesIO(archive_payload)) as source:
        members = {info.filename: source.read(info.filename) for info in source.infolist() if not info.is_dir()}
    evidence = json.loads(members["release-quality-evidence.json"])
    clean = json.loads(members["clean-report.json"])
    mutate(evidence, clean)
    members["clean-report.json"] = json.dumps(clean, sort_keys=True).encode()
    evidence["reports"]["clean"]["sha256"] = hashlib.sha256(members["clean-report.json"]).hexdigest()
    members["release-quality-evidence.json"] = json.dumps(evidence, sort_keys=True).encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return output.getvalue()


def _create_representative_state(
    directory: Path,
    *,
    live_wal: bool,
) -> list[sqlite3.Connection]:
    directory.mkdir()
    held_connections: list[sqlite3.Connection] = []
    for name in ("signals.db", "history.db", "feedback.db"):
        connection = sqlite3.connect(directory / name)
        if live_wal:
            assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
            connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE release_quality_marker(value TEXT NOT NULL)")
        connection.execute("INSERT INTO release_quality_marker(value) VALUES (?)", (name,))
        connection.commit()
        if live_wal:
            held_connections.append(connection)
        else:
            connection.close()
    return held_connections


def _marker_from_database(path: Path) -> str:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = connection.execute("SELECT value FROM release_quality_marker").fetchone()
    finally:
        connection.close()
    assert row is not None
    return str(row[0])


def test_quality_workflow_requires_explicit_spend_approval_and_protected_environment() -> None:
    workflow = yaml.load(QUALITY_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert workflow["on"] == {
        "workflow_dispatch": {
            "inputs": {
                "external_credit_spend_acknowledgement": {
                    "description": "Type AUTHORIZE UP TO 500 LLM REQUESTS to approve this external evaluation spend",
                    "required": "true",
                    "type": "string",
                }
            }
        }
    }
    assert workflow["permissions"] == {"contents": "read"}

    approval = workflow["jobs"]["authorize-external-spend"]
    assert "environment" not in approval
    approval_script = approval["steps"][0]["run"]
    assert "AUTHORIZE UP TO 500 LLM REQUESTS" in approval_script
    assert "refs/heads/main" in approval_script

    evaluation = workflow["jobs"]["evaluate-release-quality"]
    assert evaluation["needs"] == "authorize-external-spend"
    assert evaluation["environment"] == "release-quality"
    assert evaluation["runs-on"] == ["self-hosted", "linux", "x64", "release-quality"]
    assert int(evaluation["timeout-minutes"]) <= 240
    assert "${{ secrets." not in json.dumps(workflow["jobs"]["authorize-external-spend"])
    assert evaluation["env"]["RELEASE_QUALITY_TENANT"] == "${{ vars.RELEASE_QUALITY_TENANT }}"
    assert "RELEASE_QUALITY_LONG_LIVED_STATE_FINGERPRINT" not in evaluation["env"]

    command = next(
        step["run"]
        for step in evaluation["steps"]
        if step.get("name") == "Run exact-SHA clean and long-lived quality gates"
    )
    assert ".github/scripts/run_release_quality_evaluation.py" in command
    assert '--expected-sha "$GITHUB_SHA"' in command
    assert "--expected-prompt-count 100" in command
    assert "--max-llm-requests 500" in command
    assert '--long-lived-state-dir "$RELEASE_QUALITY_LONG_LIVED_STATE_DIR"' in command
    assert "--expected-long-lived-state-fingerprint" not in command
    assert '--tenant "$RELEASE_QUALITY_TENANT"' in command

    upload = next(step for step in evaluation["steps"] if "actions/upload-artifact@" in step.get("uses", ""))
    assert upload["if"] == "${{ always() }}"
    assert upload["with"]["if-no-files-found"] == "error"
    assert upload["with"]["retention-days"] == "90"
    assert (
        upload["with"]["name"]
        == "release-quality-evidence-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}"
    )


@pytest.mark.parametrize(
    ("sizes", "max_database_bytes", "max_total_bytes", "error"),
    [
        ((8, 8, 8), 8, 24, None),
        ((9, 1, 1), 8, 24, "per-database"),
        ((7, 7, 7), 8, 20, "aggregate"),
    ],
)
def test_representative_state_size_contract_enforces_exact_boundaries(
    tmp_path: Path,
    sizes: tuple[int, int, int],
    max_database_bytes: int,
    max_total_bytes: int,
    error: str | None,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    assert runner.STATE_SNAPSHOT_LIMITS.as_dict() == {
        "max_database_bytes": 2 * 1024 * 1024 * 1024,
        "max_total_bytes": 4 * 1024 * 1024 * 1024,
    }
    paths = []
    for name, size in zip(runner.LONG_LIVED_STATE_DATABASES, sizes, strict=True):
        path = tmp_path / name
        path.write_bytes(b"x" * size)
        paths.append(path)
    limits = runner.StateSnapshotLimits(
        max_database_bytes=max_database_bytes,
        max_total_bytes=max_total_bytes,
    )

    if error is None:
        assert runner._validate_state_database_sizes(tuple(paths), limits=limits) == sum(sizes)
    else:
        with pytest.raises(runner.ReleaseQualityError, match=error):
            runner._validate_state_database_sizes(tuple(paths), limits=limits)


@pytest.mark.parametrize("live_wal", [False, True], ids=["closed", "live-wal"])
def test_representative_state_is_snapshotted_once_and_consumed_as_that_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_wal: bool,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    from tacit import sqlite_identity
    from tests import validate
    from tests.eval.cold_isolation import LocalEvaluationEndpoints

    source = tmp_path / "representative"
    held_connections = _create_representative_state(source, live_wal=live_wal)
    try:
        with runner._admitted_long_lived_state(source, tenant_id="tenant-a") as admitted:
            assert admitted.fingerprint == runner._logical_snapshot_fingerprint(admitted.directory)
            assert json.loads(admitted.manifest_path.read_text(encoding="utf-8"))["fingerprint"] == admitted.fingerprint
            for name in runner.LONG_LIVED_STATE_DATABASES:
                assert _marker_from_database(admitted.directory / name) == name

            def reject_second_snapshot(*_args: Any, **_kwargs: Any) -> Any:
                raise AssertionError("the evaluator must consume the admitted snapshot without another backup")

            monkeypatch.setattr(sqlite_identity, "snapshot_sqlite_database_set", reject_second_snapshot)
            with validate.evaluation_state(
                "long-lived",
                admitted.directory,
                endpoints=LocalEvaluationEndpoints(),
                tenant_id="tenant-a",
                admitted_state_manifest=admitted.manifest_path,
            ) as selected:
                assert selected.fingerprint == admitted.fingerprint
                assert selected.tenant_id == "tenant-a"
                assert selected.isolated_state is not None
                for name in runner.LONG_LIVED_STATE_DATABASES:
                    assert _marker_from_database(selected.isolated_state.workdir / name) == name
    finally:
        for connection in held_connections:
            connection.close()


def test_admitted_state_rejects_a_symlinked_source_directory(tmp_path: Path) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    source = tmp_path / "state"
    held_connections = _create_representative_state(source, live_wal=False)
    alias = tmp_path / "state-alias"
    try:
        alias.symlink_to(source, target_is_directory=True)
        with pytest.raises(runner.ReleaseQualityError, match="real directory"):
            with runner._admitted_long_lived_state(alias, tenant_id="tenant-a"):
                pass
    finally:
        for connection in held_connections:
            connection.close()


def test_quality_runner_rejects_a_symlinked_state_before_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    corpus = tmp_path / CORPUS_PATH
    corpus.parent.mkdir(parents=True)
    corpus.write_bytes(_corpus_bytes())
    source = tmp_path / "state"
    held_connections = _create_representative_state(source, live_wal=False)
    alias = tmp_path / "state-alias"
    alias.symlink_to(source, target_is_directory=True)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="a" * 40 if command[1:3] == ["rev-parse", "HEAD"] else "",
            stderr="",
        ),
    )
    monkeypatch.setattr(
        runner,
        "_run_validation",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("state symlink reached evaluation")),
    )

    try:
        output = tmp_path / "output"
        result = runner.main(
            [
                "--repository",
                str(tmp_path),
                "--expected-sha",
                "a" * 40,
                "--repository-slug",
                "aditki/tacit",
                "--tenant",
                "tenant-a",
                "--workflow-run-id",
                "42",
                "--workflow-run-attempt",
                "1",
                "--credit-spend-acknowledgement",
                runner.SPEND_ACKNOWLEDGEMENT,
                "--corpus",
                str(corpus),
                "--max-llm-requests",
                "500",
                "--long-lived-state-dir",
                str(alias),
                "--llm-url",
                "http://127.0.0.1:11434",
                "--llm-model",
                "release-model-v1",
                "--grafana-url",
                "http://127.0.0.1:3000",
                "--output-directory",
                str(output),
            ]
        )
        assert result == 1
        failure = json.loads((output / "failure-summary.json").read_text(encoding="utf-8"))
        assert failure["reason"] == "long-lived state directory must be a real directory"
    finally:
        for connection in held_connections:
            connection.close()


def test_direct_long_lived_evaluation_uses_the_shared_release_snapshot_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    from tacit import sqlite_identity
    from tests import validate
    from tests.eval.cold_isolation import LocalEvaluationEndpoints

    source = tmp_path / "state"
    held_connections = _create_representative_state(source, live_wal=False)
    original_snapshot = sqlite_identity.snapshot_sqlite_database_set
    observed_limits: list[tuple[int | None, int | None]] = []

    def capture_snapshot(*args: Any, **kwargs: Any) -> Any:
        observed_limits.append(
            (
                kwargs.get("snapshot_max_bytes"),
                kwargs.get("snapshot_total_max_bytes"),
            )
        )
        return original_snapshot(*args, **kwargs)

    monkeypatch.setattr(sqlite_identity, "snapshot_sqlite_database_set", capture_snapshot)
    try:
        with validate.evaluation_state(
            "long-lived",
            source,
            endpoints=LocalEvaluationEndpoints(),
            tenant_id="tenant-a",
        ):
            pass
    finally:
        for connection in held_connections:
            connection.close()

    assert observed_limits == [
        (
            runner.STATE_SNAPSHOT_LIMITS.max_database_bytes,
            runner.STATE_SNAPSHOT_LIMITS.max_total_bytes,
        )
    ]


def test_release_runner_forwards_distinct_database_and_total_snapshot_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    from tacit import sqlite_identity

    source = tmp_path / "state"
    held_connections = _create_representative_state(source, live_wal=False)
    source_paths = tuple(source / name for name in runner.LONG_LIVED_STATE_DATABASES)
    limits = runner.StateSnapshotLimits(
        max_database_bytes=max(path.stat().st_size for path in source_paths),
        max_total_bytes=sum(path.stat().st_size for path in source_paths),
    )
    original_snapshot = sqlite_identity.snapshot_sqlite_database_set
    observed_limits: list[tuple[int | None, int | None]] = []

    def capture_snapshot(*args: Any, **kwargs: Any) -> Any:
        observed_limits.append(
            (
                kwargs.get("snapshot_max_bytes"),
                kwargs.get("snapshot_total_max_bytes"),
            )
        )
        return original_snapshot(*args, **kwargs)

    monkeypatch.setattr(sqlite_identity, "snapshot_sqlite_database_set", capture_snapshot)
    try:
        with runner._admitted_long_lived_state(source, tenant_id="tenant-a", limits=limits):
            pass
    finally:
        for connection in held_connections:
            connection.close()

    assert observed_limits == [(limits.max_database_bytes, limits.max_total_bytes)]


@pytest.mark.parametrize("entry_point", ["runner", "direct"])
@pytest.mark.parametrize("over_limit", [False, True], ids=["exact-limit", "limit-plus-one"])
def test_release_state_entry_points_enforce_exact_and_limit_plus_one_database_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_point: str,
    over_limit: bool,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    from tests import validate
    from tests.eval.cold_isolation import LocalEvaluationEndpoints

    source = tmp_path / f"state-{entry_point}-{over_limit}"
    held_connections = _create_representative_state(source, live_wal=False)
    source_paths = tuple(source / name for name in runner.LONG_LIVED_STATE_DATABASES)
    limits = runner.StateSnapshotLimits(
        max_database_bytes=max(path.stat().st_size for path in source_paths),
        max_total_bytes=sum(path.stat().st_size for path in source_paths),
    )
    if over_limit:
        largest_source = max(source_paths, key=lambda path: path.stat().st_size)
        with largest_source.open("ab") as source_file:
            source_file.write(b"\x00")

    try:
        if entry_point == "runner":
            if over_limit:
                with pytest.raises(runner.ReleaseQualityError, match="per-database"):
                    with runner._admitted_long_lived_state(source, tenant_id="tenant-a", limits=limits):
                        pass
            else:
                with runner._admitted_long_lived_state(source, tenant_id="tenant-a", limits=limits) as admitted:
                    assert admitted.limits == limits
            return

        contract = validate._release_quality_contract()
        monkeypatch.setattr(contract, "STATE_SNAPSHOT_LIMITS", limits)
        if over_limit:
            with pytest.raises(ValueError, match="per-database"):
                with validate.evaluation_state(
                    "long-lived",
                    source,
                    endpoints=LocalEvaluationEndpoints(),
                    tenant_id="tenant-a",
                ):
                    pass
        else:
            with validate.evaluation_state(
                "long-lived",
                source,
                endpoints=LocalEvaluationEndpoints(),
                tenant_id="tenant-a",
            ) as selected:
                assert selected.fingerprint
    finally:
        for connection in held_connections:
            connection.close()


def test_quality_runner_does_not_forward_ambient_credentials_or_proxies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    for name in ("GITHUB_TOKEN", "LLM_API_KEY", "OPENAI_API_KEY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(name, f"sentinel-{name}")

    child_environment = runner._minimal_child_environment()

    assert set(child_environment) <= {
        "HOME",
        "LANG",
        "LC_ALL",
        "PATH",
        "PYTHONHASHSEED",
        "PYTHONNOUSERSITE",
        "SSL_CERT_FILE",
        "TMPDIR",
    }
    assert not any("TOKEN" in name or "KEY" in name or "PROXY" in name for name in child_environment)


def test_quality_archive_accepts_two_exact_100_prompt_passes() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive()

    evidence = authorization.validate_release_quality_archive(
        archive,
        expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
        expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
        expected_sha="a" * 40,
        expected_repository="aditki/tacit",
        expected_run_id=42,
        expected_run_attempt=1,
    )

    assert evidence["commit_sha"] == "a" * 40
    assert evidence["corpus"]["prompt_count"] == 100
    assert evidence["llm_budget"]["forwarded_requests"] == 400
    assert evidence["evaluation"]["tenant"] == "tenant-a"


@pytest.mark.parametrize("tenant", ["", "*", "*bootstrap*", "tenant with spaces"])
def test_quality_archive_rejects_invalid_or_nonconcrete_tenant(tenant: str) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive(tenant=tenant)

    with pytest.raises(authorization.ReleaseAuthorizationError, match="tenant"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_rejects_report_tenant_mismatch() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _mutate_quality_archive(
        _quality_archive(),
        lambda _evidence, clean: clean["state"].update({"tenant": "tenant-b"}),
    )

    with pytest.raises(authorization.ReleaseAuthorizationError, match="tenant"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_rejects_state_manifest_tenant_mismatch() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive()
    with zipfile.ZipFile(io.BytesIO(archive)) as source:
        members = {info.filename: source.read(info.filename) for info in source.infolist() if not info.is_dir()}
    evidence = json.loads(members["release-quality-evidence.json"])
    manifest = json.loads(members[STATE_MANIFEST_PATH])
    manifest["tenant"] = "tenant-b"
    members[STATE_MANIFEST_PATH] = json.dumps(manifest, sort_keys=True).encode()
    evidence["long_lived_state"]["manifest"]["sha256"] = hashlib.sha256(members[STATE_MANIFEST_PATH]).hexdigest()
    members["release-quality-evidence.json"] = json.dumps(evidence, sort_keys=True).encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as rewritten:
        for name, payload in members.items():
            rewritten.writestr(name, payload)

    payload = output.getvalue()
    with pytest.raises(authorization.ReleaseAuthorizationError, match="tenant"):
        authorization.validate_release_quality_archive(
            payload,
            expected_archive_digest=f"sha256:{hashlib.sha256(payload).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_rejects_weakened_state_size_contract() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)

    def weaken_limits(evidence: dict[str, Any], _report: dict[str, Any]) -> None:
        evidence["long_lived_state"]["limits"]["max_database_bytes"] = 1024 * 1024 * 1024

    archive = _mutate_quality_archive(_quality_archive(), weaken_limits)
    with pytest.raises(authorization.ReleaseAuthorizationError, match="size contract"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_rejects_metricless_self_asserted_pass() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)

    def remove_measurements(evidence: dict[str, Any], report: dict[str, Any]) -> None:
        report["archetype"] = {
            "total": 100,
            "strict_accuracy": 1.0,
            "soft_accuracy": 1.0,
            "errors": 0,
            "details": [{"prompt_id": item["prompt_id"]} for item in report["archetype"]["details"]],
        }

    archive = _mutate_quality_archive(_quality_archive(), remove_measurements)
    with pytest.raises(authorization.ReleaseAuthorizationError, match="archetype measurement"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


@pytest.mark.parametrize("section", ["archetype", "pipeline"])
def test_quality_archive_recomputes_thresholds_from_prompt_measurements(section: str) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)

    def falsify_summary(evidence: dict[str, Any], report: dict[str, Any]) -> None:
        for detail in report[section]["details"][:30]:
            if section == "archetype":
                detail.update(
                    actual="error_spike",
                    passed=False,
                    any_match=False,
                    archetypes=[{"type": "error_spike", "confidence": 1.0}],
                )
            else:
                detail.update(
                    metric_recall=0.0,
                    critical_recall=0.0,
                    weighted_recall=0.0,
                    signal_to_noise=0.0,
                    found_metrics=[],
                    missing_metrics=["up"],
                    critical_metrics_found=[],
                    critical_metrics_missing=["up"],
                )

    archive = _mutate_quality_archive(_quality_archive(), falsify_summary)
    with pytest.raises(authorization.ReleaseAuthorizationError, match=f"{section} quality threshold"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_recomputes_actual_errors_instead_of_trusting_summary() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)

    def hide_error(evidence: dict[str, Any], report: dict[str, Any]) -> None:
        detail = report["pipeline"]["details"][0]
        detail.update(
            error="provider timeout",
            metric_recall=0.0,
            critical_recall=0.0,
            weighted_recall=0.0,
            signal_to_noise=0.0,
            found_metrics=[],
            missing_metrics=["up"],
            critical_metrics_found=[],
            critical_metrics_missing=["up"],
            panel_count=0,
            dashboard_url="",
        )

    archive = _mutate_quality_archive(_quality_archive(), hide_error)
    with pytest.raises(authorization.ReleaseAuthorizationError, match="pipeline quality threshold.*errors"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_rejects_non_finite_measurements() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)

    def add_nan(evidence: dict[str, Any], report: dict[str, Any]) -> None:
        report["archetype"]["details"][0]["top_confidence"] = float("nan")

    archive = _mutate_quality_archive(_quality_archive(), add_nan)
    with pytest.raises(authorization.ReleaseAuthorizationError, match="valid JSON|finite"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


@pytest.mark.parametrize(
    "budget",
    [
        {
            "max_requests": 500,
            "attempted_requests": 399,
            "forwarded_requests": 399,
            "rejected_requests": 0,
            "exhausted": False,
        },
        {
            "max_requests": 500,
            "attempted_requests": 501,
            "forwarded_requests": 500,
            "rejected_requests": 1,
            "exhausted": True,
        },
    ],
)
def test_quality_archive_rejects_incomplete_or_exhausted_provider_budget(budget: dict[str, Any]) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)

    def replace_budget(evidence: dict[str, Any], report: dict[str, Any]) -> None:
        evidence["llm_budget"] = budget

    archive = _mutate_quality_archive(_quality_archive(), replace_budget)
    with pytest.raises(authorization.ReleaseAuthorizationError, match="LLM request budget"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_provider_budget_refuses_before_request_beyond_hard_limit() -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    budget = runner.LLMRequestBudget(max_requests=2)

    budget.reserve()
    budget.reserve()
    with pytest.raises(runner.LLMRequestBudgetExceeded):
        budget.reserve()

    assert budget.snapshot() == {
        "max_requests": 2,
        "attempted_requests": 3,
        "forwarded_requests": 2,
        "rejected_requests": 1,
        "exhausted": True,
    }


def test_provider_proxy_refuses_over_budget_request_before_upstream_spend() -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    upstream_requests = 0

    class Upstream(runner.BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            nonlocal upstream_requests
            upstream_requests += 1
            length = int(self.headers["Content-Length"])
            self.rfile.read(length)
            payload = b'{"message":{"content":"ok"}}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    upstream = runner.HTTPServer(("127.0.0.1", 0), Upstream)
    upstream_thread = runner.threading.Thread(target=upstream.serve_forever)
    upstream_thread.start()
    try:
        budget = runner.LLMRequestBudget(max_requests=2)
        upstream_url = f"http://127.0.0.1:{upstream.server_address[1]}"
        with runner._budgeted_llm_proxy(upstream_url, budget) as proxy_url:
            proxy = runner.urlsplit(proxy_url)
            statuses = []
            for _ in range(3):
                connection = runner.http.client.HTTPConnection(proxy.hostname, proxy.port, timeout=5)
                connection.request("POST", "/api/chat", body=b"{}", headers={"Content-Type": "application/json"})
                response = connection.getresponse()
                statuses.append(response.status)
                response.read()
                connection.close()
    finally:
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join()

    assert statuses == [200, 200, 429]
    assert upstream_requests == 2
    assert budget.snapshot()["rejected_requests"] == 1


def test_exhausted_provider_budget_is_recorded_in_failure_evidence(tmp_path: Path) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    budget = runner.LLMRequestBudget(max_requests=1)
    budget.reserve()
    with pytest.raises(runner.LLMRequestBudgetExceeded):
        budget.reserve()

    runner._write_failure_summary(tmp_path, {"clean": 1}, "request budget exhausted", budget.snapshot())

    summary = json.loads((tmp_path / "failure-summary.json").read_text())
    assert summary["llm_budget"] == budget.snapshot()


def test_quality_archive_accepts_only_the_expected_parent_directory_entry() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive(include_expected_directory_entry=True)

    authorization.validate_release_quality_archive(
        archive,
        expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
        expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
        expected_sha="a" * 40,
        expected_repository="aditki/tacit",
        expected_run_id=42,
        expected_run_attempt=1,
    )


@pytest.mark.parametrize(
    ("archive_kwargs", "message"),
    [
        ({"prompt_count": 99}, "exactly 100"),
        ({"long_lived_passed": False}, "long-lived archetype quality threshold"),
        ({"evidence_sha": "b" * 40}, "commit SHA"),
        ({"extra_member": ("unexpected.txt", b"surprise")}, "unexpected files"),
        ({"extra_member": ("../escape", b"surprise")}, "unsafe path"),
    ],
)
def test_quality_archive_fails_closed_on_incomplete_or_mismatched_evidence(
    archive_kwargs: dict[str, Any],
    message: str,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive(**archive_kwargs)

    with pytest.raises(authorization.ReleaseAuthorizationError, match=message):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes(archive_kwargs.get("prompt_count", 100))).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_archive_rejects_digest_mismatch_before_parsing() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive()

    with pytest.raises(authorization.ReleaseAuthorizationError, match="artifact digest"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{'0' * 64}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_authorization_requires_successful_exact_sha_main_run_and_audited_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    sha = "a" * 40
    archive = _quality_archive(sha=sha)
    responses = iter(
        [
            {
                "workflow_runs": [
                    {
                        "id": 42,
                        "run_attempt": 1,
                        "head_sha": sha,
                        "head_branch": "main",
                        "event": "workflow_dispatch",
                        "status": "completed",
                        "conclusion": "success",
                        "path": QUALITY_WORKFLOW_RUN_PATH,
                    }
                ]
            },
            {
                "artifacts": [
                    {
                        "id": 7,
                        "name": f"release-quality-evidence-{sha}-42-1",
                        "expired": False,
                        "size_in_bytes": len(archive),
                        "digest": f"sha256:{hashlib.sha256(archive).hexdigest()}",
                        "archive_download_url": "https://api.github.com/repos/aditki/tacit/actions/artifacts/7/zip",
                        "workflow_run": {"id": 42, "head_branch": "main", "head_sha": sha},
                    }
                ]
            },
        ]
    )
    monkeypatch.setattr(authorization, "_load_actions_response", lambda request: next(responses))
    monkeypatch.setattr(authorization, "_download_actions_artifact", lambda request: archive)
    monkeypatch.setattr(
        authorization,
        "_canonical_quality_corpus_digest",
        lambda repository: hashlib.sha256(_corpus_bytes()).hexdigest(),
    )

    authorization.require_successful_release_quality(
        expected_sha=sha,
        repository_slug="aditki/tacit",
        token="token",
        api_url="https://api.github.com",
    )


def test_quality_authorization_rejects_success_from_another_commit_without_downloading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    monkeypatch.setattr(
        authorization,
        "_load_actions_response",
        lambda request: {
            "workflow_runs": [
                {
                    "id": 42,
                    "run_attempt": 1,
                    "head_sha": "b" * 40,
                    "head_branch": "main",
                    "event": "workflow_dispatch",
                    "status": "completed",
                    "conclusion": "success",
                    "path": QUALITY_WORKFLOW_RUN_PATH,
                }
            ]
        },
    )
    monkeypatch.setattr(
        authorization,
        "_download_actions_artifact",
        lambda request: pytest.fail("mismatched run must not reach artifact download"),
        raising=False,
    )

    with pytest.raises(authorization.ReleaseAuthorizationError, match="quality run"):
        authorization.require_successful_release_quality(
            expected_sha="a" * 40,
            repository_slug="aditki/tacit",
            token="token",
            api_url="https://api.github.com",
        )


def test_publication_rejects_tagged_ancestor_when_main_has_advanced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    release_sha = "a" * 40
    current_main_sha = "b" * 40

    def fake_git(repository: Path, *arguments: str) -> str:
        if arguments == ("rev-parse", "HEAD"):
            return release_sha
        if arguments == ("rev-parse", "refs/tags/v1.2.3^{commit}"):
            return release_sha
        if arguments == ("rev-parse", "refs/remotes/origin/main^{commit}"):
            return current_main_sha
        return ""

    monkeypatch.setattr(authorization, "_git", fake_git)
    with pytest.raises(authorization.ReleaseAuthorizationError, match="current origin/main tip"):
        authorization.prove_fresh_tag_on_main(
            Path.cwd(),
            expected_sha=release_sha,
            tag_name="v1.2.3",
        )


def test_publication_accepts_tagged_commit_only_at_current_main_tip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    release_sha = "a" * 40
    calls: list[tuple[str, ...]] = []

    def fake_git(repository: Path, *arguments: str) -> str:
        calls.append(arguments)
        return release_sha if arguments[0] == "rev-parse" else ""

    monkeypatch.setattr(authorization, "_git", fake_git)
    authorization.prove_fresh_tag_on_main(
        Path.cwd(),
        expected_sha=release_sha,
        tag_name="v1.2.3",
    )

    assert ("rev-parse", "refs/remotes/origin/main^{commit}") in calls


def test_idempotent_github_read_retries_transient_failures_with_bounded_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    attempts = 0
    sleeps: list[float] = []

    def load_once(request: Request) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise URLError("temporary outage")
        return {"workflow_runs": []}

    monkeypatch.setattr(authorization, "_load_actions_response_once", load_once)
    monkeypatch.setattr(authorization.time, "sleep", sleeps.append)

    assert authorization._load_actions_response(Request("https://api.github.com/example")) == {"workflow_runs": []}
    assert attempts == 3
    assert sleeps == list(authorization.IDEMPOTENT_RETRY_DELAYS)


def test_idempotent_download_exhausts_retries_without_unbounded_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    attempts = 0
    sleeps: list[float] = []

    def download_once(request: Request) -> bytes:
        nonlocal attempts
        attempts += 1
        raise URLError("temporary outage")

    monkeypatch.setattr(authorization, "_download_actions_artifact_once", download_once)
    monkeypatch.setattr(authorization.time, "sleep", sleeps.append)

    with pytest.raises(authorization.ReleaseAuthorizationError, match="after 3 attempts"):
        authorization._download_actions_artifact(Request("https://api.github.com/example"))
    assert attempts == 3
    assert sleeps == list(authorization.IDEMPOTENT_RETRY_DELAYS)


def test_idempotent_git_read_retries_but_local_mutation_is_single_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    calls: list[tuple[str, ...]] = []
    sleeps: list[float] = []

    def run_once(repository: Path, *arguments: str) -> str:
        calls.append(arguments)
        if arguments == ("rev-parse", "HEAD") and calls.count(arguments) < 3:
            raise authorization._TransientGitRead("temporary git read failure")
        return "a" * 40

    monkeypatch.setattr(authorization, "_git_once", run_once)
    monkeypatch.setattr(authorization.time, "sleep", sleeps.append)

    assert authorization._git(Path.cwd(), "rev-parse", "HEAD") == "a" * 40
    assert calls == [("rev-parse", "HEAD")] * 3
    assert sleeps == list(authorization.IDEMPOTENT_RETRY_DELAYS)

    calls.clear()
    sleeps.clear()
    assert authorization._git(Path.cwd(), "update-ref", "-d", "refs/tags/v1.2.3") == "a" * 40
    assert calls == [("update-ref", "-d", "refs/tags/v1.2.3")]
    assert sleeps == []


def test_publication_main_requires_ci_and_quality_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    calls: list[str] = []
    for name, value in {
        "GITHUB_SHA": "a" * 40,
        "GITHUB_REF_NAME": "v1.2.3",
        "GITHUB_REPOSITORY": "aditki/tacit",
        "GITHUB_TOKEN": "token",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(authorization, "prove_fresh_tag_on_main", lambda *args, **kwargs: calls.append("tag"))
    monkeypatch.setattr(authorization, "require_successful_main_ci", lambda **kwargs: calls.append("ci"))
    monkeypatch.setattr(
        authorization,
        "require_successful_release_quality",
        lambda **kwargs: calls.append("quality"),
    )

    assert authorization.main() == 0
    assert calls == ["tag", "ci", "quality"]


@pytest.mark.parametrize("live_wal", [False, True], ids=["closed", "live-wal"])
def test_quality_runner_output_round_trips_exact_snapshot_generation_through_release_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_wal: bool,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    corpus = tmp_path / CORPUS_PATH
    corpus.parent.mkdir(parents=True)
    corpus.write_bytes(_corpus_bytes())
    with io.StringIO(corpus.read_text(), newline="") as stream:
        prompt_ids = [row["prompt_id"] for row in csv.DictReader(stream)]
    state = tmp_path / "state"
    held_connections = _create_representative_state(state, live_wal=live_wal)

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="a" * 40 if command[1:3] == ["rev-parse", "HEAD"] else "",
            stderr="",
        ),
    )

    def write_report(**kwargs: Any) -> int:
        state_name = kwargs["state"]
        fingerprint = (
            hashlib.sha256(b"tacit-validation-state-v1:clean").hexdigest()
            if state_name == "clean"
            else json.loads(kwargs["admitted_state_manifest"].read_text(encoding="utf-8"))["fingerprint"]
        )
        kwargs["output"].write_text(
            json.dumps(
                _report(
                    state_name,
                    prompt_ids,
                    fingerprint=fingerprint,
                    tenant=kwargs["tenant_id"],
                )
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(runner, "_run_validation", write_report)

    @contextlib.contextmanager
    def fake_budgeted_proxy(upstream_url: str, budget: Any) -> Iterator[str]:
        for _ in range(400):
            budget.reserve()
        yield "http://127.0.0.1:12345"

    monkeypatch.setattr(runner, "_budgeted_llm_proxy", fake_budgeted_proxy)
    try:
        output = tmp_path / "output"
        result = runner.main(
            [
                "--repository",
                str(tmp_path),
                "--expected-sha",
                "a" * 40,
                "--repository-slug",
                "aditki/tacit",
                "--tenant",
                "tenant-a",
                "--workflow-run-id",
                "42",
                "--workflow-run-attempt",
                "1",
                "--credit-spend-acknowledgement",
                runner.SPEND_ACKNOWLEDGEMENT,
                "--corpus",
                str(corpus),
                "--max-llm-requests",
                "500",
                "--long-lived-state-dir",
                str(state),
                "--llm-url",
                "http://127.0.0.1:11434",
                "--llm-model",
                "release-model-v1",
                "--grafana-url",
                "http://127.0.0.1:3000",
                "--output-directory",
                str(output),
            ]
        )
        assert result == 0

        evidence = json.loads((output / "release-quality-evidence.json").read_text(encoding="utf-8"))
        state_manifest = json.loads((output / STATE_MANIFEST_PATH).read_text(encoding="utf-8"))
        assert evidence["evaluation"]["tenant"] == "tenant-a"
        assert evidence["long_lived_state"]["fingerprint"] == state_manifest["fingerprint"]
        assert evidence["long_lived_state"]["tenant"] == state_manifest["tenant"]
        assert evidence["long_lived_state"]["limits"] == state_manifest["limits"]

        archive_stream = io.BytesIO()
        with zipfile.ZipFile(archive_stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(output.rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(output).as_posix())
        archive_payload = archive_stream.getvalue()
        authorization.validate_release_quality_archive(
            archive_payload,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive_payload).hexdigest()}",
            expected_corpus_digest=hashlib.sha256(_corpus_bytes()).hexdigest(),
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )
    finally:
        for connection in held_connections:
            connection.close()


def test_quality_archive_rejects_a_corpus_other_than_the_tagged_commit() -> None:
    authorization = _load_script_module(AUTHORIZATION_SCRIPT)
    archive = _quality_archive()

    with pytest.raises(authorization.ReleaseAuthorizationError, match="tagged commit"):
        authorization.validate_release_quality_archive(
            archive,
            expected_archive_digest=f"sha256:{hashlib.sha256(archive).hexdigest()}",
            expected_corpus_digest="0" * 64,
            expected_sha="a" * 40,
            expected_repository="aditki/tacit",
            expected_run_id=42,
            expected_run_attempt=1,
        )


def test_quality_runner_invokes_validation_with_canonical_relative_corpus(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_script_module(REPOSITORY_ROOT / ".github" / "scripts" / "run_release_quality_evaluation.py")
    corpus = tmp_path / CORPUS_PATH
    corpus.parent.mkdir(parents=True)
    corpus.write_bytes(_corpus_bytes())
    commands: list[list[str]] = []

    def capture(command: list[str], **kwargs: Any) -> SimpleNamespace:
        commands.append(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(runner.subprocess, "run", capture)
    assert (
        runner._run_validation(
            repository=tmp_path,
            corpus=corpus,
            output=tmp_path / "report.json",
            state="clean",
            state_directory=tmp_path / "state",
            tenant_id="tenant-a",
            admitted_state_manifest=None,
            llm_url="http://127.0.0.1:11434",
            llm_model="release-model-v1",
            grafana_url="http://127.0.0.1:3000",
            timeout_seconds=60,
        )
        == 0
    )
    assert commands[0][2] == CORPUS_PATH
    assert "--tenant" in commands[0]
    assert commands[0][commands[0].index("--tenant") + 1] == "tenant-a"
