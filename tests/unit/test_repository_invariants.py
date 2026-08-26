from __future__ import annotations

import ast
import hashlib
import http.server
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import tomllib
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).parents[2]
RELEASE_ARCHIVE_GUARD = REPOSITORY_ROOT / ".github" / "scripts" / "release_image_archive.py"
RELEASE_BINARY_PACKAGER = REPOSITORY_ROOT / ".github" / "scripts" / "package_release_binary.py"
RELEASE_DOCKERFILE_PREPARER = REPOSITORY_ROOT / ".github" / "scripts" / "prepare_release_dockerfile.py"
GITLEAKS_RANGE_SELECTOR = REPOSITORY_ROOT / ".github" / "scripts" / "gitleaks_range.py"


def _release_workflow() -> dict[str, Any]:
    workflow_path = REPOSITORY_ROOT / ".github" / "workflows" / "release.yml"
    loaded = yaml.load(workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _ci_workflow() -> dict[str, Any]:
    workflow_path = REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml"
    loaded = yaml.load(workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _v_tag_workflows() -> list[str]:
    publishers: list[str] = []
    workflows = REPOSITORY_ROOT / ".github" / "workflows"
    for path in sorted([*workflows.glob("*.yml"), *workflows.glob("*.yaml")]):
        loaded = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        if not isinstance(loaded, dict):
            continue
        push = loaded.get("on", {}).get("push", {}) if isinstance(loaded.get("on"), dict) else {}
        tags = push.get("tags", []) if isinstance(push, dict) else []
        if any(tag == "v*" for tag in tags):
            publishers.append(path.name)
    return publishers


def _job_needs(job: dict[str, Any]) -> set[str]:
    needs = job.get("needs", [])
    return set(needs if isinstance(needs, list) else [needs])


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    return next(step for step in job["steps"] if step.get("name") == name)


def _embedded_python(script: str) -> str:
    marker = "python - <<'PY'\n"
    start = script.index(marker) + len(marker)
    return script[start : script.index("\nPY", start)]


def _load_script_module(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"test_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _transitive_needs(jobs: dict[str, Any], job_name: str) -> set[str]:
    found: set[str] = set()
    pending = list(_job_needs(jobs[job_name]))
    while pending:
        dependency = pending.pop()
        if dependency in found:
            continue
        found.add(dependency)
        pending.extend(_job_needs(jobs[dependency]))
    return found


def _python_test_files() -> list[Path]:
    return sorted((REPOSITORY_ROOT / "tests").rglob("*.py"))


def test_api_tests_do_not_use_deprecated_starlette_test_client() -> None:
    deprecated_imports: list[str] = []
    for path in _python_test_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module in {
                "fastapi.testclient",
                "starlette.testclient",
            }:
                deprecated_imports.append(f"{path.relative_to(REPOSITORY_ROOT)}:{node.lineno}")

    assert deprecated_imports == []


def test_timestamp_tests_do_not_depend_on_millisecond_sleeps() -> None:
    tiny_sleeps: list[str] = []
    for path in _python_test_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if not (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "time"
                and node.func.attr == "sleep"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, (int, float))
                and 0 < float(node.args[0].value) <= 0.001
            ):
                continue
            tiny_sleeps.append(f"{path.relative_to(REPOSITORY_ROOT)}:{node.lineno}")

    assert tiny_sleeps == []


def test_release_is_tag_only_and_tag_must_match_package_version(tmp_path: Path) -> None:
    workflow = _release_workflow()
    assert _v_tag_workflows() == ["release.yml"]
    assert not (REPOSITORY_ROOT / ".github" / "workflows" / "release-binaries.yml").exists()
    assert workflow["on"] == {"push": {"tags": ["v*"]}}
    assert workflow["concurrency"] == {
        "group": "release-publication",
        "cancel-in-progress": "false",
        "queue": "max",
    }

    validate = workflow["jobs"]["validate-release"]
    step = _step(validate, "Validate tag and package version")
    script = step["run"]
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nversion = "1.2.3-rc.4"\n',
        encoding="utf-8",
    )

    def run(tag: str) -> subprocess.CompletedProcess[str]:
        output = tmp_path / "output.txt"
        output.write_text("", encoding="utf-8")
        return subprocess.run(
            [sys.executable, "-c", script],
            cwd=tmp_path,
            env=os.environ | {"GITHUB_REF_NAME": tag, "GITHUB_OUTPUT": str(output)},
            check=False,
            capture_output=True,
            text=True,
        )

    accepted = run("v1.2.3-rc.4")
    assert accepted.returncode == 0, accepted.stderr
    assert "package_version=1.2.3-rc.4" in (tmp_path / "output.txt").read_text(encoding="utf-8")

    rejected = run("v1.2.3")
    assert rejected.returncode != 0
    assert "does not match package version" in rejected.stderr


def test_release_queue_max_documents_current_github_schema_authority() -> None:
    workflow_text = (REPOSITORY_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "queue: max" in workflow_text
    assert re.search(
        r"actionlint[^\n]*predat(?:e|ing)[^\n]*queue",
        workflow_text,
        re.IGNORECASE,
    )
    assert re.search(r"GitHub(?:\.com)?[^\n]*authoritative", workflow_text, re.IGNORECASE)


def test_release_image_build_uses_commit_epoch_and_timestamp_rewriting() -> None:
    build = _release_workflow()["jobs"]["build-release-images"]
    epoch = _step(build, "Derive reproducible image timestamp")
    assert epoch["id"] == "source_epoch"
    assert 'git show -s --format=%ct "$GITHUB_SHA"' in epoch["run"]
    assert "SOURCE_DATE_EPOCH=" in epoch["run"]
    assert "source_date_epoch=" in epoch["run"]
    assert "GITHUB_ENV" in epoch["run"]
    assert "GITHUB_OUTPUT" in epoch["run"]

    image_build = _step(build, "Build ${{ matrix.platform }} release image once")["with"]
    assert image_build["no-cache"] == "true"
    assert image_build["build-args"] == ("SOURCE_DATE_EPOCH=${{ steps.source_epoch.outputs.source_date_epoch }}")
    assert "rewrite-timestamp=true" in image_build["outputs"]
    assert "cache-from" not in image_build
    assert "cache-to" not in image_build
    assert image_build["file"] == "${{ env.RELEASE_DOCKERFILE }}"

    prepare = _step(build, "Prepare deterministic release Dockerfile")["run"]
    assert "prepare_release_dockerfile.py" in prepare
    assert '"$RELEASE_DOCKERFILE"' in prepare


def test_release_dockerfile_preparer_disables_retained_uv_cache_state(tmp_path: Path) -> None:
    assert RELEASE_DOCKERFILE_PREPARER.is_file()
    source = REPOSITORY_ROOT / "Dockerfile"
    destination = tmp_path / "Dockerfile.release"
    prepared = _run_release_script(
        RELEASE_DOCKERFILE_PREPARER,
        "--source",
        str(source),
        "--output",
        str(destination),
    )
    assert prepared.returncode == 0, prepared.stderr
    generated = destination.read_text(encoding="utf-8")
    original = source.read_text(encoding="utf-8")
    assert generated.count("RUN UV_NO_CACHE=1 UV_LINK_MODE=copy uv sync") == 2
    assert "ENV UV_NO_CACHE" not in generated
    assert "tacit_ai-*.dist-info/uv_cache.json" in generated
    assert "tacit_ai-*.dist-info/RECORD" in generated
    assert "sed -i '/uv_cache\\.json,/d'" in generated
    assert "UV_NO_CACHE" not in original

    malformed = tmp_path / "Dockerfile.malformed"
    malformed.write_text("# syntax=docker/dockerfile:1\n\nFROM scratch\n", encoding="utf-8")
    rejected_output = tmp_path / "Dockerfile.rejected"
    rejected = _run_release_script(
        RELEASE_DOCKERFILE_PREPARER,
        "--source",
        str(malformed),
        "--output",
        str(rejected_output),
    )
    assert rejected.returncode != 0
    assert "runtime stage" in rejected.stderr.lower()
    assert not rejected_output.exists()


def _run_release_script(
    script: Path,
    *arguments: str,
    cwd: Path | None = None,
    timeout: float = 30,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(script), *arguments],
        cwd=cwd or REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_release_image_archive_guard_streams_and_enforces_file_invariants(
    tmp_path: Path,
) -> None:
    assert RELEASE_ARCHIVE_GUARD.is_file()
    guard_source = RELEASE_ARCHIVE_GUARD.read_text(encoding="utf-8")
    assert ".read_bytes()" not in guard_source
    assert "READ_CHUNK_BYTES" in guard_source
    assert "copied_digest.update(chunk)" in guard_source
    archive = tmp_path / "release.tar"
    checksum = tmp_path / "release.tar.sha256"
    archive.write_bytes(b"release-image" * 4096)

    written = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "write",
        "--archive",
        str(archive),
        "--checksum",
        str(checksum),
        "--max-bytes",
        str(archive.stat().st_size),
    )
    assert written.returncode == 0, written.stderr
    assert checksum.read_text(encoding="utf-8") == (
        f"{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n"
    )

    verified = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "verify",
        "--archive",
        str(archive),
        "--checksum",
        str(checksum),
        "--max-bytes",
        str(archive.stat().st_size),
    )
    assert verified.returncode == 0, verified.stderr

    destination = tmp_path / "scan-copy.tar"
    copied = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "copy",
        "--archive",
        str(archive),
        "--checksum",
        str(checksum),
        "--destination",
        str(destination),
        "--max-bytes",
        str(archive.stat().st_size),
    )
    assert copied.returncode == 0, copied.stderr
    assert destination.read_bytes() == archive.read_bytes()

    archive.write_bytes(archive.read_bytes() + b"changed")
    changed = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "verify",
        "--archive",
        str(archive),
        "--checksum",
        str(checksum),
        "--max-bytes",
        str(archive.stat().st_size),
    )
    assert changed.returncode != 0
    assert "checksum" in changed.stderr.lower()


def test_release_image_archive_guard_rejects_empty_oversized_and_nonregular_files(
    tmp_path: Path,
) -> None:
    assert RELEASE_ARCHIVE_GUARD.is_file()
    checksum = tmp_path / "archive.sha256"

    empty = tmp_path / "empty.tar"
    empty.touch()
    empty_result = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "write",
        "--archive",
        str(empty),
        "--checksum",
        str(checksum),
        "--max-bytes",
        "1024",
    )
    assert empty_result.returncode != 0
    assert "empty" in empty_result.stderr.lower()

    oversized = tmp_path / "oversized.tar"
    with oversized.open("wb") as stream:
        stream.truncate(1025)
    oversized_result = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "write",
        "--archive",
        str(oversized),
        "--checksum",
        str(checksum),
        "--max-bytes",
        "1024",
    )
    assert oversized_result.returncode != 0
    assert "maximum" in oversized_result.stderr.lower()

    target = tmp_path / "target.tar"
    target.write_bytes(b"content")
    symlink = tmp_path / "symlink.tar"
    symlink.symlink_to(target)
    symlink_result = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "write",
        "--archive",
        str(symlink),
        "--checksum",
        str(checksum),
        "--max-bytes",
        "1024",
    )
    assert symlink_result.returncode != 0
    assert "regular" in symlink_result.stderr.lower()


def test_release_workflow_guards_image_archives_at_every_transfer_boundary() -> None:
    workflow = _release_workflow()
    maximum = workflow["env"]["MAX_RELEASE_IMAGE_ARCHIVE_BYTES"]
    assert maximum.isdecimal()
    assert 1 <= int(maximum) <= 2 * 1024 * 1024 * 1024

    jobs = workflow["jobs"]
    build_guard = _step(jobs["build-release-images"], "Validate built image archive before upload")["run"]
    assert "release_image_archive.py write" in build_guard
    assert '"$MAX_RELEASE_IMAGE_ARCHIVE_BYTES"' in build_guard

    scan_guard = _step(jobs["scan-release-images"], "Verify and isolate authoritative image archive")["run"]
    assert "release_image_archive.py copy" in scan_guard
    assert '"$MAX_RELEASE_IMAGE_ARCHIVE_BYTES"' in scan_guard

    publish_guard = _step(jobs["publish-ghcr"], "Verify transferred image archives")["run"]
    assert "release_image_archive.py verify" in publish_guard
    assert '"$MAX_RELEASE_IMAGE_ARCHIVE_BYTES"' in publish_guard
    assert "tacit-release-amd64.tar" in publish_guard
    assert "tacit-release-arm64.tar" in publish_guard

    for job_name in ("scan-release-images", "publish-ghcr"):
        checkout = next(
            step for step in jobs[job_name]["steps"] if step.get("uses", "").startswith("actions/checkout@")
        )
        assert checkout["with"] == {
            "persist-credentials": "false",
            "ref": "${{ github.sha }}",
        }


def _init_git_history(path: Path) -> tuple[str, str]:
    subprocess.run(["git", "init", "--quiet"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "tests@example.invalid"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Tacit Tests"], cwd=path, check=True)
    tracked = path / "tracked.txt"
    tracked.write_text("first\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=path, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "first"], cwd=path, check=True)
    first = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, check=True, capture_output=True, text=True
    ).stdout.strip()
    tracked.write_text("second\n", encoding="utf-8")
    subprocess.run(["git", "commit", "--quiet", "-am", "second"], cwd=path, check=True)
    second = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, check=True, capture_output=True, text=True
    ).stdout.strip()
    return first, second


def _gitleaks_range(
    repository: Path,
    *,
    event: str,
    head: str,
    base: str = "",
    before: str = "",
    baseline: str = "",
) -> tuple[subprocess.CompletedProcess[str], str]:
    output = repository / "github-output.txt"
    output.unlink(missing_ok=True)
    result = _run_release_script(
        GITLEAKS_RANGE_SELECTOR,
        "--repository",
        str(repository),
        "--event-name",
        event,
        "--head-sha",
        head,
        "--base-sha",
        base,
        "--before-sha",
        before,
        "--baseline-sha",
        baseline,
        "--output",
        str(output),
    )
    selected_range = output.read_text(encoding="utf-8").strip() if result.returncode == 0 else ""
    return result, selected_range


def test_gitleaks_range_selector_requires_full_baseline_before_incremental_ranges(
    tmp_path: Path,
) -> None:
    assert GITLEAKS_RANGE_SELECTOR.is_file()
    first, second = _init_git_history(tmp_path)

    pull_request, selected = _gitleaks_range(
        tmp_path,
        event="pull_request",
        head=second,
        base=first,
    )
    assert pull_request.returncode == 0, pull_request.stderr
    assert selected == f"log_opts={second}"
    assert "full reachable history" in pull_request.stderr.lower()

    push, selected = _gitleaks_range(
        tmp_path,
        event="push",
        head=second,
        before=first,
        baseline=first,
    )
    assert push.returncode == 0, push.stderr
    assert selected == f"log_opts={first}..{second}"

    initial, selected = _gitleaks_range(tmp_path, event="push", head=first, before="0" * 40)
    assert initial.returncode == 0, initial.stderr
    assert selected == f"log_opts={first}"

    missing_before, selected = _gitleaks_range(
        tmp_path,
        event="push",
        head=second,
        before="1" * 40,
        baseline=first,
    )
    assert missing_before.returncode == 0, missing_before.stderr
    assert selected == f"log_opts={first}..{second}"

    same_head, selected = _gitleaks_range(
        tmp_path,
        event="pull_request",
        head=second,
        base=second,
        baseline=first,
    )
    assert same_head.returncode == 0, same_head.stderr
    assert selected == f"log_opts={second}"

    untrusted, selected = _gitleaks_range(
        tmp_path,
        event="push",
        head=second,
        before=first,
        baseline="1" * 40,
    )
    assert untrusted.returncode != 0
    assert selected == ""
    assert "baseline" in untrusted.stderr.lower()


def test_gitleaks_full_baseline_reaches_an_ancestor_deleted_secret(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "tests@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Tacit Tests"], cwd=tmp_path, check=True)
    tracked = tmp_path / "tracked.txt"
    deleted_secret = "github_pat_11AA0_this_secret_only_exists_in_history"
    tracked.write_text(f"{deleted_secret}\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "add secret"], cwd=tmp_path, check=True)
    tracked.write_text("secret removed\n", encoding="utf-8")
    subprocess.run(["git", "commit", "--quiet", "-am", "delete secret"], cwd=tmp_path, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tracked.write_text("clean head\n", encoding="utf-8")
    subprocess.run(["git", "commit", "--quiet", "-am", "clean head"], cwd=tmp_path, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    result, selected = _gitleaks_range(
        tmp_path,
        event="pull_request",
        head=head,
        base=base,
    )

    assert result.returncode == 0, result.stderr
    assert selected == f"log_opts={head}"
    assert deleted_secret not in tracked.read_text(encoding="utf-8")
    reachable_history = subprocess.run(
        ["git", "log", "-p", head],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert deleted_secret in reachable_history


def test_ci_gitleaks_scans_explicit_history_range_and_gitless_snapshot() -> None:
    workflow = _ci_workflow()
    baseline = workflow["env"]["GITLEAKS_HISTORY_BASELINE_SHA"]
    assert re.fullmatch(r"[0-9a-f]{40}", baseline)
    assert (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", baseline, "HEAD"],
            cwd=REPOSITORY_ROOT,
            check=False,
        ).returncode
        == 0
    )
    secret_scan = workflow["jobs"]["secret-scan"]
    checkout = next(step for step in secret_scan["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"] == {
        "fetch-depth": "0",
        "persist-credentials": "false",
        "ref": "${{ github.event.pull_request.head.sha || github.sha }}",
    }

    selector = _step(secret_scan, "Select committed-history scan range")
    assert "gitleaks_range.py" in selector["run"]
    assert selector["env"]["BASELINE_SHA"] == "${{ env.GITLEAKS_HISTORY_BASELINE_SHA }}"
    assert '--baseline-sha "$BASELINE_SHA"' in selector["run"]
    workflow_text = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert re.search(r"one-time full reachable-history Gitleaks baseline", workflow_text, re.IGNORECASE)
    snapshot = _step(secret_scan, "Prepare current-tree scan without Git objects")["run"]
    assert "git archive" in snapshot
    assert ".gitleaks-worktree" in snapshot

    history_step = _step(secret_scan, "Gitleaks committed history")
    assert history_step["env"] == {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": "/github/workspace",
    }
    history = history_step["with"]["args"]
    assert "--log-opts=${{ steps.gitleaks_range.outputs.log_opts }}" in history
    assert "--no-git" not in history
    assert "--all" not in history

    worktree = _step(secret_scan, "Gitleaks current tree")["with"]["args"]
    assert "--no-git" in worktree
    assert "--source /github/workspace/.gitleaks-worktree" in worktree
    assert "--source /github/workspace --no-git" not in worktree


def test_release_image_is_reproducible_with_no_cache_when_docker_is_requested(
    tmp_path: Path,
) -> None:
    if os.environ.get("TACIT_RUN_DOCKER_RELEASE_REPRO") != "1":
        pytest.skip("set TACIT_RUN_DOCKER_RELEASE_REPRO=1 for the two-build image digest check")

    commit = subprocess.run(
        ["git", "show", "-s", "--format=%H:%ct", "HEAD"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    head_sha, epoch = commit.split(":", 1)
    package_version = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "version"
    ]
    context = tmp_path / "context"
    context.mkdir()
    tracked = subprocess.run(
        ["git", "ls-files", "--cached", "-z"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    for encoded in tracked:
        if not encoded:
            continue
        relative = Path(os.fsdecode(encoded))
        source = REPOSITORY_ROOT / relative
        if not source.exists() and not source.is_symlink():
            continue
        destination = context / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_symlink():
            destination.symlink_to(os.readlink(source))
        elif source.is_file():
            shutil.copy2(source, destination)

    release_dockerfile = tmp_path / "Dockerfile.release"
    prepared = _run_release_script(
        RELEASE_DOCKERFILE_PREPARER,
        "--source",
        str(context / "Dockerfile"),
        "--output",
        str(release_dockerfile),
    )
    assert prepared.returncode == 0, prepared.stderr
    digests: list[str] = []
    for attempt in range(2):
        archive = tmp_path / f"release-{attempt}.docker.tar"
        metadata = tmp_path / f"release-{attempt}.metadata.json"
        command = [
            "docker",
            "buildx",
            "build",
            "--no-cache",
            "--pull",
            "--platform",
            "linux/amd64",
            "--build-arg",
            f"SOURCE_DATE_EPOCH={epoch}",
            "--tag",
            "tacit:release-reproducibility-test",
            "--label",
            "org.opencontainers.image.source=https://github.com/aditki/tacit",
            "--label",
            f"org.opencontainers.image.revision={head_sha}",
            "--label",
            f"org.opencontainers.image.version={package_version}",
            "--file",
            str(release_dockerfile),
            "--metadata-file",
            str(metadata),
            "--output",
            f"type=docker,dest={archive},rewrite-timestamp=true",
            str(context),
        ]
        built = subprocess.run(command, check=False, capture_output=True, text=True)
        assert built.returncode == 0, built.stderr
        build_metadata = json.loads(metadata.read_text(encoding="utf-8"))
        digest = build_metadata["containerimage.digest"]
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
        digests.append(digest)

    assert digests[0] == digests[1]


def test_release_actions_and_privileged_tools_are_pinned() -> None:
    workflow = _release_workflow()
    references = [step["uses"] for job in workflow["jobs"].values() for step in job["steps"] if "uses" in step]
    assert references
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", reference) for reference in references)

    for job_name, job in workflow["jobs"].items():
        expected_runner = "${{ matrix.runner }}" if job_name == "build-binaries" else "ubuntu-24.04"
        assert job["runs-on"] == expected_runner
        for step in job["steps"]:
            reference = step.get("uses", "")
            if reference.startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] == "false"
            if reference.startswith("astral-sh/setup-uv@"):
                assert re.fullmatch(r"\d+\.\d+\.\d+", step["with"]["version"])
                checksum = step["with"]["checksum"]
                if job_name == "build-binaries":
                    assert checksum == "${{ matrix.uv_checksum }}"
                else:
                    assert re.fullmatch(r"[0-9a-f]{64}", checksum)
            if reference.startswith("docker/setup-qemu-action@"):
                assert re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", step["with"]["image"])
            if reference.startswith("aquasecurity/trivy-action@"):
                assert re.fullmatch(r"v\d+\.\d+\.\d+", step["with"]["version"])

    assert workflow["permissions"] == {"contents": "read"}
    contents_write_jobs = {
        name for name, job in workflow["jobs"].items() if job.get("permissions", {}).get("contents") == "write"
    }
    assert contents_write_jobs == {"publish-github-release"}

    assert not any(reference.startswith("docker/setup-buildx-action@") for reference in references)
    for job_name in ("build-release-images", "ghcr-preflight", "publish-ghcr"):
        install = _step(workflow["jobs"][job_name], "Install checksum-pinned Buildx")["run"]
        assert 'case "${RUNNER_OS}/${RUNNER_ARCH}"' in install
        assert "buildx-v${version}.linux-amd64" in install
        assert "sha256sum --check" in install
        assert "docker-buildx" in install
        assert "48af8a397ebd60178778bf63611dbcebe5f5e7a9be90eb9147b24b9587455778" in install

    builder = _step(workflow["jobs"]["build-release-images"], "Create pinned BuildKit builder")["run"]
    assert re.search(r"moby/buildkit:v\d+\.\d+\.\d+@sha256:[0-9a-f]{64}", builder)


def test_ci_authorization_workflow_uses_immutable_dependencies() -> None:
    workflow = _ci_workflow()
    references = [step["uses"] for job in workflow["jobs"].values() for step in job["steps"] if "uses" in step]
    assert references
    assert (
        "docker://zricethezav/gitleaks@sha256:" "e1b35e12a8c6fa8901f060459cfb6b2fc4c484d3afbe3b029733a3bbfab07055"
    ) in references
    for reference in references:
        if reference.startswith("docker://"):
            assert re.fullmatch(r"docker://[^@]+@sha256:[0-9a-f]{64}", reference)
        else:
            assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", reference)

    for job in workflow["jobs"].values():
        for step in job["steps"]:
            reference = step.get("uses", "")
            if reference.startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] == "false"
            if reference.startswith("astral-sh/setup-uv@"):
                assert step["with"]["version"] == "0.12.1"
                assert step["with"]["python-version"] == "3.12.13"
                checksum = step["with"]["checksum"]
                if checksum == "${{ matrix.uv_checksum }}":
                    continue
                assert checksum == ("90b2f223fb69d19db49e117da601f6497" "8593417988530aa733d456141b4bcbb")

    fresh_install = workflow["jobs"]["fresh-install"]
    assert fresh_install["strategy"]["matrix"]["include"] == [
        {
            "os": "ubuntu-24.04",
            "uv_checksum": "90b2f223fb69d19db49e117da601f64978593417988530aa733d456141b4bcbb",
        },
        {
            "os": "macos-15",
            "uv_checksum": "77d2906988e8074fd43f2f329ec452ebbf9b0c257ba1c66451c71de70a6baf42",
        },
    ]


def test_release_scanners_never_run_with_ghcr_write_credentials() -> None:
    jobs = _release_workflow()["jobs"]
    scanner_jobs: set[str] = set()
    for job_name, job in jobs.items():
        contains_scanner = any(
            "trivy" in str(step.get("uses", "")).lower() or "trivy" in str(step.get("run", "")).lower()
            for step in job["steps"]
        )
        if contains_scanner:
            scanner_jobs.add(job_name)
            assert job.get("permissions", {}).get("packages") != "write"

    assert scanner_jobs == {"scan-release-images"}
    scanner = jobs["scan-release-images"]
    assert scanner["permissions"] == {"contents": "read"}
    assert _job_needs(scanner) == {"build-release-images"}

    verify = _step(scanner, "Verify and isolate authoritative image archive")["run"]
    assert "release_image_archive.py copy" in verify
    assert "SCAN_IMAGE_ARCHIVE" in verify
    scan = _step(scanner, "Scan ${{ matrix.platform }} release image")
    assert scan["with"]["input"] == "${{ env.SCAN_IMAGE_ARCHIVE }}"

    publish = jobs["publish-ghcr"]
    assert publish["permissions"] == {"contents": "read", "packages": "write"}
    assert {"ghcr-preflight", "scan-release-images"} <= _job_needs(publish)


def test_release_authorizes_the_tagged_main_commit_after_all_read_only_gates() -> None:
    jobs = _release_workflow()["jobs"]
    authorization = jobs["authorize-publication"]
    required_gates = {
        "validate-release",
        "build-dist",
        "build-release-images",
        "build-binaries",
        "pypi-preflight",
        "github-release-preflight",
        "ghcr-preflight",
        "scan-release-images",
    }
    assert _job_needs(authorization) == required_gates
    assert authorization["permissions"] == {"actions": "read", "contents": "read"}

    checkout = next(step for step in authorization["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"] == {
        "fetch-depth": "0",
        "persist-credentials": "false",
        "ref": "${{ github.sha }}",
    }

    ancestry = _step(authorization, "Prove tagged commit belongs to current main")["run"]
    assert 'git fetch --no-tags origin "+refs/heads/main:refs/remotes/origin/main"' in ancestry
    assert '[[ "$(git rev-parse HEAD)" == "$GITHUB_SHA" ]]' in ancestry
    assert 'git merge-base --is-ancestor "$GITHUB_SHA" refs/remotes/origin/main' in ancestry

    ci_gate = _step(authorization, "Require successful CI for tagged commit")["run"]
    assert "/actions/workflows/ci.yml/runs" in ci_gate
    assert '"head_sha": os.environ["GITHUB_SHA"]' in ci_gate
    assert '"branch": "main"' in ci_gate
    assert 'run.get("conclusion") == "success"' in ci_gate

    for job_name in ("validate-release", "build-dist", "build-binaries", "build-release-images"):
        checkout = next(
            step for step in jobs[job_name]["steps"] if step.get("uses", "").startswith("actions/checkout@")
        )
        assert checkout["with"]["ref"] == "${{ github.sha }}"

    assert "authorize-publication" in _job_needs(jobs["publish-ghcr"])


def test_every_release_job_has_a_bounded_timeout() -> None:
    jobs = _release_workflow()["jobs"]
    assert jobs
    for name, job in jobs.items():
        timeout = int(job["timeout-minutes"])
        assert 1 <= timeout <= 90, name


def test_release_scans_and_publishes_the_same_image_archives() -> None:
    jobs = _release_workflow()["jobs"]
    build = jobs["build-release-images"]
    assert _job_needs(build) == {"validate-release"}
    assert {(entry["platform"], entry["arch"]) for entry in build["strategy"]["matrix"]["include"]} == {
        ("linux/amd64", "amd64"),
        ("linux/arm64", "arm64"),
    }

    archive = "${{ env.IMAGE_ARCHIVE }}"
    image_build = _step(build, "Build ${{ matrix.platform }} release image once")
    upload = next(step for step in build["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
    assert image_build["with"]["outputs"] == (f"type=docker,dest={archive},rewrite-timestamp=true")
    assert image_build["with"]["push"] == "false"
    assert archive in upload["with"]["path"]
    assert f"{archive}.sha256" in upload["with"]["path"]
    assert not any(
        "trivy" in str(step.get("uses", "")).lower() or "trivy" in str(step.get("run", "")).lower()
        for step in build["steps"]
    )

    scan = jobs["scan-release-images"]
    assert _job_needs(scan) == {"build-release-images"}
    download = next(step for step in scan["steps"] if step.get("uses", "").startswith("actions/download-artifact@"))
    assert download["with"]["name"] == "release-image-${{ matrix.arch }}"

    publish = jobs["publish-ghcr"]
    assert _job_needs(publish) == {
        "validate-release",
        "build-dist",
        "build-release-images",
        "build-binaries",
        "pypi-preflight",
        "github-release-preflight",
        "ghcr-preflight",
        "scan-release-images",
        "authorize-publication",
    }
    assert not any(step.get("uses", "").startswith("docker/build-push-action@") for step in publish["steps"])
    transferred = _step(publish, "Verify transferred image archives")["run"]
    pushed = _step(publish, "Publish scanned architecture images")["run"]
    assert "release_image_archive.py verify" in transferred
    assert 'docker load --input "$archive"' in pushed
    assert "remote_config" in pushed
    assert "Published ${arch} image differs from the scanned archive" in pushed
    immutable = _step(publish, "Publish immutable multi-architecture version")["run"]
    assert "docker buildx imagetools create" in immutable
    assert "org.opencontainers.image.revision" in immutable


def test_release_retry_reuses_only_the_current_build_children() -> None:
    jobs = _release_workflow()["jobs"]
    publish = jobs["publish-ghcr"]
    publish_arches = _step(publish, "Publish scanned architecture images")
    assert "if" not in publish_arches
    assert "staging_ref=" in publish_arches["run"]
    assert "checksum" in publish_arches["run"]
    assert "remote_config" in publish_arches["run"]

    pin = _step(publish, "Pin scanned architecture digests")
    assert "EXISTING" not in pin.get("env", {})
    assert "PUBLISHED_AMD64_SOURCE" in pin["env"]
    assert "PUBLISHED_ARM64_SOURCE" in pin["env"]

    compare = _step(publish, "Compare current build with existing immutable version")
    compare_script = compare["run"]
    assert compare["if"] == "steps.existing_version.outputs.exists == 'true'"
    assert compare["env"]["CURRENT_AMD64_SOURCE"] == "${{ steps.scanned_images.outputs.amd64_source }}"
    assert compare["env"]["CURRENT_ARM64_SOURCE"] == "${{ steps.scanned_images.outputs.arm64_source }}"
    assert "does not match the current build" in compare_script
    assert "EXPECTED_AMD64_SOURCE" in compare["env"]
    assert "EXPECTED_ARM64_SOURCE" in compare["env"]

    step_names = [step.get("name") for step in publish["steps"]]
    assert step_names.index("Publish scanned architecture images") < step_names.index(
        "Compare current build with existing immutable version"
    )
    assert step_names.index("Compare current build with existing immutable version") < step_names.index(
        "Pin immutable version digest"
    )


def test_release_publication_graph_is_ordered_and_retry_checked() -> None:
    jobs = _release_workflow()["jobs"]
    preflight_job = jobs["pypi-preflight"]
    assert _job_needs(preflight_job) == {"validate-release", "build-dist"}
    preflight = _step(preflight_job, "Verify any existing PyPI files")["run"]
    assert "digests" in preflight and "mismatched" in preflight

    github_preflight = jobs["github-release-preflight"]
    assert _job_needs(github_preflight) == {"validate-release", "build-binaries"}
    github_script = _step(github_preflight, "Verify any existing GitHub release assets")["run"]
    assert "hashlib.sha256" in github_script
    assert "unexpected" in github_script and "mismatched" in github_script

    ghcr_preflight = jobs["ghcr-preflight"]
    assert _job_needs(ghcr_preflight) == {"validate-release", "build-release-images"}
    assert ghcr_preflight["permissions"] == {"contents": "read", "packages": "read"}

    ghcr = jobs["publish-ghcr"]
    assert {
        "build-dist",
        "build-release-images",
        "build-binaries",
        "pypi-preflight",
        "github-release-preflight",
        "ghcr-preflight",
        "scan-release-images",
        "authorize-publication",
    } <= _job_needs(ghcr)

    pypi = jobs["publish-pypi"]
    assert _job_needs(pypi) == {"validate-release", "build-dist", "pypi-preflight", "publish-ghcr"}
    assert not any(step.get("name") == "Verify any existing PyPI files" for step in pypi["steps"])

    release = jobs["publish-github-release"]
    assert _job_needs(release) == {"validate-release", "build-binaries", "publish-pypi"}
    assert release["environment"]["name"] == "github-release"
    assert release["permissions"] == {"contents": "write"}

    required_gates = {
        "validate-release",
        "build-dist",
        "build-release-images",
        "build-binaries",
        "pypi-preflight",
        "github-release-preflight",
        "ghcr-preflight",
        "scan-release-images",
        "authorize-publication",
    }
    for publishing_job in ("publish-ghcr", "publish-pypi", "publish-github-release"):
        assert required_gates <= _transitive_needs(jobs, publishing_job)

    existing_version = _step(ghcr, "Verify GHCR preflight state")["run"]
    assert "changed after read-only preflight" in existing_version
    assert "appeared after read-only preflight" in existing_version
    assert "EXPECTED_INDEX_SOURCE" in existing_version
    assert "EXPECTED_AMD64_SOURCE" in existing_version
    assert "EXPECTED_ARM64_SOURCE" in existing_version

    assert not any(
        "trivy" in str(step.get("uses", "")).lower() or "trivy" in str(step.get("run", "")).lower()
        for step in ghcr_preflight["steps"]
    )

    pin = _step(ghcr, "Pin immutable version digest")
    assert "index_source" in pin["run"]
    assert "VERSION_REF" not in pin["env"]
    assert 'imagetools inspect "$VERSION_REF"' not in pin["run"]

    publish_version = _step(ghcr, "Publish immutable multi-architecture version")
    assert publish_version["id"] == "publish_version"
    assert '--metadata-file "$metadata_file"' in publish_version["run"]
    assert '."containerimage.descriptor".digest' in publish_version["run"]
    assert "index_source=" in publish_version["run"]

    exact_images = _step(ghcr, "Pin scanned architecture digests")
    assert exact_images["id"] == "scanned_images"
    for arch in ("amd64", "arm64"):
        assert f"{arch}_source" in exact_images["run"]

    verify_step = _step(ghcr, "Verify pinned immutable version")
    verify = verify_step["run"]
    assert verify_step["env"]["INDEX_SOURCE"] == "${{ steps.pinned_version.outputs.index_source }}"
    assert "VERSION_REF" not in verify_step["env"]
    assert 'imagetools inspect "$INDEX_SOURCE"' in verify
    assert 'imagetools inspect "$VERSION_REF"' not in verify
    assert "Immutable version child digest mismatch" in verify
    assert 'platforms" != "linux/amd64,linux/arm64"' in verify
    for label in ("source", "revision", "version"):
        assert f"org.opencontainers.image.{label}" in verify

    assert "ghcr-preflight" in _transitive_needs(jobs, "publish-ghcr")

    aliases = _step(ghcr, "Advance stable aliases monotonically")["run"]
    assert 'candidate_key="$(version_key "$CANDIDATE_VERSION")"' in aliases
    assert "same version but different images" in aliases
    assert 'imagetools inspect "$VERSION_REF"' not in aliases
    assert 'imagetools create --tag "$alias_ref" "$VERSION_SOURCE"' in aliases
    assert "EXPECTED_AMD64_DIGEST" in aliases
    assert "EXPECTED_ARM64_DIGEST" in aliases

    publish = _step(pypi, "Publish to PyPI")
    postflight = _step(pypi, "Verify published PyPI files")["run"]
    assert publish["with"]["skip-existing"] == "true"
    assert "remote == local" in postflight


def test_release_smoke_checks_all_runtime_version_surfaces() -> None:
    smoke = _step(_release_workflow()["jobs"]["build-dist"], "Smoke-test the wheel")
    script = smoke["run"]
    assert smoke["env"]["EXPECTED_VERSION"] == "${{ needs.validate-release.outputs.package_version }}"
    assert "distribution_version" in script
    assert "tacit.__version__" in script
    assert "tacit.__file__" in script
    assert '".smoke-venv/bin/tacit", "--version"' in script


def test_release_builds_verified_binaries_on_exact_runners() -> None:
    jobs = _release_workflow()["jobs"]
    binary_job = jobs["build-binaries"]
    assert _job_needs(binary_job) == {"validate-release"}
    matrix = binary_job["strategy"]["matrix"]["include"]
    assert matrix == [
        {
            "runner": "ubuntu-24.04",
            "artifact": "tacit-binary-linux-x86_64",
            "binary": "dist/tacit",
            "package": "tacit-linux-x86_64.tar.gz",
            "uv_checksum": "90b2f223fb69d19db49e117da601f64978593417988530aa733d456141b4bcbb",
        },
        {
            "runner": "macos-15",
            "artifact": "tacit-binary-macos-arm64",
            "binary": "dist/tacit",
            "package": "tacit-macos-arm64.tar.gz",
            "uv_checksum": "77d2906988e8074fd43f2f329ec452ebbf9b0c257ba1c66451c71de70a6baf42",
        },
        {
            "runner": "windows-2025",
            "artifact": "tacit-binary-windows-x86_64",
            "binary": "dist/tacit.exe",
            "package": "tacit-windows-x86_64.zip",
            "uv_checksum": "8fcb0cb46e1229065e344758980924e569bef5882ef45f46fada8fb24e06b74a",
        },
    ]

    install = _step(binary_job, "Install locked binary build dependencies")
    assert install["run"] == "uv sync --frozen --all-extras --dev"
    smoke = _step(binary_job, "Smoke-test binary version")
    assert smoke["env"]["EXPECTED_VERSION"] == "${{ needs.validate-release.outputs.package_version }}"
    assert '[str(binary), "--version"]' in smoke["run"]
    assert "Version(result.removeprefix(prefix))" in smoke["run"]
    package = _step(binary_job, "Package binary and checksum")["run"]
    assert "package_release_binary.py" in package
    assert '--source "$BINARY_PATH"' in package
    assert '--package "$PACKAGE_NAME"' in package
    assert "--max-binary-bytes 536870912" in package
    assert "--max-package-bytes 536870912" in package
    packager_source = RELEASE_BINARY_PACKAGER.read_text(encoding="utf-8")
    assert ".read_bytes()" not in packager_source
    assert ".lstat()" in packager_source
    assert "os.O_NOFOLLOW" in packager_source
    assert "os.fstat" in packager_source
    assert "stat.S_ISREG" in packager_source
    assert "READ_CHUNK_BYTES" in packager_source
    assert "gzip.GzipFile" in packager_source
    assert "mtime=0" in packager_source
    assert "TarInfo" in packager_source
    assert "ZipInfo" in packager_source
    assert "date_time = (1980, 1, 1, 0, 0, 0)" in packager_source
    assert binary_job["env"]["SOURCE_DATE_EPOCH"] == "0"
    assert binary_job["env"]["PYTHONHASHSEED"] == "0"

    release = jobs["publish-github-release"]
    verify = _step(release, "Verify binary packages and checksums")["run"]
    for filename in (
        "tacit-linux-x86_64.tar.gz",
        "tacit-macos-arm64.tar.gz",
        "tacit-windows-x86_64.zip",
    ):
        assert filename in verify
    assert "sha256sum --check ./*.sha256" in verify
    publish = _step(release, "Publish GitHub release binaries")
    assert publish["uses"] == "softprops/action-gh-release@3bb12739c298aeb8a4eeaf626c5b8d85266b0e65"
    assert publish["with"]["overwrite_files"] == "false"


def test_release_binary_packaging_rejects_oversized_input_before_reading(
    tmp_path: Path,
) -> None:
    binary = tmp_path / "oversized-binary"
    binary.write_bytes(b"123456789")
    package = tmp_path / "tacit-test.tar.gz"
    result = _run_release_script(
        RELEASE_BINARY_PACKAGER,
        "--source",
        str(binary),
        "--package",
        str(package),
        "--max-binary-bytes",
        "8",
        "--max-package-bytes",
        "1024",
    )

    assert result.returncode != 0
    assert "exceeds the size limit" in result.stderr
    assert not package.exists()
    assert not package.with_name(f"{package.name}.sha256").exists()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO admission requires POSIX")
def test_release_binary_packaging_rejects_symlink_fifo_and_nonregular_inputs(
    tmp_path: Path,
) -> None:
    regular = tmp_path / "regular-binary"
    regular.write_bytes(b"executable")
    symlink = tmp_path / "symlink-binary"
    symlink.symlink_to(regular)
    fifo = tmp_path / "fifo-binary"
    os.mkfifo(fifo)
    directory = tmp_path / "directory-binary"
    directory.mkdir()

    for source in (symlink, fifo, directory):
        package = tmp_path / f"{source.name}.tar.gz"
        result = _run_release_script(
            RELEASE_BINARY_PACKAGER,
            "--source",
            str(source),
            "--package",
            str(package),
            "--max-binary-bytes",
            "1024",
            "--max-package-bytes",
            "4096",
            timeout=5,
        )
        assert result.returncode != 0
        assert "regular file" in result.stderr.lower()
        assert not package.exists()
        assert not package.with_name(f"{package.name}.sha256").exists()


def test_release_binary_packaging_rejects_path_swap_before_archive_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packager = _load_script_module(RELEASE_BINARY_PACKAGER)
    source = tmp_path / "tacit"
    source.write_bytes(b"expected executable")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replaced executable")
    assert replacement.stat().st_size == source.stat().st_size
    displaced = tmp_path / "displaced"
    package = tmp_path / "tacit-test.tar.gz"
    original_open = packager.os.open
    observed_flags: list[int] = []

    def swap_before_open(path: Path, flags: int) -> int:
        if Path(path) == source:
            observed_flags.append(flags)
            source.rename(displaced)
            replacement.rename(source)
        return original_open(path, flags)

    monkeypatch.setattr(packager.os, "open", swap_before_open)

    with pytest.raises(packager.BinaryPackagingError, match="changed before it was opened"):
        packager.package_binary(
            source,
            package,
            maximum_binary_bytes=1024,
            maximum_package_bytes=4096,
        )

    assert observed_flags
    if hasattr(os, "O_NOFOLLOW"):
        assert observed_flags[0] & os.O_NOFOLLOW
    assert not package.exists()
    assert not package.with_name(f"{package.name}.sha256").exists()


def test_release_binary_archives_are_reproducible(tmp_path: Path) -> None:
    binary = tmp_path / "input-binary"
    binary.write_bytes(b"stable executable bytes\n")

    for package_name in ("tacit-test.tar.gz", "tacit-test.zip"):
        package = tmp_path / package_name
        arguments = (
            "--source",
            str(binary),
            "--package",
            str(package),
            "--max-binary-bytes",
            "1024",
            "--max-package-bytes",
            "4096",
        )
        first = _run_release_script(RELEASE_BINARY_PACKAGER, *arguments)
        assert first.returncode == 0, first.stderr
        first_bytes = package.read_bytes()
        os.utime(binary, (1_900_000_000, 1_900_000_000))
        second = _run_release_script(RELEASE_BINARY_PACKAGER, *arguments)
        assert second.returncode == 0, second.stderr
        assert package.read_bytes() == first_bytes
        expected_digest = hashlib.sha256(first_bytes).hexdigest()
        assert package.with_name(f"{package.name}.sha256").read_text(encoding="utf-8") == (
            f"{expected_digest}  {package.name}\n"
        )

        if package.suffix == ".zip":
            with zipfile.ZipFile(package) as archive:
                zip_info = archive.infolist()[0]
                assert zip_info.filename == "tacit.exe"
                assert zip_info.date_time == (1980, 1, 1, 0, 0, 0)
        else:
            with tarfile.open(package, "r:gz") as archive:
                tar_info = archive.getmember("tacit")
                assert (tar_info.mtime, tar_info.uid, tar_info.gid, tar_info.uname, tar_info.gname) == (
                    0,
                    0,
                    0,
                    "",
                    "",
                )


def test_pypi_preflight_and_postflight_bound_metadata_before_decoding(
    tmp_path: Path,
) -> None:
    jobs = _release_workflow()["jobs"]
    scripts = {
        "preflight": _embedded_python(_step(jobs["pypi-preflight"], "Verify any existing PyPI files")["run"]),
        "postflight": _embedded_python(_step(jobs["publish-pypi"], "Verify published PyPI files")["run"]),
    }
    endpoint = 'f"https://pypi.org/pypi/tacit-ai/' "{os.environ['PACKAGE_VERSION']}/json\""
    for script in scripts.values():
        assert "MAX_PYPI_METADATA_BYTES" in script
        assert "READ_CHUNK_BYTES = 1024 * 1024" in script
        assert "Content-Length" in script
        assert "MAX_PYPI_METADATA_BYTES + 1" in script
        assert "json.load(response)" not in script
        assert ".read_bytes()" not in script
        assert 'path.open("rb")' in script
        tree = ast.parse(script)
        assert not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "read"
            and not node.args
            and not node.keywords
            for node in ast.walk(tree)
        )

    body = json.dumps({"urls": [], "padding": "x" * (1024 * 1024)}).encode()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dist = tmp_path / "dist"
        dist.mkdir()
        for name, original in scripts.items():
            assert endpoint in original
            script = original.replace(endpoint, 'os.environ["PYPI_TEST_URL"]', 1)
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=tmp_path,
                env=os.environ
                | {
                    "PACKAGE_VERSION": "1.2.3",
                    "PYPI_TEST_URL": f"http://127.0.0.1:{server.server_port}/metadata",
                },
                check=False,
                capture_output=True,
                text=True,
            )
            assert result.returncode != 0, name
            assert "PyPI metadata exceeds the size limit" in result.stderr
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_github_release_preflight_rejects_mismatched_assets(tmp_path: Path) -> None:
    job = _release_workflow()["jobs"]["github-release-preflight"]
    script = _embedded_python(_step(job, "Verify any existing GitHub release assets")["run"])
    asset_names = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
        "tacit-macos-arm64.tar.gz",
        "tacit-macos-arm64.tar.gz.sha256",
        "tacit-windows-x86_64.zip",
        "tacit-windows-x86_64.zip.sha256",
    )
    for name in asset_names:
        (tmp_path / name).write_bytes(f"local:{name}".encode())
    local_asset = (tmp_path / asset_names[0]).read_bytes()
    remote_asset = b"x" * len(local_asset)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/repos/aditki/tacit/releases/tags/v1.2.3":
                port = int(getattr(self.server, "server_port"))
                payload = {
                    "assets": [
                        {
                            "name": asset_names[0],
                            "url": f"http://127.0.0.1:{port}/assets/1",
                            "size": len(remote_asset),
                        }
                    ]
                }
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/assets/1":
                body = remote_asset
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "test-token",
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert result.returncode != 0
    assert "GitHub release differs from local artifacts" in result.stderr


def test_github_release_preflight_rejects_unexpected_names_before_download(
    tmp_path: Path,
) -> None:
    job = _release_workflow()["jobs"]["github-release-preflight"]
    script = _embedded_python(_step(job, "Verify any existing GitHub release assets")["run"])
    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
        "tacit-macos-arm64.tar.gz",
        "tacit-macos-arm64.tar.gz.sha256",
        "tacit-windows-x86_64.zip",
        "tacit-windows-x86_64.zip.sha256",
    )
    for name in required:
        (tmp_path / name).write_bytes(f"local:{name}".encode())

    downloaded: list[str] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/repos/aditki/tacit/releases/tags/v1.2.3":
                port = int(getattr(self.server, "server_port"))
                body = json.dumps(
                    {
                        "assets": [
                            {
                                "name": "unexpected-debug-bundle.zip",
                                "size": 1,
                                "url": f"http://127.0.0.1:{port}/assets/unexpected",
                            }
                        ]
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/assets/unexpected":
                downloaded.append(self.path)
                body = b"x"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "test-token",
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert result.returncode != 0
    assert "unexpected" in result.stderr.lower()
    assert downloaded == []


def test_github_release_preflight_bounds_remote_asset_reads(tmp_path: Path) -> None:
    job = _release_workflow()["jobs"]["github-release-preflight"]
    script = _embedded_python(_step(job, "Verify any existing GitHub release assets")["run"])
    tree = ast.parse(script)
    unbounded_reads = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "read"
        and not node.args
        and not node.keywords
    ]
    assert unbounded_reads == []
    assert "MAX_RELEASE_ASSET_BYTES" in script
    assert "Content-Length" in script
    assert "declared size" in script
    assert "total > declared" in script
    assert "remaining_with_guard = declared - total + 1" in script

    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
        "tacit-macos-arm64.tar.gz",
        "tacit-macos-arm64.tar.gz.sha256",
        "tacit-windows-x86_64.zip",
        "tacit-windows-x86_64.zip.sha256",
    )
    for name in required:
        (tmp_path / name).write_bytes(f"local:{name}".encode())

    downloaded: list[str] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/repos/aditki/tacit/releases/tags/v1.2.3":
                port = int(getattr(self.server, "server_port"))
                body = json.dumps(
                    {
                        "assets": [
                            {
                                "name": required[0],
                                "size": 2**40,
                                "url": f"http://127.0.0.1:{port}/assets/oversized",
                            }
                        ]
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/assets/oversized":
                downloaded.append(self.path)
                self.send_response(200)
                self.send_header("Content-Length", "1")
                self.end_headers()
                self.wfile.write(b"x")
                return
            self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "test-token",
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert result.returncode != 0
    assert "declared size" in result.stderr
    assert downloaded == []


def test_release_build_inputs_are_immutable() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert (
        dockerfile.splitlines()[0]
        == "# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e"
    )
    assert (
        "FROM ghcr.io/astral-sh/uv:0.5.31@sha256:7bff3c3776ec467fc1437960f2c469d8beb30f536a6465a3350c647ccd260ec2 AS uv"
    ) in dockerfile
    assert (
        "FROM python:3.12.13-alpine3.22@sha256:"
        "a190708a2dec1bd18b1decb539f8e8f5407abaa9bf39cacda583f7f8c11db322 AS runtime"
    ) in dockerfile
    assert "apk upgrade" not in dockerfile

    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["build-system"]["requires"] == ["hatchling==1.32.0"]
