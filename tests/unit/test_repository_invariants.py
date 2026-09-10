from __future__ import annotations

import ast
import errno
import hashlib
import http.server
import importlib.util
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import ssl
import stat
import subprocess
import sys
import tarfile
import threading
import tomllib
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import HTTPSHandler, ProxyHandler, build_opener

import pytest
import truststore._api as truststore_api
import yaml

from tacit.config import DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES

REPOSITORY_ROOT = Path(__file__).parents[2]
RELEASE_ARCHIVE_GUARD = REPOSITORY_ROOT / ".github" / "scripts" / "release_image_archive.py"
RELEASE_IMAGE_SMOKE = REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_image.py"
RELEASE_PAYLOAD_SNAPSHOT = REPOSITORY_ROOT / ".github" / "scripts" / "release_payload_snapshot.py"
RELEASE_PUBLICATION_SNAPSHOT = REPOSITORY_ROOT / ".github" / "scripts" / "release_publication_snapshot.py"
RELEASE_BINARY_PACKAGER = REPOSITORY_ROOT / ".github" / "scripts" / "package_release_binary.py"
RELEASE_BINARY_INSPECTOR = REPOSITORY_ROOT / ".github" / "scripts" / "inspect_release_binary.py"
RELEASE_BUILD_CONSTRAINTS = REPOSITORY_ROOT / ".github" / "requirements" / "release-build-constraints.txt"
RELEASE_PUBLICATION_AUTHORIZATION = REPOSITORY_ROOT / ".github" / "scripts" / "authorize_release_publication.py"
RELEASE_GITHUB_API = REPOSITORY_ROOT / ".github" / "scripts" / "release_github_api.py"
RELEASE_GITHUB_ASSET_VERIFIER = REPOSITORY_ROOT / ".github" / "scripts" / "verify_github_release_assets.py"
RELEASE_PYPI_VERIFIER = REPOSITORY_ROOT / ".github" / "scripts" / "verify_pypi_release.py"
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


def _embedded_heredoc(script: str, marker: str) -> str:
    start = script.index(marker) + len(marker)
    return script[start : script.index("\nPY", start)]


def _load_script_module(path: Path) -> ModuleType:
    inserted = str(path.parent) not in sys.path
    if inserted:
        sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(f"test_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        if inserted:
            sys.path.remove(str(path.parent))


def _test_https_material(tmp_path: Path) -> tuple[Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("OpenSSL is required for the local HTTPS release boundary tests")
    tls_dir = tmp_path / ".release-test-tls"
    tls_dir.mkdir()
    certificate = tls_dir / "certificate.pem"
    private_key = tls_dir / "private-key.pem"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "1",
            "-nodes",
            "-keyout",
            str(private_key),
            "-out",
            str(certificate),
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
        ],
        check=True,
        capture_output=True,
    )
    return certificate, private_key


def _serve_over_https(
    server: http.server.ThreadingHTTPServer,
    *,
    certificate: Path,
    private_key: Path,
) -> None:
    context_type = getattr(truststore_api, "_original_SSLContext")
    context = context_type(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, private_key)
    server.socket = context.wrap_socket(server.socket, server_side=True)


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


def test_github_release_channel_metadata_distinguishes_stable_and_prerelease(tmp_path: Path) -> None:
    workflow = _release_workflow()
    validate = workflow["jobs"]["validate-release"]
    assert validate["outputs"]["is_stable"] == "${{ steps.validate_version.outputs.is_stable }}"

    script = _step(validate, "Validate tag and package version")["run"]
    for version, expected_stable in (("1.2.3", "true"), ("1.2.3-rc.4", "false")):
        (tmp_path / "pyproject.toml").write_text(
            f'[project]\nversion = "{version}"\n',
            encoding="utf-8",
        )
        output = tmp_path / "output.txt"
        output.write_text("", encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=tmp_path,
            env=os.environ | {"GITHUB_REF_NAME": f"v{version}", "GITHUB_OUTPUT": str(output)},
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert f"is_stable={expected_stable}\n" in output.read_text(encoding="utf-8")

    publish = _step(workflow["jobs"]["publish-github-release"], "Publish GitHub release binaries")
    assert publish["with"]["prerelease"] == "${{ needs.validate-release.outputs.is_stable != 'true' }}"
    assert publish["with"]["make_latest"] == (
        "${{ needs.validate-release.outputs.is_stable == 'true' && 'legacy' || 'false' }}"
    )
    assert publish["with"]["target_commitish"] == "${{ github.sha }}"


def test_release_concurrency_uses_only_supported_github_schema() -> None:
    concurrency = _release_workflow()["concurrency"]
    assert concurrency == {
        "group": "release-publication",
        "cancel-in-progress": "false",
        "queue": "max",
    }


def test_release_concurrency_retains_three_accepted_tags() -> None:
    concurrency = _release_workflow()["concurrency"]
    assert concurrency["cancel-in-progress"] == "false"
    pending_capacity = 100 if concurrency["queue"] == "max" else 1
    accepted_tags = ("v1.2.3", "v1.2.4", "v1.2.5")
    assert pending_capacity >= len(accepted_tags)


def test_ci_runs_checksum_pinned_actionlint_against_every_workflow() -> None:
    workflow = _ci_workflow()
    step = _step(workflow["jobs"]["lint"], "Actionlint")
    script = step["run"]

    assert re.fullmatch(r"\d+\.\d+\.\d+", workflow["env"]["ACTIONLINT_VERSION"])
    assert re.fullmatch(r"[0-9a-f]{64}", workflow["env"]["ACTIONLINT_LINUX_AMD64_SHA256"])
    assert "sha256sum --check" in script
    assert script.count('unexpected key "queue" for "concurrency" section') == 1
    assert script.count("-ignore") == 1
    assert ".github/workflows/*.yml" in script

    workflows_with_extended_queue = []
    for path in sorted((REPOSITORY_ROOT / ".github" / "workflows").glob("*.yml")):
        loaded = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        if isinstance(loaded, dict) and isinstance(loaded.get("concurrency"), dict):
            if "queue" in loaded["concurrency"]:
                workflows_with_extended_queue.append(path.name)
    assert workflows_with_extended_queue == ["release.yml"]

    actionlint_config = yaml.safe_load((REPOSITORY_ROOT / ".github" / "actionlint.yaml").read_text(encoding="utf-8"))
    assert actionlint_config == {
        "self-hosted-runner": {
            "labels": ["release-quality"],
        }
    }


def test_release_image_build_uses_commit_epoch_and_timestamp_rewriting() -> None:
    build = _release_workflow()["jobs"]["build-release-images"]
    epoch = _step(build, "Derive reproducible image timestamp")
    assert epoch["id"] == "source_epoch"
    assert 'git show -s --format=%ct "$GITHUB_SHA"' in epoch["run"]
    assert "SOURCE_DATE_EPOCH=" in epoch["run"]
    assert "source_date_epoch=" in epoch["run"]
    assert "GITHUB_ENV" in epoch["run"]
    assert "GITHUB_OUTPUT" in epoch["run"]

    for step_name in (
        "Build ${{ matrix.platform }} release image",
        "Rebuild ${{ matrix.platform }} release image independently",
    ):
        image_build = _step(build, step_name)["with"]
        assert image_build["no-cache"] == "true"
        assert image_build["pull"] == "true"
        assert image_build["build-args"] == ("SOURCE_DATE_EPOCH=${{ steps.source_epoch.outputs.source_date_epoch }}")
        assert "rewrite-timestamp=true" in image_build["outputs"]
        assert "cache-from" not in image_build
        assert "cache-to" not in image_build
        assert image_build["file"] == "Dockerfile"

    workflow_text = (REPOSITORY_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "prepare_release_dockerfile.py" not in workflow_text


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


def test_release_consumers_share_one_descriptor_bound_private_snapshot_boundary() -> None:
    assert RELEASE_PAYLOAD_SNAPSHOT.is_file()
    snapshot_source = RELEASE_PAYLOAD_SNAPSHOT.read_text(encoding="utf-8")
    assert ".read_bytes()" not in snapshot_source
    assert "O_NOFOLLOW" in snapshot_source
    assert "READ_CHUNK_BYTES" in snapshot_source
    assert "mkdtemp" in snapshot_source
    assert "0o700" in snapshot_source
    assert "expected_digest" in snapshot_source

    image_source = RELEASE_IMAGE_SMOKE.read_text(encoding="utf-8")
    binary_source = (REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_binary.py").read_text(encoding="utf-8")
    archive_source = RELEASE_ARCHIVE_GUARD.read_text(encoding="utf-8")
    for source in (image_source, binary_source, archive_source):
        assert "release_payload_snapshot" in source


def test_release_image_archive_guard_streams_and_enforces_file_invariants(
    tmp_path: Path,
) -> None:
    assert RELEASE_ARCHIVE_GUARD.is_file()
    guard_source = RELEASE_ARCHIVE_GUARD.read_text(encoding="utf-8")
    assert ".read_bytes()" not in guard_source
    assert "READ_CHUNK_BYTES" in guard_source
    assert "copy_verified_payload" in guard_source
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
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600

    public_parent = tmp_path / "public"
    public_parent.mkdir(mode=0o755)
    public_parent.chmod(0o755)
    unprotected = _run_release_script(
        RELEASE_ARCHIVE_GUARD,
        "copy",
        "--archive",
        str(archive),
        "--checksum",
        str(checksum),
        "--destination",
        str(public_parent / "release.tar"),
        "--max-bytes",
        str(archive.stat().st_size),
    )
    assert unprotected.returncode != 0
    assert "owner-only" in unprotected.stderr

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
    assert "SCAN_IMAGE_SNAPSHOT_DIR" in scan_guard
    assert "umask 077" in scan_guard

    publish_guard = _step(jobs["publish-ghcr"], "Verify transferred image archives")["run"]
    assert "release_image_archive.py copy" in publish_guard
    assert '"$MAX_RELEASE_IMAGE_ARCHIVE_BYTES"' in publish_guard
    assert "PUBLISH_IMAGE_SNAPSHOT_DIR" in publish_guard
    assert "umask 077" in publish_guard
    assert "tacit-release-amd64.tar" in publish_guard
    assert "tacit-release-arm64.tar" in publish_guard

    for job_name in ("scan-release-images", "publish-ghcr"):
        checkout = next(
            step for step in jobs[job_name]["steps"] if step.get("uses", "").startswith("actions/checkout@")
        )
        expected = {
            "persist-credentials": "false",
            "ref": "${{ github.sha }}",
        }
        if job_name == "publish-ghcr":
            expected["fetch-depth"] = "0"
        assert checkout["with"] == expected

    scan_cleanup = _step(jobs["scan-release-images"], "Remove private image snapshot")["run"]
    publish_cleanup = _step(jobs["publish-ghcr"], "Remove private image snapshots")["run"]
    assert "release_image_archive.py cleanup" in scan_cleanup
    assert "release_image_archive.py cleanup" in publish_cleanup


def test_ci_executes_both_container_architectures_under_the_release_runtime_profile() -> None:
    job = _ci_workflow()["jobs"]["docker"]
    assert int(job["timeout-minutes"]) <= 45
    assert job["strategy"]["matrix"]["include"] == [
        {"platform": "linux/amd64", "arch": "amd64"},
        {"platform": "linux/arm64", "arch": "arm64"},
    ]

    qemu = next(step for step in job["steps"] if step.get("uses", "").startswith("docker/setup-qemu-action@"))
    assert qemu["if"] == "matrix.arch == 'arm64'"
    assert re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", qemu["with"]["image"])
    assert qemu["with"]["platforms"] == "arm64"

    build = _step(job, "Build bounded ${{ matrix.platform }} image archive")["run"]
    assert "docker buildx build" in build
    assert '--platform "$PLATFORM"' in build
    assert "type=docker,dest=${IMAGE_ARCHIVE}" in build
    assert "release_image_archive.py write" in build

    smoke = _step(job, "Execute ${{ matrix.platform }} image archive")["run"]
    smoke_step = _step(job, "Execute ${{ matrix.platform }} image archive")
    assert 1 <= int(smoke_step["timeout-minutes"]) <= 5
    assert "smoke_release_image.py" in smoke
    assert '--archive "$IMAGE_ARCHIVE"' in smoke
    assert '--checksum "${IMAGE_ARCHIVE}.sha256"' in smoke
    assert '--platform "$PLATFORM"' in smoke
    assert '--image "$IMAGE_REF"' in smoke
    assert "--expected-version" in smoke
    assert "--max-archive-bytes" in smoke
    assert "--command-timeout" in smoke
    assert "--startup-timeout" in smoke
    assert "--cleanup-timeout" in smoke
    assert job["steps"].index(_step(job, "Build bounded ${{ matrix.platform }} image archive")) < job["steps"].index(
        _step(job, "Execute ${{ matrix.platform }} image archive")
    )


def test_release_executes_each_authoritative_image_archive_before_upload() -> None:
    job = _release_workflow()["jobs"]["build-release-images"]
    smoke = _step(job, "Execute authoritative ${{ matrix.platform }} release image")
    command = smoke["run"]
    assert smoke["timeout-minutes"].isdigit()
    assert int(smoke["timeout-minutes"]) <= 5
    assert "smoke_release_image.py" in command
    assert '--archive "$IMAGE_ARCHIVE"' in command
    assert '--checksum "${IMAGE_ARCHIVE}.sha256"' in command
    assert '--platform "${{ matrix.platform }}"' in command
    assert '--image "${IMAGE_NAME}:release-${GITHUB_SHA}-${{ matrix.arch }}"' in command
    assert '--expected-version "${{ needs.validate-release.outputs.package_version }}"' in command
    assert '--max-archive-bytes "$MAX_RELEASE_IMAGE_ARCHIVE_BYTES"' in command

    steps = job["steps"]
    guard_index = steps.index(_step(job, "Validate built image archive before upload"))
    smoke_index = steps.index(smoke)
    upload_index = next(
        index for index, step in enumerate(steps) if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert guard_index < smoke_index < upload_index
    assert smoke_index + 1 == upload_index

    source = RELEASE_IMAGE_SMOKE.read_text(encoding="utf-8")
    for required in (
        "--read-only",
        "--tmpfs",
        "ReadonlyRootfs",
        "Healthcheck",
        "root_read_only",
        "data_writable",
        "tmpfs",
        "required_resources",
        "verified_payload_snapshot",
        "archive_snapshot.path",
        "_require_resources_absent",
        "_cleanup_resource",
    ):
        assert required in source


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
    baseline_present = (
        subprocess.run(
            ["git", "cat-file", "-e", f"{baseline}^{{commit}}"],
            cwd=REPOSITORY_ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )
    if baseline_present:
        assert (
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", baseline, "HEAD"],
                cwd=REPOSITORY_ROOT,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        )
    else:
        shallow = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert shallow == "true"
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

    release_dockerfile = context / "Dockerfile"
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
        assert 'download="$RUNNER_TEMP/$asset"' in install
        assert '--output "$download"' in install
        assert 'printf \'%s  %s\\n\' "$expected" "$download" | sha256sum --check' in install
        assert 'install -m 0755 "$download" "$HOME/.docker/cli-plugins/docker-buildx"' in install
        assert '--output "$asset"' not in install
        assert 'install -m 0755 "$asset"' not in install
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
    assert publish["permissions"] == {
        "actions": "read",
        "contents": "read",
        "packages": "write",
    }
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

    gate = _step(authorization, "Authorize release publication")
    assert gate["env"] == {"GITHUB_TOKEN": "${{ secrets.GITHUB_TOKEN }}"}
    assert gate["run"].strip() == "python .github/scripts/authorize_release_publication.py"

    for job_name in ("validate-release", "build-dist", "build-binaries", "build-release-images"):
        checkout = next(
            step for step in jobs[job_name]["steps"] if step.get("uses", "").startswith("actions/checkout@")
        )
        assert checkout["with"]["ref"] == "${{ github.sha }}"

    assert "authorize-publication" in _job_needs(jobs["publish-ghcr"])


def test_github_release_rechecks_remote_tag_immediately_before_publication(tmp_path: Path) -> None:
    authorization = _load_script_module(RELEASE_PUBLICATION_AUTHORIZATION)

    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    runner = tmp_path / "runner"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "init", str(source)], check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "release-test@example.invalid"], cwd=source, check=True)
    subprocess.run(["git", "config", "user.name", "Release Test"], cwd=source, check=True)
    (source / "release.txt").write_text("first\n", encoding="utf-8")
    subprocess.run(["git", "add", "release.txt"], cwd=source, check=True)
    subprocess.run(["git", "commit", "-m", "first"], cwd=source, check=True, capture_output=True)
    first_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(["git", "branch", "-M", "main"], cwd=source, check=True)
    subprocess.run(["git", "tag", "v1.2.3", first_sha], cwd=source, check=True)
    subprocess.run(["git", "remote", "add", "origin", str(remote)], cwd=source, check=True)
    subprocess.run(["git", "push", "origin", "main", "refs/tags/v1.2.3"], cwd=source, check=True)
    subprocess.run(
        ["git", "clone", "--branch", "main", "--no-tags", str(remote), str(runner)],
        check=True,
        capture_output=True,
    )
    subprocess.run(["git", "checkout", "--detach", first_sha], cwd=runner, check=True, capture_output=True)

    def prove_fresh_tag() -> None:
        authorization.prove_fresh_tag_on_main(
            runner,
            expected_sha=first_sha,
            tag_name="v1.2.3",
        )

    prove_fresh_tag()

    subprocess.run(["git", "push", "origin", ":refs/tags/v1.2.3"], cwd=source, check=True)
    with pytest.raises(authorization.ReleaseAuthorizationError):
        prove_fresh_tag()

    subprocess.run(["git", "tag", "--force", "v1.2.3", first_sha], cwd=source, check=True)
    subprocess.run(["git", "push", "origin", "refs/tags/v1.2.3"], cwd=source, check=True)
    prove_fresh_tag()

    (source / "release.txt").write_text("second\n", encoding="utf-8")
    subprocess.run(["git", "commit", "-am", "second"], cwd=source, check=True, capture_output=True)
    second_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(["git", "tag", "--force", "v1.2.3", second_sha], cwd=source, check=True)
    subprocess.run(
        ["git", "push", "--force", "origin", "main", "refs/tags/v1.2.3"],
        cwd=source,
        check=True,
    )
    with pytest.raises(authorization.ReleaseAuthorizationError):
        prove_fresh_tag()


def test_release_ci_authorization_rejects_redirects_without_forwarding_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(RELEASE_PUBLICATION_AUTHORIZATION)
    redirected_authorization: list[str | None] = []

    class RedirectTarget(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            redirected_authorization.append(self.headers.get("Authorization"))
            body = json.dumps({"workflow_runs": []}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectTarget)

    class RedirectingAPI(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(302)
            self.send_header("Location", f"https://127.0.0.1:{target.server_port}/capture")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    api = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectingAPI)
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(target, certificate=certificate, private_key=private_key)
    _serve_over_https(api, certificate=certificate, private_key=private_key)
    monkeypatch.setenv("SSL_CERT_FILE", str(certificate))
    threads = [
        threading.Thread(target=target.serve_forever, daemon=True),
        threading.Thread(target=api.serve_forever, daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        with pytest.raises(authorization.ReleaseAuthorizationError):
            authorization.require_successful_main_ci(
                expected_sha="a" * 40,
                repository_slug="aditki/tacit",
                token="ci-authorization-sentinel",
                api_url=f"https://127.0.0.1:{api.server_port}",
            )
    finally:
        api.shutdown()
        target.shutdown()
        for thread in threads:
            thread.join(timeout=5)
        api.server_close()
        target.server_close()

    assert redirected_authorization == []


def test_release_ci_authorization_rejects_non_https_api_before_attaching_token() -> None:
    authorization = _load_script_module(RELEASE_PUBLICATION_AUTHORIZATION)
    observed_authorization: list[str | None] = []

    class CaptureAPI(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            observed_authorization.append(self.headers.get("Authorization"))
            body = json.dumps(
                {
                    "workflow_runs": [
                        {
                            "head_sha": "a" * 40,
                            "head_branch": "main",
                            "event": "push",
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ]
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CaptureAPI)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(authorization.ReleaseAuthorizationError, match="HTTPS"):
            authorization.require_successful_main_ci(
                expected_sha="a" * 40,
                repository_slug="aditki/tacit",
                token="must-not-cross-plaintext",
                api_url=f"http://127.0.0.1:{server.server_port}",
            )
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert observed_authorization == []


def test_release_ci_authorization_ignores_ambient_https_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorization = _load_script_module(RELEASE_PUBLICATION_AUTHORIZATION)
    proxy_requests: list[tuple[str, str | None]] = []

    class CaptureProxy(http.server.BaseHTTPRequestHandler):
        def do_CONNECT(self) -> None:  # noqa: N802
            proxy_requests.append((self.path, self.headers.get("Authorization")))
            self.send_error(502)

        def log_message(self, format: str, *args: object) -> None:
            return

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CaptureProxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    for name in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(name, f"http://127.0.0.1:{proxy.server_port}")
    try:
        with pytest.raises(authorization.ReleaseAuthorizationError):
            authorization.require_successful_main_ci(
                expected_sha="a" * 40,
                repository_slug="aditki/tacit",
                token="proxy-isolation-sentinel",
                api_url="https://release-api.invalid",
            )
    finally:
        proxy.shutdown()
        thread.join(timeout=5)
        proxy.server_close()

    assert proxy_requests == []


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
    image_build = _step(build, "Build ${{ matrix.platform }} release image")
    primary_builder = _step(build, "Create pinned BuildKit builder")["run"]
    independent_builder = _step(build, "Create independent pinned BuildKit builder")["run"]
    reproduction = _step(build, "Rebuild ${{ matrix.platform }} release image independently")
    assert "--name tacit-release-repro-builder" in independent_builder
    buildkit_identity = r"moby/buildkit:v\d+\.\d+\.\d+@sha256:[0-9a-f]{64}"
    assert re.findall(buildkit_identity, independent_builder) == re.findall(buildkit_identity, primary_builder)
    assert image_build["with"]["builder"] == "tacit-release-builder"
    assert reproduction["with"]["builder"] == "tacit-release-repro-builder"
    for key in ("context", "file", "platforms", "push", "pull", "no-cache", "build-args", "tags", "labels"):
        assert reproduction["with"][key] == image_build["with"][key]
    upload = next(step for step in build["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
    assert image_build["with"]["outputs"] == (f"type=docker,dest={archive},rewrite-timestamp=true")
    assert reproduction["with"]["outputs"] == ("type=docker,dest=${{ env.REPRO_IMAGE_ARCHIVE }},rewrite-timestamp=true")
    assert image_build["with"]["push"] == "false"
    assert archive in upload["with"]["path"]
    assert f"{archive}.sha256" in upload["with"]["path"]
    assert f"{archive}.manifest-digest" in upload["with"]["path"]
    reproducibility = _step(build, "Verify reproducible ${{ matrix.platform }} child digest")
    assert reproducibility["env"] == {
        "AUTHORITATIVE_DIGEST": "${{ steps.build_image.outputs.digest }}",
        "REPRODUCED_DIGEST": "${{ steps.reproduce_image.outputs.digest }}",
    }
    assert "sha256:[0-9a-f]{64}" in reproducibility["run"]
    assert '"$AUTHORITATIVE_DIGEST" != "$REPRODUCED_DIGEST"' in reproducibility["run"]
    assert '"${IMAGE_ARCHIVE}.manifest-digest"' in reproducibility["run"]
    step_names = [step.get("name") for step in build["steps"]]
    assert step_names.index("Verify reproducible ${{ matrix.platform }} child digest") < step_names.index(
        "Validate built image archive before upload"
    )
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
    prepared = _step(publish, "Prepare scanned architecture images")["run"]
    pushed = _step(publish, "Publish scanned architecture images")["run"]
    assert "release_image_archive.py copy" in transferred
    assert '--destination "${PUBLISH_IMAGE_SNAPSHOT_DIR}/${archive}"' in transferred
    assert 'docker load --input "$archive"' in prepared
    assert 'archive="${PUBLISH_IMAGE_SNAPSHOT_DIR}/tacit-release-${arch}.tar"' in prepared
    assert 'archive="release-images/' not in prepared
    assert "${archive}.sha256" not in prepared
    assert "docker push" not in prepared
    assert "docker load" not in pushed
    assert "docker tag" not in pushed
    assert "docker image inspect" not in pushed
    assert "remote_config" in pushed
    assert "Published ${arch} image differs from the scanned archive" in pushed
    assert "approved_digest" in pushed
    assert '"$digest" != "$approved_digest"' in pushed
    immutable = _step(publish, "Publish immutable multi-architecture version")["run"]
    assert "docker buildx imagetools create" in immutable
    assert "org.opencontainers.image.revision" in immutable


@pytest.mark.parametrize(
    ("job_name", "mutation_name", "required_preflight_names"),
    (
        ("publish-pypi", "Publish to PyPI", ()),
        (
            "publish-github-release",
            "Publish GitHub release binaries",
            (
                "Verify binary packages and checksums",
                "Verify GitHub release publication state",
            ),
        ),
    ),
)
def test_protected_release_publishers_reauthorize_adjacent_to_first_external_mutation(
    job_name: str,
    mutation_name: str,
    required_preflight_names: tuple[str, ...],
) -> None:
    jobs = _release_workflow()["jobs"]
    job = jobs[job_name]
    steps = job["steps"]
    authority = _step(job, "Re-authorize release before publication")
    mutation = _step(job, mutation_name)
    authority_index = steps.index(authority)

    assert job["permissions"]["actions"] == "read"
    assert job["environment"]["name"]
    assert authority["env"] == {"GITHUB_TOKEN": "${{ secrets.GITHUB_TOKEN }}"}
    assert authority["run"].strip() == "python .github/scripts/authorize_release_publication.py"
    assert authority_index + 1 == steps.index(mutation)
    assert all(
        index < authority_index
        for index, step in enumerate(steps)
        if step.get("uses", "").startswith("actions/download-artifact@")
    )
    for name in required_preflight_names:
        assert steps.index(_step(job, name)) < authority_index


def test_final_publishers_consume_preflight_carried_sealed_snapshots() -> None:
    jobs = _release_workflow()["jobs"]

    dist_outputs = jobs["build-dist"]["outputs"]
    assert dist_outputs == {
        "wheel_name": "${{ steps.publication_payload.outputs.wheel_name }}",
        "wheel_digest": "${{ steps.publication_payload.outputs.wheel_digest }}",
        "sdist_name": "${{ steps.publication_payload.outputs.sdist_name }}",
        "sdist_digest": "${{ steps.publication_payload.outputs.sdist_digest }}",
    }
    dist_descriptor = _step(jobs["build-dist"], "Describe PyPI publication payload")
    assert dist_descriptor["id"] == "publication_payload"
    assert "release_publication_snapshot.py describe" in dist_descriptor["run"]
    assert "--selector 'wheel=tacit_ai-*.whl'" in dist_descriptor["run"]
    assert "--selector 'sdist=tacit_ai-*.tar.gz'" in dist_descriptor["run"]

    pypi_preflight = jobs["pypi-preflight"]
    assert pypi_preflight["outputs"] == {
        "wheel_name": "${{ steps.pypi_preflight.outputs.wheel_name }}",
        "wheel_digest": "${{ steps.pypi_preflight.outputs.wheel_digest }}",
        "sdist_name": "${{ steps.pypi_preflight.outputs.sdist_name }}",
        "sdist_digest": "${{ steps.pypi_preflight.outputs.sdist_digest }}",
    }
    pypi_preflight_step = _step(pypi_preflight, "Verify any existing PyPI files")
    assert pypi_preflight_step["id"] == "pypi_preflight"
    assert pypi_preflight_step["env"] == {
        "PACKAGE_VERSION": "${{ needs.validate-release.outputs.package_version }}",
        "WHEEL_NAME": "${{ needs.build-dist.outputs.wheel_name }}",
        "WHEEL_DIGEST": "${{ needs.build-dist.outputs.wheel_digest }}",
        "SDIST_NAME": "${{ needs.build-dist.outputs.sdist_name }}",
        "SDIST_DIGEST": "${{ needs.build-dist.outputs.sdist_digest }}",
    }
    assert "verify_pypi_release.py preflight" in pypi_preflight_step["run"]
    assert '--github-output "$GITHUB_OUTPUT"' in pypi_preflight_step["run"]

    pypi = jobs["publish-pypi"]
    pypi_snapshot = _step(pypi, "Prepare sealed PyPI publisher path")
    pypi_authorization = _step(pypi, "Re-authorize release before publication")
    pypi_publish = _step(pypi, "Publish to PyPI")
    pypi_postflight = _step(pypi, "Verify published PyPI files")
    pypi_steps = pypi["steps"]
    assert pypi_snapshot["env"] == {
        "WHEEL_NAME": "${{ needs.pypi-preflight.outputs.wheel_name }}",
        "WHEEL_DIGEST": "${{ needs.pypi-preflight.outputs.wheel_digest }}",
        "SDIST_NAME": "${{ needs.pypi-preflight.outputs.sdist_name }}",
        "SDIST_DIGEST": "${{ needs.pypi-preflight.outputs.sdist_digest }}",
    }
    assert "release_publication_snapshot.py seal" in pypi_snapshot["run"]
    assert "sudo --non-interactive" in pypi_snapshot["run"]
    assert "--source-directory dist" in pypi_snapshot["run"]
    assert '--destination-directory "$PYPI_PUBLICATION_SNAPSHOT_DIR"' in pypi_snapshot["run"]
    assert '--backing-directory "$PYPI_PUBLICATION_BACKING_DIR"' in pypi_snapshot["run"]
    assert pypi_steps.index(pypi_snapshot) + 1 == pypi_steps.index(pypi_authorization)
    assert pypi_steps.index(pypi_authorization) + 1 == pypi_steps.index(pypi_publish)
    assert pypi_publish["with"]["packages-dir"] == "${{ env.PYPI_PUBLICATION_SNAPSHOT_DIR }}"
    # The sealed directory is the complete authorized payload. Publisher-generated
    # attestations would require a separately authorized writable output boundary.
    assert pypi_publish["with"]["attestations"] == "false"
    assert "dist" not in pypi_publish["with"]["packages-dir"]
    assert pypi["env"]["PYPI_PUBLICATION_SNAPSHOT_DIR"].startswith(".tacit-pypi-publication-")
    assert pypi["env"]["PYPI_PUBLICATION_BACKING_DIR"].startswith("/var/lib/tacit-release-publication-")
    pypi_cleanup = _step(pypi, "Remove sealed PyPI publisher path")
    assert pypi_cleanup["if"] == "always()"
    assert "release_publication_snapshot.py unseal" in pypi_cleanup["run"]
    assert "sudo --non-interactive" in pypi_cleanup["run"]
    assert pypi_steps.index(pypi_postflight) < pypi_steps.index(pypi_cleanup)

    binary_outputs = jobs["build-binaries"]["outputs"]
    assert binary_outputs == {
        "package_name": "${{ steps.publication_payload.outputs.package_name }}",
        "package_digest": "${{ steps.publication_payload.outputs.package_digest }}",
        "checksum_name": "${{ steps.publication_payload.outputs.checksum_name }}",
        "checksum_digest": "${{ steps.publication_payload.outputs.checksum_digest }}",
    }
    binary_descriptor = _step(jobs["build-binaries"], "Describe GitHub release publication payload")
    assert binary_descriptor["id"] == "publication_payload"
    assert "release_publication_snapshot.py" in binary_descriptor["run"]
    assert '" describe' in binary_descriptor["run"]

    github_preflight = jobs["github-release-preflight"]
    assert github_preflight["outputs"] == {
        "package_name": "${{ steps.publication_payload.outputs.package_name }}",
        "package_digest": "${{ steps.publication_payload.outputs.package_digest }}",
        "checksum_name": "${{ steps.publication_payload.outputs.checksum_name }}",
        "checksum_digest": "${{ steps.publication_payload.outputs.checksum_digest }}",
    }
    github_carry = _step(github_preflight, "Verify and carry GitHub release payload descriptors")
    assert github_carry["id"] == "publication_payload"
    assert github_carry["env"] == {
        "PACKAGE_NAME": "${{ needs.build-binaries.outputs.package_name }}",
        "PACKAGE_DIGEST": "${{ needs.build-binaries.outputs.package_digest }}",
        "CHECKSUM_NAME": "${{ needs.build-binaries.outputs.checksum_name }}",
        "CHECKSUM_DIGEST": "${{ needs.build-binaries.outputs.checksum_digest }}",
    }
    assert "release_publication_snapshot.py verify" in github_carry["run"]
    assert '--github-output "$GITHUB_OUTPUT"' in github_carry["run"]

    github = jobs["publish-github-release"]
    github_snapshot = _step(github, "Prepare sealed GitHub release publisher path")
    github_preflight_step = _step(github, "Verify GitHub release publication state")
    github_authorization = _step(github, "Re-authorize release before publication")
    github_publish = _step(github, "Publish GitHub release binaries")
    github_postflight = _step(github, "Verify published GitHub release assets")
    github_steps = github["steps"]
    assert github_snapshot["env"] == {
        "PACKAGE_NAME": "${{ needs.github-release-preflight.outputs.package_name }}",
        "PACKAGE_DIGEST": "${{ needs.github-release-preflight.outputs.package_digest }}",
        "CHECKSUM_NAME": "${{ needs.github-release-preflight.outputs.checksum_name }}",
        "CHECKSUM_DIGEST": "${{ needs.github-release-preflight.outputs.checksum_digest }}",
    }
    assert "release_publication_snapshot.py seal" in github_snapshot["run"]
    assert "sudo --non-interactive" in github_snapshot["run"]
    assert "--source-directory release-binaries" in github_snapshot["run"]
    assert '--backing-directory "$GITHUB_RELEASE_PUBLICATION_BACKING_DIR"' in github_snapshot["run"]
    assert github_preflight_step["working-directory"] == "${{ env.GITHUB_RELEASE_PUBLICATION_SNAPSHOT_DIR }}"
    assert github_steps.index(github_snapshot) + 1 == github_steps.index(github_preflight_step)
    assert github_steps.index(github_preflight_step) + 1 == github_steps.index(github_authorization)
    assert github_steps.index(github_authorization) + 1 == github_steps.index(github_publish)
    assert github_publish["with"]["files"] == "${{ env.GITHUB_RELEASE_PUBLICATION_SNAPSHOT_DIR }}/*"
    assert "release-binaries" not in github_publish["with"]["files"]
    assert github["env"]["GITHUB_RELEASE_PUBLICATION_SNAPSHOT_DIR"].startswith(".tacit-github-publication-")
    assert github["env"]["GITHUB_RELEASE_PUBLICATION_BACKING_DIR"].startswith("/var/lib/tacit-release-publication-")
    github_cleanup = _step(github, "Remove sealed GitHub release publisher path")
    assert github_cleanup["if"] == "always()"
    assert "release_publication_snapshot.py unseal" in github_cleanup["run"]
    assert "sudo --non-interactive" in github_cleanup["run"]
    assert github_steps.index(github_postflight) < github_steps.index(github_cleanup)


def test_every_ghcr_publication_command_reauthorizes_immediately_before_mutation() -> None:
    job = _release_workflow()["jobs"]["publish-ghcr"]
    authorization_command = (
        'GITHUB_TOKEN="$release_authorization_token" ' "python .github/scripts/authorize_release_publication.py"
    )
    expected_mutation_steps = {
        "Publish scanned architecture images",
        "Publish immutable multi-architecture version",
        "Advance stable aliases monotonically",
    }
    observed_mutation_steps: set[str] = set()

    prepared = _step(job, "Prepare scanned architecture images")
    published_arches = _step(job, "Publish scanned architecture images")
    assert job["steps"].index(prepared) < job["steps"].index(published_arches)
    assert authorization_command not in prepared["run"]
    assert "docker push" not in prepared["run"]
    assert "docker buildx imagetools create" not in prepared["run"]
    assert not any(step.get("name") == "Re-authorize release before publication" for step in job["steps"])

    for step in job["steps"]:
        script = step.get("run", "")
        lines = [line.strip() for line in script.splitlines() if line.strip()]
        mutation_indexes = [
            index
            for index, line in enumerate(lines)
            if line.startswith("docker push ") or line.startswith("docker buildx imagetools create ")
        ]
        if not mutation_indexes:
            continue
        observed_mutation_steps.add(step["name"])
        assert step.get("env", {}).get("GITHUB_TOKEN") == "${{ secrets.GITHUB_TOKEN }}"
        assert 'release_authorization_token="$GITHUB_TOKEN"' in lines
        assert "unset GITHUB_TOKEN" in lines
        assert lines.index("unset GITHUB_TOKEN") < mutation_indexes[0]
        for mutation_index in mutation_indexes:
            assert mutation_index > 0
            assert lines[mutation_index - 1] == authorization_command, (
                step["name"],
                lines[mutation_index - 1 : mutation_index + 1],
            )

    assert observed_mutation_steps == expected_mutation_steps


def test_release_publication_authorization_requires_fresh_tag_main_and_exact_ci_proof() -> None:
    source = RELEASE_PUBLICATION_AUTHORIZATION.read_text(encoding="utf-8")

    for required in (
        '"+refs/heads/main:refs/remotes/origin/main"',
        'f"+{tag_ref}:{tag_ref}"',
        'run.get("head_sha") == expected_sha',
        'run.get("head_branch") == "main"',
        'run.get("event") == "push"',
        'run.get("status") == "completed"',
        'run.get("conclusion") == "success"',
    ):
        assert required in source

    assert "/actions/workflows/ci.yml/runs" in source
    assert '"head_sha": expected_sha' in source
    assert '"branch": "main"' in source
    assert '"event": "push"' in source
    assert '"status": "completed"' in source
    assert '"rev-parse", "refs/remotes/origin/main^{commit}"' in source
    assert "merge-base" not in source and "--is-ancestor" not in source


def test_revoked_exact_sha_ci_authorization_prevents_publication_mutation(
    tmp_path: Path,
) -> None:
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    runner = tmp_path / "runner"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "init", str(source)], check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "release-test@example.invalid"], cwd=source, check=True)
    subprocess.run(["git", "config", "user.name", "Release Test"], cwd=source, check=True)
    (source / "release.txt").write_text("release\n", encoding="utf-8")
    subprocess.run(["git", "add", "release.txt"], cwd=source, check=True)
    subprocess.run(["git", "commit", "-m", "release"], cwd=source, check=True, capture_output=True)
    release_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(["git", "branch", "-M", "main"], cwd=source, check=True)
    subprocess.run(["git", "tag", "v1.2.3", release_sha], cwd=source, check=True)
    subprocess.run(["git", "remote", "add", "origin", str(remote)], cwd=source, check=True)
    subprocess.run(["git", "push", "origin", "main", "refs/tags/v1.2.3"], cwd=source, check=True)
    subprocess.run(
        ["git", "clone", "--branch", "main", "--no-tags", str(remote), str(runner)],
        check=True,
        capture_output=True,
    )

    authorized = [True]
    requests: list[tuple[str, dict[str, list[str]], str | None]] = []

    class ActionsAPI(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            requests.append((parsed.path, parse_qs(parsed.query), self.headers.get("Authorization")))
            exact_run = {
                "head_sha": release_sha,
                "head_branch": "main",
                "event": "push",
                "status": "completed",
                "conclusion": "success" if authorized[0] else "cancelled",
            }
            other_success = exact_run | {"head_sha": "b" * 40, "conclusion": "success"}
            body = json.dumps({"workflow_runs": [exact_run, other_success]}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    api = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ActionsAPI)
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(api, certificate=certificate, private_key=private_key)
    thread = threading.Thread(target=api.serve_forever, daemon=True)
    thread.start()
    sentinel = tmp_path / "external-mutation-sentinel"
    environment = os.environ | {
        "GITHUB_API_URL": f"https://127.0.0.1:{api.server_port}",
        "GITHUB_REF_NAME": "v1.2.3",
        "GITHUB_REPOSITORY": "aditki/tacit",
        "GITHUB_SHA": release_sha,
        "GITHUB_TOKEN": "publisher-authorization-token",
        "MUTATION_SENTINEL": str(sentinel),
        "SSL_CERT_FILE": str(certificate),
    }
    ci_authorizer = tmp_path / "authorize_exact_sha_ci.py"
    ci_authorizer.write_text(
        "\n".join(
            (
                "import os",
                "import runpy",
                "import sys",
                f"sys.path.insert(0, {str(RELEASE_PUBLICATION_AUTHORIZATION.parent)!r})",
                f"authorization = runpy.run_path({str(RELEASE_PUBLICATION_AUTHORIZATION)!r})",
                "authorization['require_successful_main_ci'](",
                "    expected_sha=os.environ['GITHUB_SHA'],",
                "    repository_slug=os.environ['GITHUB_REPOSITORY'],",
                "    token=os.environ['GITHUB_TOKEN'],",
                "    api_url=os.environ['GITHUB_API_URL'],",
                ")",
            )
        ),
        encoding="utf-8",
    )
    command = f"{shlex.quote(sys.executable)} {shlex.quote(str(ci_authorizer))} " '&& touch "$MUTATION_SENTINEL"'
    try:
        initially_authorized = subprocess.run(
            [sys.executable, str(ci_authorizer)],
            cwd=runner,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        assert initially_authorized.returncode == 0, initially_authorized.stderr

        authorized[0] = False
        revoked = subprocess.run(
            ["bash", "-c", command],
            cwd=runner,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        api.shutdown()
        thread.join(timeout=5)
        api.server_close()

    assert revoked.returncode != 0
    assert "no successful completed main CI run" in revoked.stderr
    assert not sentinel.exists()
    assert len(requests) == 2
    for path, query, authorization in requests:
        assert path == "/repos/aditki/tacit/actions/workflows/ci.yml/runs"
        assert query == {
            "head_sha": [release_sha],
            "branch": ["main"],
            "event": ["push"],
            "status": ["completed"],
            "per_page": ["100"],
        }
        assert authorization == "Bearer publisher-authorization-token"


def test_release_job_environment_uses_only_available_expression_contexts() -> None:
    for job_name, job in _release_workflow()["jobs"].items():
        rendered = repr(job.get("env", {}))
        assert "${{ runner." not in rendered, job_name


def test_documented_dev_compose_startup_sets_required_api_key() -> None:
    contributing = (REPOSITORY_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    setup = contributing.split("For local demos:", 1)[1].split("```", 2)[1]

    assert "cp .env.example .env" in setup
    assert "export API_AUTH_KEY=" in setup
    assert "docker compose -f docker-compose.dev.yml up -d" in setup
    assert setup.index("export API_AUTH_KEY=") < setup.index("docker compose")


def test_demo_readme_compose_commands_supply_auth_and_advertise_the_enabled_ui() -> None:
    demo_readme = (REPOSITORY_ROOT / "demo" / "README.md").read_text(encoding="utf-8")
    bash_blocks = re.findall(r"```bash\n(.*?)```", demo_readme, flags=re.DOTALL)
    compose_blocks = [block for block in bash_blocks if "docker compose -f docker-compose.dev.yml" in block]

    assert compose_blocks
    for block in compose_blocks:
        assert "export API_AUTH_KEY=" in block
        assert block.index("export API_AUTH_KEY=") < block.index("docker compose")

    assert "http://localhost:8000/docs" not in demo_readme
    assert "Tacit UI: <http://localhost:8000>" in demo_readme


def test_release_retry_reuses_only_the_current_build_children() -> None:
    jobs = _release_workflow()["jobs"]
    publish = jobs["publish-ghcr"]
    prepare_arches = _step(publish, "Prepare scanned architecture images")
    publish_arches = _step(publish, "Publish scanned architecture images")
    assert "if" not in publish_arches
    assert "staging_ref=" in prepare_arches["run"]
    assert "checksum" in prepare_arches["run"]
    assert "docker load" in prepare_arches["run"]
    assert "docker load" not in publish_arches["run"]
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
    assert "verify_pypi_release.py preflight" in preflight
    pypi_verifier = RELEASE_PYPI_VERIFIER.read_text(encoding="utf-8")
    assert "digests" in pypi_verifier and "mismatched" in pypi_verifier

    github_preflight = jobs["github-release-preflight"]
    assert _job_needs(github_preflight) == {"validate-release", "build-binaries"}
    github_script = _step(github_preflight, "Verify any existing GitHub release assets")["run"]
    assert "verify_github_release_assets.py --allow-absent-release" in github_script
    github_verifier = RELEASE_GITHUB_ASSET_VERIFIER.read_text(encoding="utf-8")
    assert "hashlib.sha256" in github_verifier
    assert "unexpected" in github_verifier and "mismatched" in github_verifier

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
    assert _job_needs(release) == {
        "validate-release",
        "build-binaries",
        "github-release-preflight",
        "publish-pypi",
    }
    assert release["environment"]["name"] == "github-release"
    assert release["permissions"] == {"actions": "read", "contents": "write"}

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
    assert "verify_pypi_release.py postflight" in postflight
    assert "remote == expected" in pypi_verifier


def test_release_smoke_checks_all_runtime_version_surfaces() -> None:
    smoke = _step(_release_workflow()["jobs"]["build-dist"], "Smoke-test the wheel")
    script = smoke["run"]
    assert smoke["env"]["EXPECTED_VERSION"] == "${{ needs.validate-release.outputs.package_version }}"
    assert "distribution_version" in script
    assert "tacit.__version__" in script
    assert "tacit.__file__" in script
    assert '".smoke-venv/bin/tacit", "--version"' in script


def test_release_smokes_exact_bedrock_sdk_generation_from_built_wheel() -> None:
    smoke = _step(_release_workflow()["jobs"]["build-dist"], "Smoke-test the Bedrock wheel extra")
    script = smoke["run"]

    assert "uv export --locked" in script
    assert "--extra bedrock" in script
    assert "--no-dev" in script
    assert "--no-emit-project" in script
    assert "uv pip sync --python .bedrock-smoke-venv/bin/python --require-hashes" in script
    assert "uv pip install --python .bedrock-smoke-venv/bin/python --no-deps dist/*.whl" in script
    assert 'distribution_version("boto3") != "1.43.16"' in script
    assert 'distribution_version("botocore") != "1.43.16"' in script
    assert 'distribution_requires("tacit-ai")' in script
    assert '"boto3": "==1.43.16"' in script
    assert '"botocore": "==1.43.16"' in script
    assert "botocore.session.Session" in script
    assert 'get_component("credential_provider")' in script
    assert "_bedrock_client_config" in script
    assert script.index("from tacit.agents.providers.bedrock") < script.index("import botocore.session")


def test_ci_and_artifact_builds_require_the_committed_lock() -> None:
    ci_syncs = [
        line.strip()
        for job in _ci_workflow()["jobs"].values()
        for step in job["steps"]
        for line in step.get("run", "").splitlines()
        if line.strip().startswith("uv sync ")
    ]
    assert len(ci_syncs) == 5
    assert ci_syncs.count("uv sync --locked --all-extras --dev") == 5
    assert all("--locked" in command and "--frozen" not in command for command in ci_syncs)

    release = _release_workflow()
    binary_install = _step(release["jobs"]["build-binaries"], "Install locked binary build dependencies")["run"]
    assert binary_install.count("uv sync --project") == 2
    assert binary_install.count("--locked --no-dev --extra bedrock --group release-binary") == 2
    assert '"$PRIMARY_SOURCE_ROOT"' in binary_install
    assert '"$REPRO_SOURCE_ROOT"' in binary_install

    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    docker_syncs = [line.strip() for line in dockerfile.splitlines() if line.strip().startswith("uv sync ")]
    assert len(docker_syncs) == 2
    assert all("--locked" in command and "--frozen" not in command for command in docker_syncs)


def test_standard_bedrock_install_surfaces_use_the_pinned_project_extra() -> None:
    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["optional-dependencies"]["bedrock"] == [
        "boto3==1.43.16",
        "botocore==1.43.16",
    ]

    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    docker_syncs = [line.strip() for line in dockerfile.splitlines() if line.strip().startswith("uv sync ")]
    assert docker_syncs
    assert all("--extra bedrock" in command for command in docker_syncs)

    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
    cli = (REPOSITORY_ROOT / "tacit" / "cli.py").read_text(encoding="utf-8")
    provider = (REPOSITORY_ROOT / "tacit" / "agents" / "providers" / "bedrock.py").read_text(encoding="utf-8")
    combined_advice = "\n".join((readme, cli, provider))
    assert "pip install 'tacit-ai[bedrock]'" in combined_advice
    assert "tacit[bedrock]" not in combined_advice
    assert "pip install boto3" not in combined_advice


def test_stale_direct_dependency_fails_the_lock_gate(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    assert uv is not None
    valid = subprocess.run(
        [uv, "lock", "--check"],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert valid.returncode == 0, valid.stderr

    shutil.copy2(REPOSITORY_ROOT / "uv.lock", tmp_path / "uv.lock")
    pyproject = (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    stale = pyproject.replace(
        "dependencies = [",
        'dependencies = [\n    "tacit-stale-lock-probe==0.0.1",',
        1,
    )
    assert stale != pyproject
    (tmp_path / "pyproject.toml").write_text(stale, encoding="utf-8")

    rejected = subprocess.run(
        [uv, "sync", "--locked", "--dry-run", "--all-extras", "--dev"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode != 0
    assert "lock" in rejected.stderr.casefold()


def test_release_build_closure_is_hash_pinned_and_matches_the_lock(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    assert uv is not None
    assert RELEASE_BUILD_CONSTRAINTS.is_file()

    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    build_roots = pyproject["dependency-groups"]["release-build"]
    assert build_roots == pyproject["build-system"]["requires"]

    generated = tmp_path / "release-build-constraints.txt"
    exported = subprocess.run(
        [
            uv,
            "export",
            "--locked",
            "--only-group",
            "release-build",
            "--no-emit-project",
            "--no-header",
            "--no-annotate",
            "--output-file",
            str(generated),
        ],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert exported.returncode == 0, exported.stderr
    constraints = RELEASE_BUILD_CONSTRAINTS.read_text(encoding="utf-8")
    assert constraints == generated.read_text(encoding="utf-8")

    requirement_blocks: list[str] = []
    current: list[str] = []
    for line in constraints.splitlines():
        if line and not line[0].isspace():
            if current:
                requirement_blocks.append("\n".join(current))
            current = [line]
        elif current:
            current.append(line)
    if current:
        requirement_blocks.append("\n".join(current))

    assert len(requirement_blocks) > len(build_roots)
    for block in requirement_blocks:
        requirement = block.split("\\", 1)[0].strip()
        assert re.fullmatch(r"[A-Za-z0-9_.-]+==[^ ;]+(?:\s*;\s*.+)?", requirement)
        assert "--hash=sha256:" in block


def test_release_build_and_wheel_smoke_consume_locked_inputs() -> None:
    build_dist = _release_workflow()["jobs"]["build-dist"]
    step_names = [step.get("name") for step in build_dist["steps"]]
    verify_name = "Verify release build dependency closure"
    build_name = "Build sdist and wheel"
    assert step_names.index(verify_name) < step_names.index(build_name)

    verify = _step(build_dist, verify_name)["run"]
    assert "uv export --locked" in verify
    assert "--only-group release-build" in verify
    assert "release-build-constraints.txt" in verify
    assert "cmp" in verify

    build = _step(build_dist, build_name)["run"]
    assert "uv build" in build
    assert "--clear" in build
    assert "--build-constraints .github/requirements/release-build-constraints.txt" in build
    assert "--require-hashes" in build

    smoke = _step(build_dist, "Smoke-test the wheel")["run"]
    export_at = smoke.index("uv export --locked")
    sync_at = smoke.index("uv pip sync")
    install_at = smoke.index("uv pip install")
    assert export_at < sync_at < install_at
    assert "--no-dev" in smoke
    assert "--no-emit-project" in smoke
    assert "uv pip sync --python .smoke-venv/bin/python --require-hashes" in smoke
    assert "uv pip install --python .smoke-venv/bin/python --no-deps dist/*.whl" in smoke


def test_release_builds_verified_binaries_on_exact_runners() -> None:
    jobs = _release_workflow()["jobs"]
    binary_job = jobs["build-binaries"]
    assert _job_needs(binary_job) == {"validate-release", "build-dist"}
    matrix = binary_job["strategy"]["matrix"]["include"]
    assert matrix == [
        {
            "runner": "ubuntu-22.04",
            "artifact": "tacit-binary-linux-x86_64",
            "binary": "dist/tacit",
            "package": "tacit-linux-x86_64.tar.gz",
            "glibc_baseline": "2.35",
            "uv_checksum": "90b2f223fb69d19db49e117da601f64978593417988530aa733d456141b4bcbb",
        },
    ]

    baseline = _step(binary_job, "Verify Linux glibc baseline")["run"]
    assert "getconf GNU_LIBC_VERSION" in baseline
    assert "glibc ${{ matrix.glibc_baseline }}" in baseline

    workflow_text = (REPOSITORY_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    for unsupported_surface in (
        "macos-15",
        "tacit-binary-macos",
        "tacit-macos",
        "windows-2025",
        "tacit-binary-windows",
        "tacit-windows",
        "dist/tacit.exe",
    ):
        assert unsupported_surface not in workflow_text

    matrix_text = (REPOSITORY_ROOT / "docs" / "foundation-invariant-matrix.md").read_text(encoding="utf-8")
    design_notes = (REPOSITORY_ROOT / "docs" / "engineering-design-notes.md").read_text(encoding="utf-8")
    context = (REPOSITORY_ROOT / "CONTEXT.md").read_text(encoding="utf-8")
    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
    pypi_readme = (REPOSITORY_ROOT / "README-PYPI.md").read_text(encoding="utf-8")
    assert "Only the Linux x86_64 frozen binary is published" in matrix_text
    assert "Only the Linux x86_64 frozen binary is published" in design_notes
    assert "Only the Linux x86_64 frozen binary is published" in readme
    for document in (matrix_text, design_notes, readme):
        assert re.search(r"Developer ID\s+signing and notarization", document)
    for public_document in (readme, pypi_readme):
        assert "glibc 2.35" in public_document
        assert "Ubuntu 22.04" in public_document
        assert "musl-based systems" in public_document
    assert "PyInstaller spec for macOS/Linux/Windows" not in context

    checkout_steps = [step for step in binary_job["steps"] if step.get("uses", "").startswith("actions/checkout@")]
    assert len(checkout_steps) == 2
    assert [step["with"]["path"] for step in checkout_steps] == [
        "binary-build-primary/source",
        "binary-build-repro/source",
    ]
    assert all(
        step["with"]["ref"] == "${{ github.sha }}" and step["with"]["persist-credentials"] == "false"
        for step in checkout_steps
    )

    install = _step(binary_job, "Install locked binary build dependencies")["run"]
    assert install.count("uv sync --project") == 2
    assert install.count("--locked --no-dev --extra bedrock --group release-binary") == 2
    assert '"$PRIMARY_SOURCE_ROOT"' in install
    assert '"$REPRO_SOURCE_ROOT"' in install
    assert install.count("UV_CACHE_DIR=") == 4
    assert install.count('UV_CACHE_DIR="$RUNNER_TEMP/uv-cache-primary"') == 2
    assert install.count('UV_CACHE_DIR="$RUNNER_TEMP/uv-cache-repro"') == 2

    build = _step(binary_job, "Build binaries independently")["run"]
    assert build.count("uv run --project") == 2
    assert build.count("--no-sync pyinstaller") == 2
    assert build.count("pyinstaller") == 2
    assert build.count("--clean --noconfirm") == 2
    assert '"$PRIMARY_SOURCE_ROOT/tacit.spec"' in build
    assert '"$REPRO_SOURCE_ROOT/tacit.spec"' in build
    assert '"$PRIMARY_SOURCE_ROOT/build"' in build
    assert '"$REPRO_SOURCE_ROOT/build"' in build
    smoke = _step(binary_job, "Smoke-test binary version")
    assert smoke["env"]["EXPECTED_VERSION"] == "${{ needs.validate-release.outputs.package_version }}"
    version_smoke_tree = ast.parse(_embedded_python(smoke["run"]))
    version_subprocess_calls = [
        node
        for node in ast.walk(version_smoke_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "subprocess"
        and node.func.attr == "run"
    ]
    assert len(version_subprocess_calls) == 1
    version_call = version_subprocess_calls[0]
    assert any(keyword.arg == "timeout" for keyword in version_call.keywords)
    assert "Version(result.removeprefix(prefix))" in smoke["run"]
    package = _step(binary_job, "Package binaries independently")["run"]
    assert package.count("package_release_binary.py") == 2
    assert '--source "$BINARY_PATH"' in package
    assert '--package "$PACKAGE_NAME"' in package
    assert '--source "$REPRO_BINARY_PATH"' in package
    assert '--package "$REPRO_PACKAGE_PATH"' in package
    assert package.count("--max-binary-bytes 536870912") == 2
    assert package.count("--max-package-bytes 536870912") == 2

    comparison = _step(binary_job, "Verify reproducible binary packages")["run"]
    assert 'cmp "$PACKAGE_NAME" "$REPRO_PACKAGE_PATH"' in comparison
    assert 'cmp "$PACKAGE_NAME.sha256" "$REPRO_PACKAGE_PATH.sha256"' in comparison
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
    assert binary_job["env"]["PRIMARY_PYINSTALLER_CONFIG_DIR"] == "binary-build-primary/pyinstaller-config"
    assert binary_job["env"]["REPRO_PYINSTALLER_CONFIG_DIR"] == "binary-build-repro/pyinstaller-config"
    assert 'PYINSTALLER_CONFIG_DIR="$PRIMARY_PYINSTALLER_CONFIG_DIR"' in build
    assert 'PYINSTALLER_CONFIG_DIR="$REPRO_PYINSTALLER_CONFIG_DIR"' in build

    packaged_smoke = _step(binary_job, "Smoke-test packaged binary on glibc baseline")
    packaged_smoke_script = packaged_smoke["run"]
    assert packaged_smoke["env"]["EXPECTED_GLIBC_BASELINE"] == "${{ matrix.glibc_baseline }}"
    assert 'release_image_archive.py" copy' in packaged_smoke_script
    assert '--archive "$PACKAGE_NAME"' in packaged_smoke_script
    assert '--checksum "$PACKAGE_NAME.sha256"' in packaged_smoke_script
    assert 'tar --extract --gzip --file "$private_package"' in packaged_smoke_script
    assert '--binary "$smoke_root/tacit"' in packaged_smoke_script
    assert "getconf GNU_LIBC_VERSION" in packaged_smoke_script
    assert 'tar --extract --gzip --file "$PACKAGE_NAME"' not in packaged_smoke_script

    steps = binary_job["steps"]
    build_index = steps.index(_step(binary_job, "Build binaries independently"))
    package_index = steps.index(_step(binary_job, "Package binaries independently"))
    compare_index = steps.index(_step(binary_job, "Verify reproducible binary packages"))
    upload_index = next(
        index for index, step in enumerate(steps) if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert build_index < package_index < compare_index < upload_index

    release = jobs["publish-github-release"]
    verify = _step(release, "Verify binary packages and checksums")["run"]
    assert "tacit-linux-x86_64.tar.gz" in verify
    assert "tacit-macos-arm64.tar.gz" not in verify
    assert "sha256sum --check ./*.sha256" in verify
    publish = _step(release, "Publish GitHub release binaries")
    assert publish["uses"] == "softprops/action-gh-release@3d0d9888cb7fd7b750713d6e236d1fcb99157228"
    assert publish["with"]["overwrite_files"] == "false"


def test_binary_version_smoke_runs_from_clean_job_root_with_nested_checkouts(tmp_path: Path) -> None:
    binary_job = _release_workflow()["jobs"]["build-binaries"]
    smoke = _step(binary_job, "Smoke-test binary version")["run"]
    primary_root = tmp_path / "binary-build-primary" / "source"
    repro_root = tmp_path / "binary-build-repro" / "source"
    smoke_script = primary_root / ".github" / "scripts" / "smoke_release_binary.py"
    smoke_script.parent.mkdir(parents=True)
    repro_root.mkdir(parents=True)
    smoke_script.write_text(
        """from __future__ import annotations
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--binary")
parser.add_argument("--version-only", action="store_true")
parser.add_argument("--expected-version", required=True)
parser.add_argument("--command-timeout")
args = parser.parse_args()
print(f"tacit, version {args.expected_version}")
""",
        encoding="utf-8",
    )
    binary = primary_root / "dist" / "tacit"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(b"binary-placeholder")

    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/bin/sh
set -eu
[ "$1" = "run" ]
shift
[ "$1" = "--project" ]
shift 2
[ "$1" = "--no-sync" ]
shift
exec "$@"
""",
        encoding="utf-8",
    )
    fake_uv.chmod(0o700)

    result = subprocess.run(
        ["bash", "-c", smoke],
        cwd=tmp_path,
        env=os.environ
        | {
            "BINARY_PATH": "binary-build-primary/source/dist/tacit",
            "EXPECTED_VERSION": "1.2.3-rc.4",
            "PATH": f"{fake_bin}:{Path(sys.executable).parent}:{os.environ.get('PATH', '')}",
            "PRIMARY_SOURCE_ROOT": "binary-build-primary/source",
            "REPRO_SOURCE_ROOT": "binary-build-repro/source",
        },
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_frozen_binaries_use_release_only_locked_dependencies_and_archive_audit() -> None:
    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["dependency-groups"]["release-binary"] == ["pyinstaller==6.22.1"]

    binary_job = _release_workflow()["jobs"]["build-binaries"]
    assert _job_needs(binary_job) == {"validate-release", "build-dist"}
    distribution_download = next(
        step for step in binary_job["steps"] if step.get("uses", "").startswith("actions/download-artifact@")
    )
    assert distribution_download["with"] == {"name": "dist", "path": "release-dist"}
    install = _step(binary_job, "Install locked binary build dependencies")["run"]
    assert install.count("--locked --no-dev --extra bedrock --group release-binary --no-install-project") == 2
    assert install.count("uv pip install") == 2
    assert "wheel_files=(release-dist/tacit_ai-*.whl)" in install
    assert install.count('"$wheel_path"') == 2
    assert install.count("tacit_ai-*.dist-info/uv_cache.json") == 2
    assert "--all-extras" not in install
    assert "--dev" not in install

    for step_name in (
        "Build binaries independently",
        "Audit frozen binary contents",
        "Package binaries independently",
        "Smoke-test binary version",
        "Smoke-test packaged binary on glibc baseline",
    ):
        command = _step(binary_job, step_name)["run"]
        assert command.count("uv run --project") == command.count("--no-sync")

    audit = _step(binary_job, "Audit frozen binary contents")
    audit_script = audit["run"]
    assert '"$PRIMARY_SOURCE_ROOT/.github/scripts/inspect_release_binary.py"' in audit_script
    assert '--binary "$BINARY_PATH"' in audit_script
    assert binary_job["steps"].index(audit) < binary_job["steps"].index(
        _step(binary_job, "Package binaries independently")
    )

    inspector = REPOSITORY_ROOT / ".github" / "scripts" / "inspect_release_binary.py"
    source = inspector.read_text(encoding="utf-8")
    for required in (
        "tacit.agents.providers.anthropic",
        "tacit.agents.providers.bedrock",
        "tacit.agents.providers.ollama",
        "tacit.agents.providers.openai_provider",
        "tacit.integrations.slack",
        "boto3",
        "botocore",
    ):
        assert required in source
    for forbidden in ("black", "mypy", "playwright", "pytest", "respx", "ruff"):
        assert forbidden in source
    assert "tacit_ai-*.dist-info/METADATA" in source
    assert "tacit_ai-*.dist-info/entry_points.txt" in source
    assert "tacit_ai-*.dist-info/RECORD" in source
    assert "tacit_ai-*.dist-info/uv_cache.json" in source


def test_release_binary_archive_audit_rejects_dev_modules_and_missing_runtime_providers() -> None:
    inspector = _load_script_module(RELEASE_BINARY_INSPECTOR)
    complete = {
        "tacit.agents.providers.anthropic",
        "tacit.agents.providers.bedrock",
        "tacit.agents.providers.ollama",
        "tacit.agents.providers.openai_provider",
        "tacit.integrations.slack",
        "boto3",
        "botocore",
        "tacit_ai-0.1.1rc5.dist-info/METADATA",
        "tacit_ai-0.1.1rc5.dist-info/entry_points.txt",
    }
    inspector.validate_members(complete)

    with pytest.raises(inspector.BinaryArchiveInspectionError, match="development-only"):
        inspector.validate_members(complete | {"mypy.nodes"})

    with pytest.raises(inspector.BinaryArchiveInspectionError, match="missing required"):
        inspector.validate_members(complete - {"botocore"})

    with pytest.raises(inspector.BinaryArchiveInspectionError, match="distribution metadata"):
        inspector.validate_members(complete - {"tacit_ai-0.1.1rc5.dist-info/METADATA"})

    with pytest.raises(inspector.BinaryArchiveInspectionError, match="installation record"):
        inspector.validate_members(complete | {"tacit_ai-0.1.1rc5.dist-info/RECORD"})

    with pytest.raises(inspector.BinaryArchiveInspectionError, match="build cache metadata"):
        inspector.validate_members(complete | {"tacit_ai-0.1.1rc5.dist-info/uv_cache.json"})


def test_release_binaries_pass_isolated_offline_runtime_smoke_before_upload() -> None:
    binary_job = _release_workflow()["jobs"]["build-binaries"]
    runtime_smoke = _step(binary_job, "Smoke-test packaged binary on glibc baseline")
    assert 1 <= int(runtime_smoke["timeout-minutes"]) <= 5
    assert int(runtime_smoke["timeout-minutes"]) < int(binary_job["timeout-minutes"])

    script = runtime_smoke["run"]
    assert "required_contract_snippets" not in script
    assert "grep -F" not in script
    assert 'uv run --project "$PRIMARY_SOURCE_ROOT" --no-sync python "$smoke_script"' in script
    assert "--command-timeout" in script
    assert "--startup-timeout" in script
    assert "--shutdown-timeout" in script
    smoke_source = (REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_binary.py").read_text(encoding="utf-8")
    assert "benchmark-grounding" in smoke_source
    assert "operational-learning-benchmark" in smoke_source
    assert "_run_benchmark_resource_smoke" in smoke_source

    steps = binary_job["steps"]
    runtime_smoke_index = steps.index(runtime_smoke)
    package_index = steps.index(_step(binary_job, "Package binaries independently"))
    comparison_index = steps.index(_step(binary_job, "Verify reproducible binary packages"))
    upload_index = next(
        index for index, step in enumerate(steps) if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert package_index < comparison_index < runtime_smoke_index < upload_index


def test_pyinstaller_excludes_only_installation_record_manifests() -> None:
    source = (REPOSITORY_ROOT / "tacit.spec").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_without_installation_records"
    )
    namespace: dict[str, Any] = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "tacit.spec", "exec"), namespace)
    filter_records = namespace["_without_installation_records"]

    entries = [
        ("websockets-16.0.dist-info/RECORD", "/first/.venv/record", "DATA"),
        ("websockets-16.0.dist-info/METADATA", "/first/.venv/metadata", "DATA"),
        ("websockets-16.0.dist-info/entry_points.txt", "/first/.venv/entry-points", "DATA"),
        ("tacit/data/RECORD", "/first/source/tacit/data/RECORD", "DATA"),
        (r"package-1.0.dist-info\RECORD", r"C:\second\.venv\record", "DATA"),
    ]

    assert filter_records(entries) == [
        entries[1],
        entries[2],
        entries[3],
    ]
    assert "a.datas = _without_installation_records(a.datas)" in source
    assert 'copy_metadata("tacit-ai")' in source


def test_frozen_binary_packages_and_loads_the_investigation_schema() -> None:
    spec = (REPOSITORY_ROOT / "tacit.spec").read_text(encoding="utf-8")
    smoke = (REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_binary.py").read_text(encoding="utf-8")

    assert '(os.path.join(root, "tacit", "schemas"), "tacit/schemas")' in spec
    assert "runtime_hooks=[schema_smoke_hook, metadata_smoke_hook]" in spec
    assert "load_investigation_contract_schema" in spec
    assert "_run_schema_smoke" in smoke
    assert "TACIT_RELEASE_SCHEMA_SMOKE" in smoke


def test_frozen_binary_packages_and_loads_tacit_distribution_metadata() -> None:
    spec = (REPOSITORY_ROOT / "tacit.spec").read_text(encoding="utf-8")
    smoke = (REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_binary.py").read_text(encoding="utf-8")

    assert 'copy_metadata("tacit-ai")' in spec
    assert "runtime_hooks=[schema_smoke_hook, metadata_smoke_hook]" in spec
    assert "importlib.metadata.version" in spec
    assert "importlib.metadata.entry_points" in spec
    assert "_run_metadata_smoke" in smoke
    assert "TACIT_RELEASE_METADATA_SMOKE" in smoke


def test_wheel_smoke_loads_every_packaged_benchmark_corpus() -> None:
    workflow = _release_workflow()
    smoke = _step(workflow["jobs"]["build-dist"], "Smoke-test the wheel")["run"]
    assert "load_grounding_corpus" in smoke
    assert "load_acceptance_corpus" in smoke
    assert "load_operational_learning_corpus" in smoke
    assert "grounding_benchmark_v1.json" in smoke
    assert "operational_learning_v1.json" in smoke


def test_github_release_rechecks_and_postflights_exact_remote_asset_set() -> None:
    job = _release_workflow()["jobs"]["publish-github-release"]
    steps = job["steps"]
    assets = _step(job, "Verify GitHub release publication state")
    authorization = _step(job, "Re-authorize release before publication")
    publish = _step(job, "Publish GitHub release binaries")
    postflight = _step(job, "Verify published GitHub release assets")

    assert steps.index(assets) + 1 == steps.index(authorization)
    assert steps.index(authorization) + 1 == steps.index(publish)
    assert steps.index(publish) + 1 == steps.index(postflight)
    assert "--allow-absent-release" in assets["run"]
    assert "--require-release" in postflight["run"]
    assert assets["env"]["GITHUB_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert authorization["env"]["GITHUB_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert postflight["env"]["GITHUB_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"

    verifier = RELEASE_GITHUB_ASSET_VERIFIER.read_text(encoding="utf-8")
    assert "MAX_RELEASE_METADATA_BYTES" in verifier
    assert "MAX_RELEASE_ASSET_BYTES" in verifier
    assert "remaining_with_guard = declared - total + 1" in verifier
    assert "args.require_release and set(names) != REQUIRED_ASSETS" in verifier
    assert "set(names) - REQUIRED_ASSETS" in verifier
    assert "remote[name] != local[name]" in verifier
    assert "authenticated_github_request" in verifier


def test_github_release_jobs_use_one_proxy_safe_asset_verifier() -> None:
    jobs = _release_workflow()["jobs"]
    preflight = jobs["github-release-preflight"]
    publisher = jobs["publish-github-release"]

    checkout = next(step for step in preflight["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"] == {
        "persist-credentials": "false",
        "ref": "${{ github.sha }}",
    }
    assert _step(preflight, "Verify any existing GitHub release assets")["run"].strip() == (
        "python ../.github/scripts/verify_github_release_assets.py --allow-absent-release"
    )
    assert _step(publisher, "Verify GitHub release publication state")["run"].strip() == (
        'python "$GITHUB_WORKSPACE/.github/scripts/verify_github_release_assets.py" --allow-absent-release'
    )
    assert _step(publisher, "Verify published GitHub release assets")["run"].strip() == (
        'python "$GITHUB_WORKSPACE/.github/scripts/verify_github_release_assets.py" --require-release'
    )
    assert not any(step.get("name") == "Prepare bounded GitHub release asset verifier" for step in publisher["steps"])

    verifier = RELEASE_GITHUB_ASSET_VERIFIER.read_text(encoding="utf-8")
    github_api = RELEASE_GITHUB_API.read_text(encoding="utf-8")
    assert "authenticated_github_request" in verifier
    assert "build_github_api_opener" in verifier
    assert "ProxyHandler({})" in github_api


def test_github_release_asset_verifier_rejects_plaintext_before_attaching_token(
    tmp_path: Path,
) -> None:
    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
    )
    for name in required:
        (tmp_path / name).write_bytes(name.encode())
    observed_authorization: list[str | None] = []

    class CaptureAPI(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            observed_authorization.append(self.headers.get("Authorization"))
            self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CaptureAPI)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), "--allow-absent-release"],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "plaintext-asset-token",
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
    assert "HTTPS" in result.stderr
    assert observed_authorization == []


def test_github_release_asset_verifier_ignores_ambient_https_proxy(
    tmp_path: Path,
) -> None:
    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
    )
    for name in required:
        (tmp_path / name).write_bytes(name.encode())
    proxy_requests: list[str] = []

    class CaptureProxy(http.server.BaseHTTPRequestHandler):
        def do_CONNECT(self) -> None:  # noqa: N802
            proxy_requests.append(self.path)
            self.send_error(502)

        def log_message(self, format: str, *args: object) -> None:
            return

    proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CaptureProxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    environment = os.environ.copy()
    environment.update(
        {
            "GITHUB_API_URL": "https://release-api.invalid",
            "GITHUB_REPOSITORY": "aditki/tacit",
            "GITHUB_REF_NAME": "v1.2.3",
            "GITHUB_TOKEN": "proxy-asset-token",
            "HTTPS_PROXY": f"http://127.0.0.1:{proxy.server_port}",
            "https_proxy": f"http://127.0.0.1:{proxy.server_port}",
            "ALL_PROXY": f"http://127.0.0.1:{proxy.server_port}",
            "all_proxy": f"http://127.0.0.1:{proxy.server_port}",
        }
    )
    environment.pop("NO_PROXY", None)
    environment.pop("no_proxy", None)
    try:
        result = subprocess.run(
            [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), "--allow-absent-release"],
            cwd=tmp_path,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        proxy.shutdown()
        thread.join(timeout=5)
        proxy.server_close()

    assert result.returncode != 0
    assert proxy_requests == []


def test_github_release_notes_are_generated_only_for_first_publication() -> None:
    job = _release_workflow()["jobs"]["publish-github-release"]
    preflight = _step(job, "Verify GitHub release publication state")
    publish = _step(job, "Publish GitHub release binaries")
    verifier = RELEASE_GITHUB_ASSET_VERIFIER.read_text(encoding="utf-8")

    assert preflight["id"] == "github-release-preflight"
    assert publish["with"]["generate_release_notes"] == (
        "${{ steps.github-release-preflight.outputs.release_exists != 'true' }}"
    )
    assert "_emit_release_exists(False)" in verifier
    assert "_emit_release_exists(True)" in verifier
    absent = verifier.index("_emit_release_exists(False)")
    assert "return" in verifier[absent : absent + 100]
    assert verifier.index("_emit_release_exists(True)") > verifier.index("remote[name] != local[name]")


def test_public_request_body_budget_defaults_match_runtime_configuration() -> None:
    default_bytes = DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES
    assert default_bytes == 512 * 1_024 * 1_024
    default_mib = default_bytes // (1_024 * 1_024)

    env_example = (REPOSITORY_ROOT / ".env.example").read_text(encoding="utf-8")
    yaml_example = yaml.safe_load((REPOSITORY_ROOT / "tacit.yaml.example").read_text(encoding="utf-8"))
    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
    pypi_readme = (REPOSITORY_ROOT / "README-PYPI.md").read_text(encoding="utf-8")

    assert f"API_REQUEST_BODY_MAX_BUFFERED_BYTES={default_bytes}" in env_example
    assert yaml_example["api"]["request_body_max_buffered_bytes"] == default_bytes
    assert f"API_REQUEST_BODY_MAX_BUFFERED_BYTES` ({default_mib} MiB by default)" in readme
    assert f"defaults to 16 concurrent bodies and {default_mib} MiB" in pypi_readme


def test_final_github_asset_verifier_allows_matching_partial_retry_then_requires_exact_set(
    tmp_path: Path,
) -> None:
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
    )
    payloads = {name: f"local:{name}".encode() for name in required}
    for name, payload in payloads.items():
        (assets_dir / name).write_bytes(payload)

    published_names = list(required)
    release_available = [True]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/repos/aditki/tacit/releases/tags/v1.2.3":
                if not release_available[0]:
                    self.send_error(404)
                    return
                port = int(getattr(self.server, "server_port"))
                body = json.dumps(
                    {
                        "assets": [
                            {
                                "name": name,
                                "size": len(payloads[name]),
                                "url": f"https://127.0.0.1:{port}/assets/{name}",
                            }
                            for name in published_names
                        ]
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path.startswith("/assets/"):
                name = self.path.removeprefix("/assets/")
                body = payloads[name]
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(server, certificate=certificate, private_key=private_key)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:

        def run_verifier(mode: str) -> tuple[subprocess.CompletedProcess[str], str]:
            output_path = tmp_path / "github-output"
            output_path.write_text("", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), mode],
                cwd=assets_dir,
                env=os.environ
                | {
                    "GITHUB_API_URL": f"https://127.0.0.1:{server.server_port}",
                    "GITHUB_REPOSITORY": "aditki/tacit",
                    "GITHUB_REF_NAME": "v1.2.3",
                    "GITHUB_TOKEN": "test-token",
                    "GITHUB_OUTPUT": str(output_path),
                    "SSL_CERT_FILE": str(certificate),
                },
                check=False,
                capture_output=True,
                text=True,
            )
            return result, output_path.read_text(encoding="utf-8")

        unexpected = assets_dir / "unexpected-debug-bundle.zip"
        unexpected.write_bytes(b"debug")
        rejected_local_set, _ = run_verifier("--require-release")
        unexpected.unlink()
        accepted, _ = run_verifier("--require-release")
        published_names[:] = [required[0]]
        partial_preflight, partial_output = run_verifier("--allow-absent-release")
        partial_postflight, _ = run_verifier("--require-release")
        release_available[0] = False
        absent_preflight, absent_output = run_verifier("--allow-absent-release")
        release_available[0] = True
        payloads["unexpected-remote.zip"] = b"unexpected"
        published_names[:] = ["unexpected-remote.zip"]
        unexpected_preflight, _ = run_verifier("--allow-absent-release")
        published_names[:] = [required[0]]
        payloads[required[0]] = b"x" * len(payloads[required[0]])
        mismatched_preflight, _ = run_verifier("--allow-absent-release")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    assert rejected_local_set.returncode != 0
    assert "local github release asset set differs" in rejected_local_set.stderr.lower()
    assert accepted.returncode == 0, accepted.stderr
    assert partial_preflight.returncode == 0, partial_preflight.stderr
    assert partial_output == "release_exists=true\n"
    assert partial_postflight.returncode != 0
    assert "asset set differs" in partial_postflight.stderr.lower()
    assert absent_preflight.returncode == 0, absent_preflight.stderr
    assert absent_output == "release_exists=false\n"
    assert unexpected_preflight.returncode != 0
    assert "unexpected assets" in unexpected_preflight.stderr.lower()
    assert mismatched_preflight.returncode != 0
    assert "differs from local artifacts" in mismatched_preflight.stderr.lower()


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


def test_pypi_jobs_use_one_fixed_origin_proxy_isolated_bounded_verifier() -> None:
    jobs = _release_workflow()["jobs"]
    preflight = _step(jobs["pypi-preflight"], "Verify any existing PyPI files")
    postflight = _step(jobs["publish-pypi"], "Verify published PyPI files")

    assert "verify_pypi_release.py preflight" in preflight["run"]
    assert "verify_pypi_release.py postflight" in postflight["run"]
    verifier_source = RELEASE_PYPI_VERIFIER.read_text(encoding="utf-8")
    assert "ProxyHandler({})" in verifier_source
    assert "HTTPRedirectHandler" in verifier_source
    assert 'PYPI_ORIGIN = "https://pypi.org"' in verifier_source
    assert "MAX_PYPI_METADATA_BYTES + 1" in verifier_source
    assert "Content-Length" in verifier_source
    assert "timeout=" in verifier_source
    assert "PYPI_API_URL" not in verifier_source
    tree = ast.parse(verifier_source)
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "read"
        and not node.args
        and not node.keywords
        for node in ast.walk(tree)
    )


def test_pypi_verifier_ignores_ambient_proxy_and_targets_only_pypi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verifier = _load_script_module(RELEASE_PYPI_VERIFIER)
    resolved_hosts: list[str] = []

    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    for name in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(name, "http://proxy-capture.invalid:8443")

    def reject_resolution(host: str, *args: object, **kwargs: object) -> list[object]:
        resolved_hosts.append(host)
        raise socket.gaierror("blocked test resolver")

    monkeypatch.setattr(socket, "getaddrinfo", reject_resolution)
    with pytest.raises(OSError):
        verifier.fetch_release_digests("1.2.3", timeout=0.1)

    assert resolved_hosts == ["pypi.org"]
    assert verifier.pypi_metadata_url("1.2.3") == "https://pypi.org/pypi/tacit-ai/1.2.3/json"


def test_pypi_verifier_rejects_redirects_and_oversized_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verifier = _load_script_module(RELEASE_PYPI_VERIFIER)
    redirected: list[str] = []

    class RedirectTarget(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            redirected.append(self.path)
            self.send_error(500)

        def log_message(self, format: str, *args: object) -> None:
            return

    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectTarget)

    class RedirectingPyPI(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(302)
            self.send_header("Location", f"https://127.0.0.1:{target.server_port}/redirected")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    api = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectingPyPI)
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(target, certificate=certificate, private_key=private_key)
    _serve_over_https(api, certificate=certificate, private_key=private_key)
    threads = [
        threading.Thread(target=target.serve_forever, daemon=True),
        threading.Thread(target=api.serve_forever, daemon=True),
    ]
    for thread in threads:
        thread.start()
    monkeypatch.setattr(verifier, "PYPI_ORIGIN", f"https://127.0.0.1:{api.server_port}")
    context_type = getattr(truststore_api, "_original_SSLContext")
    client_context = context_type(ssl.PROTOCOL_TLS_CLIENT)
    client_context.load_verify_locations(certificate)
    opener = build_opener(ProxyHandler({}), verifier._RejectRedirects(), HTTPSHandler(context=client_context))
    try:
        with pytest.raises(Exception) as redirect_error:
            verifier.fetch_release_digests("1.2.3", opener=opener, timeout=2)
    finally:
        api.shutdown()
        target.shutdown()
        for thread in threads:
            thread.join(timeout=5)
        api.server_close()
        target.server_close()

    assert "302" in str(redirect_error.value)
    assert redirected == []

    class OversizedResponse:
        headers = {"Content-Length": str(verifier.MAX_PYPI_METADATA_BYTES + 1)}

        def read(self, amount: int = -1) -> bytes:
            raise AssertionError("oversized metadata must be rejected before reading")

    with pytest.raises(verifier.ReleasePyPIError, match="size limit"):
        verifier.load_pypi_metadata(OversizedResponse())

    monkeypatch.setattr(verifier, "PYPI_ORIGIN", "http://pypi.org")
    with pytest.raises(verifier.ReleasePyPIError, match="fixed HTTPS origin"):
        verifier.pypi_metadata_url("1.2.3")


def test_publication_snapshot_survives_workspace_path_replacement(
    tmp_path: Path,
) -> None:
    source_directory = tmp_path / "downloaded"
    source_directory.mkdir()
    source = source_directory / "tacit_ai-1.2.3-py3-none-any.whl"
    original_bytes = b"authorized wheel bytes"
    replacement_bytes = b"workspace replacement"
    source.write_bytes(original_bytes)
    replacement = tmp_path / "replacement"
    replacement.write_bytes(replacement_bytes)
    displaced = tmp_path / "displaced"
    digest = hashlib.sha256(original_bytes).hexdigest()
    destination = tmp_path / "private-publication"

    rejected = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "create",
        "--source-directory",
        str(source_directory),
        "--destination-directory",
        str(destination),
        "--artifact",
        f"wheel={source.name}={'0' * 64}",
        "--maximum",
        "1024",
    )
    assert rejected.returncode != 0
    assert "checksum mismatch" in rejected.stderr
    assert not destination.exists()

    created = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "create",
        "--source-directory",
        str(source_directory),
        "--destination-directory",
        str(destination),
        "--artifact",
        f"wheel={source.name}={digest}",
        "--maximum",
        "1024",
    )
    assert created.returncode == 0, created.stderr
    source.rename(displaced)
    replacement.rename(source)

    snapshot = destination / source.name
    assert source.read_bytes() == replacement_bytes
    assert snapshot.read_bytes() == original_bytes
    assert stat.S_IMODE(snapshot.stat().st_mode) == 0o400
    assert stat.S_IMODE(destination.stat().st_mode) == 0o500

    cleaned = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "cleanup",
        "--directory",
        str(destination),
        "--artifact-name",
        source.name,
    )
    assert cleaned.returncode == 0, cleaned.stderr
    assert not destination.exists()


def test_unsealed_publication_directory_swap_cannot_be_the_publisher_input(
    tmp_path: Path,
) -> None:
    source_directory = tmp_path / "downloaded"
    source_directory.mkdir()
    artifacts = {
        "tacit_ai-1.2.3-py3-none-any.whl": b"authorized wheel bytes",
        "tacit_ai-1.2.3.tar.gz": b"authorized sdist bytes",
    }
    artifact_args: list[str] = []
    for index, (name, payload) in enumerate(artifacts.items()):
        (source_directory / name).write_bytes(payload)
        artifact_args.extend(
            (
                "--artifact",
                f"artifact_{index}={name}={hashlib.sha256(payload).hexdigest()}",
            )
        )

    publication_path = tmp_path / "publication"
    created = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "create",
        "--source-directory",
        str(source_directory),
        "--destination-directory",
        str(publication_path),
        *artifact_args,
    )
    assert created.returncode == 0, created.stderr

    displaced = tmp_path / "displaced-publication"
    publication_path.rename(displaced)
    publication_path.mkdir()
    for name in artifacts:
        (publication_path / name).write_bytes(b"same-uid replacement")
    assert {path.read_bytes() for path in publication_path.iterdir()} == {b"same-uid replacement"}
    assert {path.read_bytes() for path in displaced.iterdir()} == set(artifacts.values())

    jobs = _release_workflow()["jobs"]
    publisher_specs = (
        (
            jobs["publish-pypi"],
            "Prepare sealed PyPI publisher path",
            "PYPI_PUBLICATION_SNAPSHOT_DIR",
            "PYPI_PUBLICATION_BACKING_DIR",
            "packages-dir",
            "Publish to PyPI",
        ),
        (
            jobs["publish-github-release"],
            "Prepare sealed GitHub release publisher path",
            "GITHUB_RELEASE_PUBLICATION_SNAPSHOT_DIR",
            "GITHUB_RELEASE_PUBLICATION_BACKING_DIR",
            "files",
            "Publish GitHub release binaries",
        ),
    )
    for job, snapshot_name, path_variable, backing_variable, input_name, publish_name in publisher_specs:
        snapshot = _step(job, snapshot_name)["run"]
        publisher_input = _step(job, publish_name)["with"][input_name]
        assert "release_publication_snapshot.py seal" in snapshot
        assert "sudo --non-interactive" in snapshot
        assert f'--destination-directory "${path_variable}"' in snapshot
        assert f'--backing-directory "${backing_variable}"' in snapshot
        assert job["env"][backing_variable].startswith("/var/lib/tacit-release-publication-")
        assert f"${{{{ env.{path_variable} }}}}" in publisher_input


@pytest.mark.skipif(sys.platform != "linux", reason="read-only bind mounts are a Linux publication boundary")
def test_sealed_publication_path_rejects_same_uid_directory_rename_recreate_race(
    tmp_path: Path,
) -> None:
    privilege_prefix: list[str]
    if os.geteuid() == 0:
        privilege_prefix = []
    else:
        sudo = shutil.which("sudo")
        if sudo is None:
            pytest.skip("passwordless sudo is unavailable")
        probe = subprocess.run(
            [sudo, "--non-interactive", "true"],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0:
            pytest.skip("passwordless sudo is unavailable")
        privilege_prefix = [sudo, "--non-interactive"]

    source_directory = tmp_path / "downloaded"
    source_directory.mkdir()
    artifact = source_directory / "tacit_ai-1.2.3-py3-none-any.whl"
    payload = b"descriptor-bound publisher bytes"
    artifact.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    publication_path = tmp_path / "publication"
    backing_directory = Path("/var/lib") / f"tacit-release-publication-test-{secrets.token_hex(12)}"
    artifact_spec = f"wheel={artifact.name}={digest}"

    seal = subprocess.run(
        [
            *privilege_prefix,
            sys.executable,
            str(RELEASE_PUBLICATION_SNAPSHOT),
            "seal",
            "--source-directory",
            str(source_directory),
            "--destination-directory",
            str(publication_path),
            "--backing-directory",
            str(backing_directory),
            "--artifact",
            artifact_spec,
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert seal.returncode == 0, seal.stderr
    try:
        displaced = tmp_path / "displaced-publication"
        race = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import os, pathlib, sys; "
                    "source, displaced = sys.argv[1:]; "
                    "\ntry: os.rename(source, displaced)"
                    "\nexcept OSError as exc: print(exc.errno); raise SystemExit(0)"
                    "\npathlib.Path(source).mkdir(); raise SystemExit(2)"
                ),
                str(publication_path),
                str(displaced),
            ],
            cwd=tmp_path,
            check=False,
            capture_output=True,
            text=True,
        )
        assert race.returncode == 0, race.stderr
        assert race.stdout.strip() == str(errno.EBUSY)
        assert (publication_path / artifact.name).read_bytes() == payload
    finally:
        cleanup = subprocess.run(
            [
                *privilege_prefix,
                sys.executable,
                str(RELEASE_PUBLICATION_SNAPSHOT),
                "unseal",
                "--directory",
                str(publication_path),
                "--backing-directory",
                str(backing_directory),
                "--artifact-name",
                artifact.name,
            ],
            cwd=tmp_path,
            check=False,
            capture_output=True,
            text=True,
        )
        assert cleanup.returncode == 0, cleanup.stderr


@pytest.mark.skipif(sys.platform != "linux", reason="action-path mount authority requires Linux")
def test_sealed_publication_path_blocks_workspace_swap_and_cleans_displaced_mount() -> None:
    if os.geteuid() == 0:
        privilege_prefix: list[str] = []
    else:
        sudo = shutil.which("sudo")
        if sudo is None:
            pytest.skip("passwordless sudo is unavailable")
        probe = subprocess.run(
            [sudo, "--non-interactive", "true"],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0:
            pytest.skip("passwordless sudo is unavailable")
        privilege_prefix = [sudo, "--non-interactive"]

    suffix = secrets.token_hex(12)
    trusted_root = Path("/var/lib") / f"tacit-release-publication-test-root-{suffix}"
    runner_home = trusted_root / "runner"
    workspace = runner_home / "work" / "tacit" / "tacit"
    publication_path = workspace / f".tacit-pypi-publication-{suffix}"
    displaced_workspace = workspace.with_name("displaced-workspace")
    recovery_mountpoint = runner_home / "recovery" / "publication"
    backing_directory = Path("/var/lib") / f"tacit-release-publication-test-{suffix}"
    artifact_name = "tacit_ai-1.2.3-py3-none-any.whl"
    payload = b"descriptor-bound publisher bytes"
    artifact_spec = f"wheel={artifact_name}={hashlib.sha256(payload).hexdigest()}"

    setup = subprocess.run(
        [
            *privilege_prefix,
            sys.executable,
            "-c",
            (
                "import os, pathlib, sys; root, home, uid, gid = sys.argv[1:]; "
                "pathlib.Path(root).mkdir(mode=0o755); pathlib.Path(home).mkdir(mode=0o700); "
                "os.chown(home, int(uid), int(gid))"
            ),
            str(trusted_root),
            str(runner_home),
            str(os.getuid()),
            str(os.getgid()),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert setup.returncode == 0, setup.stderr

    def emergency_cleanup() -> None:
        cleanup_script = """
import os
import pathlib
import shutil
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
backing = pathlib.Path(sys.argv[2])
mounts = []
try:
    lines = pathlib.Path('/proc/self/mountinfo').read_text(encoding='utf-8').splitlines()
except OSError:
    lines = []
for line in lines:
    fields = line.split()
    if len(fields) < 10:
        continue
    mountpoint = pathlib.Path(fields[4].replace('\\040', ' '))
    if mountpoint == root or root in mountpoint.parents:
        mounts.append(mountpoint)
for mountpoint in sorted(mounts, key=lambda item: len(item.parts), reverse=True):
    subprocess.run(['/usr/bin/umount', '--', str(mountpoint)], check=False)
if root.exists():
    shutil.rmtree(root)
for candidate in pathlib.Path('/var/lib').glob(backing.name + '*'):
    if candidate.is_dir():
        os.chmod(candidate, 0o700)
        shutil.rmtree(candidate)
    else:
        candidate.unlink()
"""
        subprocess.run(
            [
                *privilege_prefix,
                sys.executable,
                "-c",
                cleanup_script,
                str(trusted_root),
                str(backing_directory),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    try:
        source_directory = workspace / "downloaded"
        source_directory.mkdir(parents=True)
        (source_directory / artifact_name).write_bytes(payload)
        recovery_mountpoint.mkdir(parents=True)

        seal = subprocess.run(
            [
                *privilege_prefix,
                sys.executable,
                str(RELEASE_PUBLICATION_SNAPSHOT),
                "seal",
                "--source-directory",
                str(source_directory),
                "--destination-directory",
                str(publication_path),
                "--backing-directory",
                str(backing_directory),
                "--artifact",
                artifact_spec,
            ],
            cwd=workspace,
            check=False,
            capture_output=True,
            text=True,
        )
        assert seal.returncode == 0, seal.stderr

        mounted_paths = {
            fields[4]
            for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
            if len(fields := line.split()) >= 10
        }
        anchored_ancestors = (workspace, workspace.parent, workspace.parent.parent, runner_home)
        assert all(str(path) in mounted_paths for path in anchored_ancestors)

        race = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import errno, os, pathlib, sys; source, displaced, artifact = sys.argv[1:]; "
                    "\ntry: os.rename(source, displaced)"
                    "\nexcept OSError as exc: print(exc.errno); raise SystemExit(0)"
                    "\npathlib.Path(source).mkdir(parents=True)"
                    "\nreplacement = pathlib.Path(source) / pathlib.Path(artifact).parent.name"
                    "\nreplacement.mkdir()"
                    "\n(replacement / pathlib.Path(artifact).name).write_bytes(b'attacker bytes')"
                    "\nraise SystemExit(2)"
                ),
                str(workspace),
                str(displaced_workspace),
                str(publication_path / artifact_name),
            ],
            cwd=runner_home,
            check=False,
            capture_output=True,
            text=True,
        )
        assert race.returncode == 0, race.stderr
        assert race.stdout.strip() == str(errno.EBUSY)
        assert (publication_path / artifact_name).read_bytes() == payload

        for index, ancestor in enumerate(anchored_ancestors[1:], start=1):
            displaced_ancestor = ancestor.with_name(f"displaced-{index}-{ancestor.name}")
            ancestor_race = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import os, pathlib, sys; source, displaced = sys.argv[1:]; "
                        "\ntry: os.rename(source, displaced)"
                        "\nexcept OSError as exc: print(exc.errno); raise SystemExit(0)"
                        "\npathlib.Path(source).mkdir(); raise SystemExit(2)"
                    ),
                    str(ancestor),
                    str(displaced_ancestor),
                ],
                cwd=trusted_root,
                check=False,
                capture_output=True,
                text=True,
            )
            assert ancestor_race.returncode == 0, ancestor_race.stderr
            assert int(ancestor_race.stdout.strip()) in {errno.EBUSY, errno.EACCES, errno.EPERM}

        moved = subprocess.run(
            [
                *privilege_prefix,
                "/usr/bin/mount",
                "--move",
                str(publication_path),
                str(recovery_mountpoint),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert moved.returncode == 0, moved.stderr

        cleanup = subprocess.run(
            [
                *privilege_prefix,
                sys.executable,
                str(RELEASE_PUBLICATION_SNAPSHOT),
                "unseal",
                "--directory",
                str(publication_path),
                "--backing-directory",
                str(backing_directory),
                "--artifact-name",
                artifact_name,
            ],
            cwd=workspace,
            check=False,
            capture_output=True,
            text=True,
        )
        assert cleanup.returncode == 0, cleanup.stderr
        assert not publication_path.exists()
        assert not backing_directory.exists()
        assert not list(Path("/var/lib").glob(f"{backing_directory.name}*"))
        mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
        assert str(trusted_root) not in mountinfo
    finally:
        emergency_cleanup()


def test_publication_descriptor_cli_hashes_and_verifies_the_exact_file_set(tmp_path: Path) -> None:
    payloads = tmp_path / "payloads"
    payloads.mkdir()
    wheel = payloads / "tacit_ai-1.2.3-py3-none-any.whl"
    sdist = payloads / "tacit_ai-1.2.3.tar.gz"
    wheel.write_bytes(b"wheel bytes")
    sdist.write_bytes(b"sdist bytes")
    output = tmp_path / "github-output"

    described = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "describe",
        "--directory",
        str(payloads),
        "--selector",
        "wheel=tacit_ai-*.whl",
        "--selector",
        "sdist=tacit_ai-*.tar.gz",
        "--github-output",
        str(output),
    )
    assert described.returncode == 0, described.stderr
    carried = dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())
    assert carried == {
        "wheel_name": wheel.name,
        "wheel_digest": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "sdist_name": sdist.name,
        "sdist_digest": hashlib.sha256(sdist.read_bytes()).hexdigest(),
    }

    verified = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "verify",
        "--directory",
        str(payloads),
        "--artifact",
        f"wheel={carried['wheel_name']}={carried['wheel_digest']}",
        "--artifact",
        f"sdist={carried['sdist_name']}={carried['sdist_digest']}",
    )
    assert verified.returncode == 0, verified.stderr

    (payloads / "unexpected.txt").write_text("not publishable", encoding="utf-8")
    rejected = _run_release_script(
        RELEASE_PUBLICATION_SNAPSHOT,
        "verify",
        "--directory",
        str(payloads),
        "--artifact",
        f"wheel={carried['wheel_name']}={carried['wheel_digest']}",
        "--artifact",
        f"sdist={carried['sdist_name']}={carried['sdist_digest']}",
    )
    assert rejected.returncode != 0
    assert "payload set differs" in rejected.stderr


def test_github_release_preflight_rejects_mismatched_assets(tmp_path: Path) -> None:
    asset_names = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
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
                            "url": f"https://127.0.0.1:{port}/assets/1",
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
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(server, certificate=certificate, private_key=private_key)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), "--allow-absent-release"],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"https://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "test-token",
                "SSL_CERT_FILE": str(certificate),
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
    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
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
                                "url": f"https://127.0.0.1:{port}/assets/unexpected",
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
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(server, certificate=certificate, private_key=private_key)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), "--allow-absent-release"],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"https://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "test-token",
                "SSL_CERT_FILE": str(certificate),
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
    script = RELEASE_GITHUB_ASSET_VERIFIER.read_text(encoding="utf-8")
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
                                "url": f"https://127.0.0.1:{port}/assets/oversized",
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
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(server, certificate=certificate, private_key=private_key)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), "--allow-absent-release"],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"https://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "test-token",
                "SSL_CERT_FILE": str(certificate),
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


def test_github_release_preflight_strips_authorization_on_asset_redirects(tmp_path: Path) -> None:
    required = (
        "tacit-linux-x86_64.tar.gz",
        "tacit-linux-x86_64.tar.gz.sha256",
    )
    payloads = {name: f"local:{name}".encode() for name in required}
    for name, payload in payloads.items():
        (tmp_path / name).write_bytes(payload)

    redirected_authorization: list[str | None] = []

    class RedirectTarget(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            redirected_authorization.append(self.headers.get("Authorization"))
            payload = payloads[required[0]]
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    target = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectTarget)

    class GitHubAPI(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            api_port = int(getattr(self.server, "server_port"))
            if self.path == "/repos/aditki/tacit/releases/tags/v1.2.3":
                body = json.dumps(
                    {
                        "assets": [
                            {
                                "name": name,
                                "size": len(payloads[name]),
                                "url": f"https://127.0.0.1:{api_port}/assets/{name}",
                            }
                            for name in required
                        ]
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            prefix = "/assets/"
            if self.path.startswith(prefix):
                name = self.path.removeprefix(prefix)
                if name == required[0]:
                    self.send_response(302)
                    self.send_header(
                        "Location",
                        f"https://127.0.0.1:{target.server_port}/redirected-asset",
                    )
                    self.end_headers()
                    return
                payload = payloads[name]
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self.send_error(404)

        def log_message(self, format: str, *args: object) -> None:
            return

    api = http.server.ThreadingHTTPServer(("127.0.0.1", 0), GitHubAPI)
    certificate, private_key = _test_https_material(tmp_path)
    _serve_over_https(target, certificate=certificate, private_key=private_key)
    _serve_over_https(api, certificate=certificate, private_key=private_key)
    threads = [
        threading.Thread(target=target.serve_forever, daemon=True),
        threading.Thread(target=api.serve_forever, daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        result = subprocess.run(
            [sys.executable, str(RELEASE_GITHUB_ASSET_VERIFIER), "--allow-absent-release"],
            cwd=tmp_path,
            env=os.environ
            | {
                "GITHUB_API_URL": f"https://127.0.0.1:{api.server_port}",
                "GITHUB_REPOSITORY": "aditki/tacit",
                "GITHUB_REF_NAME": "v1.2.3",
                "GITHUB_TOKEN": "redirect-sentinel-token",
                "SSL_CERT_FILE": str(certificate),
            },
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        api.shutdown()
        target.shutdown()
        for thread in threads:
            thread.join(timeout=5)
        api.server_close()
        target.server_close()

    assert result.returncode == 0, result.stderr
    assert redirected_authorization == [None]


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
        "FROM python:3.12.14-alpine3.24@sha256:"
        "1887c114801a8c82a4ec01daa52cfe7fc3f63573640e2247320289807ac1c3bb AS runtime"
    ) in dockerfile
    assert "apk upgrade" not in dockerfile

    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["build-system"]["requires"] == ["hatchling==1.32.0"]


def test_release_image_context_excludes_downloaded_and_repository_release_tooling() -> None:
    ignored = {
        line.strip()
        for line in (REPOSITORY_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert ".github" in ignored
    assert "buildx-v*.linux-*" in ignored

    workflow = _release_workflow()
    for job_name in ("build-release-images", "ghcr-preflight", "publish-ghcr"):
        installer = _step(workflow["jobs"][job_name], "Install checksum-pinned Buildx")["run"]
        assert 'download="$RUNNER_TEMP/$asset"' in installer
        assert 'test ! -e "$GITHUB_WORKSPACE/$asset"' in installer


def test_runtime_image_keeps_dependency_cache_out_of_layers_and_uses_allowed_probe_host() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    cache_mount = "--mount=type=cache,target=/root/.cache/uv,sharing=locked"

    assert dockerfile.count(cache_mount) == 2
    assert dockerfile.count("uv sync --locked --link-mode=copy") == 2
    assert "tacit_ai-*.dist-info/uv_cache.json" in dockerfile
    assert "sed -i '/uv_cache\\.json,/d'" in dockerfile
    assert 'CMD ["python", "/app/tacit/api/routes/system.py"]' in dockerfile

    readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
    assert "API_ALLOWED_HOSTS=tacit.example.com" in readme


def test_container_api_deployments_use_the_shared_authenticated_bind_boundary() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert 'CMD ["tacit", "serve", "--host", "0.0.0.0", "--port", "8000", "--no-slack"]' in dockerfile
    assert "API_AUTH_KEY=" not in dockerfile

    expected_services = {
        "docker-compose.yml": ("tacit",),
        "docker-compose.dev.yml": ("tacit", "gamma-tacit"),
    }
    for filename, service_names in expected_services.items():
        compose = yaml.load(
            (REPOSITORY_ROOT / filename).read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        for service_name in service_names:
            service = compose["services"][service_name]
            environment = service["environment"]
            assert environment["API_AUTH_ENABLED"] == "true"
            assert environment["API_AUTH_KEY"].startswith("${API_AUTH_KEY:?")
            assert environment["API_ALLOWED_HOSTS"] == "${API_ALLOWED_HOSTS:-localhost,127.0.0.1}"
            assert all(port.startswith("127.0.0.1:") for port in service["ports"])

    env_example = (REPOSITORY_ROOT / ".env.example").read_text(encoding="utf-8")
    yaml_example = (REPOSITORY_ROOT / "tacit.yaml.example").read_text(encoding="utf-8")
    assert "API_ALLOWED_HOSTS=" in env_example
    assert "allowed_hosts:" in yaml_example
