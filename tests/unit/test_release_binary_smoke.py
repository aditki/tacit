from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import textwrap
import time
import tracemalloc
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from tacit.config import DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES

REPOSITORY_ROOT = Path(__file__).parents[2]
RELEASE_SCRIPTS = REPOSITORY_ROOT / ".github" / "scripts"
SMOKE_SCRIPT = REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_binary.py"
IMAGE_SMOKE_SCRIPT = REPOSITORY_ROOT / ".github" / "scripts" / "smoke_release_image.py"
PAYLOAD_SNAPSHOT_SCRIPT = RELEASE_SCRIPTS / "release_payload_snapshot.py"


def _load_release_script(module_name: str, path: Path) -> ModuleType:
    inserted = str(RELEASE_SCRIPTS) not in sys.path
    if inserted:
        sys.path.insert(0, str(RELEASE_SCRIPTS))
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if inserted:
            sys.path.remove(str(RELEASE_SCRIPTS))


@pytest.fixture(scope="module")
def smoke() -> ModuleType:
    return _load_release_script("release_binary_smoke_under_test", SMOKE_SCRIPT)


@pytest.fixture(scope="module")
def image_smoke() -> ModuleType:
    return _load_release_script("release_image_smoke_under_test", IMAGE_SMOKE_SCRIPT)


@pytest.fixture(scope="module")
def payload_snapshot() -> ModuleType:
    return _load_release_script("release_payload_snapshot_under_test", PAYLOAD_SNAPSHOT_SCRIPT)


def test_verified_payload_snapshot_binds_consumed_bytes_and_uses_private_storage(
    tmp_path: Path,
    payload_snapshot: ModuleType,
) -> None:
    source = tmp_path / "release.tar"
    authoritative = b"authoritative-release-payload"
    source.write_bytes(authoritative)
    expected = hashlib.sha256(authoritative).hexdigest()

    with payload_snapshot.verified_payload_snapshot(
        source,
        maximum=1024,
        expected_digest=expected,
        executable=False,
        prefix="tacit-release-test-",
    ) as snapshot:
        private_path = snapshot.path
        private_root = private_path.parent
        assert private_path != source
        assert stat.S_IMODE(private_root.stat().st_mode) == 0o700
        assert stat.S_IMODE(private_path.stat().st_mode) == 0o600

        original = tmp_path / "release.after-copy.original"
        source.rename(original)
        source.write_bytes(b"attacker-controlled-payload")
        source.unlink()
        original.rename(source)

        assert private_path.read_bytes() == authoritative
        assert snapshot.digest == expected

    assert not private_path.exists()
    assert not private_root.exists()


def test_verified_payload_snapshot_rejects_swap_and_restore_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_snapshot: ModuleType,
) -> None:
    source = tmp_path / "release.tar"
    authoritative = b"authoritative-release-payload"
    source.write_bytes(authoritative)
    expected = hashlib.sha256(authoritative).hexdigest()
    real_stream_copy = payload_snapshot._stream_copy

    def swap_and_restore_after_open(source_descriptor: int, destination_descriptor: int, size: int) -> str:
        original = tmp_path / "release.original"
        source.rename(original)
        source.write_bytes(b"attacker-controlled-payload")
        try:
            return real_stream_copy(source_descriptor, destination_descriptor, size)
        finally:
            source.unlink()
            original.rename(source)

    monkeypatch.setattr(payload_snapshot, "_stream_copy", swap_and_restore_after_open)

    with pytest.raises(payload_snapshot.ReleasePayloadError, match="changed while copying"):
        with payload_snapshot.verified_payload_snapshot(
            source,
            maximum=1024,
            expected_digest=expected,
            executable=False,
            prefix="tacit-release-test-",
        ):
            pytest.fail("a source-path swap must fail before a snapshot is exposed")


def _write_executable(path: Path, source: str) -> Path:
    path.write_text(
        f"#!{sys.executable}\n" + textwrap.dedent(source).lstrip(),
        encoding="utf-8",
    )
    path.chmod(0o700)
    return path


def _paths(root: Path) -> dict[str, Path]:
    working = root / "work"
    working.mkdir()
    return {
        "working": working,
        "history": root / "history.db",
        "feedback": root / "feedback.db",
        "signals": root / "signals.db",
    }


def _wait_until_group_exits(smoke: ModuleType, process_group: int, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not smoke._process_group_exists(process_group):
            return
        time.sleep(0.01)
    pytest.fail(f"process group {process_group} survived release-smoke cleanup")


def _process_exists(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_until_process_exits(process_id: int, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_exists(process_id):
            return True
        time.sleep(0.01)
    return not _process_exists(process_id)


def _record_processes(monkeypatch: pytest.MonkeyPatch, smoke: ModuleType) -> list[subprocess.Popen[bytes]]:
    started: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def recording_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        process = real_popen(*args, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr(smoke.subprocess, "Popen", recording_popen)
    return started


def _occupied_loopback_port() -> tuple[socket.socket, int]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    return listener, int(listener.getsockname()[1])


def _api_test_binary(path: Path) -> Path:
    aggregate_budget = DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES
    source = """
        import argparse
        import http.server

        parser = argparse.ArgumentParser()
        parser.add_argument("command")
        parser.add_argument("--host")
        parser.add_argument("--port", type=int, required=True)
        parser.add_argument("--no-slack", action="store_true")
        args = parser.parse_args()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                payload = (
                    b'{"status":"ok","request_body_admission":{'
                    b'"active_requests":0,"reserved_bytes":0,'
                    b'"max_concurrent":16,"max_buffered_bytes":__AGGREGATE_BUDGET__,'
                    b'"active_tenant_partitions":0,'
                    b'"max_concurrent_per_tenant":16,'
                    b'"max_buffered_bytes_per_tenant":__AGGREGATE_BUDGET__,'
                    b'"rejections":{}}}'
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                return

        http.server.ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
        """.replace("__AGGREGATE_BUDGET__", str(aggregate_budget))
    return _write_executable(
        path,
        source,
    )


def test_api_smoke_fixture_uses_runtime_request_body_budget(tmp_path: Path) -> None:
    binary = _api_test_binary(tmp_path / "tacit")
    source = binary.read_text(encoding="utf-8")

    assert str(DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES) in source
    assert "67108864" not in source


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_store_smoke_enforces_its_command_timeout(tmp_path: Path, smoke: ModuleType) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import time

        time.sleep(30)
        """,
    )
    paths = _paths(tmp_path)

    started = time.monotonic()
    with pytest.raises(smoke.BinarySmokeError, match="offline history command exceeded its timeout"):
        smoke._run_store_smoke(binary, os.environ.copy(), paths, timeout=0.1)

    assert time.monotonic() - started < 2.0


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_schema_smoke_loads_the_packaged_schema_with_a_bounded_offline_command(
    tmp_path: Path,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import json
        import os

        if os.environ.get("TACIT_RELEASE_SCHEMA_SMOKE") != "1":
            raise SystemExit(9)
        print(json.dumps({
            "schema_title": "InvestigationContract",
            "schema_type": "object",
            "schema_version": "1.0",
        }, sort_keys=True, separators=(",", ":")))
        """,
    )
    paths = _paths(tmp_path)

    smoke._run_schema_smoke(binary, os.environ.copy(), paths, timeout=1.0)


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_schema_smoke_rejects_an_invalid_frozen_loader_result(
    tmp_path: Path,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        f"""
        import json

        print(json.dumps({json.dumps({"schema_title": "Wrong", "schema_type": "object", "schema_version": "1.0"})}))
        """,
    )
    paths = _paths(tmp_path)

    with pytest.raises(smoke.BinarySmokeError, match="unexpected packaged schema result"):
        smoke._run_schema_smoke(binary, os.environ.copy(), paths, timeout=1.0)


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_schema_smoke_enforces_its_command_timeout(tmp_path: Path, smoke: ModuleType) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import time

        time.sleep(30)
        """,
    )
    paths = _paths(tmp_path)

    started = time.monotonic()
    with pytest.raises(smoke.BinarySmokeError, match="packaged schema command exceeded its timeout"):
        smoke._run_schema_smoke(binary, os.environ.copy(), paths, timeout=0.1)

    assert time.monotonic() - started < 2.0


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_metadata_smoke_loads_tacit_version_and_console_entry_point(
    tmp_path: Path,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import json
        import os

        if os.environ.get("TACIT_RELEASE_METADATA_SMOKE") != "1":
            raise SystemExit(9)
        print(json.dumps({
            "console_script": "tacit.cli:main",
            "distribution": "tacit-ai",
            "version": "0.1.1rc5",
        }, sort_keys=True, separators=(",", ":")))
        """,
    )
    paths = _paths(tmp_path)

    smoke._run_metadata_smoke(
        binary,
        os.environ.copy(),
        paths,
        timeout=1.0,
        expected_version="0.1.1-rc.5",
    )


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_metadata_smoke_rejects_missing_tacit_console_entry_point(
    tmp_path: Path,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import json

        print(json.dumps({
            "console_script": "",
            "distribution": "tacit-ai",
            "version": "0.1.1rc5",
        }, sort_keys=True, separators=(",", ":")))
        """,
    )
    paths = _paths(tmp_path)

    with pytest.raises(smoke.BinarySmokeError, match="unexpected packaged metadata result"):
        smoke._run_metadata_smoke(
            binary,
            os.environ.copy(),
            paths,
            timeout=1.0,
            expected_version="0.1.1-rc.5",
        )


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_benchmark_resource_smoke_runs_both_packaged_corpora(
    tmp_path: Path,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import json
        import sys

        if sys.argv[1:] == ["benchmark-grounding"]:
            print("grounding loader diagnostic")
            print(json.dumps({"benchmark": "grounding-v1", "cases": 10, "passed": True}))
        elif sys.argv[1:] == ["operational-learning-benchmark"]:
            print("operational loader diagnostic")
            print(json.dumps({
                "benchmark_name": "operational_learning",
                "benchmark_version": "v1",
                "case_count": 10,
                "passed": True,
            }))
        else:
            raise SystemExit(9)
        """,
    )
    paths = _paths(tmp_path)

    smoke._run_benchmark_resource_smoke(binary, os.environ.copy(), paths, timeout=1.0)


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_benchmark_resource_smoke_rejects_failed_or_empty_corpus(
    tmp_path: Path,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import json
        import sys

        if sys.argv[1:] == ["benchmark-grounding"]:
            print(json.dumps({"benchmark": "grounding-v1", "cases": 0, "passed": True}))
        else:
            print(json.dumps({
                "benchmark_name": "operational_learning",
                "benchmark_version": "v1",
                "case_count": 10,
                "passed": True,
            }))
        """,
    )
    paths = _paths(tmp_path)

    with pytest.raises(smoke.BinarySmokeError, match="unexpected grounding benchmark result"):
        smoke._run_benchmark_resource_smoke(binary, os.environ.copy(), paths, timeout=1.0)


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_store_smoke_timeout_reaps_descendants(tmp_path: Path, smoke: ModuleType) -> None:
    child_pid_path = tmp_path / "child.pid"
    binary = _write_executable(
        tmp_path / "tacit",
        """
        import os
        import subprocess
        import sys
        import time

        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with open(os.environ["CHILD_PID_PATH"], "w", encoding="utf-8") as stream:
            stream.write(str(child.pid))
            stream.flush()
        time.sleep(30)
        """,
    )
    paths = _paths(tmp_path)
    environment = {**os.environ, "CHILD_PID_PATH": str(child_pid_path)}

    with pytest.raises(smoke.BinarySmokeError, match="offline history command exceeded its timeout"):
        smoke._run_store_smoke(binary, environment, paths, timeout=0.5)

    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    try:
        assert _wait_until_process_exits(child_pid, timeout=0.5)
    finally:
        if _process_exists(child_pid):
            os.kill(child_pid, signal.SIGKILL)
            _wait_until_process_exits(child_pid)


@pytest.mark.skipif(os.name != "posix", reason="release binaries are POSIX-only")
def test_store_smoke_does_not_buffer_unbounded_child_output(tmp_path: Path, smoke: ModuleType) -> None:
    output_bytes = smoke.MAX_DIAGNOSTIC_BYTES * 512
    binary = _write_executable(
        tmp_path / "tacit",
        f"""
        import os
        import sys

        chunk = b"x" * {output_bytes}
        os.write(sys.stderr.fileno(), chunk)
        raise SystemExit(7)
        """,
    )
    paths = _paths(tmp_path)

    tracemalloc.start()
    try:
        with pytest.raises(smoke.BinarySmokeError, match="offline history command failed"):
            smoke._run_store_smoke(binary, os.environ.copy(), paths, timeout=5.0)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak_bytes <= smoke.MAX_DIAGNOSTIC_BYTES * 128


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_api_smoke_success_reaps_the_owned_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smoke: ModuleType,
) -> None:
    binary = _api_test_binary(tmp_path / "tacit")
    paths = _paths(tmp_path)
    started = _record_processes(monkeypatch, smoke)

    smoke._run_api_smoke(
        binary,
        os.environ.copy(),
        paths,
        startup_timeout=2.0,
        shutdown_timeout=1.0,
    )

    assert len(started) == 1
    server = started[0]
    assert server.poll() is not None
    _wait_until_group_exits(smoke, server.pid)


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_api_smoke_retries_one_confirmed_port_collision_and_reaps_each_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smoke: ModuleType,
) -> None:
    binary = _api_test_binary(tmp_path / "tacit")
    paths = _paths(tmp_path)
    occupied, occupied_port = _occupied_loopback_port()
    second_port = smoke._available_loopback_port()
    selected_ports = iter((occupied_port, second_port))
    selection_count = 0

    def select_port() -> int:
        nonlocal selection_count
        selection_count += 1
        return next(selected_ports)

    started = _record_processes(monkeypatch, smoke)

    try:
        smoke._run_api_smoke(
            binary,
            os.environ.copy(),
            paths,
            startup_timeout=2.0,
            shutdown_timeout=1.0,
            port_selector=select_port,
        )
    finally:
        occupied.close()

    assert selection_count == 2
    assert len(started) == 2
    assert started[0].returncode not in {None, 0}
    assert started[1].returncode is not None
    for process in started:
        _wait_until_group_exits(smoke, process.pid)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reusable:
        reusable.bind(("127.0.0.1", occupied_port))


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_api_smoke_bounds_repeated_confirmed_port_collisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smoke: ModuleType,
) -> None:
    binary = _api_test_binary(tmp_path / "tacit")
    paths = _paths(tmp_path)
    occupied, occupied_port = _occupied_loopback_port()
    selection_count = 0

    def select_occupied_port() -> int:
        nonlocal selection_count
        selection_count += 1
        return occupied_port

    started_processes = _record_processes(monkeypatch, smoke)

    began = time.monotonic()
    try:
        with pytest.raises(smoke.BinarySmokeError, match="API binary"):
            smoke._run_api_smoke(
                binary,
                os.environ.copy(),
                paths,
                startup_timeout=1.0,
                shutdown_timeout=0.2,
                port_selector=select_occupied_port,
            )
    finally:
        occupied.close()
    elapsed = time.monotonic() - began

    assert selection_count == 2
    assert len(started_processes) == 2
    assert elapsed < 2.0
    for process in started_processes:
        assert process.returncode not in {None, 0}
        _wait_until_group_exits(smoke, process.pid)


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_api_smoke_failure_reaps_the_owned_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(tmp_path / "tacit", "raise SystemExit(9)\n")
    paths = _paths(tmp_path)
    started = _record_processes(monkeypatch, smoke)

    with pytest.raises(smoke.BinarySmokeError, match="exited before readiness with exit 9"):
        smoke._run_api_smoke(
            binary,
            os.environ.copy(),
            paths,
            startup_timeout=1.0,
            shutdown_timeout=0.2,
        )

    assert len(started) == 1
    server = started[0]
    assert server.poll() == 9
    _wait_until_group_exits(smoke, server.pid)


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_api_smoke_hang_kills_descendants_and_bounds_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smoke: ModuleType,
) -> None:
    output_bytes = smoke.MAX_DIAGNOSTIC_BYTES * 8
    binary = _write_executable(
        tmp_path / "tacit",
        f"""
        import os
        import signal
        import subprocess
        import sys
        import time

        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
            ]
        )
        os.write(sys.stdout.fileno(), b"x" * {output_bytes})
        time.sleep(30)
        """,
    )
    paths = _paths(tmp_path)
    started = _record_processes(monkeypatch, smoke)

    began = time.monotonic()
    with pytest.raises(smoke.BinarySmokeError, match="did not become ready"):
        smoke._run_api_smoke(
            binary,
            os.environ.copy(),
            paths,
            startup_timeout=0.2,
            shutdown_timeout=0.2,
        )
    elapsed = time.monotonic() - began

    assert len(started) == 1
    server = started[0]
    assert server.poll() is not None
    _wait_until_group_exits(smoke, server.pid)
    assert elapsed < 2.0
    assert (paths["working"] / "api.log").stat().st_size <= smoke.MAX_DIAGNOSTIC_BYTES


def test_main_consumes_private_snapshot_after_source_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    smoke: ModuleType,
) -> None:
    binary = _write_executable(tmp_path / "tacit", "raise SystemExit(0)\n")
    authoritative = binary.read_bytes()
    replacement = binary.read_bytes().replace(b"SystemExit(0)", b"SystemExit(1)")
    consumed_paths: list[Path] = []

    class Parser:
        @staticmethod
        def parse_args() -> SimpleNamespace:
            return SimpleNamespace(
                binary=binary,
                command_timeout=1.0,
                startup_timeout=1.0,
                shutdown_timeout=1.0,
            )

    def swap_source(consumed: Path, *_args: Any, **_kwargs: Any) -> None:
        consumed_paths.append(consumed)
        assert consumed != binary
        assert consumed.read_bytes() == authoritative
        original = tmp_path / "tacit.original"
        binary.rename(original)
        binary.write_bytes(replacement)
        binary.chmod(0o700)
        assert consumed.read_bytes() == authoritative
        binary.unlink()
        original.rename(binary)

    def assert_same_snapshot(consumed: Path, *_args: Any, **_kwargs: Any) -> None:
        consumed_paths.append(consumed)
        assert consumed == consumed_paths[0]
        assert consumed.read_bytes() == authoritative

    monkeypatch.setattr(smoke, "_parser", Parser)
    monkeypatch.setattr(smoke, "_run_schema_smoke", swap_source)
    monkeypatch.setattr(smoke, "_run_benchmark_resource_smoke", assert_same_snapshot)
    monkeypatch.setattr(smoke, "_run_store_smoke", assert_same_snapshot)
    monkeypatch.setattr(smoke, "_run_api_smoke", assert_same_snapshot)

    smoke.main()

    assert len(consumed_paths) == 4
    assert not consumed_paths[0].exists()
    assert binary.read_bytes() == authoritative


def test_image_smoke_executes_hardened_runtime_and_cleans_owned_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    image_smoke: ModuleType,
) -> None:
    archive = tmp_path / "release-amd64.tar"
    archive.write_bytes(b"authoritative-image-archive")
    checksum = tmp_path / "release-amd64.tar.sha256"
    checksum.write_text(
        f"{image_smoke._hash_file(archive)}  {archive.name}\n",
        encoding="utf-8",
    )
    commands: list[list[str]] = []
    containers: set[str] = set()
    volumes: set[str] = set()

    def fake_result(
        command: list[str],
        *,
        timeout: float,
        label: str,
    ) -> Any:
        assert timeout > 0
        assert label
        commands.append(command)
        if command[:3] == ["docker", "image", "inspect"]:
            return image_smoke.CommandResult(
                0,
                json.dumps(
                    {
                        "Architecture": "amd64",
                        "Os": "linux",
                        "Config": {
                            "User": "tacit",
                            "Healthcheck": {"Test": ["CMD", "python", "/app/tacit/api/routes/system.py"]},
                        },
                    }
                ),
            )
        if command[:3] == ["docker", "volume", "create"]:
            volumes.add(command[-1])
            return image_smoke.CommandResult(0, command[-1])
        if command[:3] == ["docker", "volume", "inspect"]:
            if command[-1] in volumes:
                return image_smoke.CommandResult(0, "")
            return image_smoke.CommandResult(1, f"Error: No such volume: {command[-1]}")
        if command[:3] == ["docker", "container", "inspect"]:
            if command[-1] in containers:
                return image_smoke.CommandResult(0, "")
            return image_smoke.CommandResult(1, f"Error: No such container: {command[-1]}")
        if command[:2] == ["docker", "load"]:
            consumed = Path(command[-1])
            assert consumed != archive
            assert consumed.read_bytes() == b"authoritative-image-archive"
            original = tmp_path / "release-amd64.original"
            archive.rename(original)
            archive.write_bytes(b"attacker-image-archive")
            assert consumed.read_bytes() == b"authoritative-image-archive"
            archive.unlink()
            original.rename(archive)
            return image_smoke.CommandResult(0, "Loaded image: tacit:test-amd64")
        if command[:2] == ["docker", "run"] and "--detach" in command:
            containers.add(command[command.index("--name") + 1])
            return image_smoke.CommandResult(0, "container-id")
        if command[:2] == ["docker", "run"] and command[command.index("--entrypoint") + 1] == "tacit":
            containers.add(command[command.index("--name") + 1])
            containers.discard(command[command.index("--name") + 1])
            return image_smoke.CommandResult(0, "tacit, version 0.1.1rc5")
        if command[:2] == ["docker", "run"] and command[command.index("--entrypoint") + 1] == "python":
            containers.add(command[command.index("--name") + 1])
            containers.discard(command[command.index("--name") + 1])
            return image_smoke.CommandResult(
                0,
                json.dumps(
                    {
                        "euid": 999,
                        "root_read_only": True,
                        "data_writable": True,
                        "tmpfs": True,
                        "release_tooling_absent": True,
                        "required_resources": sorted(image_smoke.REQUIRED_RESOURCES),
                    }
                ),
            )
        if command[:2] == ["docker", "inspect"]:
            return image_smoke.CommandResult(
                0,
                json.dumps(
                    {
                        "Config": {"User": "tacit"},
                        "HostConfig": {
                            "ReadonlyRootfs": True,
                            "Tmpfs": {"/tmp": "rw,noexec,nosuid,size=67108864"},
                        },
                        "State": {"Running": True, "ExitCode": 0, "Health": {"Status": "healthy"}},
                    }
                ),
            )
        if command[:2] == ["docker", "exec"]:
            return image_smoke.CommandResult(0, json.dumps({"status": "ok"}))
        if command[:3] == ["docker", "logs", "--tail"]:
            return image_smoke.CommandResult(0, "")
        if command[:3] == ["docker", "rm", "--force"]:
            containers.discard(command[-1])
            return image_smoke.CommandResult(0, "")
        if command[:3] == ["docker", "volume", "rm"]:
            volumes.discard(command[-1])
            return image_smoke.CommandResult(0, "")
        raise AssertionError(f"unexpected command: {command!r}")

    monkeypatch.setattr(image_smoke, "_run_command_result", fake_result)
    monkeypatch.setattr(image_smoke.time, "sleep", lambda _seconds: None)

    image_smoke._smoke(
        SimpleNamespace(
            archive=archive,
            checksum=checksum,
            image="tacit:test-amd64",
            platform="linux/amd64",
            expected_version="0.1.1-rc.5",
            max_archive_bytes=1024,
            command_timeout=5.0,
            startup_timeout=5.0,
            cleanup_timeout=2.0,
        )
    )

    run_commands = [command for command in commands if command[:2] == ["docker", "run"]]
    assert len(run_commands) == 3
    assert all("--platform" in command and "linux/amd64" in command for command in run_commands)
    assert all("--read-only" in command for command in run_commands)
    assert all("--name" in command for command in run_commands)
    assert len({command[command.index("--name") + 1] for command in run_commands}) == 3
    assert all("--tmpfs" in command for command in run_commands)
    assert all("--mount" in command and "target=/app/data" in " ".join(command) for command in run_commands)
    assert any("--detach" in command for command in run_commands)
    assert ["docker", "rm", "--force"] in [command[:3] for command in commands]
    assert ["docker", "volume", "rm"] in [command[:3] for command in commands]
    assert not containers
    assert not volumes
    assert image_smoke._hash_file(archive) in checksum.read_text(encoding="utf-8")


def test_image_smoke_probes_exact_absence_of_release_build_tooling(image_smoke: ModuleType) -> None:
    probe = image_smoke._runtime_probe_script()
    assert "release_tooling_absent" in probe
    assert "Path('/app/.github')" in probe
    assert "glob('buildx-v*.linux-*')" in probe


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("container", "Error: No such container: release-check"),
        ("container", "Error response from daemon: No such container: release-check"),
        ("container", "[]\nError: No such object: release-check"),
        ("volume", "Error: No such volume: release-check"),
        ("volume", "Error response from daemon: get release-check: no such volume"),
    ],
)
def test_image_smoke_accepts_only_canonical_missing_resource_responses(
    monkeypatch: pytest.MonkeyPatch,
    image_smoke: ModuleType,
    kind: str,
    message: str,
) -> None:
    monkeypatch.setattr(
        image_smoke,
        "_run_command_result",
        lambda *_args, **_kwargs: image_smoke.CommandResult(1, message),
    )

    assert image_smoke._resource_exists(kind, "release-check", 1.0) is False


@pytest.mark.parametrize(
    ("return_code", "message"),
    [
        (1, ""),
        (1, "permission denied while trying to connect to the Docker daemon socket"),
        (1, "Cannot connect to the Docker daemon at unix:///var/run/docker.sock"),
        (1, "Error response from daemon: context deadline exceeded"),
        (1, "Error: No such container: a-different-resource"),
        (1, "malformed response"),
        (2, "Error: No such container: release-check"),
        (125, "docker transport failed"),
    ],
)
def test_image_smoke_fails_closed_for_nonabsence_inspect_failures(
    monkeypatch: pytest.MonkeyPatch,
    image_smoke: ModuleType,
    return_code: int,
    message: str,
) -> None:
    monkeypatch.setattr(
        image_smoke,
        "_run_command_result",
        lambda *_args, **_kwargs: image_smoke.CommandResult(return_code, message),
    )

    with pytest.raises(image_smoke.ImageSmokeError, match="ownership inspection failed"):
        image_smoke._resource_exists("container", "release-check", 1.0)


def test_image_smoke_timeout_cleans_every_attempted_resource_and_proves_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    image_smoke: ModuleType,
) -> None:
    archive = tmp_path / "release-amd64.tar"
    archive.write_bytes(b"authoritative-image-archive")
    checksum = tmp_path / "release-amd64.tar.sha256"
    checksum.write_text(f"{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n", encoding="ascii")
    containers: set[str] = set()
    volumes: set[str] = set()
    attempted_names: list[str] = []

    def fake_result(command: list[str], *, timeout: float, label: str) -> Any:
        assert timeout > 0 and label
        if command[:2] == ["docker", "load"]:
            return image_smoke.CommandResult(0, "loaded")
        if command[:3] == ["docker", "image", "inspect"]:
            return image_smoke.CommandResult(
                0,
                json.dumps(
                    {
                        "Architecture": "amd64",
                        "Os": "linux",
                        "Config": {"User": "tacit", "Healthcheck": {"Test": ["CMD", "true"]}},
                    }
                ),
            )
        if command[:3] == ["docker", "container", "inspect"]:
            if command[-1] in containers:
                return image_smoke.CommandResult(0, "")
            return image_smoke.CommandResult(1, f"Error: No such container: {command[-1]}")
        if command[:3] == ["docker", "volume", "inspect"]:
            if command[-1] in volumes:
                return image_smoke.CommandResult(0, "")
            return image_smoke.CommandResult(1, f"Error: No such volume: {command[-1]}")
        if command[:3] == ["docker", "volume", "create"]:
            volumes.add(command[-1])
            return image_smoke.CommandResult(0, command[-1])
        if command[:2] == ["docker", "run"]:
            name = command[command.index("--name") + 1]
            attempted_names.append(name)
            containers.add(name)
            raise image_smoke.ImageSmokeError("release-image version smoke exceeded its timeout")
        if command[:3] == ["docker", "rm", "--force"]:
            containers.discard(command[-1])
            return image_smoke.CommandResult(0, "")
        if command[:3] == ["docker", "volume", "rm"]:
            volumes.discard(command[-1])
            return image_smoke.CommandResult(0, "")
        if command[:3] == ["docker", "logs", "--tail"]:
            return image_smoke.CommandResult(0, "")
        raise AssertionError(f"unexpected command: {command!r}")

    monkeypatch.setattr(image_smoke, "_run_command_result", fake_result)
    arguments = SimpleNamespace(
        archive=archive,
        checksum=checksum,
        image="tacit:test-amd64",
        platform="linux/amd64",
        expected_version="0.1.1-rc.5",
        max_archive_bytes=1024,
        command_timeout=0.1,
        startup_timeout=0.1,
        cleanup_timeout=0.1,
    )

    for _ in range(2):
        with pytest.raises(image_smoke.ImageSmokeError, match="exceeded its timeout"):
            image_smoke._smoke(arguments)
        assert not containers
        assert not volumes

    assert len(attempted_names) == 2
    assert attempted_names[0] == attempted_names[1]


def test_image_smoke_fails_when_attempted_resource_cannot_be_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    image_smoke: ModuleType,
) -> None:
    archive = tmp_path / "release-amd64.tar"
    archive.write_bytes(b"authoritative-image-archive")
    checksum = tmp_path / "release-amd64.tar.sha256"
    checksum.write_text(
        f"{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n",
        encoding="ascii",
    )
    containers: set[str] = set()
    volumes: set[str] = set()

    def fake_result(command: list[str], *, timeout: float, label: str) -> Any:
        assert timeout > 0 and label
        if command[:2] == ["docker", "load"]:
            return image_smoke.CommandResult(0, "loaded")
        if command[:3] == ["docker", "image", "inspect"]:
            return image_smoke.CommandResult(
                0,
                json.dumps(
                    {
                        "Architecture": "amd64",
                        "Os": "linux",
                        "Config": {"User": "tacit", "Healthcheck": {"Test": ["CMD", "true"]}},
                    }
                ),
            )
        if command[:3] == ["docker", "container", "inspect"]:
            if command[-1] in containers:
                return image_smoke.CommandResult(0, "")
            return image_smoke.CommandResult(1, f"Error: No such container: {command[-1]}")
        if command[:3] == ["docker", "volume", "inspect"]:
            if command[-1] in volumes:
                return image_smoke.CommandResult(0, "")
            return image_smoke.CommandResult(1, f"Error: No such volume: {command[-1]}")
        if command[:3] == ["docker", "volume", "create"]:
            volumes.add(command[-1])
            return image_smoke.CommandResult(0, command[-1])
        if command[:2] == ["docker", "run"]:
            name = command[command.index("--name") + 1]
            containers.add(name)
            raise image_smoke.ImageSmokeError("ambiguous docker return")
        if command[:3] == ["docker", "rm", "--force"]:
            return image_smoke.CommandResult(2, "daemon refused cleanup")
        if command[:3] == ["docker", "volume", "rm"]:
            volumes.discard(command[-1])
            return image_smoke.CommandResult(0, "")
        if command[:3] == ["docker", "logs", "--tail"]:
            return image_smoke.CommandResult(0, "")
        raise AssertionError(f"unexpected command: {command!r}")

    monkeypatch.setattr(image_smoke, "_run_command_result", fake_result)

    with pytest.raises(image_smoke.ImageSmokeError, match="cleanup failed") as failure:
        image_smoke._smoke(
            SimpleNamespace(
                archive=archive,
                checksum=checksum,
                image="tacit:test-amd64",
                platform="linux/amd64",
                expected_version="0.1.1-rc.5",
                max_archive_bytes=1024,
                command_timeout=0.1,
                startup_timeout=0.1,
                cleanup_timeout=0.1,
            )
        )

    assert "daemon refused cleanup" in str(failure.value)
    assert containers


@pytest.mark.skipif(os.name != "posix", reason="process-group ownership requires POSIX")
def test_image_smoke_command_timeout_reaps_and_bounds_output(image_smoke: ModuleType) -> None:
    began = time.monotonic()
    with pytest.raises(image_smoke.ImageSmokeError, match="bounded probe exceeded its timeout") as failure:
        image_smoke._run_command(
            [
                sys.executable,
                "-c",
                (
                    "import os,signal,sys,time; "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                    f"os.write(sys.stdout.fileno(), b'x' * {image_smoke.MAX_DIAGNOSTIC_BYTES * 32}); "
                    "time.sleep(30)"
                ),
            ],
            timeout=0.2,
            label="bounded probe",
        )

    assert time.monotonic() - began < 2.0
    assert len(str(failure.value)) <= image_smoke.MAX_DIAGNOSTIC_BYTES + 256
