#!/usr/bin/env python3
from __future__ import annotations

import argparse
import errno
import json
import math
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import IO, NoReturn
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from packaging.version import InvalidVersion, Version
from release_payload_snapshot import ReleasePayloadError, verified_payload_snapshot

MAX_DIAGNOSTIC_BYTES = 16 * 1024
MAX_BINARY_BYTES = 512 * 1024 * 1024
CAPTURE_READ_BYTES = 64 * 1024
PROCESS_REAP_TIMEOUT = 2.0
API_START_ATTEMPTS = 2
HEALTH_PROBE_TIMEOUT = 0.25
TRUNCATION_MARKER = b"<truncated>\n"
SCHEMA_SMOKE_ENV = "TACIT_RELEASE_SCHEMA_SMOKE"
METADATA_SMOKE_ENV = "TACIT_RELEASE_METADATA_SMOKE"
EXPECTED_SCHEMA_SMOKE_RESULT = {
    "schema_title": "InvestigationContract",
    "schema_type": "object",
    "schema_version": "1.0",
}


class BinarySmokeError(RuntimeError):
    pass


def _positive_finite_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return value


def _binary_path(raw: str) -> Path:
    path = Path(raw).expanduser().absolute()
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise argparse.ArgumentTypeError("binary does not exist") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise argparse.ArgumentTypeError("binary must be a regular file")
    if metadata.st_size <= 0 or metadata.st_size > MAX_BINARY_BYTES:
        raise argparse.ArgumentTypeError("binary is empty or exceeds the release size limit")
    if not os.access(path, os.X_OK):
        raise argparse.ArgumentTypeError("binary is not executable")
    return path


def _isolated_environment(root: Path) -> tuple[dict[str, str], dict[str, Path]]:
    home = root / "home"
    working = root / "work"
    temporary = root / "tmp"
    xdg_config = root / "xdg-config"
    xdg_cache = root / "xdg-cache"
    xdg_data = root / "xdg-data"
    for directory in (home, working, temporary, xdg_config, xdg_cache, xdg_data):
        directory.mkdir(mode=0o700)

    config = root / "tacit.yaml"
    config.write_text("{}\n", encoding="utf-8")
    config.chmod(0o600)

    stores = {
        "history": root / "history.db",
        "feedback": root / "feedback.db",
        "signals": root / "signals.db",
    }
    environment = {
        "HOME": str(home),
        "TACIT_CONFIG": str(config),
        "HISTORY_DB_PATH": str(stores["history"]),
        "FEEDBACK_DB_PATH": str(stores["feedback"]),
        "SIGNALS_DB_PATH": str(stores["signals"]),
        "TMPDIR": str(temporary),
        "TMP": str(temporary),
        "TEMP": str(temporary),
        "XDG_CONFIG_HOME": str(xdg_config),
        "XDG_CACHE_HOME": str(xdg_cache),
        "XDG_DATA_HOME": str(xdg_data),
        "PYTHONHASHSEED": "0",
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUNBUFFERED": "1",
        "TZ": "UTC",
        "API_AUTH_ENABLED": "false",
        "CONTEXT_PROVIDER": "none",
        "GRAFANA_ENABLED": "false",
        "SIGNALFX_ENABLED": "false",
        "KNOWLEDGE_TENANT_ID": "default",
        "AWS_EC2_METADATA_DISABLED": "true",
        "LOG_LEVEL": "warning",
    }
    return environment, {**stores, "working": working}


def _bounded_text(path: Path) -> str:
    try:
        with path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - MAX_DIAGNOSTIC_BYTES))
            payload = stream.read(MAX_DIAGNOSTIC_BYTES)
    except OSError:
        return "<server log unavailable>"
    prefix = "<truncated>\n" if size > MAX_DIAGNOSTIC_BYTES else ""
    return prefix + payload.decode("utf-8", errors="replace")


class _BoundedCapture:
    def __init__(self, stream: IO[bytes], path: Path) -> None:
        self._stream = stream
        self._path = path
        self._buffer = bytearray()
        self._total_bytes = 0
        self._failure: BaseException | None = None
        self._thread = threading.Thread(
            target=self._drain,
            name="tacit-release-smoke-output",
            daemon=False,
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_bytes(b"")
        self._thread.start()

    def _render(self) -> bytes:
        if self._total_bytes <= MAX_DIAGNOSTIC_BYTES:
            return bytes(self._buffer)
        visible_bytes = MAX_DIAGNOSTIC_BYTES - len(TRUNCATION_MARKER)
        return TRUNCATION_MARKER + bytes(self._buffer[-visible_bytes:])

    def _drain(self) -> None:
        try:
            with self._stream, self._path.open("r+b", buffering=0) as log:
                while True:
                    chunk = os.read(self._stream.fileno(), CAPTURE_READ_BYTES)
                    if not chunk:
                        return
                    self._total_bytes += len(chunk)
                    self._buffer.extend(chunk)
                    if len(self._buffer) > MAX_DIAGNOSTIC_BYTES:
                        del self._buffer[:-MAX_DIAGNOSTIC_BYTES]
                    log.seek(0)
                    log.write(self._render())
                    log.truncate()
        except BaseException as exc:
            self._failure = exc

    def finish(self, timeout: float) -> str:
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            try:
                self._stream.close()
            except OSError:
                pass
            self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            raise BinarySmokeError("release-smoke output drainer did not terminate")
        if self._failure is not None:
            raise BinarySmokeError("release-smoke output capture failed") from self._failure
        return self._render().decode("utf-8", errors="replace")


class _OwnedProcess:
    def __init__(self, process: subprocess.Popen[bytes], capture: _BoundedCapture) -> None:
        self.process = process
        self.capture = capture
        self.process_group = process.pid


def _start_owned_process(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    log_path: Path,
) -> _OwnedProcess:
    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    if process.stdout is None:
        _kill_process_group(process.pid)
        process.wait(timeout=PROCESS_REAP_TIMEOUT)
        raise BinarySmokeError("release-smoke command did not expose its output stream")
    try:
        capture = _BoundedCapture(process.stdout, log_path)
    except BaseException:
        _kill_process_group(process.pid)
        process.wait(timeout=PROCESS_REAP_TIMEOUT)
        raise
    return _OwnedProcess(process, capture)


def _wait_for_process_group_exit(process_group: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_group_exists(process_group):
            return
        time.sleep(0.01)
    raise BinarySmokeError("release-smoke process group retained a descendant")


def _force_terminal_cleanup(owned: _OwnedProcess, timeout: float) -> str:
    process = owned.process
    _kill_process_group(owned.process_group)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise BinarySmokeError("release-smoke process could not be reaped after forced cleanup") from exc
    _wait_for_process_group_exit(owned.process_group, timeout)
    return owned.capture.finish(timeout)


def _run_owned_command(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    timeout: float,
    log_path: Path,
    timeout_message: str,
) -> tuple[int, str]:
    owned = _start_owned_process(command, cwd=cwd, environment=environment, log_path=log_path)
    timed_out = False
    try:
        try:
            return_code = owned.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            return_code = -signal.SIGKILL
    except BaseException as primary:
        try:
            _force_terminal_cleanup(owned, PROCESS_REAP_TIMEOUT)
        except BinarySmokeError as cleanup_error:
            raise cleanup_error from primary
        raise

    diagnostic = _force_terminal_cleanup(owned, PROCESS_REAP_TIMEOUT)
    if timed_out:
        raise BinarySmokeError(timeout_message)
    return return_code, diagnostic


def _run_version_smoke(
    binary: Path,
    environment: dict[str, str],
    paths: dict[str, Path],
    timeout: float,
    expected_version: str,
) -> None:
    return_code, diagnostic = _run_owned_command(
        [str(binary), "--version"],
        cwd=paths["working"],
        environment=environment,
        timeout=timeout,
        log_path=paths["working"] / "version.log",
        timeout_message="binary version command exceeded its timeout",
    )
    if return_code != 0:
        raise BinarySmokeError(f"binary version command failed with exit {return_code}: {diagnostic}")
    result = diagnostic.strip()
    prefix = "tacit, version "
    if not result.startswith(prefix):
        raise BinarySmokeError(f"unexpected binary version output: {result!r}")
    actual_version = result.removeprefix(prefix)
    try:
        versions_match = Version(actual_version) == Version(expected_version)
    except InvalidVersion as exc:
        raise BinarySmokeError(
            f"binary or expected release version is invalid: actual={actual_version!r}, expected={expected_version!r}"
        ) from exc
    if not versions_match:
        raise BinarySmokeError(f"binary version {result.removeprefix(prefix)!r} does not match {expected_version!r}")


def _run_store_smoke(binary: Path, environment: dict[str, str], paths: dict[str, Path], timeout: float) -> None:
    return_code, diagnostic = _run_owned_command(
        [str(binary), "history", "stats"],
        cwd=paths["working"],
        environment=environment,
        timeout=timeout,
        log_path=paths["working"] / "history.log",
        timeout_message="offline history command exceeded its timeout",
    )
    if return_code != 0:
        raise BinarySmokeError(f"offline history command failed with exit {return_code}: {diagnostic}")

    history = paths["history"]
    try:
        metadata = history.lstat()
    except OSError as exc:
        raise BinarySmokeError("offline history command did not create its configured database") from exc
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0:
        raise BinarySmokeError("offline history command did not create a non-empty regular database")


def _run_schema_smoke(
    binary: Path,
    environment: dict[str, str],
    paths: dict[str, Path],
    timeout: float,
) -> None:
    schema_environment = dict(environment)
    schema_environment[SCHEMA_SMOKE_ENV] = "1"
    return_code, diagnostic = _run_owned_command(
        [str(binary)],
        cwd=paths["working"],
        environment=schema_environment,
        timeout=timeout,
        log_path=paths["working"] / "schema.log",
        timeout_message="packaged schema command exceeded its timeout",
    )
    if return_code != 0:
        raise BinarySmokeError(f"packaged schema command failed with exit {return_code}: {diagnostic}")
    try:
        result = json.loads(diagnostic)
    except json.JSONDecodeError as exc:
        raise BinarySmokeError("packaged schema command returned invalid JSON") from exc
    if result != EXPECTED_SCHEMA_SMOKE_RESULT:
        raise BinarySmokeError(f"unexpected packaged schema result: {result!r}")


def _run_metadata_smoke(
    binary: Path,
    environment: dict[str, str],
    paths: dict[str, Path],
    timeout: float,
    expected_version: str,
) -> None:
    metadata_environment = dict(environment)
    metadata_environment[METADATA_SMOKE_ENV] = "1"
    return_code, diagnostic = _run_owned_command(
        [str(binary)],
        cwd=paths["working"],
        environment=metadata_environment,
        timeout=timeout,
        log_path=paths["working"] / "metadata.log",
        timeout_message="packaged metadata command exceeded its timeout",
    )
    if return_code != 0:
        raise BinarySmokeError(f"packaged metadata command failed with exit {return_code}: {diagnostic}")
    try:
        result = json.loads(diagnostic)
    except json.JSONDecodeError as exc:
        raise BinarySmokeError("packaged metadata command returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise BinarySmokeError(f"unexpected packaged metadata result: {result!r}")
    actual_version = result.get("version")
    try:
        versions_match = isinstance(actual_version, str) and Version(actual_version) == Version(expected_version)
    except InvalidVersion as exc:
        raise BinarySmokeError(f"unexpected packaged metadata result: {result!r}") from exc
    if (
        result.get("distribution") != "tacit-ai"
        or result.get("console_script") != "tacit.cli:main"
        or not versions_match
    ):
        raise BinarySmokeError(f"unexpected packaged metadata result: {result!r}")


def _run_benchmark_resource_smoke(
    binary: Path,
    environment: dict[str, str],
    paths: dict[str, Path],
    timeout: float,
) -> None:
    def final_json_object(diagnostic: str, label: str) -> dict[str, object]:
        decoder = json.JSONDecoder()
        line_starts = [0]
        line_starts.extend(index + 1 for index, character in enumerate(diagnostic) if character == "\n")
        for start in reversed(line_starts):
            if start >= len(diagnostic) or diagnostic[start] != "{":
                continue
            try:
                parsed, end = decoder.raw_decode(diagnostic, start)
            except json.JSONDecodeError:
                continue
            if diagnostic[end:].strip() or not isinstance(parsed, dict):
                continue
            return parsed
        raise BinarySmokeError(f"packaged {label} benchmark returned invalid JSON")

    checks = (
        (
            "grounding",
            "benchmark-grounding",
            lambda result: (
                result.get("benchmark") == "grounding-v1"
                and isinstance(result.get("cases"), int)
                and not isinstance(result.get("cases"), bool)
                and result["cases"] > 0
                and result.get("passed") is True
            ),
        ),
        (
            "operational learning",
            "operational-learning-benchmark",
            lambda result: (
                result.get("benchmark_name") == "operational_learning"
                and result.get("benchmark_version") == "v1"
                and isinstance(result.get("case_count"), int)
                and not isinstance(result.get("case_count"), bool)
                and result["case_count"] > 0
                and result.get("passed") is True
            ),
        ),
    )
    for label, command, validate in checks:
        return_code, diagnostic = _run_owned_command(
            [str(binary), command],
            cwd=paths["working"],
            environment=environment,
            timeout=timeout,
            log_path=paths["working"] / f"{command}.log",
            timeout_message=f"packaged {label} benchmark exceeded its timeout",
        )
        if return_code != 0:
            raise BinarySmokeError(f"packaged {label} benchmark failed with exit {return_code}: {diagnostic}")
        result = final_json_object(diagnostic, label)
        if not validate(result):
            raise BinarySmokeError(f"unexpected {label} benchmark result: {result!r}")


def _available_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _loopback_port_is_occupied(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind(("127.0.0.1", port))
        except OSError as exc:
            return exc.errno == errno.EADDRINUSE
    return False


def _confirmed_address_in_use_collision(*, child_exited: bool, port: int, diagnostic: str) -> bool:
    if not child_exited:
        return False
    normalized = diagnostic.casefold()
    if "address already in use" not in normalized and "eaddrinuse" not in normalized:
        return False
    return _loopback_port_is_occupied(port)


def _is_ready_health_payload(payload: object) -> bool:
    if not isinstance(payload, dict) or payload.get("status") != "ok":
        return False
    admission = payload.get("request_body_admission")
    if not isinstance(admission, dict):
        return False
    zero_fields = ("active_requests", "reserved_bytes", "active_tenant_partitions")
    positive_fields = ("max_concurrent", "max_buffered_bytes")
    return (
        all(admission.get(field) == 0 for field in zero_fields)
        and all(isinstance(admission.get(field), int) and admission[field] > 0 for field in positive_fields)
        and isinstance(admission.get("rejections"), dict)
    )


def _wait_for_health(server: subprocess.Popen[bytes], port: int, timeout: float, log_path: Path) -> None:
    deadline = time.monotonic() + timeout
    opener = build_opener(ProxyHandler({}))
    request = Request(
        f"http://127.0.0.1:{port}/healthz",
        headers={"Accept": "application/json", "Connection": "close"},
        method="GET",
    )
    last_error = "server did not become ready"

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            exit_code = server.wait(timeout=min(0.05, remaining))
        except subprocess.TimeoutExpired:
            exit_code = None
        if exit_code is not None:
            raise BinarySmokeError(
                f"API binary exited before readiness with exit {exit_code}: {_bounded_text(log_path)}"
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            with opener.open(request, timeout=min(HEALTH_PROBE_TIMEOUT, remaining)) as response:
                encoded = response.read(4097)
                if len(encoded) > 4096:
                    raise BinarySmokeError("health response exceeded 4096 bytes")
                payload = json.loads(encoded)
                if response.status == 200 and _is_ready_health_payload(payload):
                    exit_code = server.poll()
                    if exit_code is not None:
                        raise BinarySmokeError(
                            f"API binary exited during readiness with exit {exit_code}: {_bounded_text(log_path)}"
                        )
                    return
                last_error = f"unexpected health response: status={response.status}, payload={payload!r}"
        except (HTTPError, URLError, TimeoutError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = type(exc).__name__
        exit_code = server.poll()
        if exit_code is not None:
            raise BinarySmokeError(
                f"API binary exited before readiness with exit {exit_code}: {_bounded_text(log_path)}"
            )
        time.sleep(min(0.1, max(deadline - time.monotonic(), 0)))

    raise BinarySmokeError(f"API binary did not become ready ({last_error}): {_bounded_text(log_path)}")


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _kill_process_group(process_group: int) -> None:
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        return


def _terminate_server(owned: _OwnedProcess, timeout: float, log_path: Path) -> None:
    server = owned.process
    try:
        os.killpg(owned.process_group, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        exit_code = server.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _force_terminal_cleanup(owned, PROCESS_REAP_TIMEOUT)
        raise BinarySmokeError(f"API binary did not terminate within its timeout: {_bounded_text(log_path)}") from exc
    if _process_group_exists(owned.process_group):
        _kill_process_group(owned.process_group)
    _wait_for_process_group_exit(owned.process_group, PROCESS_REAP_TIMEOUT)
    owned.capture.finish(PROCESS_REAP_TIMEOUT)
    if exit_code not in {0, -signal.SIGTERM}:
        raise BinarySmokeError(f"API binary terminated with exit {exit_code}: {_bounded_text(log_path)}")


def _run_api_smoke(
    binary: Path,
    environment: dict[str, str],
    paths: dict[str, Path],
    startup_timeout: float,
    shutdown_timeout: float,
    *,
    port_selector: Callable[[], int] = _available_loopback_port,
) -> None:
    startup_deadline = time.monotonic() + startup_timeout
    log_path = paths["working"] / "api.log"
    for attempt in range(API_START_ATTEMPTS):
        remaining = startup_deadline - time.monotonic()
        if remaining <= 0:
            raise BinarySmokeError(
                f"API binary did not become ready before its startup deadline: {_bounded_text(log_path)}"
            )
        port = port_selector()
        owned = _start_owned_process(
            [str(binary), "serve", "--host", "127.0.0.1", "--port", str(port), "--no-slack"],
            cwd=paths["working"],
            environment=environment,
            log_path=log_path,
        )
        try:
            _wait_for_health(owned.process, port, remaining, log_path)
        except BaseException as primary:
            child_exited = owned.process.poll() is not None
            try:
                diagnostic = _force_terminal_cleanup(owned, PROCESS_REAP_TIMEOUT)
            except BinarySmokeError as cleanup_error:
                raise cleanup_error from primary
            can_retry = (
                isinstance(primary, BinarySmokeError)
                and attempt + 1 < API_START_ATTEMPTS
                and time.monotonic() < startup_deadline
                and _confirmed_address_in_use_collision(
                    child_exited=child_exited,
                    port=port,
                    diagnostic=diagnostic,
                )
            )
            if can_retry:
                continue
            raise

        try:
            _terminate_server(owned, shutdown_timeout, log_path)
        except BaseException as primary:
            try:
                _force_terminal_cleanup(owned, PROCESS_REAP_TIMEOUT)
            except BinarySmokeError as cleanup_error:
                raise cleanup_error from primary
            raise
        return


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Exercise one packaged Tacit binary without external services")
    parser.add_argument("--binary", required=True, type=_binary_path)
    parser.add_argument("--command-timeout", type=_positive_finite_float, default=30.0)
    parser.add_argument("--startup-timeout", type=_positive_finite_float, default=30.0)
    parser.add_argument("--shutdown-timeout", type=_positive_finite_float, default=15.0)
    parser.add_argument("--expected-version")
    parser.add_argument("--version-only", action="store_true")
    return parser


def _fail(message: str) -> NoReturn:
    print(f"release binary smoke failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def _interrupt_smoke(_signum: int, _frame: object) -> NoReturn:
    raise KeyboardInterrupt


def main() -> None:
    args = _parser().parse_args()
    if os.name != "posix":
        _fail("release binaries are supported only on POSIX platforms")

    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _interrupt_smoke)
    try:
        with verified_payload_snapshot(
            args.binary,
            maximum=MAX_BINARY_BYTES,
            expected_digest=None,
            executable=True,
            prefix="tacit-release-binary-payload-",
        ) as binary_snapshot:
            with tempfile.TemporaryDirectory(prefix="tacit-release-binary-smoke-") as temporary_directory:
                root = Path(temporary_directory).resolve()
                environment, paths = _isolated_environment(root)
                binary = binary_snapshot.path
                expected_version = getattr(args, "expected_version", None)
                version_only = bool(getattr(args, "version_only", False))
                if version_only and not expected_version:
                    raise BinarySmokeError("--version-only requires --expected-version")
                if expected_version:
                    _run_version_smoke(binary, environment, paths, args.command_timeout, expected_version)
                    _run_metadata_smoke(binary, environment, paths, args.command_timeout, expected_version)
                if version_only:
                    print(f"tacit, version {expected_version}")
                    return
                _run_schema_smoke(binary, environment, paths, args.command_timeout)
                _run_benchmark_resource_smoke(binary, environment, paths, args.command_timeout)
                _run_store_smoke(binary, environment, paths, args.command_timeout)
                _run_api_smoke(
                    binary,
                    environment,
                    paths,
                    args.startup_timeout,
                    args.shutdown_timeout,
                )
    except KeyboardInterrupt:
        _fail("release binary smoke was interrupted")
    except (BinarySmokeError, ReleasePayloadError, OSError) as exc:
        _fail(str(exc))
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)


if __name__ == "__main__":
    main()
