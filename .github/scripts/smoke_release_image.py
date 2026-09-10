#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple, NoReturn

from release_payload_snapshot import ReleasePayloadError, verified_payload_snapshot

READ_CHUNK_BYTES = 1024 * 1024
CAPTURE_READ_BYTES = 64 * 1024
MAX_DIAGNOSTIC_BYTES = 16 * 1024
PROCESS_REAP_TIMEOUT = 2.0
HEALTH_POLL_INTERVAL = 0.2
TMPFS_SPEC = "/tmp:rw,noexec,nosuid,size=67108864"
REQUIRED_RESOURCES = (
    "data/archetypes.yaml",
    "data/grounding_benchmark_v1.json",
    "data/operational_learning_v1.json",
    "data/signals.yaml",
    "schemas/investigation/v1.0.schema.json",
    "static/index.html",
)


class ImageSmokeError(RuntimeError):
    pass


class CommandResult(NamedTuple):
    return_code: int
    output: str


class _BoundedCapture:
    def __init__(self, stream: object) -> None:
        self._stream = stream
        self._buffer = bytearray()
        self._total = 0
        self._failure: BaseException | None = None
        self._thread = threading.Thread(target=self._drain, name="tacit-image-smoke-output", daemon=False)
        self._thread.start()

    def _drain(self) -> None:
        try:
            fileno = self._stream.fileno()  # type: ignore[attr-defined]
            while chunk := os.read(fileno, CAPTURE_READ_BYTES):
                self._total += len(chunk)
                self._buffer.extend(chunk)
                if len(self._buffer) > MAX_DIAGNOSTIC_BYTES:
                    del self._buffer[:-MAX_DIAGNOSTIC_BYTES]
        except BaseException as exc:
            self._failure = exc

    def finish(self) -> str:
        self._thread.join(timeout=PROCESS_REAP_TIMEOUT)
        if self._thread.is_alive():
            try:
                self._stream.close()  # type: ignore[attr-defined]
            except OSError:
                pass
            self._thread.join(timeout=PROCESS_REAP_TIMEOUT)
        if self._thread.is_alive():
            raise ImageSmokeError("release-image command output drainer did not terminate")
        if self._failure is not None:
            raise ImageSmokeError("release-image command output capture failed") from self._failure
        prefix = "<truncated>\n" if self._total > MAX_DIAGNOSTIC_BYTES else ""
        return prefix + bytes(self._buffer).decode("utf-8", errors="replace")


def _positive_finite_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return value


def _positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def _regular_file(raw: str) -> Path:
    path = Path(raw).expanduser().absolute()
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise argparse.ArgumentTypeError("file does not exist") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise argparse.ArgumentTypeError("file must be regular")
    return path


def _image_reference(raw: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}", raw):
        raise argparse.ArgumentTypeError("must be a bounded Docker image reference")
    return raw


def _kill_process_group(process_group: int) -> None:
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        return


def _run_command_result(
    command: Sequence[str],
    *,
    timeout: float,
    label: str,
) -> CommandResult:
    process = subprocess.Popen(
        list(command),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    if process.stdout is None:
        _kill_process_group(process.pid)
        process.wait(timeout=PROCESS_REAP_TIMEOUT)
        raise ImageSmokeError(f"{label} did not expose command output")
    capture = _BoundedCapture(process.stdout)
    timed_out = False
    try:
        try:
            return_code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            _kill_process_group(process.pid)
            try:
                process.wait(timeout=PROCESS_REAP_TIMEOUT)
            except subprocess.TimeoutExpired as exc:
                raise ImageSmokeError(f"{label} could not be reaped after timeout") from exc
    except BaseException:
        _kill_process_group(process.pid)
        try:
            process.wait(timeout=PROCESS_REAP_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass
        raise
    finally:
        output = capture.finish()

    if timed_out:
        raise ImageSmokeError(f"{label} exceeded its timeout: {output}")
    return CommandResult(return_code, output.strip())


def _run_command(
    command: Sequence[str],
    *,
    timeout: float,
    label: str,
    check: bool = True,
) -> str:
    result = _run_command_result(command, timeout=timeout, label=label)
    if check and result.return_code != 0:
        raise ImageSmokeError(f"{label} failed with exit {result.return_code}: {result.output}")
    return result.output


def _open_no_follow(path: Path) -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return os.open(path, flags)


def _hash_file(path: Path) -> str:
    descriptor = _open_no_follow(path)
    digest = hashlib.sha256()
    try:
        while chunk := os.read(descriptor, READ_CHUNK_BYTES):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _expected_checksum(checksum: Path, archive: Path) -> str:
    try:
        metadata = checksum.lstat()
    except OSError as exc:
        raise ImageSmokeError("release-image checksum is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0 or metadata.st_size > 1024:
        raise ImageSmokeError("release-image checksum must be a small regular file")
    descriptor = _open_no_follow(checksum)
    try:
        payload = os.read(descriptor, 1025)
        if len(payload) > 1024 or os.read(descriptor, 1):
            raise ImageSmokeError("release-image checksum exceeds the size limit")
    finally:
        os.close(descriptor)
    try:
        rendered = payload.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ImageSmokeError("release-image checksum is not ASCII") from exc
    match = re.fullmatch(r"([0-9a-f]{64})  ([^\r\n]+)\n", rendered)
    if match is None or match.group(2) != archive.name:
        raise ImageSmokeError("release-image checksum has an invalid format")
    return match.group(1)


def _canonical_version(raw: str) -> str:
    match = re.fullmatch(
        r"(?P<base>(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))"
        r"(?:[-.]?(?P<label>a|alpha|b|beta|c|rc|pre|preview|dev)[.-]?(?P<number>\d+))?",
        raw.strip(),
        re.IGNORECASE,
    )
    if match is None:
        raise ImageSmokeError(f"unsupported release version {raw!r}")
    label = (match.group("label") or "").casefold()
    aliases = {"alpha": "a", "beta": "b", "c": "rc", "pre": "rc", "preview": "rc"}
    canonical_label = aliases.get(label, label)
    return f"{match.group('base')}{canonical_label}{match.group('number') or ''}"


def _parse_object(raw: str, label: str) -> dict[str, object]:
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ImageSmokeError(f"{label} returned invalid JSON") from exc
    if isinstance(loaded, list) and len(loaded) == 1:
        loaded = loaded[0]
    if not isinstance(loaded, dict):
        raise ImageSmokeError(f"{label} did not return one object")
    return loaded


def _runtime_options(platform: str, volume_name: str) -> list[str]:
    return [
        "--platform",
        platform,
        "--read-only",
        "--tmpfs",
        TMPFS_SPEC,
        "--mount",
        f"type=volume,source={volume_name},target=/app/data",
        "--security-opt",
        "no-new-privileges",
        "--cap-drop",
        "ALL",
    ]


def _runtime_probe_script() -> str:
    required = json.dumps(REQUIRED_RESOURCES)
    return f"""
import errno
import json
import os
from importlib.resources import files
from pathlib import Path

def write_probe(path):
    path.write_text('release-smoke', encoding='utf-8')
    path.unlink()

root_read_only = False
try:
    write_probe(Path('/app/.tacit-release-root-probe'))
except OSError as exc:
    root_read_only = exc.errno == errno.EROFS

data_writable = True
try:
    write_probe(Path('/app/data/.tacit-release-data-probe'))
except OSError:
    data_writable = False

tmp_writable = True
try:
    write_probe(Path('/tmp/.tacit-release-tmp-probe'))
except OSError:
    tmp_writable = False

tmpfs = False
for line in Path('/proc/self/mountinfo').read_text(encoding='utf-8').splitlines():
    fields = line.split()
    if len(fields) < 7 or fields[4] != '/tmp' or '-' not in fields:
        continue
    separator = fields.index('-')
    tmpfs = separator + 1 < len(fields) and fields[separator + 1] == 'tmpfs'
    break

required = {required}
root = files('tacit')
found = []
for relative in required:
    resource = root.joinpath(*relative.split('/'))
    if not resource.is_file():
        raise SystemExit('missing packaged resource')
    with resource.open('rb') as stream:
        if not stream.read(1):
            raise SystemExit('empty packaged resource')
    found.append(relative)

release_tooling_absent = (
    not Path('/app/.github').exists()
    and not any(Path('/app').glob('buildx-v*.linux-*'))
)

print(json.dumps({{
    'euid': os.geteuid(),
    'root_read_only': root_read_only,
    'data_writable': data_writable,
    'tmpfs': tmp_writable and tmpfs,
    'release_tooling_absent': release_tooling_absent,
    'required_resources': sorted(found),
}}, sort_keys=True, separators=(',', ':')))
""".strip()


def _health_probe_script() -> str:
    return """
import json
from urllib.request import ProxyHandler, Request, build_opener

opener = build_opener(ProxyHandler({}))
request = Request('http://127.0.0.1:8000/healthz', headers={'Host': 'localhost'})
with opener.open(request, timeout=3) as response:
    payload = response.read(8193)
if response.status != 200 or len(payload) > 8192:
    raise SystemExit('invalid health response')
loaded = json.loads(payload)
if not isinstance(loaded, dict) or loaded.get('status') != 'ok':
    raise SystemExit('runtime is not healthy')
print(json.dumps({'status': 'ok'}, sort_keys=True, separators=(',', ':')))
""".strip()


def _validate_image(image: str, platform: str, command_timeout: float) -> None:
    inspected = _parse_object(
        _run_command(
            ["docker", "image", "inspect", image],
            timeout=command_timeout,
            label="release-image inspect",
        ),
        "release-image inspect",
    )
    expected_os, expected_architecture = platform.split("/", maxsplit=1)
    if inspected.get("Os") != expected_os or inspected.get("Architecture") != expected_architecture:
        raise ImageSmokeError("loaded release image has the wrong platform")
    config = inspected.get("Config")
    if not isinstance(config, dict):
        raise ImageSmokeError("loaded release image has no configuration")
    user = str(config.get("User") or "").strip().casefold()
    if user in {"", "0", "root"}:
        raise ImageSmokeError("loaded release image defaults to a root user")
    healthcheck = config.get("Healthcheck")
    if not isinstance(healthcheck, dict) or not healthcheck.get("Test"):
        raise ImageSmokeError("loaded release image has no healthcheck")


def _wait_for_healthy(
    container_name: str,
    *,
    startup_timeout: float,
    command_timeout: float,
) -> dict[str, object]:
    deadline = time.monotonic() + startup_timeout
    last_status = "created"
    while time.monotonic() < deadline:
        inspected = _parse_object(
            _run_command(
                ["docker", "inspect", container_name],
                timeout=min(command_timeout, max(deadline - time.monotonic(), 0.1)),
                label="release-image container inspect",
            ),
            "release-image container inspect",
        )
        state = inspected.get("State")
        if not isinstance(state, dict):
            raise ImageSmokeError("release-image container has no runtime state")
        if state.get("Running") is not True:
            raise ImageSmokeError(f"release-image server exited before health: {state.get('ExitCode')!r}")
        health = state.get("Health")
        if not isinstance(health, dict):
            raise ImageSmokeError("release-image container has no runtime health state")
        last_status = str(health.get("Status") or "unknown")
        if last_status == "healthy":
            return inspected
        if last_status == "unhealthy":
            raise ImageSmokeError("release-image server became unhealthy")
        time.sleep(min(HEALTH_POLL_INTERVAL, max(deadline - time.monotonic(), 0)))
    raise ImageSmokeError(f"release-image server did not become healthy ({last_status})")


def _validate_runtime_inspect(inspected: dict[str, object]) -> None:
    config = inspected.get("Config")
    host_config = inspected.get("HostConfig")
    if not isinstance(config, dict) or not isinstance(host_config, dict):
        raise ImageSmokeError("release-image runtime configuration is incomplete")
    user = str(config.get("User") or "").strip().casefold()
    if user in {"", "0", "root"}:
        raise ImageSmokeError("release-image server is running as root")
    if host_config.get("ReadonlyRootfs") is not True:
        raise ImageSmokeError("release-image server root filesystem is writable")
    tmpfs = host_config.get("Tmpfs")
    if not isinstance(tmpfs, dict) or "/tmp" not in tmpfs:
        raise ImageSmokeError("release-image server has no /tmp tmpfs")


def _resource_exists(kind: str, name: str, timeout: float) -> bool:
    result = _run_command_result(
        ["docker", kind, "inspect", name],
        timeout=timeout,
        label=f"release-image {kind} ownership inspection",
    )
    if result.return_code == 0:
        return True
    diagnostic_lines = result.output.strip().splitlines()
    if diagnostic_lines[:1] == ["[]"]:
        diagnostic_lines = diagnostic_lines[1:]
    expected_absence = {
        "container": {
            f"Error: No such container: {name}",
            f"Error response from daemon: No such container: {name}",
            f"Error: No such object: {name}",
        },
        "volume": {
            f"Error: No such volume: {name}",
            f"Error response from daemon: get {name}: no such volume",
        },
    }
    if (
        result.return_code == 1
        and len(diagnostic_lines) == 1
        and diagnostic_lines[0] in expected_absence.get(kind, set())
    ):
        return False
    raise ImageSmokeError(
        f"release-image {kind} ownership inspection failed with exit " f"{result.return_code}: {result.output}"
    )


def _require_resources_absent(container_names: Sequence[str], volume_name: str, timeout: float) -> None:
    collisions = [name for name in container_names if _resource_exists("container", name, timeout)]
    if _resource_exists("volume", volume_name, timeout):
        collisions.append(volume_name)
    if collisions:
        raise ImageSmokeError("release-image smoke resource names already exist: " + ", ".join(collisions))


def _cleanup_resource(kind: str, name: str, timeout: float) -> None:
    if _resource_exists(kind, name, timeout):
        command = ["docker", "rm", "--force", name]
        if kind == "volume":
            command = ["docker", "volume", "rm", "--force", name]
        result = _run_command_result(
            command,
            timeout=timeout,
            label=f"release-image {kind} cleanup",
        )
        if result.return_code != 0:
            raise ImageSmokeError(
                f"release-image {kind} cleanup failed with exit {result.return_code}: {result.output}"
            )
    if _resource_exists(kind, name, timeout):
        raise ImageSmokeError(f"release-image {kind} cleanup did not remove {name}")


def _smoke(args: argparse.Namespace) -> None:
    expected_digest = _expected_checksum(args.checksum, args.archive)
    with verified_payload_snapshot(
        args.archive,
        maximum=args.max_archive_bytes,
        expected_digest=expected_digest,
        executable=False,
        prefix="tacit-release-image-payload-",
    ) as archive_snapshot:
        architecture = args.platform.split("/", maxsplit=1)[1]
        resource_suffix = f"{architecture}-{archive_snapshot.digest[:12]}-{os.getpid()}"
        resource_prefix = f"tacit-release-smoke-{resource_suffix}"
        version_container = f"{resource_prefix}-version"
        runtime_container = f"{resource_prefix}-runtime"
        server_container = f"{resource_prefix}-server"
        container_names = (version_container, runtime_container, server_container)
        volume_name = f"{resource_prefix}-data"
        failure: BaseException | None = None
        cleanup_failures: list[str] = []

        _require_resources_absent(container_names, volume_name, args.cleanup_timeout)
        try:
            _run_command(
                ["docker", "load", "--input", str(archive_snapshot.path)],
                timeout=args.command_timeout,
                label="release-image load",
            )
            _validate_image(args.image, args.platform, args.command_timeout)
            _run_command(
                [
                    "docker",
                    "volume",
                    "create",
                    "--label",
                    "tacit.release-smoke=true",
                    volume_name,
                ],
                timeout=args.command_timeout,
                label="release-image data volume creation",
            )
            runtime_options = _runtime_options(args.platform, volume_name)

            version = _run_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    version_container,
                    "--label",
                    "tacit.release-smoke=true",
                    *runtime_options,
                    "--entrypoint",
                    "tacit",
                    args.image,
                    "--version",
                ],
                timeout=args.command_timeout,
                label="release-image version smoke",
            )
            prefix = "tacit, version "
            if not version.startswith(prefix) or _canonical_version(version.removeprefix(prefix)) != _canonical_version(
                args.expected_version
            ):
                raise ImageSmokeError(f"release-image version mismatch: {version!r}")

            runtime_probe = _parse_object(
                _run_command(
                    [
                        "docker",
                        "run",
                        "--rm",
                        "--name",
                        runtime_container,
                        "--label",
                        "tacit.release-smoke=true",
                        *runtime_options,
                        "--entrypoint",
                        "python",
                        args.image,
                        "-I",
                        "-c",
                        _runtime_probe_script(),
                    ],
                    timeout=args.command_timeout,
                    label="release-image hardened runtime smoke",
                ),
                "release-image hardened runtime smoke",
            )
            if (
                not isinstance(runtime_probe.get("euid"), int)
                or isinstance(runtime_probe.get("euid"), bool)
                or runtime_probe["euid"] == 0
                or runtime_probe.get("root_read_only") is not True
                or runtime_probe.get("data_writable") is not True
                or runtime_probe.get("tmpfs") is not True
                or runtime_probe.get("release_tooling_absent") is not True
                or runtime_probe.get("required_resources") != sorted(REQUIRED_RESOURCES)
            ):
                raise ImageSmokeError(f"release-image hardened runtime contract failed: {runtime_probe!r}")

            server_environment = (
                "API_AUTH_ENABLED=true",
                "API_AUTH_KEY=release-smoke-placeholder",
                "API_ALLOWED_HOSTS=localhost,127.0.0.1",
                "AWS_EC2_METADATA_DISABLED=true",
                "CONTEXT_PROVIDER=none",
                "GRAFANA_ENABLED=false",
                "HOME=/tmp",
                "LOG_LEVEL=warning",
                "SIGNALFX_ENABLED=false",
                "TMPDIR=/tmp",
            )
            server_command = [
                "docker",
                "run",
                "--detach",
                "--name",
                server_container,
                "--label",
                "tacit.release-smoke=true",
                *runtime_options,
                "--health-interval",
                "1s",
                "--health-timeout",
                "5s",
                "--health-start-period",
                "1s",
                "--health-retries",
                "10",
            ]
            for variable in server_environment:
                server_command.extend(("--env", variable))
            server_command.append(args.image)
            _run_command(
                server_command,
                timeout=args.command_timeout,
                label="release-image server start",
            )
            runtime_inspect = _wait_for_healthy(
                server_container,
                startup_timeout=args.startup_timeout,
                command_timeout=args.command_timeout,
            )
            _validate_runtime_inspect(runtime_inspect)
            health = _parse_object(
                _run_command(
                    ["docker", "exec", server_container, "python", "-I", "-c", _health_probe_script()],
                    timeout=args.command_timeout,
                    label="release-image direct health smoke",
                ),
                "release-image direct health smoke",
            )
            if health != {"status": "ok"}:
                raise ImageSmokeError(f"release-image direct health smoke failed: {health!r}")
        except BaseException as exc:
            failure = exc
            try:
                if _resource_exists("container", server_container, args.cleanup_timeout):
                    _run_command(
                        ["docker", "logs", "--tail", "200", server_container],
                        timeout=args.cleanup_timeout,
                        label="release-image failure logs",
                        check=False,
                    )
            except BaseException:
                pass
        finally:
            for container_name in reversed(container_names):
                try:
                    _cleanup_resource("container", container_name, args.cleanup_timeout)
                except BaseException as exc:
                    cleanup_failures.append(str(exc))
            try:
                _cleanup_resource("volume", volume_name, args.cleanup_timeout)
            except BaseException as exc:
                cleanup_failures.append(str(exc))

        if cleanup_failures:
            cleanup_error = ImageSmokeError("release-image cleanup failed: " + "; ".join(cleanup_failures))
            if failure is not None:
                raise cleanup_error from failure
            raise cleanup_error
        if failure is not None:
            raise failure


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute one authoritative Tacit OCI archive before publication")
    parser.add_argument("--archive", required=True, type=_regular_file)
    parser.add_argument("--checksum", required=True, type=_regular_file)
    parser.add_argument("--image", required=True, type=_image_reference)
    parser.add_argument("--platform", required=True, choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--max-archive-bytes", required=True, type=_positive_integer)
    parser.add_argument("--command-timeout", type=_positive_finite_float, default=45.0)
    parser.add_argument("--startup-timeout", type=_positive_finite_float, default=60.0)
    parser.add_argument("--cleanup-timeout", type=_positive_finite_float, default=15.0)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"release image smoke failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    args = _parser().parse_args()
    if os.name != "posix":
        _fail("release image smoke requires POSIX process-group ownership")
    try:
        _smoke(args)
    except KeyboardInterrupt:
        _fail("release image smoke was interrupted")
    except (ImageSmokeError, ReleasePayloadError, OSError) as exc:
        _fail(str(exc))


if __name__ == "__main__":
    main()
