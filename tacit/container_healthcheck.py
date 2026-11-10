"""Lightweight, proxy-free container health probe."""

from __future__ import annotations

import json
import os
import socket
import stat
import tempfile
import time
from pathlib import Path

_CONTAINER_HEALTH_ADDRESS = "127.0.0.1"
_CONTAINER_HEALTH_PORT = 8000
_CONTAINER_HEALTH_PATH = "/healthz"
_CONTAINER_HEALTH_DEADLINE_SECONDS = 3.0
_CONTAINER_HEALTH_IO_TIMEOUT_SECONDS = 1.0
_CONTAINER_HEALTH_MAX_HEADER_BYTES = 8 * 1024
_CONTAINER_HEALTH_MAX_BODY_BYTES = 16 * 1024
_CONTAINER_HEALTH_RECV_BYTES = 4 * 1024
_CONTAINER_HEALTH_HOST_PATH = Path("/tmp/tacit-container-health-host")
_CONTAINER_HEALTH_HOST_MAX_BYTES = 256


def write_container_health_host(host: str) -> None:
    """Materialize the server-validated probe Host in the container tmpfs."""
    encoded = host.encode("ascii")
    if not encoded or len(encoded) > _CONTAINER_HEALTH_HOST_MAX_BYTES or b"\n" in encoded:
        raise ValueError("Container health host is invalid")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{_CONTAINER_HEALTH_HOST_PATH.name}.",
        suffix=".tmp",
        dir=_CONTAINER_HEALTH_HOST_PATH.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        payload = memoryview(encoded + b"\n")
        while payload:
            written = os.write(descriptor, payload)
            if written <= 0:
                raise OSError("Container health host write made no progress")
            payload = payload[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, _CONTAINER_HEALTH_HOST_PATH)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _read_container_health_host() -> str:
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(_CONTAINER_HEALTH_HOST_PATH, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _CONTAINER_HEALTH_HOST_MAX_BYTES + 1:
            raise RuntimeError("Container health host file is invalid")
        payload = os.read(descriptor, _CONTAINER_HEALTH_HOST_MAX_BYTES + 2)
    finally:
        os.close(descriptor)
    try:
        host = payload.decode("ascii").removesuffix("\n")
    except UnicodeDecodeError as exc:
        raise RuntimeError("Container health host file is invalid") from exc
    if not host or "\n" in host or "\r" in host:
        raise RuntimeError("Container health host file is invalid")
    return host


def probe_container_health(
    *,
    host: str | None = None,
    address: str = _CONTAINER_HEALTH_ADDRESS,
    port: int = _CONTAINER_HEALTH_PORT,
    path: str = _CONTAINER_HEALTH_PATH,
    deadline_seconds: float = _CONTAINER_HEALTH_DEADLINE_SECONDS,
    io_timeout_seconds: float = _CONTAINER_HEALTH_IO_TIMEOUT_SECONDS,
    max_header_bytes: int = _CONTAINER_HEALTH_MAX_HEADER_BYTES,
    max_body_bytes: int = _CONTAINER_HEALTH_MAX_BODY_BYTES,
) -> None:
    """Probe the fixed loopback API under one bounded absolute deadline."""
    deadline = time.monotonic() + deadline_seconds
    selected_host = host if host is not None else _read_container_health_host()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {selected_host}\r\n"
        "Accept: application/json\r\n"
        "Accept-Encoding: identity\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")

    try:
        connection = socket.create_connection(
            (address, port),
            timeout=_remaining_timeout(deadline, io_timeout_seconds),
        )
        with connection:
            _send(connection, request, deadline=deadline, io_timeout_seconds=io_timeout_seconds)
            status, headers, body = _response(
                connection,
                deadline=deadline,
                io_timeout_seconds=io_timeout_seconds,
                max_header_bytes=max_header_bytes,
                max_body_bytes=max_body_bytes,
            )
    except TimeoutError:
        raise
    except OSError as exc:
        raise RuntimeError("Container health check transport failed") from exc

    if not 200 <= status < 300:
        raise RuntimeError("Container health check returned a non-success HTTP status")
    content_types = headers.get("content-type", ())
    if len(content_types) != 1 or content_types[0].split(";", maxsplit=1)[0].strip().casefold() != "application/json":
        raise RuntimeError("Container health check did not return JSON content")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise RuntimeError("Container health check returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Container health check must return a JSON object")
    if payload.get("status") != "ok":
        raise RuntimeError("Container health check status is not ok")


def _remaining_timeout(deadline: float, io_timeout_seconds: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Container health check absolute deadline exceeded")
    return min(remaining, io_timeout_seconds)


def _send(
    connection: socket.socket,
    payload: bytes,
    *,
    deadline: float,
    io_timeout_seconds: float,
) -> None:
    remaining = memoryview(payload)
    while remaining:
        connection.settimeout(_remaining_timeout(deadline, io_timeout_seconds))
        sent = connection.send(remaining)
        if sent <= 0:
            raise RuntimeError("Container health check request could not be sent")
        remaining = remaining[sent:]


def _recv(
    connection: socket.socket,
    size: int,
    *,
    deadline: float,
    io_timeout_seconds: float,
) -> bytes:
    connection.settimeout(_remaining_timeout(deadline, io_timeout_seconds))
    try:
        return connection.recv(size)
    except TimeoutError as exc:
        raise TimeoutError("Container health check absolute deadline exceeded") from exc


def _response(
    connection: socket.socket,
    *,
    deadline: float,
    io_timeout_seconds: float,
    max_header_bytes: int,
    max_body_bytes: int,
) -> tuple[int, dict[str, tuple[str, ...]], bytes]:
    response = bytearray()
    header_end = -1
    while header_end < 0:
        chunk = _recv(
            connection,
            _CONTAINER_HEALTH_RECV_BYTES,
            deadline=deadline,
            io_timeout_seconds=io_timeout_seconds,
        )
        if not chunk:
            raise RuntimeError("Container health check returned incomplete HTTP headers")
        response.extend(chunk)
        header_end = response.find(b"\r\n\r\n")
        if header_end < 0 and len(response) > max_header_bytes:
            raise RuntimeError("Container health check response exceeded the header size limit")
    if header_end > max_header_bytes:
        raise RuntimeError("Container health check response exceeded the header size limit")

    header_bytes = bytes(response[:header_end])
    body_prefix = bytes(response[header_end + 4 :])
    status, headers = _headers(header_bytes)
    if not 200 <= status < 300:
        return status, headers, b""
    if headers.get("transfer-encoding"):
        raise RuntimeError("Container health check returned unsupported transfer encoding")
    content_encodings = headers.get("content-encoding", ())
    if content_encodings and any(value.strip().casefold() != "identity" for value in content_encodings):
        raise RuntimeError("Container health check returned unsupported content encoding")

    content_lengths = headers.get("content-length", ())
    if len(content_lengths) > 1:
        raise RuntimeError("Container health check returned ambiguous content length")
    if content_lengths:
        raw_length = content_lengths[0]
        if not raw_length.isascii() or not raw_length.isdigit():
            raise RuntimeError("Container health check returned invalid content length")
        expected_length = int(raw_length)
        if expected_length > max_body_bytes:
            raise RuntimeError("Container health check response exceeded the body size limit")
        if len(body_prefix) > expected_length:
            raise RuntimeError("Container health check returned excess response bytes")
        body = bytearray(body_prefix)
        while len(body) < expected_length:
            chunk = _recv(
                connection,
                min(_CONTAINER_HEALTH_RECV_BYTES, expected_length - len(body)),
                deadline=deadline,
                io_timeout_seconds=io_timeout_seconds,
            )
            if not chunk:
                raise RuntimeError("Container health check returned an incomplete response body")
            body.extend(chunk)
        return status, headers, bytes(body)

    body = bytearray(body_prefix)
    if len(body) > max_body_bytes:
        raise RuntimeError("Container health check response exceeded the body size limit")
    while True:
        chunk = _recv(
            connection,
            min(_CONTAINER_HEALTH_RECV_BYTES, max_body_bytes + 1 - len(body)),
            deadline=deadline,
            io_timeout_seconds=io_timeout_seconds,
        )
        if not chunk:
            return status, headers, bytes(body)
        body.extend(chunk)
        if len(body) > max_body_bytes:
            raise RuntimeError("Container health check response exceeded the body size limit")


def _headers(header_bytes: bytes) -> tuple[int, dict[str, tuple[str, ...]]]:
    lines = header_bytes.split(b"\r\n")
    try:
        status_line = lines[0].decode("ascii")
    except (IndexError, UnicodeDecodeError) as exc:
        raise RuntimeError("Container health check returned an invalid HTTP status") from exc
    status_parts = status_line.split(" ", maxsplit=2)
    if (
        len(status_parts) < 2
        or status_parts[0] not in {"HTTP/1.0", "HTTP/1.1"}
        or len(status_parts[1]) != 3
        or not status_parts[1].isdigit()
    ):
        raise RuntimeError("Container health check returned an invalid HTTP status")

    parsed: dict[str, list[str]] = {}
    for line in lines[1:]:
        if not line or line[:1] in {b" ", b"\t"} or b":" not in line:
            raise RuntimeError("Container health check returned invalid HTTP headers")
        raw_name, raw_value = line.split(b":", maxsplit=1)
        try:
            name = raw_name.decode("ascii").casefold()
            value = raw_value.decode("latin-1").strip()
        except UnicodeDecodeError as exc:
            raise RuntimeError("Container health check returned invalid HTTP headers") from exc
        if (
            not name
            or any(not (character.isalnum() or character == "-") for character in name)
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        ):
            raise RuntimeError("Container health check returned invalid HTTP headers")
        parsed.setdefault(name, []).append(value)
    return int(status_parts[1]), {name: tuple(values) for name, values in parsed.items()}


if __name__ == "__main__":
    probe_container_health()
