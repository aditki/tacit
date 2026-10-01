#!/usr/bin/env python3
"""Verify fixed-origin PyPI release metadata against carried descriptors."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, OpenerDirector, ProxyHandler, Request, build_opener

from release_publication_snapshot import (
    LabelledArtifact,
    PublicationSnapshotError,
    parse_labelled_artifact,
    write_github_outputs,
)

PYPI_ORIGIN = "https://pypi.org"
PYPI_PROJECT = "tacit-ai"
MAX_PYPI_METADATA_BYTES = 1024 * 1024
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_POSTFLIGHT_ATTEMPTS = 6
DEFAULT_POSTFLIGHT_RETRY_SECONDS = 5.0
_VERSION = re.compile(r"[0-9A-Za-z][0-9A-Za-z._+-]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class ReleasePyPIError(RuntimeError):
    pass


class _ReadableResponse(Protocol):
    headers: Mapping[str, str]

    def read(self, amount: int = -1) -> bytes: ...


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        fp: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        return None


def build_pypi_opener() -> OpenerDirector:
    return build_opener(ProxyHandler({}), _RejectRedirects())


def _validated_origin() -> tuple[str, str, int]:
    try:
        parsed = urlsplit(PYPI_ORIGIN)
        port = parsed.port or 443
    except ValueError as exc:
        raise ReleasePyPIError("PyPI origin is invalid") from exc
    if (
        parsed.scheme.casefold() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or not 1 <= port <= 65535
    ):
        raise ReleasePyPIError("PyPI origin must be one fixed HTTPS origin")
    return ("https", parsed.hostname.casefold(), port)


def pypi_metadata_url(version: str) -> str:
    if _VERSION.fullmatch(version) is None:
        raise ReleasePyPIError("PyPI release version contains invalid characters")
    _validated_origin()
    return f"{PYPI_ORIGIN}/pypi/{PYPI_PROJECT}/{quote(version, safe='')}/json"


def _validate_response_url(url: str, *, expected_url: str) -> None:
    try:
        parsed = urlsplit(url)
        actual_origin = (parsed.scheme.casefold(), (parsed.hostname or "").casefold(), parsed.port or 443)
    except ValueError as exc:
        raise ReleasePyPIError("PyPI response URL is invalid") from exc
    if actual_origin != _validated_origin() or url != expected_url:
        raise ReleasePyPIError("PyPI response left the fixed HTTPS origin")


def load_pypi_metadata(response: _ReadableResponse) -> object:
    declared = response.headers.get("Content-Length")
    if declared is not None:
        try:
            declared_size = int(declared)
        except ValueError as exc:
            raise ReleasePyPIError("PyPI metadata has an invalid Content-Length") from exc
        if declared_size < 0 or declared_size > MAX_PYPI_METADATA_BYTES:
            raise ReleasePyPIError("PyPI metadata exceeds the size limit")
    payload = response.read(MAX_PYPI_METADATA_BYTES + 1)
    if len(payload) > MAX_PYPI_METADATA_BYTES:
        raise ReleasePyPIError("PyPI metadata exceeds the size limit")
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleasePyPIError("PyPI metadata is not valid JSON") from exc


def _metadata_digests(metadata: object) -> dict[str, str]:
    if not isinstance(metadata, dict) or not isinstance(metadata.get("urls"), list):
        raise ReleasePyPIError("PyPI metadata has an invalid files list")
    entries = metadata["urls"]
    names: list[str] = []
    digests: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ReleasePyPIError("PyPI metadata has an invalid file entry")
        name = entry.get("filename")
        raw_digests = entry.get("digests")
        digest = raw_digests.get("sha256") if isinstance(raw_digests, dict) else None
        if not isinstance(name, str) or Path(name).name != name:
            raise ReleasePyPIError("PyPI metadata has an invalid filename")
        if not isinstance(digest, str) or _SHA256.fullmatch(digest.casefold()) is None:
            raise ReleasePyPIError(f"PyPI metadata has an invalid SHA-256 digest for {name!r}")
        names.append(name)
        digests[name] = digest.casefold()
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise ReleasePyPIError(f"PyPI metadata has duplicate filenames: {duplicates}")
    return digests


def fetch_release_digests(
    version: str,
    *,
    opener: OpenerDirector | None = None,
    timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> dict[str, str]:
    if timeout <= 0 or timeout > DEFAULT_REQUEST_TIMEOUT_SECONDS:
        raise ReleasePyPIError("PyPI request timeout is outside the allowed bound")
    url = pypi_metadata_url(version)
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tacit-release-verifier",
        },
    )
    client = opener or build_pypi_opener()
    try:
        with client.open(request, timeout=timeout) as response:
            _validate_response_url(response.geturl(), expected_url=url)
            return _metadata_digests(load_pypi_metadata(response))
    except HTTPError as exc:
        if exc.code == 404:
            return {}
        raise


def _expected_digests(artifacts: Sequence[LabelledArtifact]) -> dict[str, str]:
    expected = {item.artifact.name: item.artifact.digest for item in artifacts}
    if not expected or len(expected) != len(artifacts):
        raise ReleasePyPIError("PyPI publication descriptors must have unique names")
    return expected


def verify_preflight(version: str, artifacts: Sequence[LabelledArtifact]) -> None:
    expected = _expected_digests(artifacts)
    remote = fetch_release_digests(version)
    unexpected = sorted(set(remote) - set(expected))
    mismatched = sorted(name for name, digest in remote.items() if digest != expected.get(name))
    if unexpected or mismatched:
        raise ReleasePyPIError(
            "PyPI release differs from carried artifacts: " f"unexpected={unexpected}, mismatched={mismatched}"
        )


def verify_postflight(
    version: str,
    artifacts: Sequence[LabelledArtifact],
    *,
    attempts: int = DEFAULT_POSTFLIGHT_ATTEMPTS,
    retry_seconds: float = DEFAULT_POSTFLIGHT_RETRY_SECONDS,
) -> None:
    if attempts <= 0 or attempts > DEFAULT_POSTFLIGHT_ATTEMPTS:
        raise ReleasePyPIError("PyPI postflight attempt count is outside the allowed bound")
    if retry_seconds < 0 or retry_seconds > DEFAULT_POSTFLIGHT_RETRY_SECONDS:
        raise ReleasePyPIError("PyPI postflight retry delay is outside the allowed bound")
    expected = _expected_digests(artifacts)
    error = "PyPI did not expose the complete release"
    for attempt in range(attempts):
        remote = fetch_release_digests(version)
        if remote == expected:
            return
        error = f"PyPI artifact mismatch: expected={sorted(expected)}, remote={sorted(remote)}"
        if attempt + 1 < attempts:
            time.sleep(retry_seconds)
    raise ReleasePyPIError(error)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("preflight", "postflight"))
    parser.add_argument("--version", required=True)
    parser.add_argument("--artifact", action="append", required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    try:
        artifacts = [parse_labelled_artifact(raw) for raw in args.artifact]
        if args.mode == "preflight":
            verify_preflight(args.version, artifacts)
            if args.github_output is not None:
                write_github_outputs(args.github_output, artifacts)
        else:
            verify_postflight(args.version, artifacts)
    except (HTTPError, OSError, PublicationSnapshotError, ReleasePyPIError) as exc:
        print(f"PyPI release verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
