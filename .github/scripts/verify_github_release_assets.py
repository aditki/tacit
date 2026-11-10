#!/usr/bin/env python3
"""Verify GitHub release assets against the bounded local release set."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError

from release_github_api import (
    ReleaseGitHubAPIError,
    authenticated_github_request,
    build_github_api_opener,
    validate_github_api_url,
)

MAX_RELEASE_METADATA_BYTES = 1024 * 1024
MAX_RELEASE_ASSET_BYTES = 512 * 1024 * 1024
READ_CHUNK_BYTES = 1024 * 1024
REQUIRED_ASSETS = {
    "tacit-linux-x86_64.tar.gz",
    "tacit-linux-x86_64.tar.gz.sha256",
}


class _ReadableResponse(Protocol):
    headers: Mapping[str, str]

    def read(self, amount: int = -1) -> bytes: ...


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ReleaseGitHubAPIError(f"{name} is required")
    return value


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(READ_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _bounded_json(response: _ReadableResponse) -> object:
    declared = response.headers.get("Content-Length")
    if declared is not None:
        try:
            declared_size = int(declared)
        except ValueError as exc:
            raise ReleaseGitHubAPIError("GitHub release metadata has an invalid Content-Length") from exc
        if declared_size < 0 or declared_size > MAX_RELEASE_METADATA_BYTES:
            raise ReleaseGitHubAPIError("GitHub release metadata exceeds the size limit")
    payload = response.read(MAX_RELEASE_METADATA_BYTES + 1)
    if len(payload) > MAX_RELEASE_METADATA_BYTES:
        raise ReleaseGitHubAPIError("GitHub release metadata exceeds the size limit")
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseGitHubAPIError("GitHub release metadata is not valid JSON") from exc


def _emit_release_exists(exists: bool) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as output:
        output.write(f"release_exists={'true' if exists else 'false'}\n")


def _verify(args: argparse.Namespace) -> None:
    all_local_paths = {path.name: path for path in Path.cwd().iterdir() if path.is_file()}
    if set(all_local_paths) != REQUIRED_ASSETS:
        raise ReleaseGitHubAPIError(
            "Local GitHub release asset set differs: "
            f"missing={sorted(REQUIRED_ASSETS - set(all_local_paths))}, "
            f"unexpected={sorted(set(all_local_paths) - REQUIRED_ASSETS)}"
        )
    local_sizes = {name: path.stat().st_size for name, path in all_local_paths.items()}
    oversized = sorted(name for name, size in local_sizes.items() if size < 0 or size > MAX_RELEASE_ASSET_BYTES)
    if oversized:
        raise ReleaseGitHubAPIError(f"Local GitHub release assets exceed the size limit: {oversized}")
    local = {name: _hash_file(path) for name, path in all_local_paths.items()}

    api_url = validate_github_api_url(os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    repository = _required_environment("GITHUB_REPOSITORY")
    tag_name = _required_environment("GITHUB_REF_NAME")
    token = _required_environment("GITHUB_TOKEN")
    opener = build_github_api_opener(allow_https_redirects=True)

    def request(url: str, *, accept: str):
        return authenticated_github_request(
            url,
            api_url=api_url,
            token=token,
            accept=accept,
        )

    release_url = f"{api_url}/repos/{repository}/releases/tags/{tag_name}"
    try:
        with opener.open(
            request(release_url, accept="application/vnd.github+json"),
            timeout=30,
        ) as response:
            release = _bounded_json(response)
    except HTTPError as exc:
        if exc.code != 404:
            raise
        if args.require_release:
            raise ReleaseGitHubAPIError("GitHub release is absent after publication") from exc
        _emit_release_exists(False)
        return

    if not isinstance(release, dict) or not isinstance(release.get("assets"), list):
        raise ReleaseGitHubAPIError("GitHub release metadata has an invalid assets list")
    assets = release["assets"]
    names: list[str] = []
    for asset in assets:
        if not isinstance(asset, dict) or not isinstance(asset.get("name"), str):
            raise ReleaseGitHubAPIError("GitHub release metadata has an invalid asset name")
        names.append(asset["name"])
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise ReleaseGitHubAPIError(f"GitHub release has duplicate asset names: {duplicates}")
    unexpected = sorted(set(names) - REQUIRED_ASSETS)
    if unexpected:
        raise ReleaseGitHubAPIError(f"GitHub release has unexpected assets: {unexpected}")
    if args.require_release and set(names) != REQUIRED_ASSETS:
        raise ReleaseGitHubAPIError(
            "GitHub release asset set differs: " f"missing={sorted(REQUIRED_ASSETS - set(names))}, unexpected=[]"
        )

    remote: dict[str, str] = {}
    for asset in assets:
        if not isinstance(asset, dict):
            raise ReleaseGitHubAPIError("GitHub release metadata has an invalid asset")
        name = str(asset["name"])
        declared = asset.get("size")
        if isinstance(declared, bool) or not isinstance(declared, int):
            raise ReleaseGitHubAPIError(f"GitHub release asset {name!r} has no declared size")
        expected = local_sizes[name]
        if declared < 0 or declared > MAX_RELEASE_ASSET_BYTES or declared != expected:
            raise ReleaseGitHubAPIError(
                f"GitHub release asset {name!r} has invalid declared size {declared}; expected {expected}"
            )
        asset_url = asset.get("url")
        if not isinstance(asset_url, str):
            raise ReleaseGitHubAPIError(f"GitHub release asset {name!r} has no API URL")
        digest = hashlib.sha256()
        total = 0
        with opener.open(
            request(asset_url, accept="application/octet-stream"),
            timeout=30,
        ) as response:
            content_length = response.headers.get("Content-Length")
            if content_length is not None:
                try:
                    response_size = int(content_length)
                except ValueError as exc:
                    raise ReleaseGitHubAPIError(f"GitHub release asset {name!r} has an invalid Content-Length") from exc
                if response_size != declared:
                    raise ReleaseGitHubAPIError(
                        f"GitHub release asset {name!r} Content-Length disagrees with its declared size"
                    )
            while True:
                remaining_with_guard = declared - total + 1
                chunk = response.read(min(READ_CHUNK_BYTES, remaining_with_guard))
                if not chunk:
                    break
                total += len(chunk)
                if total > declared or total > MAX_RELEASE_ASSET_BYTES:
                    raise ReleaseGitHubAPIError(f"GitHub release asset {name!r} exceeded its declared size")
                digest.update(chunk)
        if total != declared:
            raise ReleaseGitHubAPIError(f"GitHub release asset {name!r} did not match its declared size")
        remote[name] = digest.hexdigest()

    if set(remote) != set(names):
        raise ReleaseGitHubAPIError("GitHub release asset set changed during download")
    mismatched = sorted(name for name in remote if remote[name] != local[name])
    if mismatched:
        raise ReleaseGitHubAPIError(f"GitHub release differs from local artifacts: mismatched={mismatched}")
    if args.allow_absent_release:
        _emit_release_exists(True)


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--allow-absent-release", action="store_true")
    mode.add_argument("--require-release", action="store_true")
    args = parser.parse_args()
    try:
        _verify(args)
    except (ReleaseGitHubAPIError, HTTPError, OSError) as exc:
        print(f"GitHub release asset verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
