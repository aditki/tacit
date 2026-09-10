#!/usr/bin/env python3
"""Fail closed when a frozen release omits runtime providers or ships dev tools."""

from __future__ import annotations

import argparse
import fnmatch
import json
import stat
from collections.abc import Collection
from pathlib import Path

from PyInstaller.archive.readers import pkg_archive_contents

MAX_BINARY_BYTES = 512 * 1024 * 1024

REQUIRED_RUNTIME_MODULES = (
    "boto3",
    "botocore",
    "tacit.agents.providers.anthropic",
    "tacit.agents.providers.bedrock",
    "tacit.agents.providers.ollama",
    "tacit.agents.providers.openai_provider",
    "tacit.integrations.slack",
)

FORBIDDEN_DEVELOPMENT_MODULES = (
    "black",
    "mypy",
    "playwright",
    "pytest",
    "respx",
    "ruff",
)

TACIT_METADATA_PATTERN = "tacit_ai-*.dist-info/METADATA"
TACIT_ENTRY_POINTS_PATTERN = "tacit_ai-*.dist-info/entry_points.txt"
TACIT_RECORD_PATTERN = "tacit_ai-*.dist-info/RECORD"
TACIT_UV_CACHE_PATTERN = "tacit_ai-*.dist-info/uv_cache.json"


class BinaryArchiveInspectionError(RuntimeError):
    """Raised when the frozen archive violates the release dependency policy."""


def _contains_module(members: Collection[str], module: str) -> bool:
    separators = (".", "/", "\\")
    prefixes = tuple(module + separator for separator in separators)
    return any(member == module or member.startswith(prefixes) for member in members)


def _matching_members(members: Collection[str], pattern: str) -> list[str]:
    return sorted(
        normalized for member in members if fnmatch.fnmatchcase(normalized := member.replace("\\", "/"), pattern)
    )


def validate_members(members: Collection[str]) -> None:
    """Validate one bounded PyInstaller member inventory."""
    forbidden = [module for module in FORBIDDEN_DEVELOPMENT_MODULES if _contains_module(members, module)]
    if forbidden:
        raise BinaryArchiveInspectionError("frozen binary contains development-only modules: " + ", ".join(forbidden))

    missing = [module for module in REQUIRED_RUNTIME_MODULES if not _contains_module(members, module)]
    if missing:
        raise BinaryArchiveInspectionError("frozen binary is missing required runtime modules: " + ", ".join(missing))

    metadata = _matching_members(members, TACIT_METADATA_PATTERN)
    entry_points = _matching_members(members, TACIT_ENTRY_POINTS_PATTERN)
    same_distribution = (
        len(metadata) == 1
        and len(entry_points) == 1
        and metadata[0].rsplit("/", 1)[0] == entry_points[0].rsplit("/", 1)[0]
    )
    if not same_distribution:
        raise BinaryArchiveInspectionError("frozen binary is missing a complete Tacit distribution metadata directory")
    if _matching_members(members, TACIT_RECORD_PATTERN):
        raise BinaryArchiveInspectionError("frozen binary contains a Tacit installation record")
    if _matching_members(members, TACIT_UV_CACHE_PATTERN):
        raise BinaryArchiveInspectionError("frozen binary contains Tacit build cache metadata")


def inspect_binary(path: Path) -> int:
    """Inspect a regular, bounded executable and return its archive member count."""
    listed = path.lstat()
    if not stat.S_ISREG(listed.st_mode):
        raise BinaryArchiveInspectionError("frozen binary must be a regular file")
    if listed.st_size <= 0 or listed.st_size > MAX_BINARY_BYTES:
        raise BinaryArchiveInspectionError("frozen binary is empty or exceeds the inspection limit")

    members = set(pkg_archive_contents(str(path), recursive=True))
    validate_members(members)
    return len(members)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    count = inspect_binary(args.binary)
    print(json.dumps({"archive_members": count, "status": "ok"}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (BinaryArchiveInspectionError, OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
