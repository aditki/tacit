#!/usr/bin/env python3
"""Rebase ClickStack OTLP metric timestamps while preserving relative timing."""

from __future__ import annotations

import json
import sys
import tarfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def _timestamp_values(value: Any) -> Iterator[int]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower().endswith("timeunixnano"):
                try:
                    yield int(child)
                except (TypeError, ValueError):
                    continue
            else:
                yield from _timestamp_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _timestamp_values(child)


def shift_timestamps(value: Any, offset_ns: int) -> Any:
    if isinstance(value, dict):
        shifted = {}
        for key, child in value.items():
            if key.lower().endswith("timeunixnano"):
                try:
                    shifted[key] = str(int(child) + offset_ns)
                except (TypeError, ValueError):
                    shifted[key] = child
            else:
                shifted[key] = shift_timestamps(child, offset_ns)
        return shifted
    if isinstance(value, list):
        return [shift_timestamps(child, offset_ns) for child in value]
    return value


def _records(archive_path: Path) -> Iterator[dict[str, Any]]:
    with tarfile.open(archive_path, "r:gz") as archive:
        member = archive.extractfile("metrics.json")
        if member is None:
            raise ValueError(f"metrics.json not found in {archive_path}")
        for raw_line in member:
            if raw_line.strip():
                yield json.loads(raw_line)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} ARCHIVE", file=sys.stderr)
        return 2

    archive_path = Path(sys.argv[1])
    latest = max((timestamp for record in _records(archive_path) for timestamp in _timestamp_values(record)), default=0)
    if latest <= 0:
        raise ValueError(f"no OTLP timestamps found in {archive_path}")

    # Keep the newest sample slightly behind wall-clock time to avoid future skew.
    offset_ns = time.time_ns() - latest - 5_000_000_000
    for record in _records(archive_path):
        print(json.dumps(shift_timestamps(record, offset_ns), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
