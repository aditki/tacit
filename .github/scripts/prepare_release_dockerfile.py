#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import stat
import sys
import tempfile
from pathlib import Path

MAX_DOCKERFILE_BYTES = 1024 * 1024
UV_SYNC = "RUN uv sync"
DETERMINISTIC_UV_SYNC = "RUN UV_NO_CACHE=1 UV_LINK_MODE=copy uv sync"
FINAL_PROJECT_SYNC = (
    "RUN uv sync --frozen --no-dev \\\n" "    && find /app -type d -name __pycache__ -prune -exec rm -rf {} +\n"
)
DETERMINISTIC_FINAL_PROJECT_SYNC = (
    "RUN uv sync --frozen --no-dev \\\n"
    "    && find /app/.venv -type f -path "
    "'*/tacit_ai-*.dist-info/RECORD' -exec sed -i '/uv_cache\\.json,/d' {} + \\\n"
    "    && find /app/.venv -type f -path "
    "'*/tacit_ai-*.dist-info/uv_cache.json' -delete \\\n"
    "    && find /app -type d -name __pycache__ -prune -exec rm -rf {} +\n"
)


class DockerfilePreparationError(ValueError):
    pass


def prepare(source: Path, output: Path) -> None:
    metadata = source.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise DockerfilePreparationError(f"source Dockerfile is not a regular file: {source}")
    if metadata.st_size <= 0 or metadata.st_size > MAX_DOCKERFILE_BYTES:
        raise DockerfilePreparationError("source Dockerfile is empty or exceeds the size limit")

    original = source.read_text(encoding="utf-8")
    if not original.startswith("# syntax="):
        raise DockerfilePreparationError("source Dockerfile must pin its syntax frontend")
    if "UV_NO_CACHE" in original or "UV_LINK_MODE" in original:
        raise DockerfilePreparationError("source Dockerfile already controls uv cache behavior")

    runtime_lines = [
        line
        for line in original.splitlines(keepends=True)
        if line.startswith("FROM ") and line.rstrip().endswith(" AS runtime")
    ]
    if len(runtime_lines) != 1:
        raise DockerfilePreparationError("source Dockerfile must contain exactly one runtime stage")
    anchor = f"{runtime_lines[0]}\n"
    if original.count(anchor) != 1:
        raise DockerfilePreparationError("runtime stage must be followed by one blank line")
    if original.count(FINAL_PROJECT_SYNC) != 1:
        raise DockerfilePreparationError("source Dockerfile must contain exactly one final project sync step")
    generated = original.replace(
        FINAL_PROJECT_SYNC,
        DETERMINISTIC_FINAL_PROJECT_SYNC,
        1,
    )
    if generated.count(UV_SYNC) != 2:
        raise DockerfilePreparationError("source Dockerfile must contain exactly two project sync commands")
    generated = generated.replace(UV_SYNC, DETERMINISTIC_UV_SYNC)

    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=output.parent,
        prefix=f".{output.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(generated)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare Tacit's deterministic release Dockerfile")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        prepare(arguments.source, arguments.output)
    except (DockerfilePreparationError, OSError, UnicodeError) as exc:
        print(f"Release Dockerfile preparation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
