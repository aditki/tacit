#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import BinaryIO

READ_CHUNK_BYTES = 1024 * 1024
MAX_CHECKSUM_BYTES = 1024
SHA256_PATTERN = re.compile(r"([0-9a-f]{64})  ([^/\r\n]+)\n?\Z")


class ArchiveValidationError(ValueError):
    pass


def _positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def _open_regular_file(path: Path) -> tuple[BinaryIO, os.stat_result]:
    try:
        listed = path.lstat()
    except OSError as exc:
        raise ArchiveValidationError(f"cannot stat {path}: {exc}") from exc
    if not stat.S_ISREG(listed.st_mode):
        raise ArchiveValidationError(f"archive is not a regular file: {path}")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ArchiveValidationError(f"cannot open regular file {path}: {exc}") from exc

    stream = os.fdopen(descriptor, "rb")
    opened = os.fstat(stream.fileno())
    if not stat.S_ISREG(opened.st_mode) or (listed.st_dev, listed.st_ino) != (
        opened.st_dev,
        opened.st_ino,
    ):
        stream.close()
        raise ArchiveValidationError(f"archive changed before it was opened: {path}")
    return stream, opened


def _validated_size(path: Path, metadata: os.stat_result, maximum: int) -> int:
    size = metadata.st_size
    if size == 0:
        raise ArchiveValidationError(f"archive is empty: {path}")
    if size < 0 or size > maximum:
        raise ArchiveValidationError(f"archive exceeds the explicit maximum of {maximum} bytes: {path} ({size} bytes)")
    return size


def stream_sha256(path: Path, maximum: int) -> str:
    stream, metadata = _open_regular_file(path)
    try:
        remaining = _validated_size(path, metadata, maximum)
        digest = hashlib.sha256()
        while remaining:
            chunk = stream.read(min(READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise ArchiveValidationError(f"archive shrank while hashing: {path}")
            remaining -= len(chunk)
            digest.update(chunk)
        if stream.read(1):
            raise ArchiveValidationError(f"archive grew while hashing: {path}")
        final = os.fstat(stream.fileno())
        if (final.st_dev, final.st_ino, final.st_size) != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
        ):
            raise ArchiveValidationError(f"archive changed while hashing: {path}")
        return digest.hexdigest()
    finally:
        stream.close()


def _read_checksum(checksum_path: Path, archive_name: str) -> str:
    stream, metadata = _open_regular_file(checksum_path)
    try:
        size = _validated_size(checksum_path, metadata, MAX_CHECKSUM_BYTES)
        payload = stream.read(size + 1)
    finally:
        stream.close()
    if len(payload) != size:
        raise ArchiveValidationError(f"checksum file changed while reading: {checksum_path}")
    try:
        checksum_text = payload.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ArchiveValidationError(f"checksum is not ASCII: {checksum_path}") from exc
    matched = SHA256_PATTERN.fullmatch(checksum_text)
    if matched is None or matched.group(2) != archive_name:
        raise ArchiveValidationError(f"checksum manifest must contain exactly one SHA-256 for {archive_name}")
    return matched.group(1)


def write_checksum(archive: Path, checksum_path: Path, maximum: int) -> str:
    digest = stream_sha256(archive, maximum)
    checksum_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=checksum_path.parent,
        prefix=f".{checksum_path.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii", newline="\n") as stream:
            stream.write(f"{digest}  {archive.name}\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, checksum_path)
    finally:
        temporary.unlink(missing_ok=True)
    return digest


def verify_checksum(archive: Path, checksum_path: Path, maximum: int) -> str:
    expected = _read_checksum(checksum_path, archive.name)
    actual = stream_sha256(archive, maximum)
    if not hmac.compare_digest(actual, expected):
        raise ArchiveValidationError(f"archive checksum mismatch: {archive}")
    return actual


def copy_and_verify(
    archive: Path,
    checksum_path: Path,
    destination: Path,
    maximum: int,
) -> str:
    expected = verify_checksum(archive, checksum_path, maximum)
    source, metadata = _open_regular_file(archive)
    try:
        source_size = _validated_size(archive, metadata, maximum)
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        try:
            copied_digest = hashlib.sha256()
            remaining = source_size
            with os.fdopen(descriptor, "wb") as copied_stream:
                while remaining:
                    chunk = source.read(min(READ_CHUNK_BYTES, remaining))
                    if not chunk:
                        raise ArchiveValidationError(f"archive shrank while copying: {archive}")
                    remaining -= len(chunk)
                    copied_digest.update(chunk)
                    copied_stream.write(chunk)
                if source.read(1):
                    raise ArchiveValidationError(f"archive grew while copying: {archive}")
                copied_stream.flush()
                os.fsync(copied_stream.fileno())
            copied = copied_digest.hexdigest()
            if not hmac.compare_digest(copied, expected):
                raise ArchiveValidationError(f"copied archive checksum mismatch: {archive} -> {destination}")
            if not hmac.compare_digest(stream_sha256(temporary, maximum), expected):
                raise ArchiveValidationError(f"temporary archive checksum mismatch: {temporary}")
            os.replace(temporary, destination)
            final = stream_sha256(destination, maximum)
            if not hmac.compare_digest(final, expected):
                raise ArchiveValidationError(f"destination archive checksum mismatch: {destination}")
            return final
        finally:
            temporary.unlink(missing_ok=True)
    finally:
        source.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate Tacit release image archives")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("write", "verify", "copy"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--archive", type=Path, required=True)
        subparser.add_argument("--checksum", type=Path, required=True)
        subparser.add_argument("--max-bytes", type=_positive_integer, required=True)
        if command == "copy":
            subparser.add_argument("--destination", type=Path, required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        if arguments.command == "write":
            digest = write_checksum(arguments.archive, arguments.checksum, arguments.max_bytes)
        elif arguments.command == "verify":
            digest = verify_checksum(arguments.archive, arguments.checksum, arguments.max_bytes)
        else:
            digest = copy_and_verify(
                arguments.archive,
                arguments.checksum,
                arguments.destination,
                arguments.max_bytes,
            )
    except (ArchiveValidationError, OSError) as exc:
        print(f"Release image archive validation failed: {exc}", file=sys.stderr)
        return 1
    print(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
