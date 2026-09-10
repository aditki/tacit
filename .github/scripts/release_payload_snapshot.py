#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import hmac
import os
import stat
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

READ_CHUNK_BYTES = 1024 * 1024


class ReleasePayloadError(RuntimeError):
    pass


class PayloadSnapshot(NamedTuple):
    path: Path
    digest: str
    size: int
    device: int
    inode: int


def _open_no_follow(path: Path, flags: int) -> int:
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return os.open(path, flags)


def _validated_private_directory(path: Path) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ReleasePayloadError(f"private snapshot directory is unavailable: {path}") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise ReleasePayloadError(f"private snapshot parent must be a directory: {path}")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ReleasePayloadError(f"private snapshot directory must be owner-only: {path}")
    return metadata


def _open_source(path: Path, maximum: int) -> tuple[int, os.stat_result]:
    try:
        listed = path.lstat()
    except OSError as exc:
        raise ReleasePayloadError(f"release payload is unavailable: {path}") from exc
    if not stat.S_ISREG(listed.st_mode):
        raise ReleasePayloadError(f"release payload must be a regular file: {path}")
    descriptor = _open_no_follow(path, os.O_RDONLY)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (listed.st_dev, listed.st_ino, listed.st_size) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
        ):
            raise ReleasePayloadError(f"release payload changed before copying: {path}")
        if opened.st_size <= 0:
            raise ReleasePayloadError(f"release payload is empty: {path}")
        if opened.st_size > maximum:
            raise ReleasePayloadError(f"release payload exceeds its byte limit: {path}")
        return descriptor, opened
    except BaseException:
        os.close(descriptor)
        raise


def _stream_copy(source: int, destination: int, size: int) -> str:
    digest = hashlib.sha256()
    remaining = size
    while remaining:
        chunk = os.read(source, min(READ_CHUNK_BYTES, remaining))
        if not chunk:
            raise ReleasePayloadError("release payload shrank while copying")
        remaining -= len(chunk)
        digest.update(chunk)
        view = memoryview(chunk)
        while view:
            written = os.write(destination, view)
            if written <= 0:
                raise ReleasePayloadError("private release snapshot write made no progress")
            view = view[written:]
    if os.read(source, 1):
        raise ReleasePayloadError("release payload grew while copying")
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = _open_no_follow(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verify_snapshot(snapshot: PayloadSnapshot, maximum: int) -> None:
    descriptor = _open_no_follow(snapshot.path, os.O_RDONLY)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_size != snapshot.size
            or (opened.st_dev, opened.st_ino) != (snapshot.device, snapshot.inode)
        ):
            raise ReleasePayloadError("private release snapshot identity changed")
        if opened.st_size > maximum:
            raise ReleasePayloadError("private release snapshot exceeds its byte limit")
        digest = hashlib.sha256()
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise ReleasePayloadError("private release snapshot shrank")
            remaining -= len(chunk)
            digest.update(chunk)
        if os.read(descriptor, 1):
            raise ReleasePayloadError("private release snapshot grew")
        if not hmac.compare_digest(digest.hexdigest(), snapshot.digest):
            raise ReleasePayloadError("private release snapshot content changed")
    finally:
        os.close(descriptor)


def copy_verified_payload(
    source: Path,
    destination: Path,
    *,
    maximum: int,
    expected_digest: str | None,
    executable: bool,
) -> PayloadSnapshot:
    if maximum <= 0:
        raise ReleasePayloadError("release payload byte limit must be positive")
    if expected_digest is not None and len(expected_digest) != 64:
        raise ReleasePayloadError("expected release payload digest must be SHA-256")
    parent = destination.parent
    _validated_private_directory(parent)
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ReleasePayloadError(f"private release snapshot already exists: {destination}")

    source_descriptor, source_metadata = _open_source(source, maximum)
    temporary_descriptor = -1
    temporary_path: Path | None = None
    try:
        temporary_descriptor, temporary_name = tempfile.mkstemp(
            dir=parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        digest = _stream_copy(source_descriptor, temporary_descriptor, source_metadata.st_size)
        final_source = os.fstat(source_descriptor)
        source_identity = (
            source_metadata.st_dev,
            source_metadata.st_ino,
            source_metadata.st_size,
            source_metadata.st_mtime_ns,
            source_metadata.st_ctime_ns,
        )
        if source_identity != (
            final_source.st_dev,
            final_source.st_ino,
            final_source.st_size,
            final_source.st_mtime_ns,
            final_source.st_ctime_ns,
        ):
            raise ReleasePayloadError("release payload changed while copying")
        if expected_digest is not None and not hmac.compare_digest(digest, expected_digest):
            raise ReleasePayloadError("release payload checksum mismatch")
        os.fchmod(temporary_descriptor, 0o700 if executable else 0o600)
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = -1
        os.replace(temporary_path, destination)
        temporary_path = None
        _fsync_directory(parent)
        final = destination.lstat()
        snapshot = PayloadSnapshot(destination, digest, final.st_size, final.st_dev, final.st_ino)
        _verify_snapshot(snapshot, maximum)
        return snapshot
    finally:
        os.close(source_descriptor)
        if temporary_descriptor >= 0:
            os.close(temporary_descriptor)
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


@contextmanager
def verified_payload_snapshot(
    source: Path,
    *,
    maximum: int,
    expected_digest: str | None,
    executable: bool,
    prefix: str,
) -> Iterator[PayloadSnapshot]:
    root = Path(tempfile.mkdtemp(prefix=prefix)).resolve()
    os.chmod(root, 0o700)
    snapshot: PayloadSnapshot | None = None
    try:
        snapshot = copy_verified_payload(
            source,
            root / "payload",
            maximum=maximum,
            expected_digest=expected_digest,
            executable=executable,
        )
        yield snapshot
        _verify_snapshot(snapshot, maximum)
    finally:
        cleanup_errors: list[BaseException] = []
        if snapshot is not None:
            try:
                snapshot.path.unlink(missing_ok=True)
            except OSError as exc:
                cleanup_errors.append(exc)
        try:
            root.rmdir()
        except OSError as exc:
            cleanup_errors.append(exc)
        if cleanup_errors:
            raise ReleasePayloadError("private release snapshot cleanup failed") from cleanup_errors[0]


def cleanup_private_directory(path: Path, expected_names: Sequence[str]) -> None:
    listed = _validated_private_directory(path)
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = _open_no_follow(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (listed.st_dev, listed.st_ino) != (opened.st_dev, opened.st_ino):
            raise ReleasePayloadError("private snapshot directory changed before cleanup")
        os.fchmod(descriptor, 0o700)
        actual = set(os.listdir(descriptor))
        expected = set(expected_names)
        if not actual.issubset(expected) or len(expected) != len(expected_names):
            raise ReleasePayloadError("private snapshot directory contents are unexpected")
        for name in sorted(actual):
            if not name or Path(name).name != name:
                raise ReleasePayloadError("private snapshot cleanup name is invalid")
            metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode):
                raise ReleasePayloadError("private snapshot cleanup target is not regular")
            os.unlink(name, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    current = path.lstat()
    if (current.st_dev, current.st_ino) != (listed.st_dev, listed.st_ino):
        raise ReleasePayloadError("private snapshot directory changed during cleanup")
    path.rmdir()
