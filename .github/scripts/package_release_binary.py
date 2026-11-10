#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import os
import stat
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Buffer
from pathlib import Path
from typing import IO, Any, BinaryIO

READ_CHUNK_BYTES = 1024 * 1024


class BinaryPackagingError(ValueError):
    pass


def _positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def _windows_open_no_follow(path: Path) -> int:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    windows_ctypes: Any = ctypes
    windows_msvcrt: Any = msvcrt

    generic_read = 0x80000000
    share_all = 0x00000001 | 0x00000002 | 0x00000004
    open_existing = 3
    file_flag_open_reparse_point = 0x00200000
    file_flag_sequential_scan = 0x08000000
    file_attribute_reparse_point = 0x00000400

    class FileInformation(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("creation_time", wintypes.FILETIME),
            ("last_access_time", wintypes.FILETIME),
            ("last_write_time", wintypes.FILETIME),
            ("volume_serial_number", wintypes.DWORD),
            ("file_size_high", wintypes.DWORD),
            ("file_size_low", wintypes.DWORD),
            ("number_of_links", wintypes.DWORD),
            ("file_index_high", wintypes.DWORD),
            ("file_index_low", wintypes.DWORD),
        ]

    kernel32 = windows_ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [wintypes.HANDLE, ctypes.POINTER(FileInformation)]
    get_information.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    handle = create_file(
        str(path),
        generic_read,
        share_all,
        None,
        open_existing,
        file_flag_open_reparse_point | file_flag_sequential_scan,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle == invalid_handle:
        error = windows_ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)

    information = FileInformation()
    try:
        if not get_information(handle, ctypes.byref(information)):
            error = windows_ctypes.get_last_error()
            raise OSError(error, os.strerror(error), path)
        if information.file_attributes & file_attribute_reparse_point:
            raise BinaryPackagingError(f"binary input is not a regular file: {path}")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        return windows_msvcrt.open_osfhandle(int(handle), flags)
    except BaseException:
        close_handle(handle)
        raise


def _open_no_follow(path: Path) -> int:
    if hasattr(os, "O_NOFOLLOW"):
        flags = os.O_RDONLY | os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        return os.open(path, flags)
    if os.name == "nt":
        return _windows_open_no_follow(path)
    raise BinaryPackagingError("this platform does not provide no-follow file opens")


def _identity(metadata: os.stat_result) -> tuple[int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size


def _open_binary_input(path: Path, maximum: int) -> tuple[BinaryIO, os.stat_result]:
    try:
        listed = path.lstat()
    except OSError as exc:
        raise BinaryPackagingError(f"cannot stat binary input {path}: {exc}") from exc
    if not stat.S_ISREG(listed.st_mode):
        raise BinaryPackagingError(f"binary input is not a regular file: {path}")
    if listed.st_size <= 0 or listed.st_size > maximum:
        raise BinaryPackagingError(f"binary input exceeds the size limit or is empty: {listed.st_size} bytes")

    try:
        descriptor = _open_no_follow(path)
    except OSError as exc:
        raise BinaryPackagingError(f"cannot open binary input without following links: {path}") from exc
    stream = os.fdopen(descriptor, "rb")
    opened = os.fstat(stream.fileno())
    if not stat.S_ISREG(opened.st_mode) or _identity(listed) != _identity(opened):
        stream.close()
        raise BinaryPackagingError(f"binary input changed before it was opened: {path}")
    if opened.st_size <= 0 or opened.st_size > maximum:
        stream.close()
        raise BinaryPackagingError(f"binary input exceeds the size limit or is empty: {opened.st_size} bytes")
    return stream, opened


class _BoundedWriter(io.BufferedIOBase):
    def __init__(self, stream: BinaryIO, maximum: int) -> None:
        self.stream = stream
        self.maximum = maximum

    def write(self, payload: Buffer) -> int:
        view = memoryview(payload)
        if self.stream.tell() + len(view) > self.maximum:
            raise BinaryPackagingError("binary package exceeds the size limit")
        return self.stream.write(view)

    def tell(self) -> int:
        return self.stream.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        position = self.stream.seek(offset, whence)
        if position > self.maximum:
            raise BinaryPackagingError("binary package exceeds the size limit")
        return position

    def flush(self) -> None:
        self.stream.flush()

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True


def _copy_exact(source: BinaryIO, destination: IO[bytes], expected_size: int) -> None:
    remaining = expected_size
    while remaining:
        chunk = source.read(min(READ_CHUNK_BYTES, remaining))
        if not chunk:
            raise BinaryPackagingError("binary input ended before its declared size")
        remaining -= len(chunk)
        destination.write(chunk)
    if source.read(1):
        raise BinaryPackagingError("binary input grew while it was packaged")


class _ExactReader:
    def __init__(self, stream: BinaryIO, expected_size: int) -> None:
        self.stream = stream
        self.remaining = expected_size

    def read(self, size: int = -1) -> bytes:
        if self.remaining == 0:
            return b""
        requested = self.remaining if size < 0 else min(size, self.remaining)
        chunk = self.stream.read(requested)
        if not chunk:
            raise BinaryPackagingError("binary input ended before its declared size")
        self.remaining -= len(chunk)
        return chunk

    def verify_complete(self) -> None:
        if self.remaining != 0:
            raise BinaryPackagingError("binary package did not consume the complete input")
        if self.stream.read(1):
            raise BinaryPackagingError("binary input grew while it was packaged")


def _write_package(
    source: BinaryIO,
    source_size: int,
    package: Path,
    output: BinaryIO,
    maximum: int,
) -> None:
    bounded = _BoundedWriter(output, maximum)
    if package.name.endswith(".zip"):
        zip_info = zipfile.ZipInfo("tacit.exe")
        zip_info.date_time = (1980, 1, 1, 0, 0, 0)
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        zip_info.create_system = 3
        zip_info.external_attr = (stat.S_IFREG | 0o755) << 16
        with zipfile.ZipFile(
            bounded,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            with archive.open(zip_info, mode="w", force_zip64=True) as destination:
                _copy_exact(source, destination, source_size)
        return

    if package.name.endswith(".tar.gz"):
        tar_info = tarfile.TarInfo("tacit")
        tar_info.size = source_size
        tar_info.mode = 0o755
        tar_info.uid = 0
        tar_info.gid = 0
        tar_info.uname = ""
        tar_info.gname = ""
        tar_info.mtime = 0
        with gzip.GzipFile(filename="", mode="wb", fileobj=bounded, mtime=0) as compressed:
            with tarfile.open(
                mode="w",
                fileobj=compressed,
                format=tarfile.USTAR_FORMAT,
            ) as archive:
                exact_reader = _ExactReader(source, source_size)
                archive.addfile(tar_info, fileobj=exact_reader)
                exact_reader.verify_complete()
        return

    raise BinaryPackagingError(f"unsupported binary package format: {package.name}")


def _hash_output(stream: BinaryIO, maximum: int) -> str:
    metadata = os.fstat(stream.fileno())
    if not stat.S_ISREG(metadata.st_mode):
        raise BinaryPackagingError("binary package is not a regular file")
    if metadata.st_size <= 0 or metadata.st_size > maximum:
        raise BinaryPackagingError(f"binary package exceeds the size limit or is empty: {metadata.st_size} bytes")
    stream.seek(0)
    digest = hashlib.sha256()
    remaining = metadata.st_size
    while remaining:
        chunk = stream.read(min(READ_CHUNK_BYTES, remaining))
        if not chunk:
            raise BinaryPackagingError("binary package ended before its declared size")
        remaining -= len(chunk)
        digest.update(chunk)
    if stream.read(1):
        raise BinaryPackagingError("binary package grew while it was hashed")
    return digest.hexdigest()


def _verify_input_stable(path: Path, stream: BinaryIO, opened: os.stat_result) -> None:
    final = os.fstat(stream.fileno())
    try:
        listed = path.lstat()
    except OSError as exc:
        raise BinaryPackagingError(f"binary input changed while it was packaged: {path}") from exc
    if (
        not stat.S_ISREG(final.st_mode)
        or not stat.S_ISREG(listed.st_mode)
        or _identity(final) != _identity(opened)
        or _identity(listed) != _identity(opened)
        or final.st_mtime_ns != opened.st_mtime_ns
    ):
        raise BinaryPackagingError(f"binary input changed while it was packaged: {path}")


def package_binary(
    source: Path,
    package: Path,
    *,
    maximum_binary_bytes: int,
    maximum_package_bytes: int,
) -> str:
    if maximum_binary_bytes <= 0 or maximum_package_bytes <= 0:
        raise BinaryPackagingError("size limits must be positive")
    if not package.name.endswith((".tar.gz", ".zip")):
        raise BinaryPackagingError(f"unsupported binary package format: {package.name}")

    source_stream, source_metadata = _open_binary_input(source, maximum_binary_bytes)
    with source_stream:
        package.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=package.parent,
            prefix=f".{package.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        checksum = package.with_name(f"{package.name}.sha256")
        checksum_temporary: Path | None = None
        try:
            with os.fdopen(descriptor, "w+b") as output:
                _write_package(
                    source_stream,
                    source_metadata.st_size,
                    package,
                    output,
                    maximum_package_bytes,
                )
                _verify_input_stable(source, source_stream, source_metadata)
                output.flush()
                os.fsync(output.fileno())
                digest = _hash_output(output, maximum_package_bytes)

            checksum_descriptor, checksum_name = tempfile.mkstemp(
                dir=checksum.parent,
                prefix=f".{checksum.name}.",
                suffix=".tmp",
                text=True,
            )
            checksum_temporary = Path(checksum_name)
            with os.fdopen(checksum_descriptor, "w", encoding="ascii", newline="\n") as output:
                output.write(f"{digest}  {package.name}\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, package)
            os.replace(checksum_temporary, checksum)
            return digest
        finally:
            temporary.unlink(missing_ok=True)
            if checksum_temporary is not None:
                checksum_temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a deterministic Tacit binary release archive")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--max-binary-bytes", type=_positive_integer, required=True)
    parser.add_argument("--max-package-bytes", type=_positive_integer, required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        digest = package_binary(
            arguments.source,
            arguments.package,
            maximum_binary_bytes=arguments.max_binary_bytes,
            maximum_package_bytes=arguments.max_package_bytes,
        )
    except (BinaryPackagingError, OSError) as exc:
        print(f"Release binary packaging failed: {exc}", file=sys.stderr)
        return 1
    print(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
