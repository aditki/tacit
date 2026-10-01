#!/usr/bin/env python3
"""Describe, verify, and isolate exact release publication payloads."""

from __future__ import annotations

import argparse
import ctypes
import fnmatch
import hashlib
import importlib.util
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import NamedTuple, cast

_TOOL_CLEANUP_SUFFIX = ".cleanup.py"
_TOOL_CLEANUP_HELPER_SUFFIX = ".cleanup-helper.py"
_SCRIPT_PATH = Path(__file__).resolve()
_SCRIPT_DIRECTORY = _SCRIPT_PATH.parent
_EXTERNAL_CLEANUP_HELPER = (
    _SCRIPT_PATH.with_name(f"{_SCRIPT_PATH.name.removesuffix(_TOOL_CLEANUP_SUFFIX)}{_TOOL_CLEANUP_HELPER_SUFFIX}")
    if _SCRIPT_PATH.name.endswith(_TOOL_CLEANUP_SUFFIX)
    else None
)
if _EXTERNAL_CLEANUP_HELPER is None and str(_SCRIPT_DIRECTORY) not in sys.path:
    # Release jobs install this file and its sole sibling dependency into a
    # root-owned directory, then execute with Python isolated mode.
    sys.path.insert(0, str(_SCRIPT_DIRECTORY))

try:
    if _EXTERNAL_CLEANUP_HELPER is not None:
        helper_spec = importlib.util.spec_from_file_location(
            "release_publication_cleanup_payload",
            _EXTERNAL_CLEANUP_HELPER,
        )
        if helper_spec is None or helper_spec.loader is None:
            raise ModuleNotFoundError("release_payload_snapshot")
        helper_module = importlib.util.module_from_spec(helper_spec)
        helper_spec.loader.exec_module(helper_module)
        ReleasePayloadError = helper_module.ReleasePayloadError
        cleanup_private_directory = helper_module.cleanup_private_directory
        copy_verified_payload = helper_module.copy_verified_payload
        verified_payload_snapshot = helper_module.verified_payload_snapshot
    else:
        from release_payload_snapshot import (  # noqa: E402
            ReleasePayloadError,
            cleanup_private_directory,
            copy_verified_payload,
            verified_payload_snapshot,
        )
except (FileNotFoundError, ModuleNotFoundError) as exc:
    if isinstance(exc, ModuleNotFoundError) and exc.name not in {None, "release_payload_snapshot"}:
        raise

    class ReleasePayloadError(RuntimeError):  # type: ignore[no-redef]
        pass

    def _missing_payload_helper(*_args: object, **_kwargs: object) -> object:
        raise PublicationSnapshotError("installed publication payload helper is unavailable")

    cleanup_private_directory = _missing_payload_helper
    copy_verified_payload = _missing_payload_helper
    verified_payload_snapshot = _missing_payload_helper

DEFAULT_MAXIMUM_BYTES = 512 * 1024 * 1024
_LABEL = re.compile(r"[a-z][a-z0-9_]*")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")
_MOUNT_NAME = re.compile(r"[A-Za-z0-9._+-]+")
_BACKING_PREFIX = "tacit-release-publication-"
_TOOL_PREFIX = "tacit-release-publication-tool-"
_BACKING_PARENT = Path("/var/lib")
_MOUNT_COMMAND = Path("/usr/bin/mount")
_UMOUNT_COMMAND = Path("/usr/bin/umount")
_MOUNTINFO = Path("/proc/self/mountinfo")
_MAX_MOUNTINFO_BYTES = 4 * 1024 * 1024
_MAX_MOUNT_AUTHORITY_BYTES = 64 * 1024
_MAX_TOOL_FILE_BYTES = 2 * 1024 * 1024
_MAX_ACTION_PATH_ANCHORS = 32
_MOUNT_AUTHORITY_SUFFIX = ".mount-authority.json"
_TOOL_AUTHORITY_SUFFIX = ".tool-authority.json"
_TOOL_FILES = ("release_publication_snapshot.py", "release_payload_snapshot.py")
_TOOL_DELETE_ORDER = ("release_payload_snapshot.py", "release_publication_snapshot.py")
_SEALED_DIRECTORY_MODE = 0o555
_SEALED_FILE_MODE = 0o444
_EMPTY_MOUNTPOINT_MODE = 0o700
_REQUIRED_MOUNT_OPTIONS = frozenset({"ro", "nodev", "noexec", "nosuid"})
_REQUIRED_ANCHOR_OPTIONS = frozenset({"rw", "nodev", "nosuid"})
_ROOT_UID = 0
_UNSET_PENDING_UNMOUNT = object()
_UNSET_PENDING_MOUNT = object()
_MS_RDONLY = 1
_MS_NOSUID = 2
_MS_NODEV = 4
_MS_NOEXEC = 8
_MS_REMOUNT = 32
_MS_BIND = 4096
_MS_SLAVE = 1 << 19


class PublicationSnapshotError(RuntimeError):
    pass


class ArtifactSpec(NamedTuple):
    name: str
    digest: str


class LabelledArtifact(NamedTuple):
    label: str
    artifact: ArtifactSpec


class _MountInfoEntry(NamedTuple):
    mount_id: int
    parent_id: int
    device: str
    root: str
    mount_point: str
    options: frozenset[str]
    optional_fields: frozenset[str]
    filesystem: str
    source: str
    super_options: frozenset[str]


class _MountRecord(NamedTuple):
    kind: str
    mount_id: int
    original_path: str
    device: int
    inode: int


class _PendingMountIntent(NamedTuple):
    kind: str
    original_path: str
    device: int
    inode: int
    previous_mount_ids: tuple[int, ...]
    target_device: int = 0
    target_inode: int = 0


class _MountAuthority(NamedTuple):
    state_path: Path
    workspace_path: Path
    workspace_mount_id: int | None
    workspace_identity: tuple[int, int]
    destination_name: str
    destination_identity: tuple[int, int] | None
    backing_identity: tuple[int, int] | None
    artifact_names: tuple[str, ...]
    mounts: tuple[_MountRecord, ...]
    remaining_mount_ids: tuple[int, ...]
    pending_unmount_id: int | None
    destination_removed: bool
    backing_removed: bool
    cleanup_phase: str
    pending_mount: _PendingMountIntent | None = None
    backing_phase: str = "absent"
    pending_copy_name: str | None = None
    copied_artifact_names: tuple[str, ...] = ()


class _ToolAuthority(NamedTuple):
    state_path: Path
    tool_path: Path
    digests: tuple[tuple[str, str], ...]
    phase: str
    remaining_files: tuple[str, ...]
    pending_delete: str | None


def _validate_label(label: str) -> str:
    if _LABEL.fullmatch(label) is None:
        raise PublicationSnapshotError(f"invalid publication descriptor label: {label!r}")
    return label


def _validate_name(name: str) -> str:
    if Path(name).name != name or _NAME.fullmatch(name) is None:
        raise PublicationSnapshotError(f"invalid publication artifact name: {name!r}")
    return name


def _validate_digest(digest: str) -> str:
    normalized = digest.casefold()
    if _DIGEST.fullmatch(normalized) is None:
        raise PublicationSnapshotError("publication artifact digest must be SHA-256")
    return normalized


def _validate_specs(specs: Iterable[ArtifactSpec]) -> list[ArtifactSpec]:
    validated = [ArtifactSpec(_validate_name(spec.name), _validate_digest(spec.digest)) for spec in specs]
    if not validated:
        raise PublicationSnapshotError("at least one publication artifact is required")
    names = [spec.name for spec in validated]
    if len(names) != len(set(names)):
        raise PublicationSnapshotError("publication artifact names must be unique")
    return validated


def _parse_selector(raw: str) -> tuple[str, str]:
    try:
        label, pattern = raw.split("=", 1)
    except ValueError as exc:
        raise PublicationSnapshotError("publication selector must be LABEL=PATTERN") from exc
    _validate_label(label)
    if not pattern or Path(pattern).name != pattern or "/" in pattern or "\\" in pattern:
        raise PublicationSnapshotError("publication selector pattern must match one basename")
    return label, pattern


def parse_labelled_artifact(raw: str) -> LabelledArtifact:
    try:
        label, name, digest = raw.split("=", 2)
    except ValueError as exc:
        raise PublicationSnapshotError("publication artifact must be LABEL=NAME=SHA256") from exc
    return LabelledArtifact(
        _validate_label(label),
        ArtifactSpec(_validate_name(name), _validate_digest(digest)),
    )


def _parse_tool_digest(raw: str) -> tuple[str, str]:
    try:
        name, digest = raw.split("=", 1)
    except ValueError as exc:
        raise PublicationSnapshotError("publication tool digest must be NAME=SHA256") from exc
    if name not in _TOOL_FILES:
        raise PublicationSnapshotError("publication tool digest names an unsupported file")
    return name, _validate_digest(digest)


def _regular_names(directory: Path) -> list[str]:
    try:
        listed = directory.lstat()
    except OSError as exc:
        raise PublicationSnapshotError(f"publication payload directory is unavailable: {directory}") from exc
    if not stat.S_ISDIR(listed.st_mode):
        raise PublicationSnapshotError("publication payload source must be a directory")
    names: list[str] = []
    for path in directory.iterdir():
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            raise PublicationSnapshotError(f"publication payload must be a regular file: {path.name}")
        names.append(_validate_name(path.name))
    return sorted(names)


def describe_publication_payloads(
    directory: Path,
    selectors: Sequence[tuple[str, str]],
    *,
    maximum: int,
) -> list[LabelledArtifact]:
    if maximum <= 0:
        raise PublicationSnapshotError("publication payload byte limit must be positive")
    if not selectors:
        raise PublicationSnapshotError("at least one publication selector is required")
    labels = [_validate_label(label) for label, _ in selectors]
    if len(labels) != len(set(labels)):
        raise PublicationSnapshotError("publication descriptor labels must be unique")

    available = _regular_names(directory)
    selected: list[tuple[str, str]] = []
    for label, pattern in selectors:
        matches = [name for name in available if fnmatch.fnmatchcase(name, pattern)]
        if len(matches) != 1:
            raise PublicationSnapshotError(
                f"publication selector {label!r} matched {len(matches)} files instead of one"
            )
        selected.append((label, matches[0]))
    selected_names = [name for _, name in selected]
    if len(selected_names) != len(set(selected_names)) or set(selected_names) != set(available):
        raise PublicationSnapshotError("publication selectors must cover each payload exactly once")

    described: list[LabelledArtifact] = []
    for label, name in selected:
        with verified_payload_snapshot(
            directory / name,
            maximum=maximum,
            expected_digest=None,
            executable=False,
            prefix="tacit-release-descriptor-",
        ) as snapshot:
            described.append(LabelledArtifact(label, ArtifactSpec(name, snapshot.digest)))
    return described


def verify_publication_payloads(
    directory: Path,
    artifacts: Sequence[LabelledArtifact],
    *,
    maximum: int,
) -> None:
    specs = _validate_specs([item.artifact for item in artifacts])
    labels = [_validate_label(item.label) for item in artifacts]
    if len(labels) != len(set(labels)):
        raise PublicationSnapshotError("publication descriptor labels must be unique")
    if set(_regular_names(directory)) != {spec.name for spec in specs}:
        raise PublicationSnapshotError("publication payload set differs from its carried descriptors")
    for spec in specs:
        with verified_payload_snapshot(
            directory / spec.name,
            maximum=maximum,
            expected_digest=spec.digest,
            executable=False,
            prefix="tacit-release-verification-",
        ):
            pass


def create_publication_snapshot(
    source_directory: Path,
    destination_directory: Path,
    specs: Sequence[ArtifactSpec],
    *,
    maximum: int,
) -> None:
    validated = _validate_specs(specs)
    if set(_regular_names(source_directory)) != {spec.name for spec in validated}:
        raise PublicationSnapshotError("publication payload set differs from its carried descriptors")
    try:
        destination_directory.mkdir(mode=0o700)
    except OSError as exc:
        raise PublicationSnapshotError(
            f"private publication snapshot cannot be created: {destination_directory}"
        ) from exc

    try:
        for spec in validated:
            snapshot = copy_verified_payload(
                source_directory / spec.name,
                destination_directory / spec.name,
                maximum=maximum,
                expected_digest=spec.digest,
                executable=False,
            )
            os.chmod(snapshot.path, 0o400, follow_symlinks=False)
        os.chmod(destination_directory, 0o500, follow_symlinks=False)
    except BaseException:
        try:
            cleanup_private_directory(destination_directory, [spec.name for spec in validated])
        except (OSError, ReleasePayloadError):
            pass
        raise


def _require_linux_root() -> None:
    if not sys.platform.startswith("linux"):
        raise PublicationSnapshotError("sealed publication paths require Linux")
    if os.geteuid() != 0:
        raise PublicationSnapshotError("sealed publication paths require root")


def _validated_mountpoint_path(path: Path) -> Path:
    candidate = Path(os.path.abspath(path))
    workspace = Path.cwd()
    if candidate.parent != workspace or _MOUNT_NAME.fullmatch(candidate.name) is None:
        raise PublicationSnapshotError("publication mountpoint must be one direct workspace child")
    _directory_identity(workspace)
    return candidate


def _validated_cleanup_mountpoint_path(path: Path) -> Path:
    candidate = Path(os.path.abspath(path))
    if _MOUNT_NAME.fullmatch(candidate.name) is None:
        raise PublicationSnapshotError("publication cleanup mountpoint has an invalid name")
    return candidate


def _is_root_controlled_directory(path: Path) -> bool:
    metadata = path.lstat()
    return stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == 0 and not stat.S_IMODE(metadata.st_mode) & 0o022


def _action_path_anchor_directories(workspace: Path) -> list[Path]:
    anchors: list[Path] = []
    current = workspace
    for _ in range(_MAX_ACTION_PATH_ANCHORS):
        _directory_identity(current)
        anchors.append(current)
        parent = current.parent
        if parent == current:
            raise PublicationSnapshotError("publication workspace has no root-controlled pathname boundary")
        if _is_root_controlled_directory(parent):
            return list(reversed(anchors))
        current = parent
    raise PublicationSnapshotError("publication workspace pathname exceeds its anchor limit")


def _validated_backing_path(path: Path) -> Path:
    candidate = Path(os.path.abspath(path))
    if (
        candidate.parent != _BACKING_PARENT
        or not candidate.name.startswith(_BACKING_PREFIX)
        or _MOUNT_NAME.fullmatch(candidate.name) is None
    ):
        raise PublicationSnapshotError("publication backing directory must use the reserved /var/lib prefix")
    parent = candidate.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != 0 or stat.S_IMODE(parent.st_mode) & 0o022:
        raise PublicationSnapshotError("publication backing parent is not root-controlled")
    return candidate


def _validated_tool_path(path: Path) -> Path:
    candidate = Path(os.path.abspath(path))
    if (
        candidate.parent != _BACKING_PARENT
        or not candidate.name.startswith(_TOOL_PREFIX)
        or _MOUNT_NAME.fullmatch(candidate.name) is None
    ):
        raise PublicationSnapshotError("publication tool directory must use the reserved /var/lib prefix")
    parent = candidate.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != _ROOT_UID or stat.S_IMODE(parent.st_mode) & 0o022:
        raise PublicationSnapshotError("publication tool parent is not root-controlled")
    return candidate


def _mount_authority_path(backing: Path) -> Path:
    return backing.with_name(f"{backing.name}{_MOUNT_AUTHORITY_SUFFIX}")


def _tool_authority_path(tool: Path) -> Path:
    return tool.with_name(f"{tool.name}{_TOOL_AUTHORITY_SUFFIX}")


def _tool_cleanup_launcher_path(tool: Path) -> Path:
    return tool.with_name(f"{tool.name}{_TOOL_CLEANUP_SUFFIX}")


def _tool_cleanup_helper_path(tool: Path) -> Path:
    return tool.with_name(f"{tool.name}{_TOOL_CLEANUP_HELPER_SUFFIX}")


def _open_directory_descriptor(path: Path) -> int:
    flags = getattr(os, "O_PATH", os.O_RDONLY) | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PublicationSnapshotError("publication mount directory cannot be opened") from exc
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise PublicationSnapshotError("publication mount target is not a directory")
    return descriptor


def _descriptor_path(descriptor: int) -> str:
    return f"/proc/self/fd/{descriptor}"


def _mount_syscall(source: str | None, target: str, flags: int) -> None:
    try:
        function = ctypes.CDLL(None, use_errno=True).mount
    except AttributeError as exc:
        raise PublicationSnapshotError("Linux mount syscall is unavailable") from exc
    function.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_void_p]
    function.restype = ctypes.c_int
    source_bytes = None if source is None else os.fsencode(source)
    result = function(source_bytes, os.fsencode(target), None, flags, None)
    if result != 0:
        error = ctypes.get_errno()
        raise PublicationSnapshotError(f"publication mount syscall failed: {os.strerror(error)}")


def _umount_syscall(target: str) -> None:
    try:
        function = ctypes.CDLL(None, use_errno=True).umount2
    except AttributeError as exc:
        raise PublicationSnapshotError("Linux unmount syscall is unavailable") from exc
    function.argtypes = [ctypes.c_char_p, ctypes.c_int]
    function.restype = ctypes.c_int
    result = function(os.fsencode(target), 0)
    if result != 0:
        error = ctypes.get_errno()
        raise PublicationSnapshotError(f"publication unmount syscall failed: {os.strerror(error)}")


def _bound_directory_path(path: str) -> tuple[str, int | None]:
    if path.startswith("/proc/self/fd/") and path.removeprefix("/proc/self/fd/").isdigit():
        return path, None
    descriptor = _open_directory_descriptor(Path(path))
    return _descriptor_path(descriptor), descriptor


def _run_mount_command(command: Path, *arguments: str) -> None:
    descriptors: list[int] = []
    try:
        if command == _MOUNT_COMMAND and len(arguments) == 3 and arguments[0] == "--bind":
            source, source_descriptor = _bound_directory_path(arguments[1])
            target, target_descriptor = _bound_directory_path(arguments[2])
            descriptors.extend(item for item in (source_descriptor, target_descriptor) if item is not None)
            _mount_syscall(source, target, _MS_BIND)
            return
        if command == _MOUNT_COMMAND and len(arguments) == 2 and arguments[0] == "--make-slave":
            target, descriptor = _bound_directory_path(arguments[1])
            if descriptor is not None:
                descriptors.append(descriptor)
            _mount_syscall(None, target, _MS_SLAVE)
            return
        if command == _MOUNT_COMMAND and len(arguments) == 3 and arguments[0] == "-o":
            options = frozenset(arguments[1].split(","))
            if not {"remount", "bind"}.issubset(options):
                raise PublicationSnapshotError("publication remount options are invalid")
            supported = {"remount", "bind", "ro", "rw", "nodev", "noexec", "nosuid"}
            if not options.issubset(supported) or ({"ro", "rw"} <= options):
                raise PublicationSnapshotError("publication remount options are unsupported")
            flags = _MS_REMOUNT | _MS_BIND
            if "ro" in options:
                flags |= _MS_RDONLY
            if "nodev" in options:
                flags |= _MS_NODEV
            if "noexec" in options:
                flags |= _MS_NOEXEC
            if "nosuid" in options:
                flags |= _MS_NOSUID
            target, descriptor = _bound_directory_path(arguments[2])
            if descriptor is not None:
                descriptors.append(descriptor)
            _mount_syscall(None, target, flags)
            return
        if command == _UMOUNT_COMMAND and len(arguments) == 2 and arguments[0] == "--":
            # A mounted path is rename-protected by the kernel. Keeping an
            # O_PATH descriptor open to it can itself make umount report EBUSY,
            # so cleanup uses the mountinfo-verified pathname directly.
            _umount_syscall(arguments[1])
            return
        raise PublicationSnapshotError("unsupported publication mount operation")
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _decode_mountinfo_field(value: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), value)


def _mountinfo_entries(path: Path | None = None) -> list[_MountInfoEntry]:
    try:
        with _MOUNTINFO.open("r", encoding="utf-8") as stream:
            payload = stream.read(_MAX_MOUNTINFO_BYTES + 1)
    except OSError as exc:
        raise PublicationSnapshotError("Linux mount information is unavailable") from exc
    if len(payload) > _MAX_MOUNTINFO_BYTES:
        raise PublicationSnapshotError("Linux mount information exceeds its byte limit")

    expected = str(path) if path is not None else None
    matched: list[_MountInfoEntry] = []
    for line in payload.splitlines():
        fields = line.split()
        if len(fields) < 10 or "-" not in fields:
            raise PublicationSnapshotError("Linux mount information is malformed")
        separator = fields.index("-")
        if separator < 6 or separator + 3 >= len(fields):
            raise PublicationSnapshotError("Linux mount information is malformed")
        try:
            mount_id = int(fields[0])
            parent_id = int(fields[1])
        except ValueError as exc:
            raise PublicationSnapshotError("Linux mount information has an invalid mount identity") from exc
        mount_point = _decode_mountinfo_field(fields[4])
        if expected is None or mount_point == expected:
            matched.append(
                _MountInfoEntry(
                    mount_id=mount_id,
                    parent_id=parent_id,
                    device=fields[2],
                    root=_decode_mountinfo_field(fields[3]),
                    mount_point=mount_point,
                    options=frozenset(fields[5].split(",")),
                    optional_fields=frozenset(fields[6:separator]),
                    filesystem=fields[separator + 1],
                    source=_decode_mountinfo_field(fields[separator + 2]),
                    super_options=frozenset(fields[separator + 3].split(",")),
                )
            )
    return matched


def _mountinfo_by_id() -> dict[int, _MountInfoEntry]:
    entries = _mountinfo_entries()
    indexed = {entry.mount_id: entry for entry in entries}
    if len(indexed) != len(entries):
        raise PublicationSnapshotError("Linux mount information has duplicate mount identities")
    return indexed


def _directory_identity(path: Path) -> tuple[int, int]:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise PublicationSnapshotError("sealed publication path is not a directory")
    return metadata.st_dev, metadata.st_ino


def _created_mount_entries(previous_ids: set[int], expected_identity: tuple[int, int]) -> list[_MountInfoEntry]:
    created: list[_MountInfoEntry] = []
    for entry in _mountinfo_entries():
        if entry.mount_id in previous_ids:
            continue
        try:
            identity = _directory_identity(Path(entry.mount_point))
        except (OSError, PublicationSnapshotError):
            continue
        if identity == expected_identity:
            created.append(entry)
    return created


def _created_mount_entry(previous_ids: set[int], expected_identity: tuple[int, int]) -> _MountInfoEntry:
    created = _created_mount_entries(previous_ids, expected_identity)
    if len(created) != 1:
        raise PublicationSnapshotError("publication mount creation did not produce one mount identity")
    return created[0]


def _mount_is_shared(entry: _MountInfoEntry) -> bool:
    return any(field.startswith("shared:") for field in entry.optional_fields)


def _covering_mount_entry(path: Path) -> _MountInfoEntry:
    candidates = [
        entry
        for entry in _mountinfo_entries()
        if Path(entry.mount_point) == path or Path(entry.mount_point) in path.parents
    ]
    if not candidates:
        raise PublicationSnapshotError("publication path has no covering mount")
    return max(candidates, key=lambda entry: len(Path(entry.mount_point).parts))


def _isolate_destination_mount_propagation(destination: Path) -> None:
    covering = _covering_mount_entry(destination)
    if not _mount_is_shared(covering):
        return
    _run_mount_command(_MOUNT_COMMAND, "--make-slave", covering.mount_point)
    if _mount_is_shared(_covering_mount_entry(destination)):
        raise PublicationSnapshotError("publication destination mount propagation was not isolated")


def _create_bind_mount(
    source: Path,
    destination: Path,
    *,
    kind: str,
    remount_options: str,
    required_options: frozenset[str],
    on_intent: Callable[[_PendingMountIntent], None] | None = None,
    on_intent_cleared: Callable[[], None] | None = None,
    on_created: Callable[[_MountRecord], None] | None = None,
    on_rollback_intent: Callable[[_MountRecord], None] | None = None,
    on_rolled_back: Callable[[_MountRecord], None] | None = None,
) -> _MountRecord:
    _isolate_destination_mount_propagation(destination)
    source_descriptor = _open_directory_descriptor(source)
    destination_descriptor = source_descriptor if source == destination else _open_directory_descriptor(destination)
    expected_metadata = os.fstat(source_descriptor)
    target_metadata = os.fstat(destination_descriptor)
    expected_identity = (expected_metadata.st_dev, expected_metadata.st_ino)
    target_identity = (target_metadata.st_dev, target_metadata.st_ino)
    previous_ids = set(_mountinfo_by_id())
    intent = _PendingMountIntent(
        kind=kind,
        original_path=str(destination),
        device=expected_identity[0],
        inode=expected_identity[1],
        previous_mount_ids=tuple(sorted(previous_ids)),
        target_device=target_identity[0],
        target_inode=target_identity[1],
    )
    if on_intent is not None:
        on_intent(intent)
    created_id: int | None = None
    record: _MountRecord | None = None
    created_reported = False
    try:
        _run_mount_command(
            _MOUNT_COMMAND,
            "--bind",
            _descriptor_path(source_descriptor),
            _descriptor_path(destination_descriptor),
        )
        created = _created_mount_entry(previous_ids, expected_identity)
        created_id = created.mount_id
        record = _MountRecord(
            kind=kind,
            mount_id=created_id,
            original_path=created.mount_point,
            device=expected_identity[0],
            inode=expected_identity[1],
        )
        if on_created is not None:
            on_created(record)
            created_reported = True
        # Once attached, the kernel makes the mountpoint rename-busy. Operate
        # on the mountinfo-resolved path so propagation and remount address the
        # new mount rather than the descriptor's underlying pre-mount view.
        _run_mount_command(_MOUNT_COMMAND, "--make-slave", created.mount_point)
        isolated = _mountinfo_by_id().get(created_id)
        if isolated is None or _mount_is_shared(isolated):
            raise PublicationSnapshotError("publication mount propagation was not isolated")
        _run_mount_command(_MOUNT_COMMAND, "-o", remount_options, created.mount_point)
        current = _mountinfo_by_id().get(created_id)
        if current is None or not required_options.issubset(current.options):
            raise PublicationSnapshotError("publication mount options were not applied")
        current_path = Path(current.mount_point)
        if _directory_identity(current_path) != expected_identity:
            raise PublicationSnapshotError("publication mount identity changed during creation")
        requested_entries = [entry for entry in _mountinfo_entries(destination) if entry.mount_id == created_id]
        if len(requested_entries) != 1 or created.mount_point != str(destination):
            raise PublicationSnapshotError("publication mount target changed during creation")
        return record
    except BaseException:
        if created_id is None:
            newly_created = _created_mount_entries(previous_ids, expected_identity)
        else:
            entry = _mountinfo_by_id().get(created_id)
            newly_created = [entry] if entry is not None else []
        if record is None and len(newly_created) == 1:
            recovered = newly_created[0]
            if _directory_identity(Path(recovered.mount_point)) != expected_identity:
                raise PublicationSnapshotError("publication mount identity changed during failed creation")
            created_id = recovered.mount_id
            record = _MountRecord(
                kind=kind,
                mount_id=created_id,
                original_path=recovered.mount_point,
                device=expected_identity[0],
                inode=expected_identity[1],
            )
        if record is not None and not created_reported and on_created is not None:
            on_created(record)
            created_reported = True
        for entry in newly_created:
            try:
                if record is not None and entry.mount_id == record.mount_id and on_rollback_intent is not None:
                    on_rollback_intent(record)
                _run_mount_command(_UMOUNT_COMMAND, "--", entry.mount_point)
            except BaseException:
                continue
            if record is not None and entry.mount_id == record.mount_id and on_rolled_back is not None:
                on_rolled_back(record)
        if not newly_created and on_intent_cleared is not None:
            on_intent_cleared()
        raise
    finally:
        if destination_descriptor != source_descriptor:
            os.close(destination_descriptor)
        os.close(source_descriptor)


def _mount_record_payload(record: _MountRecord) -> dict[str, object]:
    return {
        "kind": record.kind,
        "mount_id": record.mount_id,
        "original_path": record.original_path,
        "device": record.device,
        "inode": record.inode,
    }


def _pending_mount_payload(intent: _PendingMountIntent) -> dict[str, object]:
    return {
        "kind": intent.kind,
        "original_path": intent.original_path,
        "device": intent.device,
        "inode": intent.inode,
        "target_device": intent.target_device,
        "target_inode": intent.target_inode,
        "previous_mount_ids": list(intent.previous_mount_ids),
    }


def _write_bytes(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise PublicationSnapshotError("publication mount authority write made no progress")
        offset += written


def _mount_authority_payload(
    *,
    backing: Path,
    workspace: Path,
    workspace_mount_id: int | None,
    workspace_identity: tuple[int, int],
    destination_name: str,
    destination_identity: tuple[int, int] | None,
    backing_identity: tuple[int, int] | None,
    artifact_names: Sequence[str],
    mounts: Sequence[_MountRecord],
    remaining_mount_ids: Sequence[int],
    pending_mount: _PendingMountIntent | None,
    pending_unmount_id: int | None,
    destination_removed: bool,
    backing_removed: bool,
    cleanup_phase: str,
    backing_phase: str,
    pending_copy_name: str | None,
    copied_artifact_names: Sequence[str],
) -> bytes:
    payload = json.dumps(
        {
            "version": 4,
            "workspace": {
                "path": str(workspace),
                "mount_id": workspace_mount_id,
                "device": workspace_identity[0],
                "inode": workspace_identity[1],
            },
            "destination_name": destination_name,
            "destination": (
                None
                if destination_identity is None
                else {"device": destination_identity[0], "inode": destination_identity[1]}
            ),
            "backing": {
                "path": str(backing),
                "identity": (
                    None if backing_identity is None else {"device": backing_identity[0], "inode": backing_identity[1]}
                ),
            },
            "artifact_names": list(artifact_names),
            "mounts": [_mount_record_payload(record) for record in mounts],
            "remaining_mount_ids": list(remaining_mount_ids),
            "pending_mount": None if pending_mount is None else _pending_mount_payload(pending_mount),
            "pending_unmount_id": pending_unmount_id,
            "destination_removed": destination_removed,
            "backing_removed": backing_removed,
            "cleanup_phase": cleanup_phase,
            "backing_phase": backing_phase,
            "pending_copy_name": pending_copy_name,
            "copied_artifact_names": list(copied_artifact_names),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if not payload or len(payload) > _MAX_MOUNT_AUTHORITY_BYTES:
        raise PublicationSnapshotError("publication mount authority exceeds its byte limit")
    return payload


def _write_mount_authority_payload(state_path: Path, payload: bytes, *, replace: bool) -> None:
    target = state_path
    temporary = state_path.with_name(f"{state_path.name}.next")
    try:
        metadata = temporary.lstat()
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != _ROOT_UID:
            raise PublicationSnapshotError("publication mount authority temporary file is unsafe")
        temporary.unlink()
        _fsync_directory(temporary.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        _write_bytes(descriptor, payload)
        os.fsync(descriptor)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    if replace:
        os.replace(temporary, target)
    else:
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError as exc:
            temporary.unlink(missing_ok=True)
            raise PublicationSnapshotError("publication mount authority already exists") from exc
        temporary.unlink()
    metadata = target.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != _ROOT_UID or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise PublicationSnapshotError("publication mount authority ownership or mode changed")
    directory_descriptor = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _tool_authority_payload(authority: _ToolAuthority) -> bytes:
    payload = json.dumps(
        {
            "version": 1,
            "tool_path": str(authority.tool_path),
            "digests": dict(authority.digests),
            "phase": authority.phase,
            "remaining_files": list(authority.remaining_files),
            "pending_delete": authority.pending_delete,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if not payload or len(payload) > _MAX_MOUNT_AUTHORITY_BYTES:
        raise PublicationSnapshotError("publication tool authority exceeds its byte limit")
    return payload


def _persist_tool_authority(authority: _ToolAuthority, *, replace: bool = True) -> None:
    _write_mount_authority_payload(
        authority.state_path,
        _tool_authority_payload(authority),
        replace=replace,
    )


def _read_tool_authority(tool: Path) -> _ToolAuthority:
    state_path = _tool_authority_path(tool)
    metadata = state_path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != _ROOT_UID
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size <= 0
        or metadata.st_size > _MAX_MOUNT_AUTHORITY_BYTES
    ):
        raise PublicationSnapshotError("publication tool authority ownership, mode, or size changed")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(state_path, flags)
    try:
        chunks: list[bytes] = []
        remaining = _MAX_MOUNT_AUTHORITY_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(payload) > _MAX_MOUNT_AUTHORITY_BYTES:
        raise PublicationSnapshotError("publication tool authority exceeds its byte limit")
    try:
        loaded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublicationSnapshotError("publication tool authority is malformed") from exc
    if not isinstance(loaded, dict) or set(loaded) != {
        "version",
        "tool_path",
        "digests",
        "phase",
        "remaining_files",
        "pending_delete",
    }:
        raise PublicationSnapshotError("publication tool authority has an unsupported shape")
    if loaded.get("version") != 1 or loaded.get("tool_path") != str(tool):
        raise PublicationSnapshotError("publication tool authority names another installation")
    raw_digests = loaded.get("digests")
    if (
        not isinstance(raw_digests, dict)
        or set(raw_digests) != set(_TOOL_FILES)
        or not all(isinstance(value, str) for value in raw_digests.values())
    ):
        raise PublicationSnapshotError("publication tool authority has invalid file digests")
    validated_digest_payload = cast(dict[str, str], raw_digests)
    digests = tuple((name, _validate_digest(validated_digest_payload[name])) for name in _TOOL_FILES)
    phase = loaded.get("phase")
    if phase not in {"installing", "installed", "removing"}:
        raise PublicationSnapshotError("publication tool authority has an invalid phase")
    raw_remaining = loaded.get("remaining_files")
    if not isinstance(raw_remaining, list) or not all(isinstance(name, str) for name in raw_remaining):
        raise PublicationSnapshotError("publication tool authority has invalid remaining files")
    remaining_files = tuple(raw_remaining)
    if len(remaining_files) != len(set(remaining_files)) or not set(remaining_files).issubset(_TOOL_FILES):
        raise PublicationSnapshotError("publication tool authority has inconsistent remaining files")
    pending_delete = loaded.get("pending_delete")
    if pending_delete is not None and (not isinstance(pending_delete, str) or pending_delete not in remaining_files):
        raise PublicationSnapshotError("publication tool authority has an invalid pending deletion")
    return _ToolAuthority(
        state_path=state_path,
        tool_path=tool,
        digests=digests,
        phase=phase,
        remaining_files=remaining_files,
        pending_delete=pending_delete,
    )


def _hash_tool_file(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0 or before.st_size > _MAX_TOOL_FILE_BYTES:
            raise PublicationSnapshotError("publication tool file is not a bounded regular file")
        digest = hashlib.sha256()
        remaining = _MAX_TOOL_FILE_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            digest.update(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if remaining <= 0 or (before.st_dev, before.st_ino, before.st_size) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
    ):
        raise PublicationSnapshotError("publication tool file changed while hashing")
    return digest.hexdigest()


def _verify_installed_tool_file(path: Path, digest: str) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != _ROOT_UID
        or stat.S_IMODE(metadata.st_mode) != 0o500
        or _hash_tool_file(path) != digest
    ):
        raise PublicationSnapshotError("installed publication tool file changed")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_tool_file(source: Path, target: Path, digest: str) -> None:
    if _path_exists_no_follow(target):
        _verify_installed_tool_file(target, digest)
        return
    temporary = target.with_name(f".{target.name}.next")
    if _path_exists_no_follow(temporary):
        metadata = temporary.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != _ROOT_UID:
            raise PublicationSnapshotError("partial publication tool file is unsafe")
        temporary.unlink()
        _fsync_directory(temporary.parent)

    source_flags = (
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    )
    source_descriptor = os.open(source, source_flags)
    destination_descriptor = -1
    try:
        before = os.fstat(source_descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0 or before.st_size > _MAX_TOOL_FILE_BYTES:
            raise PublicationSnapshotError("publication tool source is not a bounded regular file")
        destination_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            destination_flags |= os.O_NOFOLLOW
        destination_descriptor = os.open(temporary, destination_flags, 0o600)
        copied_digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = os.read(source_descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise PublicationSnapshotError("publication tool source shrank while copying")
            _write_bytes(destination_descriptor, chunk)
            copied_digest.update(chunk)
            remaining -= len(chunk)
        if os.read(source_descriptor, 1):
            raise PublicationSnapshotError("publication tool source grew while copying")
        after = os.fstat(source_descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise PublicationSnapshotError("publication tool source changed while copying")
        if copied_digest.hexdigest() != digest:
            raise PublicationSnapshotError("publication tool source digest changed")
        os.fchmod(destination_descriptor, 0o500)
        os.fsync(destination_descriptor)
        os.close(destination_descriptor)
        destination_descriptor = -1
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    except BaseException:
        if destination_descriptor >= 0:
            os.close(destination_descriptor)
            destination_descriptor = -1
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    finally:
        if destination_descriptor >= 0:
            os.close(destination_descriptor)
        os.close(source_descriptor)
    _verify_installed_tool_file(target, digest)


def _verify_cleanup_launcher(tool: Path, digests: dict[str, str]) -> None:
    _verify_installed_tool_file(_tool_cleanup_launcher_path(tool), digests[_TOOL_FILES[0]])
    _verify_installed_tool_file(_tool_cleanup_helper_path(tool), digests[_TOOL_FILES[1]])


def _install_cleanup_launcher(source_directory: Path, tool: Path, digests: dict[str, str]) -> Path:
    launcher = _tool_cleanup_launcher_path(tool)
    _copy_tool_file(source_directory / _TOOL_FILES[0], launcher, digests[_TOOL_FILES[0]])
    _copy_tool_file(
        source_directory / _TOOL_FILES[1],
        _tool_cleanup_helper_path(tool),
        digests[_TOOL_FILES[1]],
    )
    return launcher


def _remove_cleanup_launcher(tool: Path) -> None:
    launcher = _tool_cleanup_launcher_path(tool)
    if _SCRIPT_PATH != launcher:
        raise PublicationSnapshotError("publication cleanup launcher can remove only itself")
    helper = _tool_cleanup_helper_path(tool)
    for partial in (
        launcher.with_name(f"{launcher.name}.next"),
        helper.with_name(f"{helper.name}.next"),
    ):
        if not _path_exists_no_follow(partial):
            continue
        partial_metadata = partial.lstat()
        if (
            not stat.S_ISREG(partial_metadata.st_mode)
            or partial_metadata.st_uid != _ROOT_UID
            or stat.S_IMODE(partial_metadata.st_mode) not in {0o500, 0o600}
        ):
            raise PublicationSnapshotError("publication cleanup partial is unsafe")
        partial.unlink()
        _fsync_directory(partial.parent)
    if _path_exists_no_follow(helper):
        helper_metadata = helper.lstat()
        if (
            not stat.S_ISREG(helper_metadata.st_mode)
            or helper_metadata.st_uid != _ROOT_UID
            or stat.S_IMODE(helper_metadata.st_mode) != 0o500
        ):
            raise PublicationSnapshotError("publication cleanup helper is unsafe")
        helper.unlink()
        _fsync_directory(helper.parent)
    metadata = launcher.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != _ROOT_UID or stat.S_IMODE(metadata.st_mode) != 0o500:
        raise PublicationSnapshotError("publication cleanup launcher is unsafe")
    launcher.unlink()
    _fsync_directory(launcher.parent)


def install_publication_tool(source_directory: Path, tool_directory: Path, digests: dict[str, str]) -> None:
    _require_linux_root()
    tool = _validated_tool_path(tool_directory)
    validated_digests = {name: _validate_digest(digests.get(name, "")) for name in _TOOL_FILES}
    if set(digests) != set(_TOOL_FILES):
        raise PublicationSnapshotError("publication tool install requires the exact script set")
    state_path = _tool_authority_path(tool)
    if state_path.exists():
        authority = _read_tool_authority(tool)
        if dict(authority.digests) != validated_digests:
            raise PublicationSnapshotError("publication tool install digests changed during recovery")
        if authority.phase == "removing":
            _remove_installed_tool(tool, require_active=False)
            return install_publication_tool(source_directory, tool, validated_digests)
        if authority.phase == "installed":
            _verify_cleanup_launcher(tool, validated_digests)
            metadata = tool.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != _ROOT_UID
                or stat.S_IMODE(metadata.st_mode) != 0o700
                or {entry.name for entry in tool.iterdir()} != set(_TOOL_FILES)
            ):
                raise PublicationSnapshotError("installed publication tool contents changed")
            for name in _TOOL_FILES:
                _verify_installed_tool_file(tool / name, validated_digests[name])
            return
    else:
        if _path_exists_no_follow(tool):
            raise PublicationSnapshotError("publication tool exists without durable authority")
        _install_cleanup_launcher(source_directory, tool, validated_digests)
        authority = _ToolAuthority(
            state_path=state_path,
            tool_path=tool,
            digests=tuple((name, validated_digests[name]) for name in _TOOL_FILES),
            phase="installing",
            remaining_files=(),
            pending_delete=None,
        )
        _persist_tool_authority(authority, replace=False)

    _install_cleanup_launcher(source_directory, tool, validated_digests)

    if not _path_exists_no_follow(tool):
        tool.mkdir(mode=0o700)
        _fsync_directory(tool.parent)
    metadata = tool.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != _ROOT_UID or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise PublicationSnapshotError("installed publication tool directory is unsafe")

    for name in _TOOL_FILES:
        _copy_tool_file(source_directory / name, tool / name, validated_digests[name])

    if {entry.name for entry in tool.iterdir()} != set(_TOOL_FILES):
        raise PublicationSnapshotError("publication tool install left unexpected files")
    authority = authority._replace(phase="installed", remaining_files=_TOOL_FILES, pending_delete=None)
    _persist_tool_authority(authority)


def _write_mount_authority(
    backing: Path,
    workspace: Path,
    destination: Path,
    destination_identity: tuple[int, int],
    artifact_names: Sequence[str],
    mounts: Sequence[_MountRecord],
) -> Path:
    workspace_records = [record for record in mounts if record.kind == "workspace"]
    payload_records = [record for record in mounts if record.kind == "payload"]
    if len(workspace_records) != 1 or len(payload_records) != 1:
        raise PublicationSnapshotError("publication mount authority has an incomplete action path")
    state_path = _mount_authority_path(backing)
    backing_identity = _directory_identity(backing)
    payload = _mount_authority_payload(
        backing=backing,
        workspace=workspace,
        workspace_mount_id=workspace_records[0].mount_id,
        workspace_identity=(workspace_records[0].device, workspace_records[0].inode),
        destination_name=destination.name,
        destination_identity=destination_identity,
        backing_identity=backing_identity,
        artifact_names=artifact_names,
        mounts=mounts,
        remaining_mount_ids=[record.mount_id for record in mounts],
        pending_mount=None,
        pending_unmount_id=None,
        destination_removed=False,
        backing_removed=False,
        cleanup_phase="sealed",
        backing_phase="ready",
        pending_copy_name=None,
        copied_artifact_names=artifact_names,
    )
    _write_mount_authority_payload(state_path, payload, replace=state_path.exists())
    return state_path


def _write_partial_mount_authority(
    backing: Path,
    workspace: Path,
    destination: Path,
    workspace_identity: tuple[int, int],
    destination_identity: tuple[int, int] | None,
    backing_identity: tuple[int, int] | None,
    artifact_names: Sequence[str],
    mounts: Sequence[_MountRecord],
    pending_mount: _PendingMountIntent | None = None,
    pending_unmount_id: int | None = None,
    backing_phase: str = "absent",
    pending_copy_name: str | None = None,
    copied_artifact_names: Sequence[str] = (),
    remaining_mount_ids: Sequence[int] | None = None,
    destination_removed: bool | None = None,
    replace: bool = False,
) -> Path:
    workspace_records = [record for record in mounts if record.kind == "workspace"]
    state_path = _mount_authority_path(backing)
    payload = _mount_authority_payload(
        backing=backing,
        workspace=workspace,
        workspace_mount_id=workspace_records[0].mount_id if workspace_records else None,
        workspace_identity=workspace_identity,
        destination_name=destination.name,
        destination_identity=destination_identity,
        backing_identity=backing_identity,
        artifact_names=artifact_names,
        mounts=mounts,
        remaining_mount_ids=(
            [record.mount_id for record in mounts] if remaining_mount_ids is None else remaining_mount_ids
        ),
        pending_mount=pending_mount,
        pending_unmount_id=pending_unmount_id,
        destination_removed=(destination_identity is None if destination_removed is None else destination_removed),
        backing_removed=backing_phase == "removed",
        cleanup_phase="rollback",
        backing_phase=backing_phase,
        pending_copy_name=pending_copy_name,
        copied_artifact_names=copied_artifact_names,
    )
    _write_mount_authority_payload(state_path, payload, replace=replace)
    return state_path


def _persist_mount_authority(authority: _MountAuthority) -> None:
    payload = _mount_authority_payload(
        backing=authority.state_path.with_name(authority.state_path.name.removesuffix(_MOUNT_AUTHORITY_SUFFIX)),
        workspace=authority.workspace_path,
        workspace_mount_id=authority.workspace_mount_id,
        workspace_identity=authority.workspace_identity,
        destination_name=authority.destination_name,
        destination_identity=authority.destination_identity,
        backing_identity=authority.backing_identity,
        artifact_names=authority.artifact_names,
        mounts=authority.mounts,
        remaining_mount_ids=authority.remaining_mount_ids,
        pending_mount=authority.pending_mount,
        pending_unmount_id=authority.pending_unmount_id,
        destination_removed=authority.destination_removed,
        backing_removed=authority.backing_removed,
        cleanup_phase=authority.cleanup_phase,
        backing_phase=authority.backing_phase,
        pending_copy_name=authority.pending_copy_name,
        copied_artifact_names=authority.copied_artifact_names,
    )
    _write_mount_authority_payload(authority.state_path, payload, replace=True)


def _positive_json_integer(raw: object, label: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise PublicationSnapshotError(f"publication mount authority has an invalid {label}")
    return raw


def _identity_from_payload(raw: object, label: str) -> tuple[int, int]:
    if not isinstance(raw, dict) or set(raw) != {"device", "inode"}:
        raise PublicationSnapshotError(f"publication mount authority has an invalid {label}")
    return (
        _positive_json_integer(raw.get("device"), f"{label} device"),
        _positive_json_integer(raw.get("inode"), f"{label} inode"),
    )


def _read_mount_authority(backing: Path, names: Sequence[str]) -> _MountAuthority:
    state_path = _mount_authority_path(backing)
    metadata = state_path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size <= 0
        or metadata.st_size > _MAX_MOUNT_AUTHORITY_BYTES
    ):
        raise PublicationSnapshotError("publication mount authority ownership, mode, or size changed")
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(state_path, flags)
    try:
        chunks: list[bytes] = []
        remaining = _MAX_MOUNT_AUTHORITY_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(payload) > _MAX_MOUNT_AUTHORITY_BYTES:
        raise PublicationSnapshotError("publication mount authority exceeds its byte limit")
    try:
        loaded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublicationSnapshotError("publication mount authority is malformed") from exc
    expected_keys = {
        "version",
        "workspace",
        "destination_name",
        "destination",
        "backing",
        "artifact_names",
        "mounts",
        "remaining_mount_ids",
        "pending_mount",
        "pending_unmount_id",
        "destination_removed",
        "backing_removed",
        "cleanup_phase",
        "backing_phase",
        "pending_copy_name",
        "copied_artifact_names",
    }
    if not isinstance(loaded, dict) or set(loaded) != expected_keys or loaded.get("version") != 4:
        raise PublicationSnapshotError("publication mount authority has an unsupported shape")

    workspace_payload = loaded.get("workspace")
    if not isinstance(workspace_payload, dict) or set(workspace_payload) != {
        "path",
        "mount_id",
        "device",
        "inode",
    }:
        raise PublicationSnapshotError("publication mount authority has an invalid workspace")
    workspace_path_raw = workspace_payload.get("path")
    if not isinstance(workspace_path_raw, str) or len(workspace_path_raw) > 4096:
        raise PublicationSnapshotError("publication mount authority has an invalid workspace path")
    workspace_path = Path(workspace_path_raw)
    if not workspace_path.is_absolute():
        raise PublicationSnapshotError("publication mount authority workspace must be absolute")
    workspace_mount_id_raw = workspace_payload.get("mount_id")
    workspace_mount_id = (
        None
        if workspace_mount_id_raw is None
        else _positive_json_integer(workspace_mount_id_raw, "workspace mount identity")
    )
    workspace_identity = (
        _positive_json_integer(workspace_payload.get("device"), "workspace device"),
        _positive_json_integer(workspace_payload.get("inode"), "workspace inode"),
    )

    destination_name_raw = loaded.get("destination_name")
    if (
        not isinstance(destination_name_raw, str)
        or Path(destination_name_raw).name != destination_name_raw
        or _MOUNT_NAME.fullmatch(destination_name_raw) is None
    ):
        raise PublicationSnapshotError("publication mount authority has an invalid destination name")
    destination_name = destination_name_raw
    destination_payload = loaded.get("destination")
    destination_identity = (
        None if destination_payload is None else _identity_from_payload(destination_payload, "destination identity")
    )

    backing_payload = loaded.get("backing")
    if not isinstance(backing_payload, dict) or set(backing_payload) != {"path", "identity"}:
        raise PublicationSnapshotError("publication mount authority has an invalid backing identity")
    if backing_payload.get("path") != str(backing):
        raise PublicationSnapshotError("publication mount authority names another backing directory")
    backing_identity_payload = backing_payload.get("identity")
    backing_identity = (
        None
        if backing_identity_payload is None
        else _identity_from_payload(backing_identity_payload, "backing identity")
    )

    artifact_payload = loaded.get("artifact_names")
    if not isinstance(artifact_payload, list) or not all(isinstance(name, str) for name in artifact_payload):
        raise PublicationSnapshotError("publication mount authority has invalid artifact names")
    artifact_names = tuple(_validate_name(name) for name in artifact_payload)
    if artifact_names != tuple(names):
        raise PublicationSnapshotError("publication mount authority artifact names changed")

    mount_payload = loaded.get("mounts")
    if not isinstance(mount_payload, list) or len(mount_payload) > _MAX_ACTION_PATH_ANCHORS + 1:
        raise PublicationSnapshotError("publication mount authority has an invalid mount count")
    mounts: list[_MountRecord] = []
    for item in mount_payload:
        if not isinstance(item, dict) or set(item) != {"kind", "mount_id", "original_path", "device", "inode"}:
            raise PublicationSnapshotError("publication mount authority has an invalid mount record")
        kind = item.get("kind")
        original_path = item.get("original_path")
        if kind not in {"ancestor", "workspace", "payload"}:
            raise PublicationSnapshotError("publication mount authority has an invalid mount kind")
        if not isinstance(original_path, str) or len(original_path) > 4096 or not Path(original_path).is_absolute():
            raise PublicationSnapshotError("publication mount authority has an invalid original mount path")
        mounts.append(
            _MountRecord(
                kind=kind,
                mount_id=_positive_json_integer(item.get("mount_id"), "mount identity"),
                original_path=original_path,
                device=_positive_json_integer(item.get("device"), "mount device"),
                inode=_positive_json_integer(item.get("inode"), "mount inode"),
            )
        )
    mount_ids = [record.mount_id for record in mounts]
    if len(mount_ids) != len(set(mount_ids)):
        raise PublicationSnapshotError("publication mount authority repeats a mount identity")

    pending_mount_payload = loaded.get("pending_mount")
    pending_mount: _PendingMountIntent | None = None
    if pending_mount_payload is not None:
        if not isinstance(pending_mount_payload, dict) or set(pending_mount_payload) != {
            "kind",
            "original_path",
            "device",
            "inode",
            "target_device",
            "target_inode",
            "previous_mount_ids",
        }:
            raise PublicationSnapshotError("publication mount authority has an invalid pending mount")
        pending_kind = pending_mount_payload.get("kind")
        pending_path = pending_mount_payload.get("original_path")
        previous_payload = pending_mount_payload.get("previous_mount_ids")
        if pending_kind not in {"ancestor", "workspace", "payload"}:
            raise PublicationSnapshotError("publication mount authority has an invalid pending mount kind")
        if not isinstance(pending_path, str) or len(pending_path) > 4096 or not Path(pending_path).is_absolute():
            raise PublicationSnapshotError("publication mount authority has an invalid pending mount path")
        if not isinstance(previous_payload, list):
            raise PublicationSnapshotError("publication mount authority has invalid prior mount identities")
        previous_mount_ids = tuple(_positive_json_integer(value, "prior mount identity") for value in previous_payload)
        if len(previous_mount_ids) != len(set(previous_mount_ids)):
            raise PublicationSnapshotError("publication mount authority repeats a prior mount identity")
        pending_mount = _PendingMountIntent(
            kind=pending_kind,
            original_path=pending_path,
            device=_positive_json_integer(pending_mount_payload.get("device"), "pending mount device"),
            inode=_positive_json_integer(pending_mount_payload.get("inode"), "pending mount inode"),
            previous_mount_ids=previous_mount_ids,
            target_device=_positive_json_integer(
                pending_mount_payload.get("target_device"), "pending mount target device"
            ),
            target_inode=_positive_json_integer(
                pending_mount_payload.get("target_inode"), "pending mount target inode"
            ),
        )
    workspace_records = [record for record in mounts if record.kind == "workspace"]
    payload_records = [record for record in mounts if record.kind == "payload"]
    if len(workspace_records) > 1 or len(payload_records) > 1:
        raise PublicationSnapshotError("publication mount authority action path changed")
    if workspace_records and (
        workspace_records[0].mount_id != workspace_mount_id or workspace_records[0].original_path != str(workspace_path)
    ):
        raise PublicationSnapshotError("publication mount authority workspace changed")
    if not workspace_records and workspace_mount_id is not None:
        raise PublicationSnapshotError("publication mount authority workspace identity is incomplete")
    remaining_payload = loaded.get("remaining_mount_ids")
    if not isinstance(remaining_payload, list):
        raise PublicationSnapshotError("publication mount authority has invalid remaining mounts")
    remaining_mount_ids = tuple(
        _positive_json_integer(value, "remaining mount identity") for value in remaining_payload
    )
    if len(remaining_mount_ids) != len(set(remaining_mount_ids)) or not set(remaining_mount_ids).issubset(mount_ids):
        raise PublicationSnapshotError("publication mount authority has inconsistent remaining mounts")
    pending_payload = loaded.get("pending_unmount_id")
    pending_unmount_id = (
        None if pending_payload is None else _positive_json_integer(pending_payload, "pending unmount identity")
    )
    if pending_unmount_id is not None and pending_unmount_id not in remaining_mount_ids:
        raise PublicationSnapshotError("publication mount authority pending unmount is inconsistent")
    destination_removed = loaded.get("destination_removed")
    backing_removed = loaded.get("backing_removed")
    cleanup_phase = loaded.get("cleanup_phase")
    backing_phase = loaded.get("backing_phase")
    pending_copy_name = loaded.get("pending_copy_name")
    copied_artifact_payload = loaded.get("copied_artifact_names")
    if not isinstance(destination_removed, bool) or not isinstance(backing_removed, bool):
        raise PublicationSnapshotError("publication mount authority cleanup flags are invalid")
    if cleanup_phase not in {
        "sealed",
        "rollback",
        "unmounting",
        "removing_destination",
        "removing_backing",
        "complete",
    }:
        raise PublicationSnapshotError("publication mount authority cleanup phase is invalid")
    if backing_phase not in {"absent", "creating", "copying", "ready", "removing", "removed"}:
        raise PublicationSnapshotError("publication mount authority backing phase is invalid")
    if pending_copy_name is not None:
        if not isinstance(pending_copy_name, str) or _validate_name(pending_copy_name) not in artifact_names:
            raise PublicationSnapshotError("publication mount authority pending copy is invalid")
    if not isinstance(copied_artifact_payload, list) or not all(
        isinstance(name, str) for name in copied_artifact_payload
    ):
        raise PublicationSnapshotError("publication mount authority copied artifacts are invalid")
    copied_artifact_names = tuple(_validate_name(name) for name in copied_artifact_payload)
    if len(copied_artifact_names) != len(set(copied_artifact_names)) or not set(copied_artifact_names).issubset(
        artifact_names
    ):
        raise PublicationSnapshotError("publication mount authority copied artifacts are inconsistent")
    if backing_phase == "ready" and (
        backing_identity is None or pending_copy_name is not None or set(copied_artifact_names) != set(artifact_names)
    ):
        raise PublicationSnapshotError("publication mount authority backing state is incomplete")
    if backing_phase == "removed" and not backing_removed:
        raise PublicationSnapshotError("publication mount authority backing removal is inconsistent")
    if cleanup_phase == "sealed" and (
        len(workspace_records) != 1
        or len(payload_records) != 1
        or destination_identity is None
        or backing_identity is None
        or pending_mount is not None
    ):
        raise PublicationSnapshotError("sealed publication mount authority is incomplete")
    if (
        cleanup_phase == "sealed"
        and payload_records
        and payload_records[0].original_path != str(workspace_path / destination_name)
    ):
        raise PublicationSnapshotError("sealed publication mount authority payload changed")
    return _MountAuthority(
        state_path=state_path,
        workspace_path=workspace_path,
        workspace_mount_id=workspace_mount_id,
        workspace_identity=workspace_identity,
        destination_name=destination_name,
        destination_identity=destination_identity,
        backing_identity=backing_identity,
        artifact_names=artifact_names,
        mounts=tuple(mounts),
        remaining_mount_ids=remaining_mount_ids,
        pending_unmount_id=pending_unmount_id,
        destination_removed=destination_removed,
        backing_removed=backing_removed,
        cleanup_phase=cleanup_phase,
        pending_mount=pending_mount,
        backing_phase=backing_phase,
        pending_copy_name=pending_copy_name,
        copied_artifact_names=copied_artifact_names,
    )


def _recover_pending_mount(authority: _MountAuthority) -> _MountAuthority:
    intent = authority.pending_mount
    if intent is None:
        return authority
    expected_identity = (intent.device, intent.inode)
    created = _created_mount_entries(set(intent.previous_mount_ids), expected_identity)
    if not created:
        return _updated_mount_authority(authority, pending_mount=None)
    if len(created) != 1:
        raise PublicationSnapshotError("pending publication mount intent has an ambiguous kernel result")
    entry = created[0]
    if _mount_is_shared(entry):
        _run_mount_command(_MOUNT_COMMAND, "--make-slave", entry.mount_point)
        isolated = _mountinfo_by_id().get(entry.mount_id)
        if isolated is None or _mount_is_shared(isolated):
            raise PublicationSnapshotError("pending publication mount propagation could not be isolated")
        entry = isolated
    record = _MountRecord(
        kind=intent.kind,
        mount_id=entry.mount_id,
        original_path=entry.mount_point,
        device=intent.device,
        inode=intent.inode,
    )
    return _updated_mount_authority(
        authority,
        workspace_mount_id=(entry.mount_id if intent.kind == "workspace" else authority.workspace_mount_id),
        mounts=(*authority.mounts, record),
        remaining_mount_ids=(*authority.remaining_mount_ids, entry.mount_id),
        pending_mount=None,
    )


def _verified_recorded_mounts(authority: _MountAuthority) -> dict[int, _MountInfoEntry]:
    mounted = _mountinfo_by_id()
    selected: dict[int, _MountInfoEntry] = {}
    for record in authority.mounts:
        entry = mounted.get(record.mount_id)
        expected = record.mount_id in authority.remaining_mount_ids
        pending = record.mount_id == authority.pending_unmount_id
        if entry is None:
            if expected and not pending:
                raise PublicationSnapshotError("recorded publication mount identity is absent")
            continue
        if not expected:
            raise PublicationSnapshotError("completed publication unmount remains active")
        required = _REQUIRED_MOUNT_OPTIONS if record.kind == "payload" else _REQUIRED_ANCHOR_OPTIONS
        if authority.cleanup_phase != "rollback" and not required.issubset(entry.options):
            raise PublicationSnapshotError("recorded publication mount options changed")
        if _mount_is_shared(entry):
            if authority.cleanup_phase != "rollback":
                raise PublicationSnapshotError("recorded publication mount propagation changed")
            _run_mount_command(_MOUNT_COMMAND, "--make-slave", entry.mount_point)
            isolated = _mountinfo_by_id().get(record.mount_id)
            if isolated is None or _mount_is_shared(isolated):
                raise PublicationSnapshotError("rollback publication mount propagation could not be isolated")
            entry = isolated
        if _directory_identity(Path(entry.mount_point)) != (record.device, record.inode):
            raise PublicationSnapshotError("recorded publication mount directory changed")
        selected[record.mount_id] = entry
    return selected


def _verify_sealed_layout(directory: Path, names: Sequence[str]) -> None:
    metadata = directory.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != _SEALED_DIRECTORY_MODE
    ):
        raise PublicationSnapshotError("sealed publication directory ownership or mode changed")
    actual = set(_regular_names(directory))
    if actual != set(names):
        raise PublicationSnapshotError("sealed publication payload set changed")
    for name in names:
        file_metadata = (directory / name).lstat()
        if (
            not stat.S_ISREG(file_metadata.st_mode)
            or file_metadata.st_uid != 0
            or stat.S_IMODE(file_metadata.st_mode) != _SEALED_FILE_MODE
        ):
            raise PublicationSnapshotError("sealed publication artifact ownership or mode changed")


def _verify_sealed_mount(
    destination_directory: Path,
    backing_directory: Path,
    artifacts: Sequence[LabelledArtifact],
    *,
    maximum: int,
    mount_id: int,
) -> None:
    entries = [entry for entry in _mountinfo_entries(destination_directory) if entry.mount_id == mount_id]
    if len(entries) != 1 or not _REQUIRED_MOUNT_OPTIONS.issubset(entries[0].options) or _mount_is_shared(entries[0]):
        raise PublicationSnapshotError("publisher path is not one read-only hardened bind mount")
    if _directory_identity(destination_directory) != _directory_identity(backing_directory):
        raise PublicationSnapshotError("publisher path is not bound to the sealed backing directory")
    names = [item.artifact.name for item in artifacts]
    _verify_sealed_layout(backing_directory, names)
    _verify_sealed_layout(destination_directory, names)
    verify_publication_payloads(destination_directory, artifacts, maximum=maximum)


def _remove_empty_mountpoint(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != _EMPTY_MOUNTPOINT_MODE
        or any(path.iterdir())
    ):
        raise PublicationSnapshotError("publication mountpoint cannot be safely removed")
    path.rmdir()


def _remove_backing_directory(
    path: Path,
    names: Sequence[str],
    *,
    pending_copy_name: str | None = None,
) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != _ROOT_UID:
        raise PublicationSnapshotError("publication backing directory cannot be safely removed")
    os.chmod(path, 0o700, follow_symlinks=False)
    cleanup_private_directory(path, names, pending_temporary_name=pending_copy_name)


def _updated_mount_authority(
    authority: _MountAuthority,
    *,
    workspace_mount_id: int | None | object = _UNSET_PENDING_MOUNT,
    mounts: tuple[_MountRecord, ...] | None = None,
    remaining_mount_ids: tuple[int, ...] | None = None,
    pending_mount: _PendingMountIntent | None | object = _UNSET_PENDING_MOUNT,
    pending_unmount_id: int | None | object = _UNSET_PENDING_UNMOUNT,
    destination_identity: tuple[int, int] | None | object = _UNSET_PENDING_MOUNT,
    destination_removed: bool | None = None,
    backing_removed: bool | None = None,
    cleanup_phase: str | None = None,
    backing_identity: tuple[int, int] | None | object = _UNSET_PENDING_MOUNT,
    backing_phase: str | None = None,
    pending_copy_name: str | None | object = _UNSET_PENDING_MOUNT,
    copied_artifact_names: tuple[str, ...] | None = None,
) -> _MountAuthority:
    updated = authority._replace(
        workspace_mount_id=(
            authority.workspace_mount_id
            if workspace_mount_id is _UNSET_PENDING_MOUNT
            else cast(int | None, workspace_mount_id)
        ),
        mounts=authority.mounts if mounts is None else mounts,
        remaining_mount_ids=(authority.remaining_mount_ids if remaining_mount_ids is None else remaining_mount_ids),
        pending_mount=(
            authority.pending_mount
            if pending_mount is _UNSET_PENDING_MOUNT
            else cast(_PendingMountIntent | None, pending_mount)
        ),
        pending_unmount_id=(
            authority.pending_unmount_id
            if pending_unmount_id is _UNSET_PENDING_UNMOUNT
            else cast(int | None, pending_unmount_id)
        ),
        destination_identity=(
            authority.destination_identity
            if destination_identity is _UNSET_PENDING_MOUNT
            else cast(tuple[int, int] | None, destination_identity)
        ),
        destination_removed=(authority.destination_removed if destination_removed is None else destination_removed),
        backing_removed=authority.backing_removed if backing_removed is None else backing_removed,
        cleanup_phase=authority.cleanup_phase if cleanup_phase is None else cleanup_phase,
        backing_identity=(
            authority.backing_identity
            if backing_identity is _UNSET_PENDING_MOUNT
            else cast(tuple[int, int] | None, backing_identity)
        ),
        backing_phase=authority.backing_phase if backing_phase is None else backing_phase,
        pending_copy_name=(
            authority.pending_copy_name
            if pending_copy_name is _UNSET_PENDING_MOUNT
            else cast(str | None, pending_copy_name)
        ),
        copied_artifact_names=(
            authority.copied_artifact_names if copied_artifact_names is None else copied_artifact_names
        ),
    )
    _persist_mount_authority(updated)
    return updated


def _path_exists_no_follow(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _remove_installed_tool(path: Path, *, require_active: bool = True) -> None:
    tool = _validated_tool_path(path)
    launcher = _tool_cleanup_launcher_path(tool)
    if require_active and _SCRIPT_PATH != launcher:
        raise PublicationSnapshotError("publication cleanup can remove only its active installed tool")
    try:
        authority = _read_tool_authority(tool)
    except FileNotFoundError:
        if _path_exists_no_follow(tool):
            raise PublicationSnapshotError("publication tool exists without durable authority") from None
        if require_active and _path_exists_no_follow(launcher):
            _remove_cleanup_launcher(tool)
            return
        raise
    expected_digests = dict(authority.digests)
    _verify_cleanup_launcher(tool, expected_digests)
    if not _path_exists_no_follow(tool):
        if authority.phase == "installed" or authority.remaining_files:
            raise PublicationSnapshotError("publication tool disappeared before its files were retired")
        authority.state_path.unlink()
        _fsync_directory(authority.state_path.parent)
        if require_active:
            _remove_cleanup_launcher(tool)
        return
    metadata = tool.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != _ROOT_UID or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise PublicationSnapshotError("installed publication tool directory is unsafe")
    if authority.phase == "installing":
        installed_names = {entry.name for entry in tool.iterdir()}
        partial_names = {f".{name}.next" for name in _TOOL_FILES}
        if not installed_names.issubset(set(_TOOL_FILES) | partial_names):
            raise PublicationSnapshotError("partial publication tool contents changed")
        for name in _TOOL_FILES:
            installed = tool / name
            if _path_exists_no_follow(installed):
                _verify_installed_tool_file(installed, expected_digests[name])
                installed.unlink()
                _fsync_directory(tool)
            partial = tool / f".{name}.next"
            if _path_exists_no_follow(partial):
                metadata = partial.lstat()
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_uid != _ROOT_UID
                    or stat.S_IMODE(metadata.st_mode) not in {0o500, 0o600}
                ):
                    raise PublicationSnapshotError("partial publication tool file is unsafe")
                partial.unlink()
                _fsync_directory(tool)
        if any(tool.iterdir()):
            raise PublicationSnapshotError("partial publication tool cleanup left unexpected files")
        tool.rmdir()
        _fsync_directory(tool.parent)
        authority.state_path.unlink()
        _fsync_directory(authority.state_path.parent)
        if require_active:
            _remove_cleanup_launcher(tool)
        return
    if authority.phase == "installed":
        installed_names = {entry.name for entry in tool.iterdir()}
        missing_names = set(_TOOL_FILES) - installed_names
        if missing_names:
            raise PublicationSnapshotError("publication tool file disappeared before its deletion intent")
        if installed_names != set(_TOOL_FILES):
            raise PublicationSnapshotError("installed publication tool contents changed")
        for name in _TOOL_FILES:
            _verify_installed_tool_file(tool / name, expected_digests[name])
        authority = authority._replace(phase="removing", remaining_files=_TOOL_FILES, pending_delete=None)
        _persist_tool_authority(authority)

    for name in _TOOL_DELETE_ORDER:
        if name not in authority.remaining_files:
            continue
        script = tool / name
        existed = _path_exists_no_follow(script)
        if not existed and authority.pending_delete != name:
            raise PublicationSnapshotError("publication tool file disappeared before its deletion intent")
        if existed:
            if authority.pending_delete not in {None, name}:
                raise PublicationSnapshotError("publication tool deletion intent names another file")
            if authority.pending_delete is None:
                authority = authority._replace(pending_delete=name)
                _persist_tool_authority(authority)
            _verify_installed_tool_file(script, expected_digests[name])
            script.unlink()
            _fsync_directory(tool)
        authority = authority._replace(
            remaining_files=tuple(remaining for remaining in authority.remaining_files if remaining != name),
            pending_delete=None,
        )
        _persist_tool_authority(authority)
    if any(tool.iterdir()):
        raise PublicationSnapshotError("publication tool removal left unexpected files")
    tool.rmdir()
    _fsync_directory(tool.parent)
    authority.state_path.unlink()
    _fsync_directory(authority.state_path.parent)
    if require_active:
        _remove_cleanup_launcher(tool)


def unseal_publication_snapshot(
    destination_directory: Path,
    backing_directory: Path,
    names: Sequence[str],
    *,
    tool_directory: Path | None = None,
) -> None:
    _require_linux_root()
    destination = _validated_cleanup_mountpoint_path(destination_directory)
    backing = _validated_backing_path(backing_directory)
    validated_names = [_validate_name(name) for name in names]
    if len(validated_names) != len(set(validated_names)):
        raise PublicationSnapshotError("publication artifact names must be unique")
    try:
        authority = _read_mount_authority(backing, validated_names)
    except FileNotFoundError:
        if not _path_exists_no_follow(destination) and not _path_exists_no_follow(backing):
            if tool_directory is not None and (
                _path_exists_no_follow(tool_directory)
                or _path_exists_no_follow(_tool_cleanup_launcher_path(tool_directory))
            ):
                _remove_installed_tool(tool_directory)
            return
        raise PublicationSnapshotError("publication cleanup authority is absent") from None
    if destination.parent != authority.workspace_path or destination.name != authority.destination_name:
        raise PublicationSnapshotError("publication cleanup names another action path")
    authority = _recover_pending_mount(authority)
    if not authority.backing_removed:
        if _path_exists_no_follow(backing):
            current_backing_identity = _directory_identity(backing)
            if authority.backing_identity is None:
                metadata = backing.lstat()
                if authority.backing_phase != "creating" or metadata.st_uid != _ROOT_UID:
                    raise PublicationSnapshotError("publication backing directory appeared without creation authority")
                authority = _updated_mount_authority(
                    authority,
                    backing_identity=current_backing_identity,
                    backing_phase="copying",
                )
            elif current_backing_identity != authority.backing_identity:
                raise PublicationSnapshotError("publication backing directory identity changed")
        elif authority.backing_phase not in {"absent", "creating", "removing"}:
            raise PublicationSnapshotError("publication backing directory disappeared before cleanup")
    if not authority.destination_removed and authority.destination_identity is None:
        intended_destination = authority.workspace_path / authority.destination_name
        if _path_exists_no_follow(intended_destination):
            metadata = intended_destination.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != _ROOT_UID
                or stat.S_IMODE(metadata.st_mode) != _EMPTY_MOUNTPOINT_MODE
            ):
                raise PublicationSnapshotError("publication mountpoint appeared without creation authority")
            authority = _updated_mount_authority(
                authority,
                destination_identity=_directory_identity(intended_destination),
            )
        else:
            authority = _updated_mount_authority(authority, destination_removed=True)
    mounted = _verified_recorded_mounts(authority)
    workspace_records = [record for record in authority.mounts if record.kind == "workspace"]
    payload_records = [record for record in authority.mounts if record.kind == "payload"]
    if workspace_records and (workspace_records[0].device, workspace_records[0].inode) != authority.workspace_identity:
        raise PublicationSnapshotError("publication workspace authority is internally inconsistent")
    if (
        payload_records
        and authority.backing_identity is not None
        and (
            payload_records[0].device,
            payload_records[0].inode,
        )
        != authority.backing_identity
    ):
        raise PublicationSnapshotError("publication payload authority is internally inconsistent")
    if authority.cleanup_phase != "rollback" and payload_records:
        payload_entry = mounted.get(payload_records[0].mount_id)
        if payload_entry is not None:
            _verify_sealed_layout(backing, validated_names)
            _verify_sealed_layout(Path(payload_entry.mount_point), validated_names)

    if authority.cleanup_phase == "sealed":
        authority = _updated_mount_authority(authority, cleanup_phase="unmounting")

    os.chdir(_BACKING_PARENT)
    if payload_records:
        payload_record = payload_records[0]
        if payload_record.mount_id in authority.remaining_mount_ids:
            authority = _updated_mount_authority(authority, pending_unmount_id=payload_record.mount_id)
            mounted = _verified_recorded_mounts(authority)
            payload_entry = mounted.get(payload_record.mount_id)
            if payload_entry is not None:
                _run_mount_command(_UMOUNT_COMMAND, "--", payload_entry.mount_point)
            authority = _updated_mount_authority(
                authority,
                remaining_mount_ids=tuple(
                    mount_id for mount_id in authority.remaining_mount_ids if mount_id != payload_record.mount_id
                ),
                pending_unmount_id=None,
            )

    if not authority.destination_removed:
        authority = _updated_mount_authority(authority, cleanup_phase="removing_destination")
        mounted = _verified_recorded_mounts(authority)
        workspace_entry = mounted.get(workspace_records[0].mount_id) if workspace_records else None
        current_workspace = (
            Path(workspace_entry.mount_point) if workspace_entry is not None else authority.workspace_path
        )
        underlying_destination = current_workspace / authority.destination_name
        if payload_records:
            recorded_destination = Path(payload_records[0].original_path)
            if recorded_destination.parent == current_workspace:
                underlying_destination = recorded_destination
        if _path_exists_no_follow(underlying_destination):
            if (
                authority.destination_identity is None
                or _directory_identity(underlying_destination) != authority.destination_identity
            ):
                raise PublicationSnapshotError("publication mountpoint identity changed beneath its payload mount")
            _remove_empty_mountpoint(underlying_destination)
        authority = _updated_mount_authority(
            authority,
            destination_removed=True,
            cleanup_phase="unmounting",
        )

    for record in reversed(authority.mounts):
        if record.kind == "payload" or record.mount_id not in authority.remaining_mount_ids:
            continue
        authority = _updated_mount_authority(authority, pending_unmount_id=record.mount_id)
        mounted = _verified_recorded_mounts(authority)
        entry = mounted.get(record.mount_id)
        if entry is not None:
            _run_mount_command(_UMOUNT_COMMAND, "--", entry.mount_point)
        authority = _updated_mount_authority(
            authority,
            remaining_mount_ids=tuple(
                mount_id for mount_id in authority.remaining_mount_ids if mount_id != record.mount_id
            ),
            pending_unmount_id=None,
        )
    remaining = set(_mountinfo_by_id()).intersection(record.mount_id for record in authority.mounts)
    if remaining or authority.remaining_mount_ids:
        raise PublicationSnapshotError("publication cleanup left recorded mounts active")

    if not authority.backing_removed:
        authority = _updated_mount_authority(
            authority,
            cleanup_phase="removing_backing",
            backing_phase="removing",
        )
        if _path_exists_no_follow(backing):
            if authority.backing_identity is None or _directory_identity(backing) != authority.backing_identity:
                raise PublicationSnapshotError("publication backing directory identity changed")
            if authority.pending_copy_name is None:
                _remove_backing_directory(backing, validated_names)
            else:
                _remove_backing_directory(
                    backing,
                    validated_names,
                    pending_copy_name=authority.pending_copy_name,
                )
        authority = _updated_mount_authority(
            authority,
            backing_removed=True,
            backing_phase="removed",
            pending_copy_name=None,
            cleanup_phase="complete",
        )
    authority.state_path.unlink()
    directory_descriptor = os.open(authority.state_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    if tool_directory is not None:
        _remove_installed_tool(tool_directory)


def seal_publication_snapshot(
    source_directory: Path,
    destination_directory: Path,
    backing_directory: Path,
    artifacts: Sequence[LabelledArtifact],
    *,
    maximum: int,
) -> None:
    _require_linux_root()
    validated_specs = _validate_specs([item.artifact for item in artifacts])
    validated = [
        LabelledArtifact(_validate_label(item.label), spec)
        for item, spec in zip(artifacts, validated_specs, strict=True)
    ]
    labels = [item.label for item in validated]
    if len(labels) != len(set(labels)):
        raise PublicationSnapshotError("publication descriptor labels must be unique")
    destination = _validated_mountpoint_path(destination_directory)
    backing = _validated_backing_path(backing_directory)
    if _path_exists_no_follow(_mount_authority_path(backing)):
        raise PublicationSnapshotError("publication mount authority already exists; cleanup is required before sealing")
    workspace = destination.parent
    workspace_identity = _directory_identity(workspace)
    names = [item.artifact.name for item in validated]
    if set(_regular_names(source_directory)) != set(names):
        raise PublicationSnapshotError("publication payload set differs from its carried descriptors")

    backing_created = False
    destination_created = False
    destination_creation_intended = False
    authority_created = False
    partial_authority_created = False
    destination_identity: tuple[int, int] | None = None
    mounts: list[_MountRecord] = []
    remaining_mount_ids: list[int] = []
    pending_mount: _PendingMountIntent | None = None
    pending_unmount_id: int | None = None
    backing_phase = "absent"
    pending_copy_name: str | None = None
    copied_artifact_names: list[str] = []

    def persist_partial_authority() -> None:
        nonlocal partial_authority_created
        _write_partial_mount_authority(
            backing,
            workspace,
            destination,
            workspace_identity,
            destination_identity if destination_created else None,
            _directory_identity(backing) if backing_created else None,
            names,
            mounts,
            pending_mount,
            pending_unmount_id,
            backing_phase,
            pending_copy_name,
            copied_artifact_names,
            remaining_mount_ids,
            not destination_creation_intended,
            partial_authority_created,
        )
        partial_authority_created = True

    def mount_intended(intent: _PendingMountIntent) -> None:
        nonlocal pending_mount
        pending_mount = intent
        persist_partial_authority()

    def mount_intent_cleared() -> None:
        nonlocal pending_mount
        pending_mount = None
        persist_partial_authority()

    def mount_created(record: _MountRecord) -> None:
        nonlocal pending_mount
        mounts.append(record)
        remaining_mount_ids.append(record.mount_id)
        pending_mount = None
        persist_partial_authority()

    def mount_rollback_intended(record: _MountRecord) -> None:
        nonlocal pending_unmount_id
        pending_unmount_id = record.mount_id
        persist_partial_authority()

    def mount_rolled_back(record: _MountRecord) -> None:
        nonlocal pending_mount, pending_unmount_id
        remaining_mount_ids[:] = [mount_id for mount_id in remaining_mount_ids if mount_id != record.mount_id]
        pending_mount = None
        pending_unmount_id = None
        persist_partial_authority()

    try:
        persist_partial_authority()
        for anchor in _action_path_anchor_directories(workspace):
            _create_bind_mount(
                anchor,
                anchor,
                kind="workspace" if anchor == workspace else "ancestor",
                remount_options="remount,bind,rw,nodev,nosuid",
                required_options=_REQUIRED_ANCHOR_OPTIONS,
                on_intent=mount_intended,
                on_intent_cleared=mount_intent_cleared,
                on_created=mount_created,
                on_rollback_intent=mount_rollback_intended,
                on_rolled_back=mount_rolled_back,
            )
        backing_phase = "creating"
        persist_partial_authority()
        backing.mkdir(mode=0o700)
        backing_created = True
        backing_phase = "copying"
        persist_partial_authority()
        for item in validated:
            pending_copy_name = item.artifact.name
            persist_partial_authority()
            snapshot = copy_verified_payload(
                source_directory / item.artifact.name,
                backing / item.artifact.name,
                maximum=maximum,
                expected_digest=item.artifact.digest,
                executable=False,
            )
            os.chmod(snapshot.path, _SEALED_FILE_MODE, follow_symlinks=False)
            copied_artifact_names.append(item.artifact.name)
            pending_copy_name = None
            persist_partial_authority()
        os.chmod(backing, _SEALED_DIRECTORY_MODE, follow_symlinks=False)
        backing_phase = "ready"
        persist_partial_authority()
        destination_creation_intended = True
        persist_partial_authority()
        destination.mkdir(mode=_EMPTY_MOUNTPOINT_MODE)
        destination_created = True
        destination_identity = _directory_identity(destination)
        persist_partial_authority()
        payload_mount = _create_bind_mount(
            backing,
            destination,
            kind="payload",
            remount_options="remount,bind,ro,nodev,noexec,nosuid",
            required_options=_REQUIRED_MOUNT_OPTIONS,
            on_intent=mount_intended,
            on_intent_cleared=mount_intent_cleared,
            on_created=mount_created,
            on_rollback_intent=mount_rollback_intended,
            on_rolled_back=mount_rolled_back,
        )
        _verify_sealed_mount(
            destination,
            backing,
            validated,
            maximum=maximum,
            mount_id=payload_mount.mount_id,
        )
        _write_mount_authority(
            backing,
            workspace,
            destination,
            destination_identity,
            names,
            mounts,
        )
        authority_created = True
    except BaseException as seal_error:
        if not authority_created:
            persist_partial_authority()
        try:
            unseal_publication_snapshot(destination, backing, names)
        except BaseException as cleanup_error:
            raise PublicationSnapshotError("publication seal rollback did not reach terminal zero") from cleanup_error
        raise seal_error


def write_github_outputs(path: Path, artifacts: Sequence[LabelledArtifact]) -> None:
    with path.open("a", encoding="utf-8") as output:
        for label, artifact in artifacts:
            output.write(f"{label}_name={artifact.name}\n")
            output.write(f"{label}_digest={artifact.digest}\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    describe = subparsers.add_parser("describe")
    describe.add_argument("--directory", type=Path, required=True)
    describe.add_argument("--selector", action="append", required=True)
    describe.add_argument("--maximum", type=int, default=DEFAULT_MAXIMUM_BYTES)
    describe.add_argument("--github-output", type=Path, required=True)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--directory", type=Path, required=True)
    verify.add_argument("--artifact", action="append", required=True)
    verify.add_argument("--maximum", type=int, default=DEFAULT_MAXIMUM_BYTES)
    verify.add_argument("--github-output", type=Path)

    create = subparsers.add_parser("create")
    create.add_argument("--source-directory", type=Path, required=True)
    create.add_argument("--destination-directory", type=Path, required=True)
    create.add_argument("--artifact", action="append", required=True)
    create.add_argument("--maximum", type=int, default=DEFAULT_MAXIMUM_BYTES)

    seal = subparsers.add_parser("seal")
    seal.add_argument("--source-directory", type=Path, required=True)
    seal.add_argument("--destination-directory", type=Path, required=True)
    seal.add_argument("--backing-directory", type=Path, required=True)
    seal.add_argument("--artifact", action="append", required=True)
    seal.add_argument("--maximum", type=int, default=DEFAULT_MAXIMUM_BYTES)

    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--directory", type=Path, required=True)
    cleanup.add_argument("--artifact-name", action="append", required=True)

    unseal = subparsers.add_parser("unseal")
    unseal.add_argument("--directory", type=Path, required=True)
    unseal.add_argument("--backing-directory", type=Path, required=True)
    unseal.add_argument("--artifact-name", action="append", required=True)
    unseal.add_argument("--tool-directory", type=Path)

    install_tool = subparsers.add_parser("install-tool")
    install_tool.add_argument("--source-directory", type=Path, required=True)
    install_tool.add_argument("--tool-directory", type=Path, required=True)
    install_tool.add_argument("--script-digest", action="append", required=True)

    remove_tool = subparsers.add_parser("remove-tool")
    remove_tool.add_argument("--tool-directory", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "describe":
            artifacts = describe_publication_payloads(
                args.directory,
                [_parse_selector(raw) for raw in args.selector],
                maximum=args.maximum,
            )
            write_github_outputs(args.github_output, artifacts)
        elif args.command == "verify":
            artifacts = [parse_labelled_artifact(raw) for raw in args.artifact]
            verify_publication_payloads(args.directory, artifacts, maximum=args.maximum)
            if args.github_output is not None:
                write_github_outputs(args.github_output, artifacts)
        elif args.command == "create":
            artifacts = [parse_labelled_artifact(raw) for raw in args.artifact]
            create_publication_snapshot(
                args.source_directory,
                args.destination_directory,
                [item.artifact for item in artifacts],
                maximum=args.maximum,
            )
        elif args.command == "seal":
            seal_publication_snapshot(
                args.source_directory,
                args.destination_directory,
                args.backing_directory,
                [parse_labelled_artifact(raw) for raw in args.artifact],
                maximum=args.maximum,
            )
        elif args.command == "cleanup":
            names = [_validate_name(name) for name in args.artifact_name]
            if len(names) != len(set(names)):
                raise PublicationSnapshotError("publication artifact names must be unique")
            cleanup_private_directory(args.directory, names)
        elif args.command == "unseal":
            unseal_publication_snapshot(
                args.directory,
                args.backing_directory,
                args.artifact_name,
                tool_directory=args.tool_directory,
            )
        elif args.command == "install-tool":
            parsed_digests = [_parse_tool_digest(raw) for raw in args.script_digest]
            digests = dict(parsed_digests)
            if len(parsed_digests) != len(digests):
                raise PublicationSnapshotError("publication tool digests must be unique")
            install_publication_tool(args.source_directory, args.tool_directory, digests)
        else:
            _require_linux_root()
            _remove_installed_tool(args.tool_directory)
    except (OSError, ReleasePayloadError, PublicationSnapshotError) as exc:
        print(f"Release publication snapshot failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
