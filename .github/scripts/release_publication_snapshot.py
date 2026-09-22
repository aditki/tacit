#!/usr/bin/env python3
"""Describe, verify, and isolate exact release publication payloads."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import NamedTuple

from release_payload_snapshot import (
    ReleasePayloadError,
    cleanup_private_directory,
    copy_verified_payload,
    verified_payload_snapshot,
)

DEFAULT_MAXIMUM_BYTES = 512 * 1024 * 1024
_LABEL = re.compile(r"[a-z][a-z0-9_]*")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")
_MOUNT_NAME = re.compile(r"[A-Za-z0-9._+-]+")
_BACKING_PREFIX = "tacit-release-publication-"
_BACKING_PARENT = Path("/var/lib")
_MOUNT_COMMAND = Path("/usr/bin/mount")
_UMOUNT_COMMAND = Path("/usr/bin/umount")
_MOUNTINFO = Path("/proc/self/mountinfo")
_MAX_MOUNTINFO_BYTES = 4 * 1024 * 1024
_MAX_MOUNT_AUTHORITY_BYTES = 64 * 1024
_MAX_ACTION_PATH_ANCHORS = 32
_MOUNT_AUTHORITY_SUFFIX = ".mount-authority.json"
_SEALED_DIRECTORY_MODE = 0o555
_SEALED_FILE_MODE = 0o444
_EMPTY_MOUNTPOINT_MODE = 0o700
_REQUIRED_MOUNT_OPTIONS = frozenset({"ro", "nodev", "noexec", "nosuid"})
_REQUIRED_ANCHOR_OPTIONS = frozenset({"rw", "nodev", "nosuid"})


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
    filesystem: str
    source: str
    super_options: frozenset[str]


class _MountRecord(NamedTuple):
    kind: str
    mount_id: int
    original_path: str
    device: int
    inode: int


class _MountAuthority(NamedTuple):
    state_path: Path
    workspace_path: Path
    workspace_mount_id: int | None
    workspace_identity: tuple[int, int]
    destination_name: str
    destination_identity: tuple[int, int]
    backing_identity: tuple[int, int]
    artifact_names: tuple[str, ...]
    mounts: tuple[_MountRecord, ...]


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


def _mount_authority_path(backing: Path) -> Path:
    return backing.with_name(f"{backing.name}{_MOUNT_AUTHORITY_SUFFIX}")


def _run_mount_command(command: Path, *arguments: str) -> None:
    if not command.is_file():
        raise PublicationSnapshotError(f"required publication mount command is unavailable: {command.name}")
    try:
        completed = subprocess.run(
            [str(command), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
            },
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PublicationSnapshotError(f"publication mount command failed: {command.name}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip().splitlines()
        suffix = f": {detail[-1][:512]}" if detail else ""
        raise PublicationSnapshotError(f"publication mount command failed: {command.name}{suffix}")


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


def _created_mount_entry(path: Path, previous_ids: set[int]) -> _MountInfoEntry:
    created = [entry for entry in _mountinfo_entries(path) if entry.mount_id not in previous_ids]
    if len(created) != 1:
        raise PublicationSnapshotError("publication mount creation did not produce one mount identity")
    return created[0]


def _create_bind_mount(
    source: Path,
    destination: Path,
    *,
    kind: str,
    remount_options: str,
    required_options: frozenset[str],
) -> _MountRecord:
    expected_identity = _directory_identity(source)
    previous_ids = {entry.mount_id for entry in _mountinfo_entries(destination)}
    created_id: int | None = None
    try:
        _run_mount_command(_MOUNT_COMMAND, "--bind", str(source), str(destination))
        created = _created_mount_entry(destination, previous_ids)
        created_id = created.mount_id
        _run_mount_command(_MOUNT_COMMAND, "-o", remount_options, str(destination))
        current = _mountinfo_by_id().get(created_id)
        if current is None or not required_options.issubset(current.options):
            raise PublicationSnapshotError("publication mount options were not applied")
        current_path = Path(current.mount_point)
        if _directory_identity(current_path) != expected_identity:
            raise PublicationSnapshotError("publication mount identity changed during creation")
        return _MountRecord(
            kind=kind,
            mount_id=created_id,
            original_path=str(destination),
            device=expected_identity[0],
            inode=expected_identity[1],
        )
    except BaseException:
        candidates = _mountinfo_by_id()
        if created_id is None:
            newly_created = [entry for entry in _mountinfo_entries(destination) if entry.mount_id not in previous_ids]
        else:
            entry = candidates.get(created_id)
            newly_created = [entry] if entry is not None else []
        for entry in newly_created:
            try:
                _run_mount_command(_UMOUNT_COMMAND, "--", entry.mount_point)
            except BaseException:
                pass
        raise


def _mount_record_payload(record: _MountRecord) -> dict[str, object]:
    return {
        "kind": record.kind,
        "mount_id": record.mount_id,
        "original_path": record.original_path,
        "device": record.device,
        "inode": record.inode,
    }


def _write_bytes(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise PublicationSnapshotError("publication mount authority write made no progress")
        offset += written


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
    payload = json.dumps(
        {
            "version": 1,
            "workspace": {
                "path": str(workspace),
                "mount_id": workspace_records[0].mount_id,
                "device": workspace_records[0].device,
                "inode": workspace_records[0].inode,
            },
            "destination_name": destination.name,
            "destination": {
                "device": destination_identity[0],
                "inode": destination_identity[1],
            },
            "backing": {
                "path": str(backing),
                "device": _directory_identity(backing)[0],
                "inode": _directory_identity(backing)[1],
            },
            "artifact_names": list(artifact_names),
            "mounts": [_mount_record_payload(record) for record in mounts],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if not payload or len(payload) > _MAX_MOUNT_AUTHORITY_BYTES:
        raise PublicationSnapshotError("publication mount authority exceeds its byte limit")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(state_path, flags, 0o600)
    try:
        _write_bytes(descriptor, payload)
        os.fsync(descriptor)
    except BaseException:
        try:
            state_path.unlink()
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    metadata = state_path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise PublicationSnapshotError("publication mount authority ownership or mode changed")
    return state_path


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
    }
    if not isinstance(loaded, dict) or set(loaded) != expected_keys or loaded.get("version") != 1:
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
    workspace_mount_id = _positive_json_integer(workspace_payload.get("mount_id"), "workspace mount identity")
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
    destination_identity = _identity_from_payload(loaded.get("destination"), "destination identity")

    backing_payload = loaded.get("backing")
    if not isinstance(backing_payload, dict) or set(backing_payload) != {"path", "device", "inode"}:
        raise PublicationSnapshotError("publication mount authority has an invalid backing identity")
    if backing_payload.get("path") != str(backing):
        raise PublicationSnapshotError("publication mount authority names another backing directory")
    backing_identity = (
        _positive_json_integer(backing_payload.get("device"), "backing device"),
        _positive_json_integer(backing_payload.get("inode"), "backing inode"),
    )

    artifact_payload = loaded.get("artifact_names")
    if not isinstance(artifact_payload, list) or not all(isinstance(name, str) for name in artifact_payload):
        raise PublicationSnapshotError("publication mount authority has invalid artifact names")
    artifact_names = tuple(_validate_name(name) for name in artifact_payload)
    if artifact_names != tuple(names):
        raise PublicationSnapshotError("publication mount authority artifact names changed")

    mount_payload = loaded.get("mounts")
    if not isinstance(mount_payload, list) or not 2 <= len(mount_payload) <= _MAX_ACTION_PATH_ANCHORS + 1:
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
    workspace_records = [record for record in mounts if record.kind == "workspace"]
    payload_records = [record for record in mounts if record.kind == "payload"]
    if (
        len(workspace_records) != 1
        or workspace_records[0].mount_id != workspace_mount_id
        or workspace_records[0].original_path != str(workspace_path)
        or len(payload_records) != 1
        or Path(payload_records[0].original_path).parent != workspace_path
        or Path(payload_records[0].original_path).name != destination_name
    ):
        raise PublicationSnapshotError("publication mount authority action path changed")
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
    )


def _verified_recorded_mounts(authority: _MountAuthority) -> dict[int, _MountInfoEntry]:
    mounted = _mountinfo_by_id()
    selected: dict[int, _MountInfoEntry] = {}
    for record in authority.mounts:
        entry = mounted.get(record.mount_id)
        if entry is None:
            raise PublicationSnapshotError("recorded publication mount identity is absent")
        required = _REQUIRED_MOUNT_OPTIONS if record.kind == "payload" else _REQUIRED_ANCHOR_OPTIONS
        if not required.issubset(entry.options):
            raise PublicationSnapshotError("recorded publication mount options changed")
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
    if len(entries) != 1 or not _REQUIRED_MOUNT_OPTIONS.issubset(entries[0].options):
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


def _remove_backing_directory(path: Path, names: Sequence[str]) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != 0:
        raise PublicationSnapshotError("publication backing directory cannot be safely removed")
    os.chmod(path, 0o700, follow_symlinks=False)
    cleanup_private_directory(path, names)


def unseal_publication_snapshot(
    destination_directory: Path,
    backing_directory: Path,
    names: Sequence[str],
) -> None:
    _require_linux_root()
    destination = _validated_mountpoint_path(destination_directory)
    backing = _validated_backing_path(backing_directory)
    validated_names = [_validate_name(name) for name in names]
    if len(validated_names) != len(set(validated_names)):
        raise PublicationSnapshotError("publication artifact names must be unique")
    authority = _read_mount_authority(backing, validated_names)
    if destination.parent != authority.workspace_path or destination.name != authority.destination_name:
        raise PublicationSnapshotError("publication cleanup names another action path")
    if _directory_identity(backing) != authority.backing_identity:
        raise PublicationSnapshotError("publication backing directory identity changed")
    mounted = _verified_recorded_mounts(authority)
    workspace_record = next(record for record in authority.mounts if record.kind == "workspace")
    payload_record = next(record for record in authority.mounts if record.kind == "payload")
    if (workspace_record.device, workspace_record.inode) != authority.workspace_identity or (
        payload_record.device,
        payload_record.inode,
    ) != authority.backing_identity:
        raise PublicationSnapshotError("publication action-path authority is internally inconsistent")
    workspace_entry = mounted[authority.workspace_mount_id]
    payload_entry = mounted[payload_record.mount_id]
    current_workspace = Path(workspace_entry.mount_point)
    current_payload = Path(payload_entry.mount_point)
    _verify_sealed_layout(backing, validated_names)
    _verify_sealed_layout(current_payload, validated_names)

    os.chdir(_BACKING_PARENT)
    _run_mount_command(_UMOUNT_COMMAND, "--", str(current_payload))
    underlying_destination = current_workspace / authority.destination_name
    if _directory_identity(underlying_destination) != authority.destination_identity:
        raise PublicationSnapshotError("publication mountpoint identity changed beneath its payload mount")
    _remove_empty_mountpoint(underlying_destination)

    for record in reversed(authority.mounts):
        if record.kind == "payload":
            continue
        entry = mounted[record.mount_id]
        _run_mount_command(_UMOUNT_COMMAND, "--", entry.mount_point)
    remaining = set(_mountinfo_by_id()).intersection(record.mount_id for record in authority.mounts)
    if remaining:
        raise PublicationSnapshotError("publication cleanup left recorded mounts active")

    _remove_backing_directory(backing, validated_names)
    authority.state_path.unlink()


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
    workspace = destination.parent
    names = [item.artifact.name for item in validated]
    if set(_regular_names(source_directory)) != set(names):
        raise PublicationSnapshotError("publication payload set differs from its carried descriptors")

    backing_created = False
    destination_created = False
    authority_created = False
    destination_identity: tuple[int, int] | None = None
    mounts: list[_MountRecord] = []
    try:
        for anchor in _action_path_anchor_directories(workspace):
            mounts.append(
                _create_bind_mount(
                    anchor,
                    anchor,
                    kind="workspace" if anchor == workspace else "ancestor",
                    remount_options="remount,bind,rw,nodev,nosuid",
                    required_options=_REQUIRED_ANCHOR_OPTIONS,
                )
            )
        backing.mkdir(mode=0o700)
        backing_created = True
        for item in validated:
            snapshot = copy_verified_payload(
                source_directory / item.artifact.name,
                backing / item.artifact.name,
                maximum=maximum,
                expected_digest=item.artifact.digest,
                executable=False,
            )
            os.chmod(snapshot.path, _SEALED_FILE_MODE, follow_symlinks=False)
        os.chmod(backing, _SEALED_DIRECTORY_MODE, follow_symlinks=False)
        destination.mkdir(mode=_EMPTY_MOUNTPOINT_MODE)
        destination_created = True
        destination_identity = _directory_identity(destination)
        payload_mount = _create_bind_mount(
            backing,
            destination,
            kind="payload",
            remount_options="remount,bind,ro,nodev,noexec,nosuid",
            required_options=_REQUIRED_MOUNT_OPTIONS,
        )
        mounts.append(payload_mount)
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
    except BaseException:
        state_path = _mount_authority_path(backing)
        if authority_created or state_path.exists():
            try:
                state_path.unlink()
            except OSError:
                pass
        os.chdir(_BACKING_PARENT)
        mounted = _mountinfo_by_id()
        payload_records = [record for record in mounts if record.kind == "payload"]
        for record in reversed(mounts):
            entry = mounted.get(record.mount_id)
            if entry is None:
                continue
            try:
                _run_mount_command(_UMOUNT_COMMAND, "--", entry.mount_point)
            except BaseException:
                pass
        if destination_created and not payload_records:
            try:
                _remove_empty_mountpoint(destination)
            except BaseException:
                pass
        elif destination_created:
            try:
                _remove_empty_mountpoint(destination)
            except BaseException:
                pass
        if backing_created:
            try:
                _remove_backing_directory(backing, names)
            except BaseException:
                pass
        raise


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
        else:
            unseal_publication_snapshot(
                args.directory,
                args.backing_directory,
                args.artifact_name,
            )
    except (OSError, ReleasePayloadError, PublicationSnapshotError) as exc:
        print(f"Release publication snapshot failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
