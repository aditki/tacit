"""Filesystem quarantine for generated archetypes.

The quarantine is intentionally separate from ``TACIT_ARCHETYPES_PATH``. Only
an explicit experimental exact-scope query may read from it.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import secrets
import stat
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import structlog
import yaml
from pydantic import BaseModel, ValidationError

from tacit.archetypes.generated.schema import (
    GENERATED_ARCHETYPE_ENVIRONMENT_SCOPE_REQUIRED,
    GENERATED_ARCHETYPE_SERVICE_SCOPE_REQUIRED,
    GeneratedArchetype,
    GeneratedArchetypeQuery,
    GeneratedArchetypeRetrieval,
    GeneratedArchetypeRetrievalStatus,
    GeneratedArchetypeStatus,
)
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS

logger = structlog.get_logger()
DEFAULT_GENERATED_RETRIEVAL_MAX_FILES = 256
DEFAULT_GENERATED_RETRIEVAL_MAX_DIRECTORY_ENTRIES = 1_024
DEFAULT_GENERATED_RETRIEVAL_MAX_FILE_BYTES = 512 * 1024
DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_BYTES = 8 * 1024 * 1024
DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_NODES = 12_000
DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_DEPTH = 32
DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALARS = 8_000
DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALAR_BYTES = 64 * 1024
DEFAULT_GENERATED_RETRIEVAL_MAX_ARTIFACTS_PER_FILE = 64
DEFAULT_GENERATED_RETRIEVAL_MAX_PANELS_PER_FILE = 256
DEFAULT_GENERATED_RETRIEVAL_MAX_QUERIES_PER_FILE = 1_024
DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_ARTIFACTS = 256
DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_PANELS = 1_024
DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_QUERIES = 4_096
DEFAULT_GENERATED_RETRIEVAL_MAX_RESULTS = 256
_DIRECTORY_ENTRY_LIMIT_EXCEEDED = "generated_archetype_directory_entry_limit_exceeded"
_FILE_COUNT_LIMIT_EXCEEDED = "generated_archetype_file_count_limit_exceeded"
_FILE_SIZE_LIMIT_EXCEEDED = "generated_archetype_file_size_limit_exceeded"
_TOTAL_BYTES_LIMIT_EXCEEDED = "generated_archetype_total_bytes_limit_exceeded"
_YAML_NODE_LIMIT_EXCEEDED = "generated_archetype_yaml_node_limit_exceeded"
_YAML_DEPTH_LIMIT_EXCEEDED = "generated_archetype_yaml_depth_limit_exceeded"
_YAML_SCALAR_LIMIT_EXCEEDED = "generated_archetype_yaml_scalar_limit_exceeded"
_YAML_SCALAR_SIZE_LIMIT_EXCEEDED = "generated_archetype_yaml_scalar_size_limit_exceeded"
_ARTIFACT_LIMIT_EXCEEDED = "generated_archetype_artifact_limit_exceeded"
_PANEL_LIMIT_EXCEEDED = "generated_archetype_panel_limit_exceeded"
_QUERY_LIMIT_EXCEEDED = "generated_archetype_query_limit_exceeded"
_TOTAL_ARTIFACT_LIMIT_EXCEEDED = "generated_archetype_total_artifact_limit_exceeded"
_TOTAL_PANEL_LIMIT_EXCEEDED = "generated_archetype_total_panel_limit_exceeded"
_TOTAL_QUERY_LIMIT_EXCEEDED = "generated_archetype_total_query_limit_exceeded"
_RESULT_LIMIT_EXCEEDED = "generated_archetype_result_limit_exceeded"
_YAML_ALIAS_REJECTED = "generated_archetype_yaml_alias_rejected"
_YAML_PARSE_FAILED = "generated_archetype_yaml_parse_failed"
_YAML_DECODE_FAILED = "generated_archetype_yaml_decode_failed"
_SCHEMA_VALIDATION_FAILED = "generated_archetype_schema_validation_failed"
_DOCUMENT_INVALID = "generated_archetype_document_invalid"
_ROOT_OPEN_FAILED = "generated_archetype_root_open_failed"
_SCOPE_OPEN_FAILED = "generated_archetype_scope_directory_open_failed"
_SCOPE_SYMLINK_REJECTED = "generated_archetype_scope_path_symlink_rejected"
_DIRECTORY_LIST_FAILED = "generated_archetype_directory_list_failed"
_FILE_OPEN_FAILED = "generated_archetype_file_open_failed"
_FILE_STAT_FAILED = "generated_archetype_file_stat_failed"
_FILE_READ_FAILED = "generated_archetype_file_read_failed"
_NON_REGULAR_FILE_REJECTED = "generated_archetype_non_regular_file_rejected"
_PLATFORM_UNSUPPORTED = "generated_archetype_descriptor_access_unsupported"
_LIMIT_REASON_CODES = frozenset(
    {
        _DIRECTORY_ENTRY_LIMIT_EXCEEDED,
        _FILE_COUNT_LIMIT_EXCEEDED,
        _FILE_SIZE_LIMIT_EXCEEDED,
        _TOTAL_BYTES_LIMIT_EXCEEDED,
        _YAML_NODE_LIMIT_EXCEEDED,
        _YAML_DEPTH_LIMIT_EXCEEDED,
        _YAML_SCALAR_LIMIT_EXCEEDED,
        _YAML_SCALAR_SIZE_LIMIT_EXCEEDED,
        _ARTIFACT_LIMIT_EXCEEDED,
        _PANEL_LIMIT_EXCEEDED,
        _QUERY_LIMIT_EXCEEDED,
        _TOTAL_ARTIFACT_LIMIT_EXCEEDED,
        _TOTAL_PANEL_LIMIT_EXCEEDED,
        _TOTAL_QUERY_LIMIT_EXCEEDED,
        _RESULT_LIMIT_EXCEEDED,
    }
)


@dataclass(frozen=True)
class _YamlLimits:
    nodes: int
    depth: int
    scalars: int
    scalar_bytes: int
    artifacts: int
    panels: int
    queries: int


@dataclass(frozen=True)
class _RawGeneratedFile:
    """Structurally admitted file contents before schema validation."""

    items: list[Any]
    artifact_count: int
    panel_count: int
    query_count: int


_PERSISTENCE_LIMITS = _YamlLimits(
    nodes=DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_NODES,
    depth=DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_DEPTH,
    scalars=DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALARS,
    scalar_bytes=DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALAR_BYTES,
    artifacts=DEFAULT_GENERATED_RETRIEVAL_MAX_ARTIFACTS_PER_FILE,
    panels=DEFAULT_GENERATED_RETRIEVAL_MAX_PANELS_PER_FILE,
    queries=DEFAULT_GENERATED_RETRIEVAL_MAX_QUERIES_PER_FILE,
)


class _ArtifactRejected(Exception):
    def __init__(self, reason_code: str, *, limited: bool = False) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.limited = limited


class _PathSymlinkRejected(OSError):
    """A configured retrieval path crossed a symlink component."""


@dataclass
class _RetrievalState:
    directory_entries_discovered: int = 0
    files_discovered: int = 0
    files_scanned: int = 0
    bytes_scanned: int = 0
    quarantined: int = 0
    rejected_by_scope: int = 0
    rejected_by_limit: int = 0
    oversized_files: int = 0
    symlinks_rejected: int = 0
    invalid: int = 0
    total_artifacts: int = 0
    total_panels: int = 0
    total_queries: int = 0
    reasons: Counter[str] = field(default_factory=Counter)
    skipped_reason: str = ""

    def reject(
        self,
        reason_code: str,
        *,
        invalid: bool = False,
        limited: bool = False,
        skipped: bool = False,
    ) -> None:
        self.reasons[reason_code] += 1
        if invalid:
            self.invalid += 1
        if limited:
            self.rejected_by_limit += 1
        if skipped and not self.skipped_reason:
            self.skipped_reason = reason_code

    def result(self, archetypes: list[GeneratedArchetype] | None = None) -> GeneratedArchetypeRetrieval:
        if self.skipped_reason:
            status = GeneratedArchetypeRetrievalStatus.SKIPPED
            reason_code = self.skipped_reason
            resolved_archetypes: list[GeneratedArchetype] = []
        elif self.reasons:
            status = GeneratedArchetypeRetrievalStatus.PARTIAL
            reason_code = min(self.reasons)
            resolved_archetypes = archetypes or []
        else:
            status = GeneratedArchetypeRetrievalStatus.PASSED
            reason_code = "generated_archetype_retrieval_complete"
            resolved_archetypes = archetypes or []
        return GeneratedArchetypeRetrieval(
            archetypes=resolved_archetypes,
            directory_entries_discovered=self.directory_entries_discovered,
            files_discovered=self.files_discovered,
            files_scanned=self.files_scanned,
            bytes_scanned=self.bytes_scanned,
            quarantined=self.quarantined,
            rejected_by_scope=self.rejected_by_scope,
            rejected_by_limit=self.rejected_by_limit,
            oversized_files=self.oversized_files,
            symlinks_rejected=self.symlinks_rejected,
            invalid=self.invalid,
            total_artifacts=self.total_artifacts,
            total_panels=self.total_panels,
            total_queries=self.total_queries,
            status=status,
            reason_code=reason_code,
            limit_reason_codes=tuple(sorted(self.reasons.keys() & _LIMIT_REASON_CODES)),
            reason_counts=tuple(sorted(self.reasons.items())),
        )


def _safe_segment(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_.-]+", "-", value.casefold()).strip("-") or "unknown"
    digest = hashlib.sha256(value.encode()).hexdigest()[:8]
    return f"{normalized[:64]}-{digest}"


def _scope_directory(root: Path, query: GeneratedArchetypeQuery) -> Path:
    service_scope = "|".join(sorted(query.service_refs))
    return root / _safe_segment(query.tenant_id) / _safe_segment(service_scope)


def _artifact_fingerprint(archetype: GeneratedArchetype) -> str:
    payload = archetype.model_dump(mode="json", exclude={"created_at", "retrieval_status"})
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _content_fingerprint(payload: bytes | str) -> str:
    encoded = payload.encode() if isinstance(payload, str) else payload
    return hashlib.sha256(encoded).hexdigest()[:16]


def _descriptor_access_supported() -> bool:
    return bool(
        hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
        and os.open in os.supports_dir_fd
        and os.stat in os.supports_dir_fd
        and os.scandir in os.supports_fd
    )


def _descriptor_write_supported() -> bool:
    return bool(
        _descriptor_access_supported()
        and os.mkdir in os.supports_dir_fd
        and os.rename in os.supports_dir_fd
        and os.unlink in os.supports_dir_fd
    )


def _open_flags(*, directory: bool = False) -> int:
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    if directory:
        flags |= os.O_DIRECTORY
    else:
        flags |= getattr(os, "O_NONBLOCK", 0)
    return flags


def _close_descriptor(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except OSError:
        pass


def _is_symlink_at(parent_descriptor: int, name: str) -> bool:
    try:
        metadata = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except OSError:
        return False
    return stat.S_ISLNK(metadata.st_mode)


def _open_root_without_symlinks(root_path: Path, *, create: bool = False) -> int | None:
    """Open every root component relative to a trusted filesystem anchor."""
    absolute_root = Path(os.path.abspath(os.fspath(root_path)))
    anchor = absolute_root.anchor or os.curdir
    segments = absolute_root.parts[1:] if absolute_root.anchor else absolute_root.parts
    descriptors: list[int] = []
    try:
        descriptors.append(os.open(anchor, _open_flags(directory=True)))
        for segment in segments:
            try:
                descriptor = os.open(
                    segment,
                    _open_flags(directory=True),
                    dir_fd=descriptors[-1],
                )
            except FileNotFoundError:
                if not create:
                    return None
                try:
                    os.mkdir(segment, mode=0o750, dir_fd=descriptors[-1])
                except FileExistsError:
                    pass
                descriptor = os.open(
                    segment,
                    _open_flags(directory=True),
                    dir_fd=descriptors[-1],
                )
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.EMLINK} or _is_symlink_at(
                    descriptors[-1],
                    segment,
                ):
                    raise _PathSymlinkRejected from exc
                raise
            descriptors.append(descriptor)

        return descriptors.pop()
    finally:
        for descriptor in reversed(descriptors):
            _close_descriptor(descriptor)


def _open_or_create_directory(parent_descriptor: int, name: str) -> int:
    try:
        return os.open(name, _open_flags(directory=True), dir_fd=parent_descriptor)
    except FileNotFoundError:
        try:
            os.mkdir(name, mode=0o750, dir_fd=parent_descriptor)
        except FileExistsError:
            pass
        return os.open(name, _open_flags(directory=True), dir_fd=parent_descriptor)
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK} or _is_symlink_at(parent_descriptor, name):
            raise _PathSymlinkRejected from exc
        raise


def _log_retrieval_failure(
    *,
    reason_code: str,
    exception_class: str,
    fingerprint: str,
    state: _RetrievalState,
) -> None:
    logger.warning(
        "generated_archetype_retrieval_rejected",
        reason_code=reason_code,
        exception_class=exception_class,
        artifact_fingerprint=fingerprint,
        directory_entries_discovered=state.directory_entries_discovered,
        files_discovered=state.files_discovered,
        files_scanned=state.files_scanned,
        bytes_scanned=state.bytes_scanned,
        invalid=state.invalid,
        oversized_files=state.oversized_files,
        symlinks_rejected=state.symlinks_rejected,
    )


def _read_descriptor(descriptor: int, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    remaining = max_bytes + 1
    while remaining:
        chunk = os.read(descriptor, min(remaining, 64 * 1024))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _validate_yaml_events(payload: str, limits: _YamlLimits) -> None:
    node_count = 0
    scalar_count = 0
    depth = 0
    for event in yaml.parse(payload, Loader=yaml.SafeLoader):
        if isinstance(event, yaml.AliasEvent):
            raise _ArtifactRejected(_YAML_ALIAS_REJECTED)
        if isinstance(event, (yaml.MappingStartEvent, yaml.SequenceStartEvent)):
            node_count += 1
            depth += 1
            if node_count > limits.nodes:
                raise _ArtifactRejected(_YAML_NODE_LIMIT_EXCEEDED, limited=True)
            if depth > limits.depth:
                raise _ArtifactRejected(_YAML_DEPTH_LIMIT_EXCEEDED, limited=True)
        elif isinstance(event, (yaml.MappingEndEvent, yaml.SequenceEndEvent)):
            depth -= 1
        elif isinstance(event, yaml.ScalarEvent):
            node_count += 1
            scalar_count += 1
            if node_count > limits.nodes:
                raise _ArtifactRejected(_YAML_NODE_LIMIT_EXCEEDED, limited=True)
            if scalar_count > limits.scalars:
                raise _ArtifactRejected(_YAML_SCALAR_LIMIT_EXCEEDED, limited=True)
            if len(event.value.encode("utf-8")) > limits.scalar_bytes:
                raise _ArtifactRejected(_YAML_SCALAR_SIZE_LIMIT_EXCEEDED, limited=True)


def _admission_error(reason_code: str) -> ValueError:
    return ValueError(f"Generated archetype admission failed: {reason_code}")


def _admit_yaml_text(payload: str, limits: _YamlLimits) -> bytes:
    if len(payload) > DEFAULT_GENERATED_RETRIEVAL_MAX_FILE_BYTES:
        raise _admission_error(_FILE_SIZE_LIMIT_EXCEEDED)
    encoded = payload.encode("utf-8")
    if len(encoded) > DEFAULT_GENERATED_RETRIEVAL_MAX_FILE_BYTES:
        raise _admission_error(_FILE_SIZE_LIMIT_EXCEEDED)
    try:
        _validate_yaml_events(payload, limits)
    except _ArtifactRejected as exc:
        raise _admission_error(exc.reason_code) from None
    return encoded


def _python_scalar_text(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, (str, bool, int, float, date, datetime)):
        return str(value)
    raise _admission_error(_DOCUMENT_INVALID)


def _validate_python_document(value: object, limits: _YamlLimits) -> None:
    """Bound model-copy bypasses before Pydantic or YAML traverses them."""
    node_count = 0
    scalar_count = 0
    seen_containers: set[int] = set()
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        node_count += 1
        if node_count > limits.nodes:
            raise _admission_error(_YAML_NODE_LIMIT_EXCEEDED)
        if depth > limits.depth:
            raise _admission_error(_YAML_DEPTH_LIMIT_EXCEEDED)

        if isinstance(current, BaseModel):
            identity = id(current)
            if identity in seen_containers:
                raise _admission_error(_YAML_ALIAS_REJECTED)
            seen_containers.add(identity)
            fields = type(current).model_fields
            stack.extend((getattr(current, name), depth + 1) for name in fields)
            stack.extend((name, depth + 1) for name in fields)
            continue
        if isinstance(current, Mapping):
            identity = id(current)
            if identity in seen_containers:
                raise _admission_error(_YAML_ALIAS_REJECTED)
            seen_containers.add(identity)
            for key, item in current.items():
                stack.append((item, depth + 1))
                stack.append((key, depth + 1))
            continue
        if isinstance(current, (list, tuple, set, frozenset)):
            identity = id(current)
            if identity in seen_containers:
                raise _admission_error(_YAML_ALIAS_REJECTED)
            seen_containers.add(identity)
            stack.extend((item, depth + 1) for item in current)
            continue

        scalar_count += 1
        if scalar_count > limits.scalars:
            raise _admission_error(_YAML_SCALAR_LIMIT_EXCEEDED)
        scalar_text = _python_scalar_text(current)
        if len(scalar_text.encode("utf-8")) > limits.scalar_bytes:
            raise _admission_error(_YAML_SCALAR_SIZE_LIMIT_EXCEEDED)


def _raw_collection_counts(raw_items: list[Any], limits: _YamlLimits) -> tuple[int, int]:
    panel_count = 0
    query_count = 0
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        raw_panels = raw_item.get("panels", [])
        if not isinstance(raw_panels, list):
            continue
        panel_count += len(raw_panels)
        if panel_count > limits.panels:
            raise _ArtifactRejected(_PANEL_LIMIT_EXCEEDED, limited=True)
        for raw_panel in raw_panels:
            if not isinstance(raw_panel, dict):
                continue
            raw_queries = raw_panel.get("queries", [])
            if not isinstance(raw_queries, list):
                continue
            query_count += len(raw_queries)
            if query_count > limits.queries:
                raise _ArtifactRejected(_QUERY_LIMIT_EXCEEDED, limited=True)
    return panel_count, query_count


def _generated_model_fields(archetype: GeneratedArchetype) -> dict[str, object]:
    return {name: getattr(archetype, name) for name in GeneratedArchetype.model_fields}


def _prepare_generated_archetype(
    archetype: GeneratedArchetype,
    *,
    force_quarantined: bool = False,
) -> tuple[GeneratedArchetype, bytes]:
    raw_item = _generated_model_fields(archetype)
    if force_quarantined:
        raw_item["retrieval_status"] = GeneratedArchetypeStatus.QUARANTINED
    _validate_python_document({"generated_archetypes": [raw_item]}, _PERSISTENCE_LIMITS)
    validated = GeneratedArchetype.model_validate(raw_item)
    errors = validated.registration_errors()
    if errors:
        raise ValueError("Generated archetype is not registrable: " + "; ".join(errors))
    GeneratedArchetypeQuery.exact(
        tenant_id=validated.tenant_id,
        service_refs=validated.service_refs,
        environment_refs=validated.environment_refs,
        archetype_kind=validated.archetype_kind,
        generation_version=validated.generation_version,
    )
    document = {"generated_archetypes": [validated.model_dump(mode="json")]}
    _validate_python_document(document, _PERSISTENCE_LIMITS)
    text = yaml.safe_dump(document, sort_keys=False, width=120)
    return validated, _admit_yaml_text(text, _PERSISTENCE_LIMITS)


def _load_file_atomically(
    payload: bytes,
    _query: GeneratedArchetypeQuery,
    limits: _YamlLimits,
) -> _RawGeneratedFile:
    """Parse one immutable payload and expose its raw work before validation."""
    text = payload.decode("utf-8")
    _validate_yaml_events(text, limits)
    document = yaml.safe_load(text) or {}
    if not isinstance(document, dict):
        raise _ArtifactRejected(_DOCUMENT_INVALID)
    raw_items = document.get("generated_archetypes", []) or []
    if not isinstance(raw_items, list):
        raise _ArtifactRejected(_DOCUMENT_INVALID)
    if len(raw_items) > limits.artifacts:
        raise _ArtifactRejected(_ARTIFACT_LIMIT_EXCEEDED, limited=True)

    panel_count, query_count = _raw_collection_counts(raw_items, limits)

    return _RawGeneratedFile(
        items=raw_items,
        artifact_count=len(raw_items),
        panel_count=panel_count,
        query_count=query_count,
    )


def _validate_raw_generated_file(
    raw_file: _RawGeneratedFile,
    query: GeneratedArchetypeQuery,
) -> tuple[list[GeneratedArchetype], int, int]:
    """Validate an already-accounted file and classify its artifacts."""

    matches: list[GeneratedArchetype] = []
    quarantined = 0
    rejected_by_scope = 0
    for raw_item in raw_file.items:
        archetype = GeneratedArchetype.model_validate(raw_item)
        if archetype.retrieval_status == GeneratedArchetypeStatus.QUARANTINED:
            quarantined += 1
        elif experimental_archetype_applicable(archetype, query):
            matches.append(archetype)
        else:
            rejected_by_scope += 1
    return matches, quarantined, rejected_by_scope


def _write_prepared_generated_archetype(
    archetype: GeneratedArchetype,
    payload: bytes,
    root_path: Path,
) -> Path:
    if not _descriptor_write_supported():
        raise OSError(errno.ENOTSUP, "descriptor-relative generated archetype writes are unsupported")
    query = GeneratedArchetypeQuery.exact(
        tenant_id=archetype.tenant_id,
        service_refs=archetype.service_refs,
        environment_refs=archetype.environment_refs,
        archetype_kind=archetype.archetype_kind,
        generation_version=archetype.generation_version,
    )
    target_name = f"{_safe_segment(archetype.id)}-{_artifact_fingerprint(archetype)}.yaml"
    tenant_segment = _safe_segment(query.tenant_id)
    service_segment = _safe_segment("|".join(sorted(query.service_refs)))
    descriptors: list[int] = []
    temporary_name = ""
    temporary_descriptor: int | None = None
    renamed = False
    try:
        root_descriptor = _open_root_without_symlinks(root_path, create=True)
        if root_descriptor is None:  # pragma: no cover - create=True guarantees a descriptor
            raise OSError(errno.ENOENT, "generated archetype root could not be created")
        descriptors.append(root_descriptor)
        tenant_descriptor = _open_or_create_directory(root_descriptor, tenant_segment)
        descriptors.append(tenant_descriptor)
        scope_descriptor = _open_or_create_directory(tenant_descriptor, service_segment)
        descriptors.append(scope_descriptor)

        write_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        for _ in range(8):
            temporary_name = f".{target_name}.{secrets.token_hex(8)}.tmp"
            try:
                temporary_descriptor = os.open(
                    temporary_name,
                    write_flags,
                    0o640,
                    dir_fd=scope_descriptor,
                )
                break
            except FileExistsError:
                continue
        if temporary_descriptor is None:
            raise FileExistsError("could not reserve generated archetype temporary file")

        written = 0
        while written < len(payload):
            count = os.write(temporary_descriptor, payload[written:])
            if count < 1:
                raise OSError(errno.EIO, "generated archetype write made no progress")
            written += count
        os.fsync(temporary_descriptor)
        _close_descriptor(temporary_descriptor)
        temporary_descriptor = None
        os.rename(
            temporary_name,
            target_name,
            src_dir_fd=scope_descriptor,
            dst_dir_fd=scope_descriptor,
        )
        renamed = True
    finally:
        if temporary_descriptor is not None:
            _close_descriptor(temporary_descriptor)
        if temporary_name and not renamed and descriptors:
            try:
                os.unlink(temporary_name, dir_fd=descriptors[-1])
            except FileNotFoundError:
                pass
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except OSError:
                pass
        for descriptor in reversed(descriptors):
            _close_descriptor(descriptor)
    return _scope_directory(root_path, query) / target_name


def write_generated_archetype(archetype: GeneratedArchetype, root_path: Path) -> Path:
    """Write one bounded generated artifact atomically under its exact scope."""
    validated, payload = _prepare_generated_archetype(archetype)
    return _write_prepared_generated_archetype(validated, payload, root_path)


def quarantine_generated_archetype_yaml(archetype_yaml: str, root_path: Path) -> list[Path]:
    """Validate and persist generated YAML without touching the curated registry."""
    _admit_yaml_text(archetype_yaml, _PERSISTENCE_LIMITS)
    document = yaml.safe_load(archetype_yaml) or {}
    if not isinstance(document, dict):
        raise ValueError("Generated archetype YAML must contain a document mapping")
    raw_items = document.get("archetypes", []) or document.get("generated_archetypes", []) or []
    if not isinstance(raw_items, list) or not raw_items:
        raise ValueError("Generated archetype YAML must contain a non-empty archetypes list")
    if len(raw_items) > _PERSISTENCE_LIMITS.artifacts:
        raise _admission_error(_ARTIFACT_LIMIT_EXCEEDED)
    try:
        _raw_collection_counts(raw_items, _PERSISTENCE_LIMITS)
    except _ArtifactRejected as exc:
        raise _admission_error(exc.reason_code) from None

    prepared: list[tuple[GeneratedArchetype, bytes]] = []
    for raw_item in raw_items:
        archetype = GeneratedArchetype.model_validate(raw_item)
        prepared.append(_prepare_generated_archetype(archetype, force_quarantined=True))

    paths = [_write_prepared_generated_archetype(archetype, payload, root_path) for archetype, payload in prepared]

    logger.info(
        "generated_archetypes_quarantined",
        count=len(paths),
        root_fingerprint=_content_fingerprint(str(root_path)),
    )
    return paths


def experimental_archetype_applicable(
    archetype: GeneratedArchetype,
    query: GeneratedArchetypeQuery,
) -> bool:
    """Require exact canonical scope and lifecycle matches; no fuzzy fallback."""
    return (
        archetype.retrieval_status == GeneratedArchetypeStatus.EXPERIMENTAL
        and archetype.tenant_id == query.tenant_id
        and bool(archetype.service_refs)
        and archetype.service_refs == query.service_refs
        and archetype.environment_refs == query.environment_refs
        and archetype.archetype_kind == query.archetype_kind
        and archetype.generation_version == query.generation_version
    )


def load_experimental_archetypes(
    root_path: Path,
    query: GeneratedArchetypeQuery,
    *,
    max_directory_entries: int = DEFAULT_GENERATED_RETRIEVAL_MAX_DIRECTORY_ENTRIES,
    max_files: int = DEFAULT_GENERATED_RETRIEVAL_MAX_FILES,
    max_file_bytes: int = DEFAULT_GENERATED_RETRIEVAL_MAX_FILE_BYTES,
    max_total_bytes: int = DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_BYTES,
    max_yaml_nodes: int = DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_NODES,
    max_yaml_depth: int = DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_DEPTH,
    max_yaml_scalars: int = DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALARS,
    max_yaml_scalar_bytes: int = DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALAR_BYTES,
    max_artifacts_per_file: int = DEFAULT_GENERATED_RETRIEVAL_MAX_ARTIFACTS_PER_FILE,
    max_panels_per_file: int = DEFAULT_GENERATED_RETRIEVAL_MAX_PANELS_PER_FILE,
    max_queries_per_file: int = DEFAULT_GENERATED_RETRIEVAL_MAX_QUERIES_PER_FILE,
    max_total_artifacts: int = DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_ARTIFACTS,
    max_total_panels: int = DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_PANELS,
    max_total_queries: int = DEFAULT_GENERATED_RETRIEVAL_MAX_TOTAL_QUERIES,
    max_results: int = DEFAULT_GENERATED_RETRIEVAL_MAX_RESULTS,
) -> GeneratedArchetypeRetrieval:
    """Load one exact scope through descriptor-relative, bounded reads."""
    limits = (
        max_directory_entries,
        max_files,
        max_file_bytes,
        max_total_bytes,
        max_yaml_nodes,
        max_yaml_depth,
        max_yaml_scalars,
        max_yaml_scalar_bytes,
        max_artifacts_per_file,
        max_panels_per_file,
        max_queries_per_file,
        max_total_artifacts,
        max_total_panels,
        max_total_queries,
        max_results,
    )
    if min(limits) < 1:
        raise ValueError("generated archetype retrieval limits must be positive")
    state = _RetrievalState()
    if not query.service_refs:
        state.rejected_by_scope = 1
        state.reject(
            GENERATED_ARCHETYPE_SERVICE_SCOPE_REQUIRED,
            skipped=True,
        )
        return state.result()
    if not query.environment_refs:
        state.rejected_by_scope = 1
        state.reject(
            GENERATED_ARCHETYPE_ENVIRONMENT_SCOPE_REQUIRED,
            skipped=True,
        )
        return state.result()

    scope_fingerprint = _content_fingerprint("|".join((query.tenant_id, *sorted(query.service_refs))))
    if not _descriptor_access_supported():
        state.reject(_PLATFORM_UNSUPPORTED, invalid=True, skipped=True)
        _log_retrieval_failure(
            reason_code=_PLATFORM_UNSUPPORTED,
            exception_class="UnsupportedOperation",
            fingerprint=scope_fingerprint,
            state=state,
        )
        return state.result()

    yaml_limits = _YamlLimits(
        nodes=max_yaml_nodes,
        depth=max_yaml_depth,
        scalars=max_yaml_scalars,
        scalar_bytes=max_yaml_scalar_bytes,
        artifacts=max_artifacts_per_file,
        panels=max_panels_per_file,
        queries=max_queries_per_file,
    )
    tenant_segment = _safe_segment(query.tenant_id)
    service_segment = _safe_segment("|".join(sorted(query.service_refs)))
    directory_descriptors: list[int] = []
    try:
        try:
            root_descriptor = _open_root_without_symlinks(root_path)
        except _PathSymlinkRejected as exc:
            state.symlinks_rejected += 1
            state.reject(_SCOPE_SYMLINK_REJECTED, invalid=True, skipped=True)
            _log_retrieval_failure(
                reason_code=_SCOPE_SYMLINK_REJECTED,
                exception_class=type(exc).__name__,
                fingerprint=_content_fingerprint(str(root_path)),
                state=state,
            )
            return state.result()
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except OSError as exc:
            state.reject(_ROOT_OPEN_FAILED, invalid=True, skipped=True)
            _log_retrieval_failure(
                reason_code=_ROOT_OPEN_FAILED,
                exception_class=type(exc).__name__,
                fingerprint=_content_fingerprint(str(root_path)),
                state=state,
            )
            return state.result()
        if root_descriptor is None:
            return state.result()
        directory_descriptors.append(root_descriptor)

        parent_descriptor = root_descriptor
        for segment in (tenant_segment, service_segment):
            try:
                child_descriptor = os.open(
                    segment,
                    _open_flags(directory=True),
                    dir_fd=parent_descriptor,
                )
            except FileNotFoundError:
                return state.result()
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except OSError as exc:
                if _is_symlink_at(parent_descriptor, segment):
                    reason_code = _SCOPE_SYMLINK_REJECTED
                    state.symlinks_rejected += 1
                else:
                    reason_code = _SCOPE_OPEN_FAILED
                state.reject(reason_code, invalid=True, skipped=True)
                _log_retrieval_failure(
                    reason_code=reason_code,
                    exception_class=type(exc).__name__,
                    fingerprint=_content_fingerprint(segment),
                    state=state,
                )
                return state.result()
            directory_descriptors.append(child_descriptor)
            parent_descriptor = child_descriptor
        scope_descriptor = parent_descriptor

        candidate_names: list[str] = []
        try:
            with os.scandir(scope_descriptor) as entries:
                for entry in entries:
                    state.directory_entries_discovered += 1
                    if state.directory_entries_discovered > max_directory_entries:
                        state.reject(
                            _DIRECTORY_ENTRY_LIMIT_EXCEEDED,
                            limited=True,
                            skipped=True,
                        )
                        _log_retrieval_failure(
                            reason_code=_DIRECTORY_ENTRY_LIMIT_EXCEEDED,
                            exception_class="SemanticLimitExceeded",
                            fingerprint=scope_fingerprint,
                            state=state,
                        )
                        return state.result()
                    if not entry.name.casefold().endswith(".yaml"):
                        continue
                    try:
                        is_symlink = entry.is_symlink()
                    except AUTHORITY_BOUNDARY_ERRORS:
                        raise
                    except OSError as exc:
                        state.reject(_FILE_STAT_FAILED, invalid=True)
                        _log_retrieval_failure(
                            reason_code=_FILE_STAT_FAILED,
                            exception_class=type(exc).__name__,
                            fingerprint=_content_fingerprint(entry.name),
                            state=state,
                        )
                        continue
                    if is_symlink:
                        state.symlinks_rejected += 1
                        state.reject(_SCOPE_SYMLINK_REJECTED)
                        continue
                    candidate_names.append(entry.name)
                    state.files_discovered += 1
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except OSError as exc:
            state.reject(_DIRECTORY_LIST_FAILED, invalid=True, skipped=True)
            _log_retrieval_failure(
                reason_code=_DIRECTORY_LIST_FAILED,
                exception_class=type(exc).__name__,
                fingerprint=scope_fingerprint,
                state=state,
            )
            return state.result()

        matches: list[GeneratedArchetype] = []
        applicable_files = 0
        for name in sorted(candidate_names):
            fingerprint = _content_fingerprint(name)
            descriptor: int | None = None
            try:
                try:
                    descriptor = os.open(name, _open_flags(), dir_fd=scope_descriptor)
                except AUTHORITY_BOUNDARY_ERRORS:
                    raise
                except OSError as exc:
                    if exc.errno in {errno.ELOOP, errno.EMLINK} or _is_symlink_at(
                        scope_descriptor,
                        name,
                    ):
                        state.symlinks_rejected += 1
                        state.reject(_SCOPE_SYMLINK_REJECTED)
                        reason_code = _SCOPE_SYMLINK_REJECTED
                    else:
                        state.reject(_FILE_OPEN_FAILED, invalid=True)
                        reason_code = _FILE_OPEN_FAILED
                    _log_retrieval_failure(
                        reason_code=reason_code,
                        exception_class=type(exc).__name__,
                        fingerprint=fingerprint,
                        state=state,
                    )
                    continue

                try:
                    metadata = os.fstat(descriptor)
                except AUTHORITY_BOUNDARY_ERRORS:
                    raise
                except OSError as exc:
                    state.reject(_FILE_STAT_FAILED, invalid=True)
                    _log_retrieval_failure(
                        reason_code=_FILE_STAT_FAILED,
                        exception_class=type(exc).__name__,
                        fingerprint=fingerprint,
                        state=state,
                    )
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    state.reject(_NON_REGULAR_FILE_REJECTED, invalid=True)
                    _log_retrieval_failure(
                        reason_code=_NON_REGULAR_FILE_REJECTED,
                        exception_class="FileTypeRejected",
                        fingerprint=fingerprint,
                        state=state,
                    )
                    continue
                if metadata.st_size > max_file_bytes:
                    state.oversized_files += 1
                    state.reject(_FILE_SIZE_LIMIT_EXCEEDED, limited=True)
                    _log_retrieval_failure(
                        reason_code=_FILE_SIZE_LIMIT_EXCEEDED,
                        exception_class="SemanticLimitExceeded",
                        fingerprint=fingerprint,
                        state=state,
                    )
                    continue
                if state.bytes_scanned + metadata.st_size > max_total_bytes:
                    state.reject(
                        _TOTAL_BYTES_LIMIT_EXCEEDED,
                        limited=True,
                        skipped=True,
                    )
                    _log_retrieval_failure(
                        reason_code=_TOTAL_BYTES_LIMIT_EXCEEDED,
                        exception_class="SemanticLimitExceeded",
                        fingerprint=fingerprint,
                        state=state,
                    )
                    return state.result()
                try:
                    payload = _read_descriptor(descriptor, max_file_bytes)
                except AUTHORITY_BOUNDARY_ERRORS:
                    raise
                except OSError as exc:
                    state.reject(_FILE_READ_FAILED, invalid=True)
                    _log_retrieval_failure(
                        reason_code=_FILE_READ_FAILED,
                        exception_class=type(exc).__name__,
                        fingerprint=fingerprint,
                        state=state,
                    )
                    continue
                if len(payload) > max_file_bytes:
                    state.oversized_files += 1
                    state.reject(_FILE_SIZE_LIMIT_EXCEEDED, limited=True)
                    _log_retrieval_failure(
                        reason_code=_FILE_SIZE_LIMIT_EXCEEDED,
                        exception_class="SemanticLimitExceeded",
                        fingerprint=_content_fingerprint(payload),
                        state=state,
                    )
                    continue
                if state.bytes_scanned + len(payload) > max_total_bytes:
                    state.reject(
                        _TOTAL_BYTES_LIMIT_EXCEEDED,
                        limited=True,
                        skipped=True,
                    )
                    _log_retrieval_failure(
                        reason_code=_TOTAL_BYTES_LIMIT_EXCEEDED,
                        exception_class="SemanticLimitExceeded",
                        fingerprint=_content_fingerprint(payload),
                        state=state,
                    )
                    return state.result()
                state.files_scanned += 1
                state.bytes_scanned += len(payload)
            finally:
                if descriptor is not None:
                    _close_descriptor(descriptor)

            artifact_fingerprint = _content_fingerprint(payload)
            try:
                raw_file = _load_file_atomically(payload, query, yaml_limits)
            except _ArtifactRejected as exc:
                state.reject(exc.reason_code, invalid=True, limited=exc.limited)
                _log_retrieval_failure(
                    reason_code=exc.reason_code,
                    exception_class=("SemanticLimitExceeded" if exc.limited else type(exc).__name__),
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                continue
            except UnicodeError as exc:
                state.reject(_YAML_DECODE_FAILED, invalid=True)
                _log_retrieval_failure(
                    reason_code=_YAML_DECODE_FAILED,
                    exception_class=type(exc).__name__,
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                continue
            except yaml.YAMLError as exc:
                state.reject(_YAML_PARSE_FAILED, invalid=True)
                _log_retrieval_failure(
                    reason_code=_YAML_PARSE_FAILED,
                    exception_class=type(exc).__name__,
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                continue
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except (ValidationError, TypeError, ValueError) as exc:
                state.reject(_SCHEMA_VALIDATION_FAILED, invalid=True)
                _log_retrieval_failure(
                    reason_code=_SCHEMA_VALIDATION_FAILED,
                    exception_class=type(exc).__name__,
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                continue

            aggregate_limit: str | None = None
            if state.total_artifacts + raw_file.artifact_count > max_total_artifacts:
                aggregate_limit = _TOTAL_ARTIFACT_LIMIT_EXCEEDED
            elif state.total_panels + raw_file.panel_count > max_total_panels:
                aggregate_limit = _TOTAL_PANEL_LIMIT_EXCEEDED
            elif state.total_queries + raw_file.query_count > max_total_queries:
                aggregate_limit = _TOTAL_QUERY_LIMIT_EXCEEDED
            if aggregate_limit is not None:
                reason_code = aggregate_limit
                state.reject(reason_code, limited=True, skipped=True)
                _log_retrieval_failure(
                    reason_code=reason_code,
                    exception_class="SemanticLimitExceeded",
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                return state.result()

            state.total_artifacts += raw_file.artifact_count
            state.total_panels += raw_file.panel_count
            state.total_queries += raw_file.query_count
            try:
                file_matches, file_quarantined, file_rejected_by_scope = _validate_raw_generated_file(
                    raw_file,
                    query,
                )
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except (ValidationError, TypeError, ValueError) as exc:
                state.reject(_SCHEMA_VALIDATION_FAILED, invalid=True)
                _log_retrieval_failure(
                    reason_code=_SCHEMA_VALIDATION_FAILED,
                    exception_class=type(exc).__name__,
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                continue

            if file_matches and applicable_files + 1 > max_files:
                reason_code = _FILE_COUNT_LIMIT_EXCEEDED
                state.reject(reason_code, limited=True, skipped=True)
                _log_retrieval_failure(
                    reason_code=reason_code,
                    exception_class="SemanticLimitExceeded",
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                return state.result()
            if len(matches) + len(file_matches) > max_results:
                reason_code = _RESULT_LIMIT_EXCEEDED
                state.reject(reason_code, limited=True, skipped=True)
                _log_retrieval_failure(
                    reason_code=reason_code,
                    exception_class="SemanticLimitExceeded",
                    fingerprint=artifact_fingerprint,
                    state=state,
                )
                return state.result()

            state.quarantined += file_quarantined
            state.rejected_by_scope += file_rejected_by_scope
            if file_matches:
                applicable_files += 1
            matches.extend(file_matches)
        return state.result(matches)
    finally:
        for descriptor in reversed(directory_descriptors):
            _close_descriptor(descriptor)
