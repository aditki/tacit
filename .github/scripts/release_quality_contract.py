"""Shared, fail-closed identity for release-quality long-lived state."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

LONG_LIVED_STATE_DATABASES = ("signals.db", "history.db", "feedback.db")
ADMITTED_STATE_MANIFEST = ".tacit-release-quality-state.json"
ARCHIVED_STATE_MANIFEST = "long-lived-state-manifest.json"
LOGICAL_STATE_FINGERPRINT_PREFIX = b"tacit-validation-state-v1:long-lived\0"
MAX_STATE_DATABASE_BYTES = 2 * 1024 * 1024 * 1024
MAX_STATE_TOTAL_BYTES = 4 * 1024 * 1024 * 1024
MAX_TENANT_LENGTH = 128
FINGERPRINT_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
TENANT_PATTERN = re.compile(r"[A-Za-z0-9_.:-]+\Z")
READ_CHUNK_BYTES = 1024 * 1024
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


class ReleaseQualityContractError(ValueError):
    """A release-quality evidence identity is invalid or incomplete."""


@dataclass(frozen=True)
class StateSnapshotLimits:
    """The only supported per-database and aggregate snapshot limits."""

    max_database_bytes: int
    max_total_bytes: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_database_bytes, bool)
            or not isinstance(self.max_database_bytes, int)
            or self.max_database_bytes <= 0
        ):
            raise ReleaseQualityContractError("max_database_bytes must be a positive integer")
        if (
            isinstance(self.max_total_bytes, bool)
            or not isinstance(self.max_total_bytes, int)
            or self.max_total_bytes < self.max_database_bytes
        ):
            raise ReleaseQualityContractError("max_total_bytes must not be smaller than max_database_bytes")

    def as_dict(self) -> dict[str, int]:
        return {
            "max_database_bytes": self.max_database_bytes,
            "max_total_bytes": self.max_total_bytes,
        }


STATE_SNAPSHOT_LIMITS = StateSnapshotLimits(
    max_database_bytes=MAX_STATE_DATABASE_BYTES,
    max_total_bytes=MAX_STATE_TOTAL_BYTES,
)


def validate_concrete_tenant(value: object) -> str:
    """Return a canonical concrete tenant suitable for release evidence."""
    if not isinstance(value, str):
        raise ReleaseQualityContractError("release-quality tenant is invalid")
    tenant = value.strip()
    if not tenant or tenant == "*" or len(tenant) > MAX_TENANT_LENGTH or TENANT_PATTERN.fullmatch(tenant) is None:
        raise ReleaseQualityContractError("release-quality tenant is invalid")
    return tenant


def validate_state_snapshot_limits(value: object, *, require_release_limits: bool = False) -> StateSnapshotLimits:
    """Parse one state-size contract, optionally requiring the release values."""
    if not isinstance(value, dict) or set(value) != {"max_database_bytes", "max_total_bytes"}:
        raise ReleaseQualityContractError("release-quality state size contract is invalid")
    try:
        limits = StateSnapshotLimits(
            max_database_bytes=value["max_database_bytes"],
            max_total_bytes=value["max_total_bytes"],
        )
    except (KeyError, ReleaseQualityContractError) as exc:
        raise ReleaseQualityContractError("release-quality state size contract is invalid") from exc
    if require_release_limits and limits != STATE_SNAPSHOT_LIMITS:
        raise ReleaseQualityContractError("release-quality state size contract does not match the release contract")
    return limits


def validate_snapshot_database_sizes(
    paths: Iterable[Path],
    *,
    limits: StateSnapshotLimits = STATE_SNAPSHOT_LIMITS,
) -> int:
    """Validate the disposable logical database generation after one snapshot."""
    total_size = 0
    for path in paths:
        try:
            details = path.stat()
        except OSError as exc:
            raise ReleaseQualityContractError(f"long-lived state database {path.name} could not be inspected") from exc
        if not path.is_file() or path.is_symlink() or details.st_size > limits.max_database_bytes:
            raise ReleaseQualityContractError(
                f"long-lived state database {path.name} exceeds the per-database size limit"
            )
        total_size += details.st_size
        if total_size > limits.max_total_bytes:
            raise ReleaseQualityContractError("long-lived state exceeds the aggregate size limit")
    return total_size


def validate_source_database_sizes(
    paths: Iterable[Path],
    *,
    limits: StateSnapshotLimits = STATE_SNAPSHOT_LIMITS,
) -> int:
    """Bound every source main/sidecar set before snapshot materialization."""
    total_size = 0
    for path in paths:
        database_size = 0
        for candidate in (path, *(Path(f"{path}{suffix}") for suffix in SQLITE_SIDECAR_SUFFIXES)):
            if not candidate.exists():
                continue
            try:
                details = candidate.stat()
            except OSError as exc:
                raise ReleaseQualityContractError(
                    f"long-lived state database {path.name} could not be inspected"
                ) from exc
            if not candidate.is_file() or candidate.is_symlink():
                raise ReleaseQualityContractError(f"long-lived state database {path.name} is not a regular file set")
            database_size += details.st_size
        if database_size > limits.max_database_bytes:
            raise ReleaseQualityContractError(
                f"long-lived state database {path.name} exceeds the per-database size limit"
            )
        total_size += database_size
        if total_size > limits.max_total_bytes:
            raise ReleaseQualityContractError("long-lived state exceeds the aggregate size limit")
    return total_size


def logical_snapshot_fingerprint(paths: Iterable[Path]) -> str:
    """Hash the single SQLite-backed logical generation supplied to evaluation."""
    digest = hashlib.sha256()
    digest.update(LOGICAL_STATE_FINGERPRINT_PREFIX)
    for path in paths:
        details = path.stat()
        digest.update(path.name.encode("utf-8"))
        digest.update(details.st_size.to_bytes(8, "big"))
        with path.open("rb") as source:
            while chunk := source.read(READ_CHUNK_BYTES):
                digest.update(chunk)
    return digest.hexdigest()


def build_admitted_state_manifest(
    *,
    fingerprint: str,
    tenant_id: str,
    limits: StateSnapshotLimits,
) -> dict[str, object]:
    """Construct the immutable identity record attached to one snapshot."""
    if FINGERPRINT_PATTERN.fullmatch(fingerprint) is None:
        raise ReleaseQualityContractError("release-quality state fingerprint is invalid")
    return {
        "schema_version": 1,
        "tenant": validate_concrete_tenant(tenant_id),
        "fingerprint": fingerprint,
        "databases": list(LONG_LIVED_STATE_DATABASES),
        "limits": limits.as_dict(),
    }


def validate_admitted_state_manifest(
    value: object,
    *,
    expected_tenant: str | None = None,
    expected_fingerprint: str | None = None,
    require_release_limits: bool = False,
) -> tuple[str, str, StateSnapshotLimits]:
    """Validate the snapshot identity record independently of its container."""
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "tenant",
        "fingerprint",
        "databases",
        "limits",
    }:
        raise ReleaseQualityContractError("release-quality state manifest is invalid")
    if value.get("schema_version") != 1 or value.get("databases") != list(LONG_LIVED_STATE_DATABASES):
        raise ReleaseQualityContractError("release-quality state manifest is invalid")
    tenant = validate_concrete_tenant(value.get("tenant"))
    fingerprint = value.get("fingerprint")
    if not isinstance(fingerprint, str) or FINGERPRINT_PATTERN.fullmatch(fingerprint) is None:
        raise ReleaseQualityContractError("release-quality state manifest fingerprint is invalid")
    limits = validate_state_snapshot_limits(value.get("limits"), require_release_limits=require_release_limits)
    if expected_tenant is not None and tenant != validate_concrete_tenant(expected_tenant):
        raise ReleaseQualityContractError("release-quality state manifest tenant does not match")
    if expected_fingerprint is not None and fingerprint != expected_fingerprint:
        raise ReleaseQualityContractError("release-quality state manifest fingerprint does not match")
    return tenant, fingerprint, limits
