"""Schemas for generated archetypes kept outside the curated registry."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, field_validator

from tacit.archetypes.schema import InvestigationArchetype
from tacit.tenancy import TenantBoundaryError, resolve_tenant_boundary

MAX_GENERATED_QUERY_SERVICE_REFS = 64
MAX_GENERATED_QUERY_ENVIRONMENT_REFS = 64
GENERATED_ARCHETYPE_ENVIRONMENT_SCOPE_REQUIRED = "generated_archetype_environment_scope_required"
GENERATED_ARCHETYPE_SERVICE_SCOPE_REQUIRED = "generated_archetype_service_scope_required"
MAX_GENERATED_QUERY_SCOPE_SCALAR_CHARS = 1_024
MAX_GENERATED_QUERY_SCOPE_SCALAR_BYTES = 2_048


class GeneratedArchetypeQueryWorkLimitError(ValueError):
    """An exact generated-archetype query exceeded its raw scope budget."""

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"generated_archetype_query_work_limit_exceeded: {dimension} exceeds {limit}")


class ArchetypeRetrievalMode(StrEnum):
    CURATED_ONLY = "curated_only"
    CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE = "curated_with_experimental_exact_scope"


class GeneratedArchetypeOrigin(StrEnum):
    GENERATED_EXPERIMENTAL = "generated_experimental"


class GeneratedArchetypeStatus(StrEnum):
    QUARANTINED = "quarantined"
    EXPERIMENTAL = "experimental"


class GeneratedArchetypeRetrievalStatus(StrEnum):
    PASSED = "passed"
    PARTIAL = "partial"
    SKIPPED = "skipped"


def _normalize_ref(value: str) -> str:
    return re.sub(r"[^a-z0-9_.:-]+", "-", value.strip().casefold()).strip("-")


def normalize_tenant_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw or raw == "*":
        return raw
    try:
        return resolve_tenant_boundary(raw, None)
    except TenantBoundaryError as exc:
        raise ValueError(exc.detail) from None


def normalize_service_ref(value: str) -> str:
    normalized = _normalize_ref(value)
    if normalized.startswith("entity:service:"):
        return normalized
    if normalized.startswith("service:"):
        return f"entity:{normalized}"
    return f"entity:service:{normalized}" if normalized else ""


def normalize_environment_ref(value: str) -> str:
    normalized = _normalize_ref(value)
    if normalized.startswith("environment:"):
        return normalized
    return f"environment:{normalized}" if normalized else ""


def _admit_query_scope_scalar(value: str, dimension: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Invalid generated archetype query {dimension}")
    if len(value) > MAX_GENERATED_QUERY_SCOPE_SCALAR_CHARS:
        raise GeneratedArchetypeQueryWorkLimitError(
            "scope_scalar_chars",
            len(value),
            MAX_GENERATED_QUERY_SCOPE_SCALAR_CHARS,
        )
    encoded_length = len(value.encode("utf-8"))
    if encoded_length > MAX_GENERATED_QUERY_SCOPE_SCALAR_BYTES:
        raise GeneratedArchetypeQueryWorkLimitError(
            "scope_scalar_bytes",
            encoded_length,
            MAX_GENERATED_QUERY_SCOPE_SCALAR_BYTES,
        )
    return value


class GeneratedArchetype(InvestigationArchetype):
    """A generated template artifact that is never part of the curated registry."""

    origin: Literal[GeneratedArchetypeOrigin.GENERATED_EXPERIMENTAL] = GeneratedArchetypeOrigin.GENERATED_EXPERIMENTAL
    retrieval_status: GeneratedArchetypeStatus = GeneratedArchetypeStatus.QUARANTINED
    tenant_id: str = ""
    service_refs: frozenset[str] = Field(default_factory=frozenset)
    environment_refs: frozenset[str] = Field(default_factory=frozenset)
    archetype_kind: str = "investigation_dashboard"
    generation_version: str = "generated-archetype-v1"
    generation_run_id: str = ""
    source_refs: list[str] = Field(default_factory=list)
    created_at: datetime

    @field_validator("tenant_id", "archetype_kind", "generation_version", mode="before")
    @classmethod
    def _admit_scope_identity(cls, value: str) -> str:
        return _admit_query_scope_scalar(value, "scope_identity")

    @field_validator("service_refs", mode="before")
    @classmethod
    def _admit_service_refs(cls, values: object) -> object:
        if not isinstance(values, (list, set, frozenset, tuple)):
            return values
        if len(values) > MAX_GENERATED_QUERY_SERVICE_REFS:
            raise GeneratedArchetypeQueryWorkLimitError(
                "service_refs",
                len(values),
                MAX_GENERATED_QUERY_SERVICE_REFS,
            )
        for value in values:
            _admit_query_scope_scalar(value, "service_ref")
        return values

    @field_validator("environment_refs", mode="before")
    @classmethod
    def _admit_environment_refs(cls, values: object) -> object:
        if not isinstance(values, (list, set, frozenset, tuple)):
            return values
        if len(values) > MAX_GENERATED_QUERY_ENVIRONMENT_REFS:
            raise GeneratedArchetypeQueryWorkLimitError(
                "environment_refs",
                len(values),
                MAX_GENERATED_QUERY_ENVIRONMENT_REFS,
            )
        for value in values:
            _admit_query_scope_scalar(value, "environment_ref")
        return values

    @field_validator("tenant_id")
    @classmethod
    def _normalize_tenant(cls, value: str) -> str:
        return normalize_tenant_id(value)

    @field_validator("service_refs")
    @classmethod
    def _normalize_services(cls, values: frozenset[str]) -> frozenset[str]:
        return frozenset(ref for value in values if (ref := normalize_service_ref(value)))

    @field_validator("environment_refs")
    @classmethod
    def _normalize_environments(cls, values: frozenset[str]) -> frozenset[str]:
        return frozenset(ref for value in values if (ref := normalize_environment_ref(value)))

    def registration_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.id.strip():
            errors.append("id is required")
        if not self.tenant_id:
            errors.append("tenant_id is required")
        elif self.tenant_id == "*":
            errors.append("tenant_id must be concrete")
        if not self.service_refs:
            errors.append("at least one service_ref is required")
        if not self.environment_refs:
            errors.append("at least one environment_ref is required")
        if not self.archetype_kind:
            errors.append("archetype_kind is required")
        if not self.generation_version:
            errors.append("generation_version is required")
        if not self.generation_run_id:
            errors.append("generation_run_id is required")
        if not self.source_refs:
            errors.append("at least one source_ref is required")
        return errors


@dataclass(frozen=True)
class GeneratedArchetypeQuery:
    tenant_id: str
    service_refs: frozenset[str]
    environment_refs: frozenset[str] = field(default_factory=frozenset)
    archetype_kind: str = "investigation_dashboard"
    generation_version: str = "generated-archetype-v1"

    def __post_init__(self) -> None:
        if len(self.service_refs) > MAX_GENERATED_QUERY_SERVICE_REFS:
            raise GeneratedArchetypeQueryWorkLimitError(
                "service_refs",
                len(self.service_refs),
                MAX_GENERATED_QUERY_SERVICE_REFS,
            )
        if len(self.environment_refs) > MAX_GENERATED_QUERY_ENVIRONMENT_REFS:
            raise GeneratedArchetypeQueryWorkLimitError(
                "environment_refs",
                len(self.environment_refs),
                MAX_GENERATED_QUERY_ENVIRONMENT_REFS,
            )
        if not isinstance(self.tenant_id, str) or not self.tenant_id.strip() or self.tenant_id.strip() == "*":
            raise ValueError("Invalid knowledge tenant")
        raw_tenant = _admit_query_scope_scalar(self.tenant_id, "tenant_id")
        normalized_tenant = normalize_tenant_id(raw_tenant)
        if not normalized_tenant or normalized_tenant == "*":
            raise ValueError("Invalid knowledge tenant")

        normalized_services = frozenset(
            ref
            for value in self.service_refs
            if (ref := normalize_service_ref(_admit_query_scope_scalar(value, "service_ref")))
        )
        normalized_environments = frozenset(
            ref
            for value in self.environment_refs
            if (ref := normalize_environment_ref(_admit_query_scope_scalar(value, "environment_ref")))
        )
        _admit_query_scope_scalar(self.archetype_kind, "archetype_kind")
        _admit_query_scope_scalar(self.generation_version, "generation_version")
        object.__setattr__(self, "tenant_id", normalized_tenant)
        object.__setattr__(self, "service_refs", normalized_services)
        object.__setattr__(self, "environment_refs", normalized_environments)

    @classmethod
    def exact(
        cls,
        *,
        tenant_id: str,
        service_refs: set[str] | frozenset[str] | list[str],
        environment_refs: set[str] | frozenset[str] | list[str] | None = None,
        archetype_kind: str = "investigation_dashboard",
        generation_version: str = "generated-archetype-v1",
    ) -> GeneratedArchetypeQuery:
        if not isinstance(tenant_id, str) or not tenant_id.strip() or tenant_id.strip() == "*":
            raise ValueError("Invalid knowledge tenant")
        if len(service_refs) > MAX_GENERATED_QUERY_SERVICE_REFS:
            raise GeneratedArchetypeQueryWorkLimitError(
                "service_refs",
                len(service_refs),
                MAX_GENERATED_QUERY_SERVICE_REFS,
            )
        raw_environment_refs = environment_refs or ()
        if len(raw_environment_refs) > MAX_GENERATED_QUERY_ENVIRONMENT_REFS:
            raise GeneratedArchetypeQueryWorkLimitError(
                "environment_refs",
                len(raw_environment_refs),
                MAX_GENERATED_QUERY_ENVIRONMENT_REFS,
            )
        _admit_query_scope_scalar(tenant_id, "tenant_id")
        for value in service_refs:
            _admit_query_scope_scalar(value, "service_ref")
        for value in raw_environment_refs:
            _admit_query_scope_scalar(value, "environment_ref")
        _admit_query_scope_scalar(archetype_kind, "archetype_kind")
        _admit_query_scope_scalar(generation_version, "generation_version")
        return cls(
            tenant_id=tenant_id,
            service_refs=frozenset(service_refs),
            environment_refs=frozenset(raw_environment_refs),
            archetype_kind=archetype_kind,
            generation_version=generation_version,
        )


@dataclass(frozen=True)
class GeneratedArchetypeRetrieval:
    archetypes: list[GeneratedArchetype] = field(default_factory=list)
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
    status: GeneratedArchetypeRetrievalStatus = GeneratedArchetypeRetrievalStatus.PASSED
    reason_code: str = "generated_archetype_retrieval_complete"
    limit_reason_codes: tuple[str, ...] = ()
    reason_counts: tuple[tuple[str, int], ...] = ()
