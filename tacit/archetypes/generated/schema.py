"""Schemas for generated archetypes kept outside the curated registry."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, field_validator

from tacit.archetypes.schema import InvestigationArchetype


class ArchetypeRetrievalMode(StrEnum):
    CURATED_ONLY = "curated_only"
    CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE = "curated_with_experimental_exact_scope"


class GeneratedArchetypeOrigin(StrEnum):
    GENERATED_EXPERIMENTAL = "generated_experimental"


class GeneratedArchetypeStatus(StrEnum):
    QUARANTINED = "quarantined"
    EXPERIMENTAL = "experimental"


def _normalize_ref(value: str) -> str:
    return re.sub(r"[^a-z0-9_.:-]+", "-", value.strip().casefold()).strip("-")


def normalize_tenant_id(value: str) -> str:
    return _normalize_ref(value)


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
        if not self.service_refs:
            errors.append("at least one service_ref is required")
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
        return cls(
            tenant_id=normalize_tenant_id(tenant_id),
            service_refs=frozenset(ref for value in service_refs if (ref := normalize_service_ref(value))),
            environment_refs=frozenset(
                ref for value in environment_refs or () if (ref := normalize_environment_ref(value))
            ),
            archetype_kind=archetype_kind,
            generation_version=generation_version,
        )


@dataclass(frozen=True)
class GeneratedArchetypeRetrieval:
    archetypes: list[GeneratedArchetype] = field(default_factory=list)
    files_scanned: int = 0
    quarantined: int = 0
    rejected_by_scope: int = 0
    invalid: int = 0
