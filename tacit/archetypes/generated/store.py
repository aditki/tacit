"""Filesystem quarantine for generated archetypes.

The quarantine is intentionally separate from ``TACIT_ARCHETYPES_PATH``. Only
an explicit experimental exact-scope query may read from it.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import structlog
import yaml

from tacit.archetypes.generated.schema import (
    GeneratedArchetype,
    GeneratedArchetypeQuery,
    GeneratedArchetypeRetrieval,
    GeneratedArchetypeStatus,
)

logger = structlog.get_logger()


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


def write_generated_archetype(archetype: GeneratedArchetype, root_path: Path) -> Path:
    """Write one generated artifact atomically under its exact tenant/service scope."""
    errors = archetype.registration_errors()
    if errors:
        raise ValueError("Generated archetype is not registrable: " + "; ".join(errors))

    query = GeneratedArchetypeQuery.exact(
        tenant_id=archetype.tenant_id,
        service_refs=archetype.service_refs,
        environment_refs=archetype.environment_refs,
        archetype_kind=archetype.archetype_kind,
        generation_version=archetype.generation_version,
    )
    directory = _scope_directory(root_path, query)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{_safe_segment(archetype.id)}-{_artifact_fingerprint(archetype)}.yaml"
    temporary = target.with_suffix(".yaml.tmp")
    document = {"generated_archetypes": [archetype.model_dump(mode="json")]}
    temporary.write_text(yaml.safe_dump(document, sort_keys=False, width=120))
    temporary.replace(target)
    return target


def quarantine_generated_archetype_yaml(archetype_yaml: str, root_path: Path) -> list[Path]:
    """Validate and persist generated YAML without touching the curated registry."""
    document = yaml.safe_load(archetype_yaml) or {}
    raw_items = document.get("archetypes", []) or document.get("generated_archetypes", []) or []
    if not isinstance(raw_items, list) or not raw_items:
        raise ValueError("Generated archetype YAML must contain a non-empty archetypes list")

    paths: list[Path] = []
    for raw_item in raw_items:
        archetype = GeneratedArchetype.model_validate(raw_item)
        if archetype.retrieval_status != GeneratedArchetypeStatus.QUARANTINED:
            archetype = archetype.model_copy(update={"retrieval_status": GeneratedArchetypeStatus.QUARANTINED})
        paths.append(write_generated_archetype(archetype, root_path))

    logger.info(
        "generated_archetypes_quarantined",
        count=len(paths),
        root=str(root_path),
        paths=[str(path) for path in paths],
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
) -> GeneratedArchetypeRetrieval:
    """Load only the exact tenant/service directory requested by experimental mode."""
    if not query.tenant_id or not query.service_refs:
        return GeneratedArchetypeRetrieval(rejected_by_scope=1)

    directory = _scope_directory(root_path, query)
    if not directory.is_dir():
        return GeneratedArchetypeRetrieval()

    matches: list[GeneratedArchetype] = []
    files_scanned = 0
    quarantined = 0
    rejected_by_scope = 0
    invalid = 0
    for path in sorted(directory.glob("*.yaml")):
        files_scanned += 1
        try:
            document = yaml.safe_load(path.read_text()) or {}
            raw_items = document.get("generated_archetypes", []) or []
            if not isinstance(raw_items, list):
                raise ValueError("generated_archetypes must be a list")
            for raw_item in raw_items:
                archetype = GeneratedArchetype.model_validate(raw_item)
                if archetype.retrieval_status == GeneratedArchetypeStatus.QUARANTINED:
                    quarantined += 1
                elif experimental_archetype_applicable(archetype, query):
                    matches.append(archetype)
                else:
                    rejected_by_scope += 1
        except Exception:
            invalid += 1
            logger.warning("generated_archetype_quarantine_read_failed", path=str(path), exc_info=True)

    return GeneratedArchetypeRetrieval(
        archetypes=matches,
        files_scanned=files_scanned,
        quarantined=quarantined,
        rejected_by_scope=rejected_by_scope,
        invalid=invalid,
    )
