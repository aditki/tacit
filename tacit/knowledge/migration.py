"""Adapters from legacy artifact-learning payloads into governance envelopes."""

from __future__ import annotations

from typing import Any

from tacit.knowledge.enums import EvidenceRole, KnowledgeKind, LineageKind, Predicate, ReviewState
from tacit.knowledge.models import KnowledgeEvidenceReference, KnowledgeScope, MigrationProvenance
from tacit.knowledge.normalization import normalize_ref, stable_fingerprint
from tacit.knowledge.service import KnowledgeService, _source_family


def migrate_artifact_extractions(
    *,
    artifact_id: str,
    artifact_type: str,
    rows: dict[str, list[dict[str, Any]]],
    service: KnowledgeService,
    tenant_id: str = "default",
) -> list[str]:
    """Wrap existing typed rows without changing their payload semantics."""
    created = []
    for collection, kind in (
        ("dependency_hints", KnowledgeKind.DEPENDENCY),
        ("ownership_hints", KnowledgeKind.OWNERSHIP),
        ("signal_mapping_candidates", KnowledgeKind.SIGNAL_MAPPING),
        ("evidence_requirements", KnowledgeKind.EVIDENCE_REQUIREMENT),
    ):
        for row in rows.get(collection, []):
            legacy_id = str(row["id"])
            proposition = _proposition(kind, row)
            evidence = KnowledgeEvidenceReference(
                evidence_ref=f"artifact:{artifact_id}:{row['id']}",
                evidence_role=EvidenceRole.SUPPORTING,
                source_family=_source_family(artifact_type),
                lineage_group=f"artifact:{artifact_id}",
                lineage_kind=LineageKind.INDEPENDENT,
                provenance_refs=[f"prov_artifact:{artifact_id}"],
            )
            scope_service = row.get("source_entity") if kind == KnowledgeKind.DEPENDENCY else row.get("target_entity")
            scope = KnowledgeScope(
                tenant_id=tenant_id,
                service_refs=[_service_ref(str(scope_service))] if scope_service else [],
            )
            semantic_id = stable_fingerprint(
                {
                    "kind": kind.value,
                    "subject_ref": proposition.get("subject_ref", ""),
                    "predicate": str(proposition.get("predicate", "")),
                    "object_ref": proposition.get("object_ref", ""),
                    "concept_ref": proposition.get("concept_ref", ""),
                    "scope": scope.model_dump(mode="json"),
                }
            ).split(":", 1)[1][:10]
            tenant_prefix = "" if tenant_id == "default" else f"{tenant_id}_"
            candidate_id = f"kc_{tenant_prefix}{legacy_id}_{semantic_id}"
            candidate = service.create_candidate(
                kind=kind,
                payload_ref=f"{collection}:{row['id']}",
                typed_payload=row,
                proposition=proposition,
                scope=scope,
                evidence=[evidence],
                provenance_refs=[f"prov_artifact:{artifact_id}"],
                tenant_id=tenant_id,
                candidate_id=candidate_id,
                migration_provenance=MigrationProvenance(original_record_ref=f"{collection}:{row['id']}"),
            )
            legacy_review = str(row.get("review_state", ReviewState.CANDIDATE.value))
            if legacy_review in {state.value for state in ReviewState} and legacy_review != "candidate":
                state = candidate.state.model_copy(update={"review_state": ReviewState(legacy_review)})
                candidate = candidate.model_copy(update={"state": state})
                service.repository.save_candidate(candidate)
            created.append(candidate.id)
    return created


def migrate_signal_mapping(
    row: dict[str, Any],
    *,
    service: KnowledgeService,
    tenant_id: str = "default",
) -> str:
    """Wrap a legacy active signal mapping without rewriting the signal store."""
    signal = str(row.get("signal_type") or "unknown")
    metric = str(row.get("metric_pattern") or row.get("candidate_metric") or "unknown")
    record_ref = str(row.get("id") or f"{signal}:{metric}")
    source_refs = [str(value) for value in row.get("source_refs", [])] or [f"signal_mapping:{record_ref}"]
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref=f"signal_mapping:{record_ref}",
        typed_payload=row,
        proposition={
            "subject_ref": f"concept:{signal}",
            "predicate": Predicate.REPRESENTED_BY,
            "concept_ref": f"signal:{signal}",
            "object_ref": f"concept:{metric}",
        },
        scope=KnowledgeScope(
            tenant_id=tenant_id,
            service_refs=[str(value) for value in row.get("context_services", [])],
            environment_refs=[str(value) for value in row.get("context_environments", [])],
            archetype_refs=[str(value) for value in row.get("context_archetypes", [])],
        ),
        provenance_refs=source_refs,
        tenant_id=tenant_id,
        candidate_id=(f"kc_signal_{record_ref}" if tenant_id == "default" else f"kc_signal_{tenant_id}_{record_ref}"),
        migration_provenance=MigrationProvenance(original_record_ref=f"signal_mapping:{record_ref}"),
    )
    review = str(row.get("review_state", ReviewState.CANDIDATE.value))
    if review in {state.value for state in ReviewState} and review != ReviewState.CANDIDATE.value:
        candidate = candidate.model_copy(
            update={"state": candidate.state.model_copy(update={"review_state": ReviewState(review)})}
        )
        service.repository.save_candidate(candidate)
    return candidate.id


def _service_ref(value: str) -> str:
    normalized = normalize_ref(value)
    return normalized if normalized.startswith("entity:") else f"entity:service:{normalized}"


def _proposition(kind: KnowledgeKind, row: dict[str, Any]) -> dict[str, Any]:
    if kind == KnowledgeKind.DEPENDENCY:
        direction = str(row.get("direction") or "depends_on")
        return {
            "subject_ref": row.get("source_entity", ""),
            "predicate": direction,
            "object_ref": row.get("target_entity", ""),
            "source_wording": row.get("source_excerpt", ""),
        }
    if kind == KnowledgeKind.OWNERSHIP:
        return {
            "subject_ref": row.get("entity", ""),
            "predicate": Predicate.OWNED_BY,
            "object_ref": row.get("owner", ""),
            "source_wording": row.get("source_excerpt", ""),
        }
    if kind == KnowledgeKind.SIGNAL_MAPPING:
        source = str(row.get("source") or row.get("symptom") or "unknown")
        metric = str(row.get("candidate_metric") or row.get("metric_pattern") or "unknown")
        return {
            "subject_ref": f"concept:{source}",
            "predicate": Predicate.REPRESENTED_BY,
            "object_ref": f"concept:{metric}",
            "concept_ref": f"signal:{row.get('signal_type') or source}",
            "source_wording": row.get("source_excerpt", ""),
        }
    subject = str(row.get("target_entity") or row.get("subject") or "unknown")
    return {
        "subject_ref": f"concept:{subject}" if not row.get("target_entity") else subject,
        "predicate": Predicate.REQUIRES_OBSERVATION,
        "concept_ref": f"signal:{row.get('evidence_kind') or 'unknown'}",
        "source_wording": row.get("source_excerpt", ""),
    }
