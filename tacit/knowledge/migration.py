"""Adapters from legacy artifact-learning payloads into governance envelopes."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from tacit.knowledge.authorization import KnowledgeAction, enforce_knowledge_action
from tacit.knowledge.enums import (
    EvidenceRole,
    KnowledgeEligibility,
    KnowledgeKind,
    LifecycleStatus,
    LineageKind,
    Predicate,
    ReviewState,
)
from tacit.knowledge.models import (
    KnowledgeCandidate,
    KnowledgeEvidenceReference,
    KnowledgeScope,
    MigrationProvenance,
    utc_now,
)
from tacit.knowledge.normalization import canonical_scope_payload, normalize_service_ref, stable_fingerprint
from tacit.knowledge.repository import CandidateReviewConflictError
from tacit.knowledge.service import KnowledgeService, _source_family


def migrate_artifact_extractions(
    *,
    artifact_id: str,
    artifact_type: str,
    artifact_fingerprint: str = "",
    artifact_content_fingerprint: str = "",
    source_vendor: str = "",
    source_instance: str = "",
    external_id: str = "",
    rows: dict[str, list[dict[str, Any]]],
    service: KnowledgeService,
    tenant_id: str = "default",
    max_candidate_count: int | None = None,
) -> list[str]:
    """Wrap existing typed rows without changing their payload semantics."""
    collections = (
        ("dependency_hints", KnowledgeKind.DEPENDENCY),
        ("ownership_hints", KnowledgeKind.OWNERSHIP),
        ("signal_mapping_candidates", KnowledgeKind.SIGNAL_MAPPING),
        ("evidence_requirements", KnowledgeKind.EVIDENCE_REQUIREMENT),
    )
    review_states = [
        str(row.get("review_state", ReviewState.CANDIDATE.value))
        for collection, _kind in collections
        for row in rows.get(collection, [])
    ]
    candidate_limit = (
        int(service._runtime_settings.knowledge_source_atomic_candidate_limit)
        if max_candidate_count is None
        else int(max_candidate_count)
    )
    if len(review_states) > candidate_limit:
        raise ValueError(
            f"artifact extraction fan-out {len(review_states)} exceeds atomic candidate limit {candidate_limit}"
        )
    _preflight_imported_review_states(service, review_states)
    if rows.get("signal_mapping_candidates"):
        service._signal_store()

    created = []
    with service.repository.transaction():
        for collection, kind in collections:
            for row in rows.get(collection, []):
                legacy_id = str(row["id"])
                proposition = _proposition(kind, row)
                lineage_group, lineage_kind = _artifact_lineage(
                    row,
                    artifact_id=artifact_id,
                    artifact_fingerprint=artifact_fingerprint,
                    artifact_content_fingerprint=artifact_content_fingerprint,
                    source_vendor=source_vendor,
                    source_instance=source_instance,
                    external_id=external_id,
                )
                evidence = KnowledgeEvidenceReference(
                    evidence_ref=f"artifact:{artifact_id}:{row['id']}",
                    evidence_role=EvidenceRole.SUPPORTING,
                    source_family=_source_family(artifact_type),
                    lineage_group=lineage_group,
                    lineage_kind=lineage_kind,
                    provenance_refs=[f"prov_artifact:{artifact_id}"],
                )
                if kind == KnowledgeKind.DEPENDENCY:
                    scope_service = row.get("source_entity")
                elif kind == KnowledgeKind.OWNERSHIP:
                    scope_service = row.get("entity")
                else:
                    scope_service = row.get("target_entity")
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
                existing = service.repository.get_candidate(candidate_id, tenant_id)
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
                    reactivate_stale=True,
                )
                legacy_review = str(row.get("review_state", ReviewState.CANDIDATE.value))
                candidate = _apply_imported_review_state(
                    service,
                    candidate,
                    legacy_review,
                    was_existing=existing is not None,
                )
                _evaluate_imported_approval(service, candidate, previous_candidate=existing)
                created.append(candidate.id)
    return created


def _artifact_lineage(
    row: dict[str, Any],
    *,
    artifact_id: str,
    artifact_fingerprint: str,
    artifact_content_fingerprint: str,
    source_vendor: str,
    source_instance: str,
    external_id: str,
) -> tuple[str, LineageKind]:
    explicit_group = str(row.get("lineage_group") or "").strip()
    explicit_kind = str(row.get("lineage_kind") or "").strip()
    copied_from = str(row.get("copied_from") or "").strip()
    if copied_from:
        return explicit_group or f"artifact_copy:{copied_from}", LineageKind.COPIED_FROM
    if explicit_kind:
        try:
            kind = LineageKind(explicit_kind)
        except ValueError:
            kind = LineageKind.UNKNOWN
        return explicit_group or f"artifact:{artifact_id}", kind
    content_key = artifact_content_fingerprint or artifact_fingerprint
    group = explicit_group or (f"artifact_content:{content_key}" if content_key else f"artifact:{artifact_id}")
    if source_instance and external_id:
        return group, LineageKind.INDEPENDENT
    if source_vendor:
        return group, LineageKind.SAME_VENDOR_EXPORT
    return group, LineageKind.UNKNOWN


def _mapping_scope_values(value: Any) -> list[str]:
    """Deserialize legacy resolver scope values without iterating JSON strings."""
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [value]
    if not isinstance(value, (list, tuple, set, frozenset)):
        value = [value]
    return [str(item).strip() for item in value if str(item).strip()]


def _mapping_scope_datetime(value: Any) -> datetime | None:
    """Convert persisted resolver epochs or ISO timestamps into UTC datetimes."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC)
    normalized = str(value).strip()
    try:
        return datetime.fromtimestamp(float(normalized), UTC)
    except ValueError:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))


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
    lineage_group = str(row.get("lineage_group") or row.get("source_fingerprint") or "").strip()
    raw_lineage_kind = str(row.get("lineage_kind") or LineageKind.INDEPENDENT.value)
    try:
        lineage_kind = LineageKind(raw_lineage_kind)
    except ValueError:
        lineage_kind = LineageKind.UNKNOWN
    evidence = [
        KnowledgeEvidenceReference(
            evidence_ref=f"signal_mapping:{record_ref}:{index}",
            evidence_role=EvidenceRole.SUPPORTING,
            source_family=_source_family(str(row.get("source_type") or "unknown")),
            lineage_group=lineage_group or source_ref,
            lineage_kind=lineage_kind,
            provenance_refs=[source_ref],
        )
        for index, source_ref in enumerate(source_refs, 1)
    ]
    scope = KnowledgeScope(
        tenant_id=tenant_id,
        service_refs=[normalize_service_ref(value) for value in _mapping_scope_values(row.get("context_services"))],
        environment_refs=_mapping_scope_values(row.get("context_environments")),
        region_refs=_mapping_scope_values(row.get("context_regions")),
        cluster_refs=_mapping_scope_values(row.get("context_clusters")),
        namespace_refs=_mapping_scope_values(row.get("context_namespaces")),
        archetype_refs=_mapping_scope_values(row.get("context_archetypes")),
        version_constraints=_mapping_scope_values(row.get("context_versions")),
        valid_from=_mapping_scope_datetime(row.get("valid_from")),
        valid_until=_mapping_scope_datetime(row.get("valid_until")),
    )
    candidate_digest = stable_fingerprint(
        {
            "tenant_id": tenant_id,
            "record_ref": record_ref,
            "scope": canonical_scope_payload(scope),
            "valid_from": scope.valid_from.isoformat() if scope.valid_from else "",
            "valid_until": scope.valid_until.isoformat() if scope.valid_until else "",
            "context_datasource_types": sorted(set(_mapping_scope_values(row.get("context_datasource_types")))),
        }
    ).split(":", 1)[1][:20]
    candidate_id = f"kc_signal_{candidate_digest}"
    review = str(row.get("review_state", ReviewState.CANDIDATE.value))
    _preflight_imported_review_states(service, [review])
    reviewed_source_change = review in {ReviewState.APPROVED.value, ReviewState.TRUSTED.value}
    if reviewed_source_change:
        enforce_knowledge_action(service._runtime_settings, KnowledgeAction.APPLY)
    service._signal_store()
    with service.repository.transaction():
        existing = service.repository.get_candidate(candidate_id, tenant_id)
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
            scope=scope,
            evidence=evidence,
            provenance_refs=source_refs,
            tenant_id=tenant_id,
            candidate_id=candidate_id,
            migration_provenance=MigrationProvenance(original_record_ref=f"signal_mapping:{record_ref}"),
            reactivate_stale=True,
            reactivate_source_change=reviewed_source_change,
        )
        candidate = _apply_imported_review_state(
            service,
            candidate,
            review,
            was_existing=existing is not None,
        )
        _evaluate_imported_approval(service, candidate, previous_candidate=existing)
    return candidate.id


def _preflight_imported_review_states(service: KnowledgeService, raw_states: list[str]) -> None:
    """Authorize a whole legacy batch before reading or persisting any row."""
    if not raw_states:
        return
    service._enforce_candidate_review_action(approved=True)
    valid_states = {state.value for state in ReviewState}
    for raw_state in raw_states:
        if raw_state not in valid_states or raw_state == ReviewState.CANDIDATE.value:
            continue
        review_state = ReviewState(raw_state)
        service._enforce_candidate_review_action(
            approved=review_state != ReviewState.REJECTED,
            trust=review_state == ReviewState.TRUSTED,
        )


def _service_ref(value: str) -> str:
    return normalize_service_ref(value)


def _apply_imported_review_state(
    service: KnowledgeService,
    candidate: KnowledgeCandidate,
    raw_review_state: str,
    *,
    was_existing: bool,
) -> KnowledgeCandidate:
    """Import legacy review only if no governed transition won concurrently."""
    if (
        was_existing
        or raw_review_state not in {state.value for state in ReviewState}
        or raw_review_state == ReviewState.CANDIDATE.value
        or candidate.state.review_state != ReviewState.CANDIDATE
    ):
        return candidate
    review_state = ReviewState(raw_review_state)
    service._enforce_candidate_review_action(
        approved=review_state != ReviewState.REJECTED,
        trust=review_state == ReviewState.TRUSTED,
    )
    updated = candidate.model_copy(
        update={
            "state": candidate.state.model_copy(update={"review_state": review_state}),
            "updated_at": utc_now(),
        }
    )
    try:
        return service.repository.transition_candidate_review(updated, expected=candidate)
    except CandidateReviewConflictError:
        current = service.repository.get_candidate(candidate.id, candidate.tenant_id)
        if current is None:
            raise
        return current


def _evaluate_imported_approval(
    service: KnowledgeService,
    candidate: KnowledgeCandidate,
    *,
    previous_candidate: KnowledgeCandidate | None = None,
) -> None:
    if candidate.state.review_state not in {ReviewState.APPROVED, ReviewState.TRUSTED}:
        return
    item = service.repository.find_knowledge_by_proposition(
        candidate.tenant_id,
        candidate.proposition.proposition_key,
    )
    current = service.repository.get_revision(item.id, tenant_id=candidate.tenant_id) if item is not None else None
    if current is not None and current.state.lifecycle_status == LifecycleStatus.STALE:
        if current.revision_reason not in {"source_stale", "source_changed"}:
            return
        if candidate.state.lifecycle_status != LifecycleStatus.ACTIVE:
            return
    authority_inputs_changed = previous_candidate is not None and (
        previous_candidate.evidence != candidate.evidence
        or previous_candidate.provenance_refs != candidate.provenance_refs
        or previous_candidate.scope != candidate.scope
    )
    should_evaluate = (
        authority_inputs_changed
        or current is None
        or (
            current.state.lifecycle_status == LifecycleStatus.STALE
            or (
                current.state.lifecycle_status == LifecycleStatus.ACTIVE
                and current.state.eligibility == KnowledgeEligibility.INELIGIBLE
            )
        )
    )
    if should_evaluate:
        service.evaluate_candidate(
            candidate.id,
            tenant_id=candidate.tenant_id,
            authoritative_source=candidate.policy.authoritative_source,
            live_verified=candidate.policy.live_verified,
        )


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
