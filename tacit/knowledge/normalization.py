"""Deterministic proposition identity and bounded predicate normalization."""

from __future__ import annotations

import hashlib
import json
import re

from tacit.knowledge.enums import EntityKind, KnowledgeKind, Predicate
from tacit.knowledge.models import (
    SCOPE_REFERENCE_PREFIXES,
    KnowledgeProposition,
    KnowledgeScope,
    canonical_scope_references,
)

PREDICATE_ALIASES = {
    "depends on": Predicate.DEPENDS_ON,
    "depends_on": Predicate.DEPENDS_ON,
    "dependency": Predicate.DEPENDS_ON,
    "does not depend on": Predicate.DOES_NOT_DEPEND_ON,
    "does_not_depend_on": Predicate.DOES_NOT_DEPEND_ON,
    "calls": Predicate.CALLS,
    "downstream": Predicate.CALLS,
    "reads from": Predicate.READS_FROM,
    "reads_from": Predicate.READS_FROM,
    "writes to": Predicate.WRITES_TO,
    "writes_to": Predicate.WRITES_TO,
    "owned by": Predicate.OWNED_BY,
    "owned_by": Predicate.OWNED_BY,
    "owner": Predicate.OWNED_BY,
    "represented by": Predicate.REPRESENTED_BY,
    "represented_by": Predicate.REPRESENTED_BY,
    "requires observation": Predicate.REQUIRES_OBSERVATION,
    "requires_observation": Predicate.REQUIRES_OBSERVATION,
    "useful for investigation": Predicate.USEFUL_FOR_INVESTIGATION,
    "useful_for_investigation": Predicate.USEFUL_FOR_INVESTIGATION,
}


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_fingerprint(value) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode()).hexdigest()


def normalize_ref(value: str) -> str:
    return value.strip().casefold().replace(" ", "-")


def normalize_entity(value: str) -> str:
    return re.sub(r"[^a-z0-9_.:-]+", "-", value.strip().casefold()).strip("-")


def normalize_entity_id(value: str, kind: EntityKind) -> str:
    """Build a canonical, kind-safe entity identifier from operator input."""
    normalized = normalize_entity(value)
    if not normalized:
        raise ValueError("entity id must contain canonical identifier characters")
    known_kinds = {item.value for item in EntityKind}
    parts = normalized.split(":")
    if parts[0] == "entity":
        if len(parts) < 3 or not parts[2]:
            raise ValueError("entity id must use entity:<kind>:<name>")
        declared_kind = parts[1]
        canonical = normalized
    elif len(parts) >= 2 and parts[0] in known_kinds:
        declared_kind = parts[0]
        canonical = f"entity:{normalized}"
    else:
        declared_kind = kind.value
        canonical = f"entity:{kind.value}:{normalized}"
    if declared_kind != kind.value:
        raise ValueError(f"entity id kind {declared_kind!r} does not match declared kind {kind.value!r}")
    return canonical


def normalize_service_ref(value: str) -> str:
    """Canonicalize service mentions to the scope reference format."""
    normalized = normalize_entity(value)
    if normalized.startswith("entity:"):
        return normalized
    if normalized.startswith("service:"):
        return f"entity:{normalized}"
    return f"entity:service:{normalized}"


def candidate_ref_from_entity_ref(entity_ref: str) -> str:
    """Convert a governed entity reference into the ranking candidate form."""
    parts = entity_ref.split(":")
    if len(parts) >= 3 and parts[0] == "entity":
        return f"{parts[1]}:{':'.join(parts[2:])}"
    if len(parts) >= 2:
        return entity_ref
    return f"service:{entity_ref}"


SCOPE_LIST_FIELDS = tuple(SCOPE_REFERENCE_PREFIXES)


def canonical_scope_payload(scope: KnowledgeScope) -> dict:
    payload = scope.model_dump(mode="json", exclude={"valid_from", "valid_until"})
    for field_name in SCOPE_LIST_FIELDS:
        payload[field_name] = canonical_scope_references(field_name, payload.get(field_name) or [])
    return payload


class PropositionNormalizer:
    def normalize_predicate(self, value: str | Predicate) -> Predicate:
        if isinstance(value, Predicate):
            return value
        normalized = value.strip().casefold()
        if normalized in PREDICATE_ALIASES:
            return PREDICATE_ALIASES[normalized]
        try:
            return Predicate(normalized.replace(" ", "_"))
        except ValueError as exc:
            raise ValueError(f"Unsupported knowledge predicate: {value}") from exc

    def normalize(
        self,
        *,
        kind: KnowledgeKind | str,
        subject_ref: str,
        predicate: Predicate | str,
        scope: KnowledgeScope,
        object_ref: str = "",
        concept_ref: str = "",
        source_wording: str = "",
        uncertainty: str = "unknown",
    ) -> KnowledgeProposition:
        knowledge_kind = KnowledgeKind(kind)
        proposition = KnowledgeProposition(
            kind=knowledge_kind,
            subject_ref=normalize_ref(subject_ref),
            predicate=self.normalize_predicate(predicate),
            object_ref=normalize_ref(object_ref) if object_ref else "",
            concept_ref=normalize_ref(concept_ref) if concept_ref else "",
            source_wording=source_wording,
            uncertainty=uncertainty,
        )
        key = stable_fingerprint(
            {
                "schema_version": proposition.schema_version,
                "kind": proposition.kind.value,
                "subject_ref": proposition.subject_ref,
                "predicate": proposition.predicate.value,
                "object_ref": proposition.object_ref,
                "concept_ref": proposition.concept_ref,
                "scope": canonical_scope_payload(scope),
                "valid_from": scope.valid_from,
                "valid_until": scope.valid_until,
            }
        )
        return proposition.model_copy(update={"proposition_key": key})
