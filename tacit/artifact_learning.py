"""Tacit Artifact Learning v1.

Operational artifacts are converted into a small reviewable IR. The extractor
layer never emits culprits, RCA claims, or ranked causes.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import structlog

from tacit.config import Settings, settings
from tacit.dependencies import resolve_owned_database_path
from tacit.knowledge.authorization import KnowledgeAction, enforce_knowledge_action
from tacit.runtime_ownership import (
    RuntimeOwner,
    describe_runtime_owner,
    resolve_runtime_settings,
)
from tacit.signals import get_signal_store as _default_get_signal_store

MAX_ARTIFACT_BODY_LENGTH = 200_000
MAX_SOURCE_EXCERPT_LENGTH = 2_000
logger = structlog.get_logger()


def _diagnostic_fingerprint(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:16]


def get_signal_store():
    """Resolve through the package facade for test isolation."""
    import tacit.signals as signals_pkg

    return getattr(signals_pkg, "get_signal_store", _default_get_signal_store)()


@dataclass
class LearnedArtifact:
    id: str
    artifact_type: str
    source_vendor: str | None
    source_instance: str | None
    external_id: str
    title: str
    body_text: str
    provenance_url: str | None
    fingerprint: str
    first_seen_at: datetime
    last_seen_at: datetime
    updated_at: datetime
    stale: bool = False
    missing_since: datetime | None = None


@dataclass
class EvidenceRequirement:
    id: str
    subject: str
    evidence_kind: str
    target_entity: str | None
    signal_hint: str | None
    query_hint: str | None
    priority: int | None
    source_artifact_id: str
    source_excerpt: str
    source_type: str
    confidence_prior: float
    review_state: str
    created_at: datetime
    observation_state: str = "indeterminate"


@dataclass
class OwnershipHint:
    id: str
    entity: str
    owner: str
    hint_kind: str
    source_artifact_id: str
    source_excerpt: str
    source_type: str
    confidence_prior: float
    review_state: str


@dataclass
class DependencyHint:
    id: str
    source_entity: str
    target_entity: str
    direction: str
    source_artifact_id: str
    source_excerpt: str
    source_type: str
    confidence_prior: float
    review_state: str


@dataclass
class SignalMappingCandidate:
    id: str
    source: str
    candidate_metric: str
    symptom: str
    signal_type: str
    source_artifact_id: str
    source_excerpt: str
    query_hint: str | None = None
    review_state: str = "candidate"
    confidence_prior: float = 0.45


@dataclass
class ExtractionResult:
    evidence_requirements: list[EvidenceRequirement] = field(default_factory=list)
    ownership_hints: list[OwnershipHint] = field(default_factory=list)
    dependency_hints: list[DependencyHint] = field(default_factory=list)
    signal_mapping_candidates: list[SignalMappingCandidate] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class ArtifactExtractor(Protocol):
    artifact_type: str

    def extract(self, artifact: LearnedArtifact) -> ExtractionResult: ...


CHECK_RE = re.compile(r"\b(check|verify|look at|inspect|observe|confirm)\b\s+(?P<body>.+)", re.I)
OWNERSHIP_RE = re.compile(
    r"(?:(?:\b(?:escalate to|contact|owned by)\b)|(?:\b(?:owner|maintainer):))\s*(?P<owner>.+)",
    re.I,
)
DEPENDENCY_RE = re.compile(
    r"^(?P<src>[a-zA-Z0-9_.-]+)\s+(?P<dir>depends on|calls|downstream)\s+(?P<tgt>[a-zA-Z0-9_.-]+)",
    re.I,
)
DEPENDENCY_SHORTHAND_RE = re.compile(
    r"^(?:also\s+|then\s+|next\s+)?(?P<dir>depends on|calls|downstream)\s+(?P<tgt>[a-zA-Z0-9_.-]+)",
    re.I,
)
MITIGATION_RE = re.compile(r"\b(restart|rollback|scale|redeploy|flush|kill|delete)\b", re.I)
INCIDENT_OBSERVED_RE = re.compile(
    r"(?:(?:\b(?:observed|saw|detected|confirmed)\b)|(?:\b(?:evidence|signal|symptom|impact):))\s*(?P<body>.+)",
    re.I,
)
CAUSAL_CLAIM_RE = re.compile(
    r"\b("
    r"rca|root cause|root-cause|culprit|caused by|caused when|primary issue|underlying issue|"
    r"postmortem conclusion|contributing factor|contributing factors|lesson learned|lessons learned|"
    r"resolution|fix|fix was|resolved by|remediated by|recovered after|"
    r"rollback fixed|introduced by|triggered by|regression from|fault was|due to"
    r")",
    re.I,
)
LEADING_CAUSAL_CLAIM_RE = re.compile(
    r"^(?:"
    r"rca|root cause|root-cause|culprit|caused by|caused when|primary issue|underlying issue|"
    r"postmortem conclusion|contributing factor|contributing factors|lesson learned|lessons learned|"
    r"resolution|fix|fix was|resolved by|remediated by|recovered after|"
    r"rollback fixed|introduced by|triggered by|regression from|fault was|due to"
    r")\b",
    re.I,
)
METRIC_RE = re.compile(r"\b[a-zA-Z_:][a-zA-Z0-9_:]*(?:[._:][a-zA-Z0-9_:]+)+\b")
CODE_RE = re.compile(r"`([^`]+)`")
BULLET_PREFIX_RE = re.compile(r"^\s*(?:[-*]\s+|\d+[.)]\s+)")
TRAILING_ENTITY_PUNCTUATION = ".,;:)]}"


def _now() -> datetime:
    return datetime.now(UTC)


def _fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _artifact_content_fingerprint(body_text: str) -> str:
    """Build a MinHash-style lineage signature tolerant of small copied edits."""
    lines = body_text.splitlines()
    first_content = next((index for index, line in enumerate(lines) if line.strip()), None)
    if first_content is not None and lines[first_content].lstrip().startswith("#"):
        lines.pop(first_content)
    tokens = re.sub(r"[^a-z0-9]+", " ", "\n".join(lines).casefold()).split()
    if not tokens:
        return _fingerprint("")
    shingle_size = min(5, len(tokens))
    shingles = {" ".join(tokens[index : index + shingle_size]) for index in range(len(tokens) - shingle_size + 1)}
    return min(_fingerprint(shingle) for shingle in shingles)


def _row_id(*parts: str) -> str:
    payload = "\0".join(parts)
    return hashlib.sha256(payload.encode()).hexdigest()[:24]


def _artifact_id(
    artifact_type: str,
    external_id: str,
    source_instance: str = "",
    source_vendor: str = "",
) -> str:
    stable_parts = [source_vendor, source_instance, external_id]
    stable = ":".join(part for part in stable_parts if part)
    return f"{artifact_type}:{_fingerprint(stable)[:20]}"


def _normalized_line(line: str) -> str:
    return BULLET_PREFIX_RE.sub("", line.strip(), count=1).strip()


def _clean_line(line: str) -> str:
    return _normalized_line(line)[:MAX_SOURCE_EXCERPT_LENGTH]


def _normalize_entity_token(value: str) -> str:
    return value.strip().rstrip(TRAILING_ENTITY_PUNCTUATION)


def _is_causal_heading(line: str) -> bool:
    if not line.lstrip().startswith("#"):
        return False
    cleaned = line.strip().strip("#").strip().rstrip(":").lower()
    if cleaned == "resolution":
        return False
    return cleaned in {"rca", "root cause", "root-cause"} or bool(CAUSAL_CLAIM_RE.search(cleaned))


def _is_causal_section_label(line: str) -> bool:
    cleaned = _normalized_line(line)
    return cleaned.endswith(":") and bool(CAUSAL_CLAIM_RE.search(cleaned))


def _starts_causal_claim(line: str) -> bool:
    cleaned = _normalized_line(line).lower()
    return bool(LEADING_CAUSAL_CLAIM_RE.search(cleaned))


def _next_row_id(
    artifact_id: str,
    occurrences: dict[tuple[str, str], int],
    kind: str,
    stable_text: str,
) -> str:
    key = (kind, stable_text)
    occurrences[key] = occurrences.get(key, 0) + 1
    return _row_id(artifact_id, kind, stable_text, str(occurrences[key]))


def _infer_evidence_kind(text: str) -> str:
    lowered = text.lower()
    if "miss" in lowered or "cache" in lowered:
        return "cache_misses"
    if "latency" in lowered or "p95" in lowered or "p99" in lowered:
        return "latency"
    if "error" in lowered or "5xx" in lowered:
        return "errors"
    if "saturat" in lowered or "pool" in lowered or "cpu" in lowered or "memory" in lowered:
        return "saturation"
    if "deploy" in lowered or "rollback" in lowered:
        return "deployment_age"
    return "unknown"


def _infer_signal_type(metric: str, text: str) -> str:
    kind = _infer_evidence_kind(f"{metric} {text}")
    return {
        "cache_misses": "cache_misses",
        "latency": "request_latency",
        "errors": "error_rate",
        "saturation": "resource_saturation",
        "deployment_age": "deployment_age",
    }.get(kind, "operational_signal")


def _entity_from_text(text: str) -> str | None:
    code = CODE_RE.findall(text)
    if code:
        return code[0].strip()
    tokens = [token for token in re.findall(r"\b[a-zA-Z][a-zA-Z0-9_.-]+\b", text) if len(token) > 2]
    ignored = {
        "check",
        "verify",
        "look",
        "inspect",
        "observe",
        "latency",
        "errors",
        "misses",
        "saturation",
        "dashboard",
        "query",
    }
    for token in tokens:
        if token.lower() not in ignored:
            return token
    return None


def _metric_candidates(text: str) -> list[str]:
    metrics = []
    for candidate in METRIC_RE.findall(text):
        candidate = candidate.rstrip(".,;:)]}")
        lowered = candidate.lower()
        if lowered in {"http", "https"} or candidate.isupper():
            continue
        if candidate not in metrics:
            metrics.append(candidate)
    return metrics


def _section_name(line: str) -> str:
    cleaned = line.strip().strip("#").strip().rstrip(":").lower()
    return (
        cleaned
        if cleaned
        in {
            "symptoms",
            "checks",
            "diagnosis",
            "verify",
            "escalation",
            "owners",
            "dependencies",
            "dashboards",
            "queries",
            "impact",
            "observed evidence",
            "evidence",
            "timeline",
            "investigation references",
            "references",
            "resolution",
        }
        else ""
    )


class RunbookExtractor:
    artifact_type = "runbook"

    def extract(self, artifact: LearnedArtifact) -> ExtractionResult:
        result = ExtractionResult()
        section = ""
        priority = 0
        symptom = artifact.title
        row_occurrences: dict[tuple[str, str], int] = {}
        lines = artifact.body_text.splitlines()
        for raw in lines:
            if _is_causal_heading(raw):
                line = _clean_line(raw)
                result.warnings.append(f"ignored_causal_claim:{line}")
                section = "suppressed_causal"
                continue
            maybe_section = _section_name(raw)
            if maybe_section:
                section = "suppressed_causal" if maybe_section == "resolution" else maybe_section
                continue
            scan_line = _normalized_line(raw)
            line = scan_line[:MAX_SOURCE_EXCERPT_LENGTH]
            if not line:
                continue
            if section == "suppressed_causal":
                result.warnings.append(f"ignored_causal_claim:{line}")
                continue
            if _is_causal_section_label(scan_line):
                result.warnings.append(f"ignored_causal_claim:{line}")
                section = "suppressed_causal"
                continue
            if CAUSAL_CLAIM_RE.search(scan_line):
                result.warnings.append(f"ignored_causal_claim:{line}")
                if _starts_causal_claim(scan_line):
                    section = "suppressed_causal"
                continue
            if section == "symptoms":
                symptom = line
            if MITIGATION_RE.search(line) and not CHECK_RE.search(line):
                result.warnings.append(f"ignored_mitigation:{line}")
                continue

            dep = DEPENDENCY_RE.search(line)
            if dep and section == "dependencies" and dep.group("src").lower() in {"also", "then", "next"}:
                dep = None
            dep_source = _normalize_entity_token(dep.group("src")) if dep else ""
            dep_target = _normalize_entity_token(dep.group("tgt")) if dep else ""
            dep_direction = dep.group("dir") if dep else ""
            if not dep and section == "dependencies":
                shorthand = DEPENDENCY_SHORTHAND_RE.search(line)
                if shorthand:
                    dep_source = _normalize_entity_token(_entity_from_text(artifact.title) or artifact.title)
                    dep_target = _normalize_entity_token(shorthand.group("tgt"))
                    dep_direction = shorthand.group("dir")
            if dep:
                result.dependency_hints.append(
                    DependencyHint(
                        id=_next_row_id(artifact.id, row_occurrences, "dependency", line),
                        source_entity=dep_source,
                        target_entity=dep_target,
                        direction="depends_on" if dep_direction.lower() == "depends on" else "calls",
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.55,
                        review_state="candidate",
                    )
                )
                continue
            if dep_target:
                result.dependency_hints.append(
                    DependencyHint(
                        id=_next_row_id(artifact.id, row_occurrences, "dependency", line),
                        source_entity=dep_source,
                        target_entity=dep_target,
                        direction="depends_on" if dep_direction.lower() == "depends on" else "calls",
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.55,
                        review_state="candidate",
                    )
                )
                continue

            ownership = OWNERSHIP_RE.search(line)
            if ownership:
                owner = ownership.group("owner").strip().strip(".")
                result.ownership_hints.append(
                    OwnershipHint(
                        id=_next_row_id(artifact.id, row_occurrences, "ownership", line),
                        entity=artifact.title,
                        owner=owner,
                        hint_kind=(
                            "escalation" if "escalate" in line.lower() or "contact" in line.lower() else "owner_label"
                        ),
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.55,
                        review_state="candidate",
                    )
                )
                continue

            check = CHECK_RE.search(line)
            if check:
                priority += 1
                check_body = check.group("body").strip()
                metrics = _metric_candidates(check_body)
                signal_hint = metrics[0] if metrics else None
                result.evidence_requirements.append(
                    EvidenceRequirement(
                        id=_next_row_id(artifact.id, row_occurrences, "evidence", line),
                        subject=check_body,
                        evidence_kind=_infer_evidence_kind(check_body),
                        target_entity=_entity_from_text(check_body),
                        signal_hint=signal_hint,
                        query_hint=check_body if metrics else None,
                        priority=priority,
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.55,
                        review_state="candidate",
                        created_at=_now(),
                        observation_state="indeterminate",
                    )
                )

            if section == "queries" or check:
                for metric in _metric_candidates(line):
                    result.signal_mapping_candidates.append(
                        SignalMappingCandidate(
                            id=_next_row_id(artifact.id, row_occurrences, "signal", f"{line}\0{metric}"),
                            source=artifact.artifact_type,
                            candidate_metric=metric,
                            symptom=symptom,
                            signal_type=_infer_signal_type(metric, line),
                            source_artifact_id=artifact.id,
                            source_excerpt=line,
                            query_hint=line,
                            review_state="candidate",
                            confidence_prior=0.45,
                        )
                    )
        return result


class IncidentExtractor:
    artifact_type = "incident"

    def extract(self, artifact: LearnedArtifact) -> ExtractionResult:
        result = ExtractionResult()
        section = ""
        priority = 0
        symptom = artifact.title
        row_occurrences: dict[tuple[str, str], int] = {}
        for raw in artifact.body_text.splitlines():
            if _is_causal_heading(raw):
                line = _clean_line(raw)
                result.warnings.append(f"ignored_causal_claim:{line}")
                section = "suppressed_causal"
                continue
            maybe_section = _section_name(raw)
            if maybe_section:
                section = "suppressed_causal" if maybe_section == "resolution" else maybe_section
                continue
            scan_line = _normalized_line(raw)
            line = scan_line[:MAX_SOURCE_EXCERPT_LENGTH]
            if not line:
                continue
            if section == "suppressed_causal":
                result.warnings.append(f"ignored_causal_claim:{line}")
                continue
            if _is_causal_section_label(scan_line):
                result.warnings.append(f"ignored_causal_claim:{line}")
                section = "suppressed_causal"
                continue
            if CAUSAL_CLAIM_RE.search(scan_line):
                result.warnings.append(f"ignored_causal_claim:{line}")
                if _starts_causal_claim(scan_line):
                    section = "suppressed_causal"
                continue

            dep = DEPENDENCY_RE.search(line)
            if dep and section == "dependencies" and dep.group("src").lower() in {"also", "then", "next"}:
                dep = None
            dep_source = _normalize_entity_token(dep.group("src")) if dep else ""
            dep_target = _normalize_entity_token(dep.group("tgt")) if dep else ""
            dep_direction = dep.group("dir") if dep else ""
            if not dep and section == "dependencies":
                shorthand = DEPENDENCY_SHORTHAND_RE.search(line)
                if shorthand:
                    dep_source = _normalize_entity_token(_entity_from_text(artifact.title) or artifact.title)
                    dep_target = _normalize_entity_token(shorthand.group("tgt"))
                    dep_direction = shorthand.group("dir")
            if dep:
                result.dependency_hints.append(
                    DependencyHint(
                        id=_next_row_id(artifact.id, row_occurrences, "dependency", line),
                        source_entity=dep_source,
                        target_entity=dep_target,
                        direction="depends_on" if dep_direction.lower() == "depends on" else "calls",
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.5,
                        review_state="candidate",
                    )
                )
                continue
            if dep_target:
                result.dependency_hints.append(
                    DependencyHint(
                        id=_next_row_id(artifact.id, row_occurrences, "dependency", line),
                        source_entity=dep_source,
                        target_entity=dep_target,
                        direction="depends_on" if dep_direction.lower() == "depends on" else "calls",
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.5,
                        review_state="candidate",
                    )
                )
                continue

            ownership = OWNERSHIP_RE.search(line)
            if ownership:
                owner = ownership.group("owner").strip().strip(".")
                result.ownership_hints.append(
                    OwnershipHint(
                        id=_next_row_id(artifact.id, row_occurrences, "ownership", line),
                        entity=artifact.title,
                        owner=owner,
                        hint_kind=(
                            "escalation" if "escalate" in line.lower() or "contact" in line.lower() else "owner_label"
                        ),
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.5,
                        review_state="candidate",
                    )
                )
                continue

            observed = INCIDENT_OBSERVED_RE.search(line)
            check = CHECK_RE.search(line)
            if MITIGATION_RE.search(line) and not (check or observed):
                result.warnings.append(f"ignored_mitigation:{line}")
                continue
            evidence_body = ""
            observation_state = "indeterminate"
            if observed:
                evidence_body = observed.group("body").strip()
                observation_state = "observed"
            elif section in {"symptoms", "impact", "observed evidence", "evidence"}:
                evidence_body = line
                observation_state = "observed"
            elif check:
                evidence_body = check.group("body").strip()

            if evidence_body:
                priority += 1
                metrics = _metric_candidates(evidence_body)
                signal_hint = metrics[0] if metrics else None
                result.evidence_requirements.append(
                    EvidenceRequirement(
                        id=_next_row_id(artifact.id, row_occurrences, "evidence", line),
                        subject=evidence_body,
                        evidence_kind=_infer_evidence_kind(evidence_body),
                        target_entity=_entity_from_text(evidence_body),
                        signal_hint=signal_hint,
                        query_hint=evidence_body if metrics else None,
                        priority=priority,
                        source_artifact_id=artifact.id,
                        source_excerpt=line,
                        source_type=artifact.artifact_type,
                        confidence_prior=0.5,
                        review_state="candidate",
                        created_at=_now(),
                        observation_state=observation_state,
                    )
                )

            if section in {"queries", "observed evidence", "evidence"} or observed or check:
                for metric in _metric_candidates(line):
                    result.signal_mapping_candidates.append(
                        SignalMappingCandidate(
                            id=_next_row_id(artifact.id, row_occurrences, "signal", f"{line}\0{metric}"),
                            source=artifact.artifact_type,
                            candidate_metric=metric,
                            symptom=symptom,
                            signal_type=_infer_signal_type(metric, line),
                            source_artifact_id=artifact.id,
                            source_excerpt=line,
                            query_hint=line,
                            review_state="candidate",
                            confidence_prior=0.4,
                        )
                    )
        return result


def artifact_from_text(
    *,
    artifact_type: str,
    title: str,
    body_text: str,
    external_id: str,
    source_vendor: str | None = None,
    source_instance: str | None = None,
    provenance_url: str | None = None,
) -> LearnedArtifact:
    if len(body_text) > MAX_ARTIFACT_BODY_LENGTH:
        raise ValueError(f"artifact body exceeds {MAX_ARTIFACT_BODY_LENGTH} characters")
    now = _now()
    return LearnedArtifact(
        id=_artifact_id(artifact_type, external_id, source_instance or "", source_vendor or ""),
        artifact_type=artifact_type,
        source_vendor=source_vendor,
        source_instance=source_instance,
        external_id=external_id,
        title=title,
        body_text=body_text,
        provenance_url=provenance_url,
        fingerprint=_fingerprint(f"{title}\0{body_text}"),
        first_seen_at=now,
        last_seen_at=now,
        updated_at=now,
    )


def runbook_from_file(path: Path) -> LearnedArtifact:
    body = path.read_text()
    title = path.stem.replace("-", " ").replace("_", " ").strip() or path.name
    return artifact_from_text(
        artifact_type="runbook",
        title=title,
        body_text=body,
        external_id=str(path.resolve()),
        source_vendor="file",
        source_instance=str(path.parent.resolve()),
        provenance_url=str(path.resolve()),
    )


def _as_store_rows(items: Sequence[Any]) -> list[dict[str, Any]]:
    rows = []
    for item in items:
        row = asdict(item)
        row["created_at"] = row.get("created_at", _now())
        if isinstance(row["created_at"], datetime):
            row["created_at"] = row["created_at"].timestamp()
        row["extraction_hash"] = _fingerprint(json.dumps(row, sort_keys=True, default=str))
        rows.append(row)
    return rows


def _sanitized_body_text_for_index(artifact: LearnedArtifact, result: ExtractionResult) -> str:
    suppressed = {
        warning.split(":", 1)[1]
        for warning in result.warnings
        if warning.startswith(("ignored_causal_claim:", "ignored_mitigation:"))
    }
    kept = []
    for raw in artifact.body_text.splitlines():
        line = _clean_line(raw)
        if line and line in suppressed:
            continue
        kept.append(raw)
    return "\n".join(kept)


def _extraction_generation_matches(
    existing: dict[str, list[dict[str, Any]]],
    *,
    evidence_rows: list[dict[str, Any]],
    ownership_rows: list[dict[str, Any]],
    dependency_rows: list[dict[str, Any]],
    signal_rows: list[dict[str, Any]],
) -> bool:
    expected = {
        "evidence_requirements": evidence_rows,
        "ownership_hints": ownership_rows,
        "dependency_hints": dependency_rows,
        "signal_mapping_candidates": signal_rows,
    }
    return all(
        sorted(str(row.get("id", "")) for row in existing.get(kind, []))
        == sorted(str(row.get("id", "")) for row in expected_rows)
        for kind, expected_rows in expected.items()
    )


def _resolve_tenant_id(
    tenant_id: str | None,
    *,
    runtime_settings: Settings | None = None,
) -> str:
    from tacit.dashboard_ingest.service import resolve_learning_tenant

    return resolve_learning_tenant(tenant_id, runtime_settings=runtime_settings)


def resolve_artifact_runtime_settings(
    *,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    knowledge_service: Any | None = None,
    connector_settings: Settings | None = None,
) -> Settings:
    """Resolve one fail-closed composition owner for artifact learning."""
    store_owner = describe_runtime_owner("signal_store", store)
    service_owner = describe_runtime_owner("knowledge_service", knowledge_service)
    connector_owner = RuntimeOwner(
        name="connector",
        supplied=connector_settings is not None,
        settings=connector_settings,
    )
    active_settings = resolve_runtime_settings(
        boundary="Artifact learning",
        explicit_settings=runtime_settings,
        owners=(store_owner, service_owner, connector_owner),
        fallback_settings=settings,
    )
    supplied_owners = tuple(
        (name, owner)
        for name, owner in (("signal_store", store), ("knowledge_service", knowledge_service))
        if owner is not None
    )
    if supplied_owners:
        resolve_owned_database_path(
            boundary="Artifact learning",
            database_role="signals",
            owners=supplied_owners,
            runtime_settings=active_settings,
        )
    return active_settings


def _active_runtime_settings(
    runtime_settings: Settings | None,
    store: Any | None,
) -> Settings:
    return resolve_artifact_runtime_settings(runtime_settings=runtime_settings, store=store)


def authorize_artifact_learning(
    *,
    dry_run: bool,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    knowledge_service: Any | None = None,
    connector_settings: Settings | None = None,
    tenant_id: str | None = None,
) -> tuple[Settings, str]:
    """Resolve the tenant and authorize before reading any artifact source."""
    active_settings = resolve_artifact_runtime_settings(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        connector_settings=connector_settings,
    )
    resolved_tenant = _resolve_tenant_id(tenant_id, runtime_settings=active_settings)
    action = KnowledgeAction.READ if dry_run else KnowledgeAction.LEARN_ARTIFACTS
    enforce_knowledge_action(active_settings, action)
    return active_settings, resolved_tenant


def _resolve_artifact_store(
    *,
    dry_run: bool,
    store: Any | None,
    runtime_settings: Settings,
    knowledge_service: Any | None = None,
    allow_global_fallback: bool = False,
) -> Any | None:
    if dry_run or store is not None:
        return store
    if knowledge_service is not None:
        database_path = resolve_owned_database_path(
            boundary="Artifact learning signal store resolution",
            database_role="signals",
            owners=(("knowledge_service", knowledge_service),),
            runtime_settings=runtime_settings,
        )
        from tacit.signals import SignalStore

        return SignalStore(database_path, runtime_settings=runtime_settings)
    if allow_global_fallback:
        return get_signal_store()

    from tacit.runtime_stores import RuntimeStores

    return RuntimeStores(runtime_settings).signals()


def _reconcile_stale_artifact_knowledge(
    *,
    store,
    tenant_id: str,
    artifact_type: str,
    runtime_settings: Settings | None = None,
    knowledge_service: Any | None = None,
) -> None:
    from tacit.dashboard_ingest.service import _knowledge_service_for_store

    active_settings = resolve_artifact_runtime_settings(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
    )
    enforce_knowledge_action(active_settings, KnowledgeAction.LEARN_ARTIFACTS)
    knowledge_service = knowledge_service or _knowledge_service_for_store(
        store,
        runtime_settings=active_settings,
    )
    resolve_artifact_runtime_settings(
        runtime_settings=active_settings,
        store=store,
        knowledge_service=knowledge_service,
    )
    bind_connection = knowledge_service.repository.bind_transaction_connection
    store.ensure_governed_projection_audit_current()
    after_id: int | None = None
    page_size = 1_000
    pages = 0
    reconciled = 0
    while True:
        artifacts = store.list_unreconciled_stale_artifacts(
            tenant_id=tenant_id,
            artifact_type=artifact_type,
            limit=page_size,
            after_id=after_id,
        )
        if not artifacts:
            break
        pages += 1
        for artifact in artifacts:
            after_id = int(artifact["id"])
            with store.transaction() as conn:
                if not store.governed_projection_audit_is_current(conn):
                    raise RuntimeError("governed projection changed before stale artifact reconciliation; retry")
                with bind_connection(conn):
                    knowledge_service.reconcile_source_lifecycle(
                        provenance_ref=f"prov_artifact:{artifact['artifact_id']}",
                        tenant_id=tenant_id,
                        source_stale=True,
                        source_generation_guard=lambda guard_conn, artifact=artifact: (
                            store.artifact_stale_generation_is_current(
                                guard_conn,
                                tenant_id=tenant_id,
                                artifact_id=str(artifact["artifact_id"]),
                                missing_since=artifact["missing_since"],
                            )
                        ),
                    )
                    checkpointed = store.mark_artifact_knowledge_reconciled(
                        tenant_id=tenant_id,
                        artifact_id=str(artifact["artifact_id"]),
                        missing_since=artifact["missing_since"],
                    )
                    if not checkpointed:
                        raise RuntimeError("stale artifact generation changed during knowledge reconciliation")
            reconciled += 1
        if len(artifacts) < page_size:
            break
    logger.info(
        "stale_artifact_knowledge_reconciled",
        reason_code="stale_artifact_knowledge_reconciled",
        tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
        artifact_type_fingerprint=_diagnostic_fingerprint(artifact_type),
        pages=pages,
        reconciled=reconciled,
    )


def _stale_artifact_authority_reconciler(*, store: Any, knowledge_service: Any, tenant_id: str):
    """Bind governed retirement to the signal-store transaction for one source."""
    bind_connection = knowledge_service.repository.bind_transaction_connection
    store.ensure_governed_projection_audit_current()

    def reconcile(conn, artifact: dict[str, Any]) -> None:
        if not store.governed_projection_audit_is_current(conn):
            raise RuntimeError("governed projection changed before stale artifact reconciliation; retry")
        with bind_connection(conn):
            knowledge_service.reconcile_source_lifecycle(
                provenance_ref=f"prov_artifact:{artifact['artifact_id']}",
                tenant_id=tenant_id,
                source_stale=True,
                source_generation_guard=lambda guard_conn: store.artifact_stale_generation_is_current(
                    guard_conn,
                    tenant_id=tenant_id,
                    artifact_id=str(artifact["artifact_id"]),
                    missing_since=artifact["missing_since"],
                ),
            )

    return reconcile


def _preserve_review_states(rows: list[dict[str, Any]], existing_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing_state_by_id = {row.get("id"): row.get("review_state") for row in existing_rows if row.get("id")}
    preserved = []
    for row in rows:
        updated = dict(row)
        existing_state = existing_state_by_id.get(row.get("id"))
        if existing_state:
            updated["review_state"] = existing_state
        preserved.append(updated)
    return preserved


def learn_artifact(
    artifact: LearnedArtifact,
    extractor: ArtifactExtractor,
    *,
    dry_run: bool = False,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, object]:
    allow_global_fallback = runtime_settings is None and store is None and knowledge_service is None
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    store = _resolve_artifact_store(
        dry_run=dry_run,
        store=store,
        runtime_settings=active_settings,
        knowledge_service=knowledge_service,
        allow_global_fallback=allow_global_fallback,
    )
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    if not dry_run:
        assert store is not None
        from tacit.dashboard_ingest.service import _knowledge_service_for_store

        knowledge_service = knowledge_service or _knowledge_service_for_store(
            store,
            runtime_settings=active_settings,
        )
        active_settings, tenant_id = authorize_artifact_learning(
            dry_run=dry_run,
            runtime_settings=runtime_settings,
            store=store,
            knowledge_service=knowledge_service,
            tenant_id=tenant_id,
        )
    result = extractor.extract(artifact)
    evidence_rows = _as_store_rows(result.evidence_requirements)
    ownership_rows = _as_store_rows(result.ownership_hints)
    dependency_rows = _as_store_rows(result.dependency_hints)
    signal_rows = _as_store_rows(result.signal_mapping_candidates)
    change_state = "dry_run"
    indexed_context_rows = 0
    mappings_created = 0
    governed_candidate_ids: list[str] = []
    if not dry_run:
        assert store is not None
        candidate_count = len(evidence_rows) + len(ownership_rows) + len(dependency_rows) + len(signal_rows)
        atomic_candidate_limit = int(active_settings.knowledge_source_atomic_candidate_limit)
        if candidate_count > atomic_candidate_limit:
            logger.warning(
                "artifact_authority_fanout_rejected",
                reason_code="artifact_authority_fanout_rejected",
                tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                artifact_fingerprint=_diagnostic_fingerprint(artifact.id),
                candidate_count=candidate_count,
                candidate_limit=atomic_candidate_limit,
            )
            raise ValueError(
                f"artifact produced {candidate_count} candidates; the atomic source limit is {atomic_candidate_limit}"
            )

        from tacit.knowledge.migration import migrate_artifact_extractions

        assert knowledge_service is not None
        bind_connection = getattr(knowledge_service.repository, "bind_transaction_connection", None)
        if bind_connection is None:
            raise ValueError("artifact governance requires a transactional knowledge repository")
        store.ensure_governed_projection_audit_current()

        index_evidence_rows = evidence_rows
        index_ownership_rows = ownership_rows
        index_dependency_rows = dependency_rows
        index_signal_rows = signal_rows
        should_replace_extractions = True
        authority_started_at = time.monotonic()
        try:
            # The source, extraction, search, and governed authority rows are one
            # generation. A failed late stage rolls the complete generation back.
            with store.transaction() as conn:
                with bind_connection(conn):
                    change_state = store.record_learned_artifact(
                        tenant_id=tenant_id,
                        artifact_id=artifact.id,
                        artifact_type=artifact.artifact_type,
                        source_vendor=artifact.source_vendor or "",
                        source_instance=artifact.source_instance or "",
                        external_id=artifact.external_id,
                        title=artifact.title,
                        body_text=artifact.body_text,
                        provenance_url=artifact.provenance_url or "",
                        fingerprint=artifact.fingerprint,
                    )
                    should_replace_extractions = change_state != "skipped"
                    existing_rows = (
                        store.list_artifact_extractions(artifact.id, tenant_id=tenant_id)
                        if change_state in {"updated", "restored", "skipped"}
                        else None
                    )
                    if change_state == "skipped":
                        assert existing_rows is not None
                        should_replace_extractions = not _extraction_generation_matches(
                            existing_rows,
                            evidence_rows=evidence_rows,
                            ownership_rows=ownership_rows,
                            dependency_rows=dependency_rows,
                            signal_rows=signal_rows,
                        )
                        if should_replace_extractions:
                            logger.warning(
                                "artifact_extraction_generation_repaired",
                                reason_code="artifact_extraction_generation_repaired",
                                tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                                artifact_fingerprint=_diagnostic_fingerprint(artifact.id),
                            )
                            evidence_rows = _preserve_review_states(
                                evidence_rows,
                                existing_rows["evidence_requirements"],
                            )
                            ownership_rows = _preserve_review_states(
                                ownership_rows,
                                existing_rows["ownership_hints"],
                            )
                            dependency_rows = _preserve_review_states(
                                dependency_rows,
                                existing_rows["dependency_hints"],
                            )
                            signal_rows = _preserve_review_states(
                                signal_rows,
                                existing_rows["signal_mapping_candidates"],
                            )
                            index_evidence_rows = evidence_rows
                            index_ownership_rows = ownership_rows
                            index_dependency_rows = dependency_rows
                            index_signal_rows = signal_rows
                        else:
                            index_evidence_rows = existing_rows["evidence_requirements"]
                            index_ownership_rows = existing_rows["ownership_hints"]
                            index_dependency_rows = existing_rows["dependency_hints"]
                            index_signal_rows = existing_rows["signal_mapping_candidates"]
                            evidence_rows = index_evidence_rows
                            ownership_rows = index_ownership_rows
                            dependency_rows = index_dependency_rows
                            signal_rows = index_signal_rows
                    elif existing_rows is not None:
                        evidence_rows = _preserve_review_states(
                            evidence_rows,
                            existing_rows["evidence_requirements"],
                        )
                        ownership_rows = _preserve_review_states(
                            ownership_rows,
                            existing_rows["ownership_hints"],
                        )
                        dependency_rows = _preserve_review_states(
                            dependency_rows,
                            existing_rows["dependency_hints"],
                        )
                        signal_rows = _preserve_review_states(
                            signal_rows,
                            existing_rows["signal_mapping_candidates"],
                        )
                        index_evidence_rows = evidence_rows
                        index_ownership_rows = ownership_rows
                        index_dependency_rows = dependency_rows
                        index_signal_rows = signal_rows
                    if should_replace_extractions:
                        store.replace_artifact_extractions(
                            tenant_id=tenant_id,
                            artifact_id=artifact.id,
                            evidence_requirements=evidence_rows,
                            ownership_hints=ownership_rows,
                            dependency_hints=dependency_rows,
                            signal_mapping_candidates=signal_rows,
                        )
                    if (
                        change_state != "skipped"
                        or should_replace_extractions
                        or not store.artifact_context_indexed(
                            tenant_id=tenant_id,
                            artifact_id=artifact.id,
                            artifact_type=artifact.artifact_type,
                            strict=True,
                        )
                    ):
                        indexed_context_rows = store.index_artifact_context(
                            tenant_id=tenant_id,
                            artifact_id=artifact.id,
                            artifact_type=artifact.artifact_type,
                            title=artifact.title,
                            body_text=_sanitized_body_text_for_index(artifact, result),
                            evidence_requirements=index_evidence_rows,
                            ownership_hints=index_ownership_rows,
                            dependency_hints=index_dependency_rows,
                            signal_mapping_candidates=index_signal_rows,
                            strict=True,
                        )
                    governed_candidate_ids = migrate_artifact_extractions(
                        artifact_id=artifact.id,
                        artifact_type=artifact.artifact_type,
                        artifact_fingerprint=artifact.fingerprint,
                        artifact_content_fingerprint=_artifact_content_fingerprint(artifact.body_text),
                        source_vendor=artifact.source_vendor or "",
                        source_instance=artifact.source_instance or "",
                        external_id=artifact.external_id,
                        rows={
                            "evidence_requirements": evidence_rows,
                            "ownership_hints": ownership_rows,
                            "dependency_hints": dependency_rows,
                            "signal_mapping_candidates": signal_rows,
                        },
                        service=knowledge_service,
                        tenant_id=tenant_id,
                        max_candidate_count=atomic_candidate_limit,
                    )
                    knowledge_service.reconcile_source_lifecycle(
                        provenance_ref=f"prov_artifact:{artifact.id}",
                        tenant_id=tenant_id,
                        active_candidate_ids=set(governed_candidate_ids),
                        max_candidate_count=atomic_candidate_limit,
                    )
        except Exception as exc:
            logger.error(
                "artifact_authority_transaction_failed",
                reason_code="artifact_authority_transaction_failed",
                exception_class=type(exc).__name__[:64],
                error_fingerprint=_diagnostic_fingerprint(exc),
                tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                artifact_fingerprint=_diagnostic_fingerprint(artifact.id),
                candidate_count=candidate_count,
                candidate_limit=atomic_candidate_limit,
                duration_ms=round((time.monotonic() - authority_started_at) * 1000, 2),
            )
            raise
        logger.info(
            "artifact_authority_transaction_committed",
            reason_code="artifact_authority_transaction_committed",
            tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
            artifact_fingerprint=_diagnostic_fingerprint(artifact.id),
            candidate_count=candidate_count,
            governed_candidate_count=len(governed_candidate_ids),
            indexed_context_rows=indexed_context_rows,
            duration_ms=round((time.monotonic() - authority_started_at) * 1000, 2),
        )
    artifact_summary = asdict(artifact)
    artifact_summary.pop("body_text", None)
    return {
        "artifact": artifact_summary,
        "artifact_id": artifact.id,
        "artifact_type": artifact.artifact_type,
        "title": artifact.title,
        "change_state": change_state,
        "dry_run": dry_run,
        "evidence_requirements": evidence_rows,
        "ownership_hints": ownership_rows,
        "dependency_hints": dependency_rows,
        "signal_mapping_candidates": signal_rows,
        "warnings": result.warnings,
        "indexed_context_rows": indexed_context_rows,
        "mappings_created": mappings_created,
        "knowledge_candidate_ids": governed_candidate_ids,
        "summary": {
            "artifact_type": artifact.artifact_type,
            "learned": 0 if dry_run else 1,
            "updated": int(change_state == "updated"),
            "skipped": int(change_state == "skipped"),
            "restored": int(change_state == "restored"),
            "evidence_requirements": len(result.evidence_requirements),
            "ownership_hints": len(result.ownership_hints),
            "dependency_hints": len(result.dependency_hints),
            "signal_mapping_candidates": len(result.signal_mapping_candidates),
            "warnings": result.warnings,
        },
    }


def learn_runbook_file(
    path: Path,
    *,
    dry_run: bool = False,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, object]:
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    return learn_artifact(
        runbook_from_file(path),
        RunbookExtractor(),
        dry_run=dry_run,
        runtime_settings=active_settings,
        store=store,
        tenant_id=tenant_id,
        knowledge_service=knowledge_service,
    )


def incident_from_file(path: Path) -> LearnedArtifact:
    body = path.read_text()
    title = path.stem.replace("-", " ").replace("_", " ").strip() or path.name
    return artifact_from_text(
        artifact_type="incident",
        title=title,
        body_text=body,
        external_id=str(path.resolve()),
        source_vendor="file",
        source_instance=str(path.parent.resolve()),
        provenance_url=str(path.resolve()),
    )


def learn_incident_file(
    path: Path,
    *,
    dry_run: bool = False,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, object]:
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    return learn_artifact(
        incident_from_file(path),
        IncidentExtractor(),
        dry_run=dry_run,
        runtime_settings=active_settings,
        store=store,
        tenant_id=tenant_id,
        knowledge_service=knowledge_service,
    )


def _bounded_artifact_files(path: Path, *, limit: int) -> list[Path]:
    """Collect one bounded crawl before any artifact source is read."""
    files: list[Path] = []
    for candidate in path.rglob("*"):
        if candidate.suffix.lower() not in {".md", ".txt"} or not candidate.is_file():
            continue
        if len(files) >= limit:
            logger.warning(
                "artifact_directory_file_limit_exceeded",
                reason_code="artifact_directory_file_limit_exceeded",
                path_fingerprint=_diagnostic_fingerprint(path),
                limit=limit,
            )
            raise ValueError(f"Artifact directory exceeds the configured file limit ({limit})")
        files.append(candidate)
    return sorted(files)


def learn_incident_dir(
    path: Path,
    *,
    dry_run: bool = False,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, object]:
    allow_global_fallback = runtime_settings is None and store is None and knowledge_service is None
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    store = _resolve_artifact_store(
        dry_run=dry_run,
        store=store,
        runtime_settings=active_settings,
        knowledge_service=knowledge_service,
        allow_global_fallback=allow_global_fallback,
    )
    if not dry_run:
        assert store is not None
        from tacit.dashboard_ingest.service import _knowledge_service_for_store

        knowledge_service = knowledge_service or _knowledge_service_for_store(store, runtime_settings=active_settings)
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=active_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    crawl_started_at = time.time()
    files = _bounded_artifact_files(
        path,
        limit=int(active_settings.artifact_learning_directory_file_limit),
    )
    learned = [
        learn_incident_file(
            file,
            dry_run=dry_run,
            runtime_settings=active_settings,
            store=store,
            tenant_id=tenant_id,
            knowledge_service=knowledge_service,
        )
        for file in files
    ]

    def _count(key: str) -> int:
        total = 0
        for item in learned:
            value = item.get(key, [])
            if isinstance(value, list):
                total += len(value)
        return total

    stale_marked = 0
    if not dry_run:
        assert store is not None
        seen = {str(item["artifact_id"]) for item in learned}
        _reconcile_stale_artifact_knowledge(
            store=store,
            tenant_id=tenant_id,
            artifact_type="incident",
            runtime_settings=active_settings,
            knowledge_service=knowledge_service,
        )
        stale_marked = store.mark_missing_artifacts_stale(
            tenant_id=tenant_id,
            artifact_type="incident",
            seen_artifact_ids=seen,
            source_vendor="file",
            external_id_prefix=f"{path.resolve()}/",
            crawl_started_at=crawl_started_at,
            authority_reconciler=_stale_artifact_authority_reconciler(
                store=store,
                knowledge_service=knowledge_service,
                tenant_id=tenant_id,
            ),
        )
    return {
        "artifact_type": "incident",
        "dry_run": dry_run,
        "artifacts_discovered": len(files),
        "artifacts_learned": 0 if dry_run else len(learned),
        "stale_marked": stale_marked,
        "learned": learned,
        "summary": {
            "artifact_type": "incident",
            "learned": 0 if dry_run else len(learned),
            "evidence_requirements": _count("evidence_requirements"),
            "ownership_hints": _count("ownership_hints"),
            "dependency_hints": _count("dependency_hints"),
            "signal_mapping_candidates": _count("signal_mapping_candidates"),
            "stale_marked": stale_marked,
        },
    }


def learn_runbook_dir(
    path: Path,
    *,
    dry_run: bool = False,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, object]:
    allow_global_fallback = runtime_settings is None and store is None and knowledge_service is None
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    store = _resolve_artifact_store(
        dry_run=dry_run,
        store=store,
        runtime_settings=active_settings,
        knowledge_service=knowledge_service,
        allow_global_fallback=allow_global_fallback,
    )
    if not dry_run:
        assert store is not None
        from tacit.dashboard_ingest.service import _knowledge_service_for_store

        knowledge_service = knowledge_service or _knowledge_service_for_store(store, runtime_settings=active_settings)
    active_settings, tenant_id = authorize_artifact_learning(
        dry_run=dry_run,
        runtime_settings=active_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
    )
    crawl_started_at = time.time()
    files = _bounded_artifact_files(
        path,
        limit=int(active_settings.artifact_learning_directory_file_limit),
    )
    learned = [
        learn_runbook_file(
            file,
            dry_run=dry_run,
            runtime_settings=active_settings,
            store=store,
            tenant_id=tenant_id,
            knowledge_service=knowledge_service,
        )
        for file in files
    ]

    def _count(key: str) -> int:
        total = 0
        for item in learned:
            value = item.get(key, [])
            if isinstance(value, list):
                total += len(value)
        return total

    stale_marked = 0
    if not dry_run:
        assert store is not None
        seen = {str(item["artifact_id"]) for item in learned}
        _reconcile_stale_artifact_knowledge(
            store=store,
            tenant_id=tenant_id,
            artifact_type="runbook",
            runtime_settings=active_settings,
            knowledge_service=knowledge_service,
        )
        stale_marked = store.mark_missing_artifacts_stale(
            tenant_id=tenant_id,
            artifact_type="runbook",
            seen_artifact_ids=seen,
            source_vendor="file",
            external_id_prefix=f"{path.resolve()}/",
            crawl_started_at=crawl_started_at,
            authority_reconciler=_stale_artifact_authority_reconciler(
                store=store,
                knowledge_service=knowledge_service,
                tenant_id=tenant_id,
            ),
        )
    return {
        "artifact_type": "runbook",
        "dry_run": dry_run,
        "artifacts_discovered": len(files),
        "artifacts_learned": 0 if dry_run else len(learned),
        "stale_marked": stale_marked,
        "learned": learned,
        "summary": {
            "artifact_type": "runbook",
            "learned": 0 if dry_run else len(learned),
            "evidence_requirements": _count("evidence_requirements"),
            "ownership_hints": _count("ownership_hints"),
            "dependency_hints": _count("dependency_hints"),
            "signal_mapping_candidates": _count("signal_mapping_candidates"),
            "stale_marked": stale_marked,
        },
    }
