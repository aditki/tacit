"""Build governed-knowledge lookup scope from explicit investigation language."""

from __future__ import annotations

import re
from collections.abc import Iterable

from tacit.knowledge.models import KnowledgeScope
from tacit.knowledge.normalization import normalize_entity, normalize_service_ref

_ENVIRONMENT_ALIASES = {
    "prod": "production",
    "production": "production",
    "stage": "staging",
    "staging": "staging",
    "dev": "development",
    "development": "development",
    "qa": "qa",
    "test": "test",
    "sandbox": "sandbox",
}
_REGION_PATTERN = re.compile(
    r"\b(?:us|eu|ap|sa|ca|me|af|il)-(?:north|south|east|west|central|northeast|northwest|southeast|southwest)-\d\b",
    re.IGNORECASE,
)
_VERSION_VALUE = r"v?\d+\.\d+(?:\.\d+)?(?:[-+][a-z0-9_.-]+)?"
_PREFIXED_VERSION_PATTERN = re.compile(r"\bv\d+\.\d+(?:\.\d+)?(?:[-+][a-z0-9_.-]+)?\b", re.IGNORECASE)
_LABELED_VERSION_PATTERN = re.compile(
    rf"\b(?:version|release)\b\s*(?:(?:is|=|:)\s*)?(?P<version>{_VERSION_VALUE})\b",
    re.IGNORECASE,
)
_SCOPE_IDENTIFIER_TOKEN = r"[a-z0-9][a-z0-9_.-]*"
_QUOTED_SCOPE_IDENTIFIER = r"[a-z0-9](?:[a-z0-9_. -]*[a-z0-9])?"
_UNQUOTED_SCOPE_CANDIDATE = r"[^\s,;!?()[\]{}]+"
_SCOPE_IDENTIFIER_PATTERN = re.compile(rf"{_SCOPE_IDENTIFIER_TOKEN}\Z", re.IGNORECASE)


def investigation_knowledge_scope(
    *,
    tenant_id: str,
    prompt: str,
    services: Iterable[str],
    archetype_ids: Iterable[str],
) -> KnowledgeScope:
    """Extract only explicit scope dimensions; absent dimensions remain unscoped."""
    lowered = prompt.casefold()
    environments = {
        canonical
        for alias, canonical in _ENVIRONMENT_ALIASES.items()
        if re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", lowered)
    }
    regions = set(_REGION_PATTERN.findall(prompt))
    clusters = _labeled_values(prompt, "cluster")
    namespaces = _labeled_values(prompt, "namespace")
    versions = _version_values(prompt)
    return KnowledgeScope(
        tenant_id=tenant_id,
        environment_refs=_dimension_refs("environment", environments),
        region_refs=_dimension_refs("region", regions),
        cluster_refs=_dimension_refs("cluster", clusters),
        namespace_refs=_dimension_refs("namespace", namespaces),
        service_refs=sorted({normalize_service_ref(value) for value in services if value}),
        archetype_refs=_dimension_refs("archetype", archetype_ids),
        version_constraints=_dimension_refs("version", versions),
    )


def _labeled_values(prompt: str, label: str) -> set[str]:
    escaped_label = re.escape(label)
    unquoted_patterns = (
        re.compile(
            rf"\b{escaped_label}\b\s*[:=]\s*(?P<candidate>{_UNQUOTED_SCOPE_CANDIDATE})",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b{escaped_label}\b\s+(?:named|called)\s+(?P<candidate>{_UNQUOTED_SCOPE_CANDIDATE})",
            re.IGNORECASE,
        ),
    )
    quoted_patterns = (
        re.compile(
            rf"\b{escaped_label}\b\s*(?::|=)?\s*(?P<quote>['\"])" rf"(?P<value>{_QUOTED_SCOPE_IDENTIFIER})(?P=quote)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b{escaped_label}\b\s+(?:named|called)\s+(?P<quote>['\"])"
            rf"(?P<value>{_QUOTED_SCOPE_IDENTIFIER})(?P=quote)",
            re.IGNORECASE,
        ),
    )
    values = set()
    for pattern in unquoted_patterns:
        for match in pattern.finditer(prompt):
            value = match.group("candidate").rstrip(".")
            if _SCOPE_IDENTIFIER_PATTERN.fullmatch(value):
                values.add(value)
    for pattern in quoted_patterns:
        values.update(match.group("value").strip() for match in pattern.finditer(prompt))
    return values


def _version_values(prompt: str) -> set[str]:
    values = set(_PREFIXED_VERSION_PATTERN.findall(prompt))
    values.update(match.group("version") for match in _LABELED_VERSION_PATTERN.finditer(prompt))
    return values


def _dimension_refs(prefix: str, values: Iterable[str]) -> list[str]:
    refs = set()
    for value in values:
        normalized = normalize_entity(value)
        if not normalized:
            continue
        refs.add(normalized if normalized.startswith(f"{prefix}:") else f"{prefix}:{normalized}")
    return sorted(refs)
