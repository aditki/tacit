"""Canonical Operational Knowledge lifecycle vocabularies."""

from enum import StrEnum


class KnowledgeKind(StrEnum):
    DEPENDENCY = "dependency"
    OWNERSHIP = "ownership"
    SIGNAL_MAPPING = "signal_mapping"
    EVIDENCE_REQUIREMENT = "evidence_requirement"
    ARTIFACT_QUALITY = "artifact_quality"
    INVESTIGATION_PATTERN = "investigation_pattern"


class ReviewState(StrEnum):
    CANDIDATE = "candidate"
    APPROVED = "approved"
    TRUSTED = "trusted"
    REJECTED = "rejected"


class LifecycleStatus(StrEnum):
    ACTIVE = "active"
    STALE = "stale"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"
    WITHDRAWN = "withdrawn"


class KnowledgeEligibility(StrEnum):
    INELIGIBLE = "ineligible"
    CONTEXTUAL_ONLY = "contextual_only"
    HISTORICAL_SUPPORT = "historical_support"
    LIVE_VERIFIED = "live_verified"


class EntityKind(StrEnum):
    SERVICE = "service"
    TEAM = "team"
    DATASTORE = "datastore"
    ENVIRONMENT = "environment"
    REGION = "region"
    CLUSTER = "cluster"
    NAMESPACE = "namespace"
    SIGNAL = "signal"
    UNKNOWN = "unknown"


class EntityStatus(StrEnum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    WITHDRAWN = "withdrawn"


class EntityResolutionStatus(StrEnum):
    RESOLVED = "resolved"
    AMBIGUOUS = "ambiguous"
    UNRESOLVED = "unresolved"
    REJECTED = "rejected"


class EntityBindingMethod(StrEnum):
    EXACT_ID = "exact_id"
    EXACT_NAME = "exact_name"
    EXACT_ALIAS = "exact_alias"
    CATALOG_REFERENCE = "catalog_reference"
    ARTIFACT_REFERENCE = "artifact_reference"
    HUMAN_CORRECTION = "human_correction"
    VENDOR_MAPPING = "vendor_mapping"
    DETERMINISTIC_NORMALIZATION = "deterministic_normalization"
    FUZZY_CANDIDATE = "fuzzy_candidate"


class EvidenceRole(StrEnum):
    SUPPORTING = "supporting"
    CONTRADICTING = "contradicting"
    NEUTRAL = "neutral"


class SourceFamily(StrEnum):
    DASHBOARD = "dashboard"
    ALERT = "alert"
    RUNBOOK = "runbook"
    INCIDENT = "incident"
    SERVICE_CATALOG = "service_catalog"
    DEPLOYMENT = "deployment"
    REPOSITORY = "repository"
    HUMAN_CORRECTION = "human_correction"
    LIVE_OBSERVATION = "live_observation"
    VENDOR_METADATA = "vendor_metadata"
    MIGRATION = "migration"
    UNKNOWN = "unknown"


class LineageKind(StrEnum):
    INDEPENDENT = "independent"
    COPIED_FROM = "copied_from"
    GENERATED_FROM = "generated_from"
    SAME_VENDOR_EXPORT = "same_vendor_export"
    SAME_SOURCE_REVISION = "same_source_revision"
    UNKNOWN = "unknown"


class Predicate(StrEnum):
    DEPENDS_ON = "depends_on"
    DOES_NOT_DEPEND_ON = "does_not_depend_on"
    CALLS = "calls"
    READS_FROM = "reads_from"
    WRITES_TO = "writes_to"
    PUBLISHES_TO = "publishes_to"
    CONSUMES_FROM = "consumes_from"
    FRONTED_BY = "fronted_by"
    HOSTED_ON = "hosted_on"
    MOUNTED_FROM = "mounted_from"
    OWNED_BY = "owned_by"
    SUPPORTED_BY = "supported_by"
    ESCALATES_TO = "escalates_to"
    REPRESENTED_BY = "represented_by"
    INDICATES = "indicates"
    CORRELATES_WITH = "correlates_with"
    REQUIRES_OBSERVATION = "requires_observation"
    USEFUL_FOR_INVESTIGATION = "useful_for_investigation"


class PromotionDecisionType(StrEnum):
    PROMOTE = "promote"
    RETAIN_CANDIDATE = "retain_candidate"
    DEMOTE = "demote"
    REJECT = "reject"
    EXPIRE = "expire"
    SUPERSEDE = "supersede"


class CorroborationStatus(StrEnum):
    UNCORROBORATED = "uncorroborated"
    SINGLE_SOURCE = "single_source"
    MULTI_SOURCE = "multi_source"
    MULTI_FAMILY = "multi_family"
    LIVE_CORROBORATED = "live_corroborated"


class ConflictKind(StrEnum):
    DIRECT_NEGATION = "direct_negation"
    COMPETING_OWNER = "competing_owner"
    COMPETING_DEPENDENCY = "competing_dependency"
    COMPETING_SIGNAL_MAPPING = "competing_signal_mapping"
    TEMPORAL_SUPERSESSION = "temporal_supersession"
    SCOPE_MISMATCH = "scope_mismatch"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    REGION_MISMATCH = "region_mismatch"
    VERSION_MISMATCH = "version_mismatch"
    ARTIFACT_STALENESS = "artifact_staleness"
    HISTORICAL_VS_LIVE = "historical_vs_live"


class ConflictResolutionStatus(StrEnum):
    UNRESOLVED = "unresolved"
    RESOLVED_BY_SCOPE = "resolved_by_scope"
    RESOLVED_BY_TIME = "resolved_by_time"
    RESOLVED_BY_AUTHORITY = "resolved_by_authority"
    RESOLVED_BY_REVIEW = "resolved_by_review"
    SUPERSEDED = "superseded"
    ACCEPTED_AMBIGUITY = "accepted_ambiguity"


class KnowledgeUsageDisposition(StrEnum):
    APPLIED = "applied"
    CONSIDERED_NOT_APPLIED = "considered_not_applied"
    REJECTED_BY_SCOPE = "rejected_by_scope"
    REJECTED_AS_STALE = "rejected_as_stale"
    REJECTED_BY_CONFLICT = "rejected_by_conflict"
    REJECTED_BY_REVIEW_STATE = "rejected_by_review_state"
    REJECTED_BY_ELIGIBILITY = "rejected_by_eligibility"
    CONTRADICTED_BY_OBSERVATION = "contradicted_by_observation"
    UNRESOLVED_ENTITY = "unresolved_entity"


class CorrectionType(StrEnum):
    ENTITY_MAPPING = "entity_mapping"
    SIGNAL_MEANING = "signal_meaning"
    DEPENDENCY = "dependency"
    OWNERSHIP = "ownership"
    ARTIFACT_QUALITY = "artifact_quality"
    RANKING_FEEDBACK = "ranking_feedback"
    MISSING_CHECK = "missing_check"
    FALSE_CULPRIT = "false_culprit"
    MISSING_CULPRIT = "missing_culprit"
    OBSERVATION_DISPUTE = "observation_dispute"
    SCOPE_CORRECTION = "scope_correction"
    TIME_WINDOW_CORRECTION = "time_window_correction"
    KNOWLEDGE_STALE = "knowledge_stale"
    KNOWLEDGE_INCORRECT = "knowledge_incorrect"
