"""Assessment report export helpers.

The anonymous export is intentionally aggregate-first. It preserves counts,
relationships, status distributions, and failure diagnostics while avoiding raw
operational text such as prompts, dashboard bodies, runbooks, alert bodies, and
comments.
"""

from __future__ import annotations

import json
import re
import tarfile
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tacit import __version__

AssessmentReport = dict[str, Any]

ASSESSMENT_VERSION = "1"
ANONYMOUS_BUNDLE_FILES = (
    "README.txt",
    "metadata.json",
    "assessment_summary.json",
    "knowledge_coverage.json",
    "artifact_stats.json",
    "ranking_summary.json",
    "robustness_summary.json",
    "warnings.json",
    "validation_report.json",
)

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
URL_RE = re.compile(r"https?://[^\s\"'<>]+")
PATH_RE = re.compile(r"(?:/[A-Za-z0-9._\-/]+)|(?:[A-Za-z]:\\[^\s\"'<>]+)")
SECRET_RE = re.compile(
    r"(?i)(?:api[_-]?key|token|secret|password|passwd|authorization|bearer)\s*[:=]\s*[A-Za-z0-9._\-+/=]{8,}"
)
HOSTNAME_RE = re.compile(r"\b(?:[a-zA-Z0-9-]+\.){2,}[a-zA-Z]{2,}\b")

_LEAKAGE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("email", EMAIL_RE),
    ("url", URL_RE),
    ("path", PATH_RE),
    ("secret", SECRET_RE),
    ("hostname", HOSTNAME_RE),
)


@dataclass
class ExportResult:
    output_path: Path
    validation_report: dict[str, Any]
    files: list[str]


class ReportAnonymizer:
    """Deterministic per-export anonymizer.

    The mapping lives only in memory and is never included in anonymous bundles.
    """

    def __init__(self) -> None:
        self._mapping: dict[tuple[str, str], str] = {}
        self._counters: dict[str, int] = defaultdict(int)

    def anonymize_report(self, report: AssessmentReport) -> AssessmentReport:
        return self._walk(report)

    def anonymize_value(self, value: str, kind: str) -> str:
        key = (kind, value)
        if key not in self._mapping:
            self._counters[kind] += 1
            self._mapping[key] = f"{kind}_{self._counters[kind]:03d}"
        return self._mapping[key]

    def _walk(self, value: Any) -> Any:
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in value.items():
                if isinstance(item, str):
                    kind = _kind_for_key(str(key))
                    out[key] = self._sanitize_string(item, kind)
                else:
                    out[key] = self._walk(item)
            return out
        if isinstance(value, list):
            return [self._walk(item) for item in value]
        if isinstance(value, str):
            return self._sanitize_string(value, "value")
        return value

    def _sanitize_string(self, value: str, kind: str) -> str:
        if not value:
            return value
        if kind in {
            "service",
            "team",
            "dashboard",
            "alert",
            "runbook",
            "incident",
            "repo",
            "cluster",
            "namespace",
            "label_value",
            "path",
            "url",
            "email",
            "hostname",
        }:
            return self.anonymize_value(value, kind)
        return redact_text(value)


def redact_text(value: str) -> str:
    """Redact obvious sensitive substrings without preserving a mapping."""
    value = SECRET_RE.sub("<redacted_secret>", value)
    value = EMAIL_RE.sub("<redacted_email>", value)
    value = URL_RE.sub("<redacted_url>", value)
    value = PATH_RE.sub("<redacted_path>", value)
    value = HOSTNAME_RE.sub("<redacted_hostname>", value)
    return value


def build_assessment_report(*, anonymous: bool) -> AssessmentReport:
    """Build the report sections from local Tacit stores."""
    from tacit.feedback import get_feedback_store
    from tacit.history import get_investigation_store
    from tacit.signals import get_signal_store

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    history_store = get_investigation_store()
    feedback_store = get_feedback_store()
    signal_store = get_signal_store()

    investigations = history_store.list_recent(limit=10_000)
    history_stats = history_store.stats()
    feedback_stats = feedback_store.get_aggregate_stats()
    feedback_analysis = feedback_store.analyze()
    signal_stats = signal_store.stats()
    dashboards = signal_store.list_ingested_dashboards(limit=10_000)
    alerts = signal_store.list_ingested_alerts(limit=10_000)
    learned_artifacts = signal_store.list_learned_artifacts(limit=10_000)

    report: AssessmentReport = {
        "metadata": _metadata(generated_at, anonymous=anonymous),
        "assessment_summary": _assessment_summary(
            investigations=investigations,
            history_stats=history_stats,
            feedback_stats=feedback_stats,
            signal_stats=signal_stats,
        ),
        "knowledge_coverage": _knowledge_coverage(signal_stats, dashboards, alerts, learned_artifacts),
        "artifact_stats": _artifact_stats(dashboards, alerts, learned_artifacts),
        "ranking_summary": _ranking_summary(investigations),
        "robustness_summary": _robustness_summary(investigations, feedback_analysis),
        "warnings": {
            "export_warnings": _export_warnings(anonymous=anonymous),
            "validation_warnings_by_kind": _validation_warning_counts(investigations),
        },
    }

    if not anonymous:
        report["raw_local_details"] = _raw_local_details(
            investigations=investigations,
            dashboards=dashboards,
            alerts=alerts,
            learned_artifacts=learned_artifacts,
            feedback_analysis=feedback_analysis,
        )
    return report


def export_assessment_report(
    *,
    output: Path | None = None,
    anonymous: bool = False,
    validate: bool = False,
) -> ExportResult:
    """Write a tar.gz assessment bundle and return export metadata."""
    report = build_assessment_report(anonymous=anonymous)
    if anonymous:
        report = ReportAnonymizer().anonymize_report(report)

    validation_report = validate_report_for_leakage(report if anonymous else _anonymous_projection(report))
    report["validation_report"] = validation_report
    if anonymous and validate and not validation_report["passed"]:
        findings = validation_report["findings_count"]
        raise ValueError(f"Anonymous report failed leakage validation with {findings} finding(s)")

    output_path = output or default_report_path(anonymous=anonymous)
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="tacit-assessment-") as tmp:
        tmp_path = Path(tmp)
        files = _write_report_files(tmp_path, report, anonymous=anonymous)
        with tarfile.open(output_path, "w:gz") as tar:
            for name in files:
                tar.add(tmp_path / name, arcname=name)
    return ExportResult(output_path=output_path, validation_report=validation_report, files=list(files))


def default_report_path(*, anonymous: bool) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    suffix = "-anonymous" if anonymous else ""
    return Path(f"tacit-assessment-{stamp}{suffix}.tar.gz")


def validate_report_for_leakage(report: AssessmentReport) -> dict[str, Any]:
    """Scan exported anonymous content for obvious leakage patterns."""
    findings: list[dict[str, str]] = []
    for path, value in _iter_strings(report):
        if _path_is_allowed_for_validation(path):
            continue
        for kind, pattern in _LEAKAGE_PATTERNS:
            match = pattern.search(value)
            if match:
                findings.append(
                    {
                        "path": path,
                        "kind": kind,
                        "sample": _sample(match.group(0)),
                    }
                )
    return {
        "passed": not findings,
        "findings_count": len(findings),
        "findings": findings[:100],
        "checks": [kind for kind, _pattern in _LEAKAGE_PATTERNS],
    }


def _metadata(generated_at: str, *, anonymous: bool) -> dict[str, Any]:
    return {
        "tacit_version": __version__,
        "assessment_version": ASSESSMENT_VERSION,
        "generated_at": generated_at,
        "anonymous": anonymous,
        "mapping_included": False,
        "graph_preserved": True,
        "raw_artifacts_included": not anonymous,
        "telemetry_included": False,
        "hostnames_included": False,
        "emails_included": False,
    }


def _assessment_summary(
    *,
    investigations: list[dict[str, Any]],
    history_stats: dict[str, Any],
    feedback_stats: dict[str, Any],
    signal_stats: dict[str, Any],
) -> dict[str, Any]:
    return {
        "investigations": _safe_numbers(history_stats),
        "feedback": _safe_numbers(feedback_stats),
        "learning": {
            "signal_types": signal_stats.get("signal_types", 0),
            "metric_mappings": signal_stats.get("metric_mappings", 0),
            "ingested_dashboards": signal_stats.get("ingested_dashboards", 0),
            "ingested_alerts": signal_stats.get("ingested_alerts", 0),
            "learned_artifacts": signal_stats.get("learned_artifacts", 0),
        },
        "status_counts": dict(Counter(str(inv.get("status", "")) for inv in investigations)),
        "path_counts": dict(Counter(str(inv.get("path_used", "")) for inv in investigations)),
    }


def _knowledge_coverage(
    signal_stats: dict[str, Any],
    dashboards: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    learned_artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    dashboard_status = Counter(str(item.get("status", "")) for item in dashboards)
    alert_status = Counter(str(item.get("status", "")) for item in alerts)
    artifact_types = Counter(str(item.get("artifact_type", "")) for item in learned_artifacts)
    backends = Counter(str(item.get("backend_name", "")) for item in dashboards + alerts)
    return {
        "signals_by_category": signal_stats.get("signals_by_category", {}),
        "mappings_by_source": signal_stats.get("mappings_by_source", {}),
        "dashboard_review_states": dict(dashboard_status),
        "alert_review_states": dict(alert_status),
        "learned_artifact_types": dict(artifact_types),
        "backend_distribution": dict(backends),
    }


def _artifact_stats(
    dashboards: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    learned_artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "dashboards": {
            "count": len(dashboards),
            "by_backend": dict(Counter(str(item.get("backend_name", "")) for item in dashboards)),
            "by_status": dict(Counter(str(item.get("status", "")) for item in dashboards)),
            "panel_count": _numeric_summary([item.get("panel_count", 0) for item in dashboards]),
            "metrics_found": _numeric_summary([len(item.get("metrics_found", []) or []) for item in dashboards]),
            "signals_inferred": _numeric_summary(
                [len(item.get("signals_inferred", []) or []) for item in dashboards]
            ),
        },
        "alerts": {
            "count": len(alerts),
            "by_backend": dict(Counter(str(item.get("backend_name", "")) for item in alerts)),
            "by_status": dict(Counter(str(item.get("status", "")) for item in alerts)),
            "metrics_found": _numeric_summary([len(item.get("metrics_found", []) or []) for item in alerts]),
            "signals_inferred": _numeric_summary([len(item.get("signals_inferred", []) or []) for item in alerts]),
        },
        "learned_artifacts": {
            "count": len(learned_artifacts),
            "by_type": dict(Counter(str(item.get("artifact_type", "")) for item in learned_artifacts)),
            "stale": sum(1 for item in learned_artifacts if item.get("stale")),
        },
    }


def _ranking_summary(investigations: list[dict[str, Any]]) -> dict[str, Any]:
    archetype_counts: Counter[str] = Counter()
    top_archetype_counts: Counter[str] = Counter()
    confidence_buckets: Counter[str] = Counter()
    selected_metric_counts: list[int] = []
    for inv in investigations:
        archetypes = inv.get("archetypes", []) or []
        if archetypes:
            first = archetypes[0]
            top_archetype_counts[str(first.get("type", ""))] += 1
            confidence_buckets[_confidence_bucket(float(first.get("confidence", 0) or 0))] += 1
        for archetype in archetypes:
            archetype_counts[str(archetype.get("type", ""))] += 1
        selected_metric_counts.append(len(inv.get("metrics_selected", []) or []))
    return {
        "path_counts": dict(Counter(str(inv.get("path_used", "")) for inv in investigations)),
        "top_archetype_counts": dict(top_archetype_counts),
        "all_archetype_counts": dict(archetype_counts),
        "top_archetype_confidence_buckets": dict(confidence_buckets),
        "datasource_type_counts": dict(
            Counter(
                str(ds)
                for inv in investigations
                for ds in (inv.get("datasource_types", []) or [])
            )
        ),
        "metrics_catalog_size": _numeric_summary([inv.get("metrics_catalog_size", 0) for inv in investigations]),
        "metrics_ranked_size": _numeric_summary([inv.get("metrics_ranked_size", 0) for inv in investigations]),
        "metrics_selected_count": _numeric_summary(selected_metric_counts),
        "panel_count": _numeric_summary([inv.get("panel_count", 0) for inv in investigations]),
        "panels_dropped": _numeric_summary([inv.get("panels_dropped", 0) for inv in investigations]),
    }


def _robustness_summary(
    investigations: list[dict[str, Any]],
    feedback_analysis: dict[str, Any],
) -> dict[str, Any]:
    stage_status: Counter[str] = Counter()
    reason_codes: Counter[str] = Counter()
    errors: Counter[str] = Counter()
    for inv in investigations:
        for stage, outcome in (inv.get("stage_outcomes", {}) or {}).items():
            if isinstance(outcome, dict):
                stage_status[f"{stage}:{outcome.get('status', '')}"] += 1
                reason = outcome.get("reason_code", "")
                if reason:
                    reason_codes[str(reason)] += 1
        if inv.get("error"):
            errors[_error_kind(str(inv.get("error", "")))] += 1
    return {
        "stage_status_counts": dict(stage_status),
        "reason_code_counts": dict(reason_codes),
        "error_kind_counts": dict(errors),
        "validation_warning_counts": _validation_warning_counts(investigations),
        "feedback_recommendation_count": len(feedback_analysis.get("recommendations", []) or []),
        "feedback_analysis_status": feedback_analysis.get("status", "available"),
    }


def _raw_local_details(
    *,
    investigations: list[dict[str, Any]],
    dashboards: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    learned_artifacts: list[dict[str, Any]],
    feedback_analysis: dict[str, Any],
) -> dict[str, Any]:
    return {
        "note": "Raw local details are intended for the local user only. Use anonymous exports for sharing.",
        "recent_investigations": investigations,
        "ingested_dashboards": dashboards,
        "ingested_alerts": alerts,
        "learned_artifacts": learned_artifacts,
        "feedback_analysis": feedback_analysis,
    }


def _anonymous_projection(report: AssessmentReport) -> AssessmentReport:
    return {
        key: value
        for key, value in report.items()
        if key in {
            "metadata",
            "assessment_summary",
            "knowledge_coverage",
            "artifact_stats",
            "ranking_summary",
            "robustness_summary",
            "warnings",
        }
    }


def _write_report_files(root: Path, report: AssessmentReport, *, anonymous: bool) -> tuple[str, ...]:
    readme = _anonymous_readme() if anonymous else _raw_readme()
    (root / "README.txt").write_text(readme, encoding="utf-8")

    section_files = list(ANONYMOUS_BUNDLE_FILES[1:])
    for filename in section_files:
        key = filename.removesuffix(".json")
        _write_json(root / filename, report.get(key, {}))

    files = ["README.txt", *section_files]
    if not anonymous and "raw_local_details" in report:
        _write_json(root / "raw_local_details.json", report["raw_local_details"])
        files.append("raw_local_details.json")
    return tuple(files)


def _anonymous_readme() -> str:
    return (
        "Tacit anonymous assessment export\n\n"
        "This bundle is designed for sharing with Tacit maintainers or evaluators.\n"
        "It contains aggregate counts, review-state summaries, ranking diagnostics,\n"
        "failure summaries, and leakage-validation results.\n\n"
        "It intentionally excludes raw dashboards, raw runbooks, raw incidents,\n"
        "raw alert bodies, raw telemetry, raw logs, secret values, and the\n"
        "anonymization mapping table.\n"
    )


def _raw_readme() -> str:
    return (
        "Tacit local assessment export\n\n"
        "This bundle may contain raw local prompts, comments, URLs, dashboard IDs,\n"
        "and other operational details. It is intended for the local user only.\n"
        "Use tacit export-report with anonymous mode for shareable feedback.\n"
    )


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _safe_numbers(value: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, int | float | bool) or item is None:
            out[key] = item
        elif isinstance(item, dict):
            out[key] = _safe_numbers(item)
    return out


def _numeric_summary(values: list[Any]) -> dict[str, Any]:
    nums = [float(v or 0) for v in values if isinstance(v, int | float) or str(v or "").replace(".", "", 1).isdigit()]
    if not nums:
        return {"count": 0, "min": None, "max": None, "avg": None}
    return {
        "count": len(nums),
        "min": min(nums),
        "max": max(nums),
        "avg": round(sum(nums) / len(nums), 3),
    }


def _validation_warning_counts(investigations: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for inv in investigations:
        for warning in inv.get("validation_warnings", []) or []:
            counts[_warning_kind(str(warning))] += 1
    return dict(counts)


def _warning_kind(warning: str) -> str:
    lower = warning.lower()
    if "invalid syntax" in lower:
        return "invalid_syntax"
    if "no series" in lower:
        return "no_series"
    if "not in catalog" in lower:
        return "metric_not_in_catalog"
    if "datasource" in lower:
        return "datasource_issue"
    if "all panels" in lower:
        return "empty_dashboard"
    return "other"


def _error_kind(error: str) -> str:
    lower = error.lower()
    if "timeout" in lower:
        return "timeout"
    if "auth" in lower or "401" in lower or "403" in lower:
        return "auth"
    if "not found" in lower or "404" in lower:
        return "not_found"
    if "no active backends" in lower:
        return "backend_config"
    if "llm" in lower or "model" in lower:
        return "llm"
    return "other"


def _confidence_bucket(value: float) -> str:
    if value >= 0.8:
        return "high_ge_0.8"
    if value >= 0.5:
        return "medium_ge_0.5"
    if value > 0:
        return "low_gt_0"
    return "none"


def _export_warnings(*, anonymous: bool) -> list[str]:
    if anonymous:
        return [
            "Anonymous export preserves aggregate diagnostics only.",
            "Metric names are excluded from anonymous metric-quality summaries in v1.",
            "The anonymization mapping is not included.",
        ]
    return [
        "Raw local export may include prompts, comments, URLs, dashboard IDs, and operational details.",
        "Use anonymous mode before sharing outside your organization.",
    ]


def _iter_strings(value: Any, path: str = "$"):
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _iter_strings(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _iter_strings(item, f"{path}[{index}]")
    elif isinstance(value, str):
        yield path, value


def _path_is_allowed_for_validation(path: str) -> bool:
    allowed = {
        "$.metadata.generated_at",
        "$.metadata.tacit_version",
        "$.metadata.assessment_version",
    }
    return path in allowed


def _sample(value: str) -> str:
    return value[:4] + "..." if len(value) > 4 else value


def _kind_for_key(key: str) -> str:
    normalized = key.lower()
    if "service" in normalized:
        return "service"
    if "team" in normalized or "owner" in normalized or "reviewer" in normalized or "user" in normalized:
        return "team"
    if "dashboard" in normalized:
        return "dashboard"
    if "alert" in normalized:
        return "alert"
    if "runbook" in normalized:
        return "runbook"
    if "incident" in normalized:
        return "incident"
    if "repo" in normalized or "branch" in normalized:
        return "repo"
    if "cluster" in normalized:
        return "cluster"
    if "namespace" in normalized:
        return "namespace"
    if "url" in normalized:
        return "url"
    if "email" in normalized:
        return "email"
    if "host" in normalized:
        return "hostname"
    if "path" in normalized:
        return "path"
    if "label" in normalized:
        return "label_value"
    return "value"
