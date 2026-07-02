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
EXPORT_ROW_LIMIT = 10_000
EXPORT_PAGE_SIZE = 1_000
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

SAFE_DATA_KEYS: dict[str, set[str]] = {
    "status": {
        "",
        "approved",
        "failed",
        "ignored",
        "pending",
        "rejected",
        "skipped",
        "success",
        "timeout",
    },
    "path": {"", "archetype", "freeform"},
    "signal_category": {
        "",
        "availability",
        "errors",
        "latency",
        "resource",
        "saturation",
        "throughput",
    },
    "mapping_source": {
        "",
        "alert_ingest",
        "artifact_learning",
        "bootstrap",
        "dashboard_ingest",
        "manual",
    },
    "backend": {"", "grafana", "signalfx"},
    "artifact_type": {"", "alert", "dashboard", "incident", "runbook"},
    "datasource_type": {
        "",
        "cloudwatch",
        "cortex",
        "elasticsearch",
        "grafana",
        "graphite",
        "influxdb",
        "loki",
        "mimir",
        "opensearch",
        "prometheus",
        "signalfx",
        "thanos",
    },
    "confidence_bucket": {"high_ge_0.8", "low_gt_0", "medium_ge_0.5", "none"},
    "error_kind": {"auth", "backend_config", "llm", "not_found", "other", "timeout"},
    "reason_code": {
        "",
        "backend_config",
        "datasource_issue",
        "invalid_syntax",
        "metric_not_in_catalog",
        "no_data",
        "no_series",
        "not_implemented",
        "other",
        "timeout",
        "validation_failed",
    },
    "warning_kind": {
        "datasource_issue",
        "empty_dashboard",
        "invalid_syntax",
        "metric_not_in_catalog",
        "no_series",
        "other",
    },
}

DATA_KEY_PATHS: dict[str, str] = {
    "$.assessment_summary.status_counts": "status",
    "$.assessment_summary.path_counts": "path",
    "$.knowledge_coverage.signals_by_category": "signal_category",
    "$.knowledge_coverage.mappings_by_source": "mapping_source",
    "$.knowledge_coverage.dashboard_review_states": "status",
    "$.knowledge_coverage.alert_review_states": "status",
    "$.knowledge_coverage.learned_artifact_types": "artifact_type",
    "$.knowledge_coverage.backend_distribution": "backend",
    "$.artifact_stats.dashboards.by_backend": "backend",
    "$.artifact_stats.dashboards.by_status": "status",
    "$.artifact_stats.alerts.by_backend": "backend",
    "$.artifact_stats.alerts.by_status": "status",
    "$.artifact_stats.learned_artifacts.by_type": "artifact_type",
    "$.ranking_summary.path_counts": "path",
    "$.ranking_summary.top_archetype_counts": "archetype",
    "$.ranking_summary.all_archetype_counts": "archetype",
    "$.ranking_summary.top_archetype_confidence_buckets": "confidence_bucket",
    "$.ranking_summary.datasource_type_counts": "datasource_type",
    "$.robustness_summary.stage_status_counts": "stage_status",
    "$.robustness_summary.reason_code_counts": "reason_code",
    "$.robustness_summary.error_kind_counts": "error_kind",
    "$.robustness_summary.validation_warning_counts": "warning_kind",
    "$.warnings.validation_warnings_by_kind": "warning_kind",
}

SAFE_SCHEMA_KEYS = {
    "alert_review_states",
    "alerts",
    "anonymous",
    "artifact_stats",
    "assessment_summary",
    "assessment_version",
    "avg",
    "backend_distribution",
    "by_backend",
    "by_status",
    "by_type",
    "checks",
    "collection",
    "count",
    "dashboard_review_states",
    "dashboards",
    "emails_included",
    "error_kind_counts",
    "export_warnings",
    "feedback",
    "feedback_analysis_status",
    "feedback_recommendation_count",
    "findings",
    "findings_count",
    "generated_at",
    "graph_preserved",
    "hostnames_included",
    "ingested_alerts",
    "ingested_dashboards",
    "investigations",
    "knowledge_coverage",
    "learned_artifact_types",
    "learned_artifacts",
    "learning",
    "mapping_included",
    "mappings_by_source",
    "max",
    "metadata",
    "min",
    "panel_count",
    "passed",
    "path_counts",
    "ranking_summary",
    "raw_artifacts_included",
    "reason_code_counts",
    "robustness_summary",
    "row_limit",
    "rows_exported",
    "scope",
    "signal_types",
    "signals_by_category",
    "signals_inferred",
    "source_total",
    "stage_status_counts",
    "status_counts",
    "stale",
    "tacit_version",
    "telemetry_included",
    "top_archetype_confidence_buckets",
    "top_archetype_counts",
    "truncated",
    "validation_report",
    "validation_warning_counts",
    "validation_warnings_by_kind",
    "warnings",
}


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
        return self._walk(report, "$")

    def anonymize_value(self, value: str, kind: str) -> str:
        key = (kind, value)
        if key not in self._mapping:
            self._counters[kind] += 1
            self._mapping[key] = f"{kind}_{self._counters[kind]:03d}"
        return self._mapping[key]

    def _walk(self, value: Any, path: str) -> Any:
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            key_kind = DATA_KEY_PATHS.get(path)
            for key, item in value.items():
                safe_key = self._sanitize_key(str(key), key_kind)
                if isinstance(item, str):
                    kind = _kind_for_key(safe_key)
                    out[safe_key] = self._sanitize_string(item, kind)
                else:
                    out[safe_key] = self._walk(item, f"{path}.{safe_key}")
            return out
        if isinstance(value, list):
            return [self._walk(item, f"{path}[]") for item in value]
        if isinstance(value, str):
            return self._sanitize_string(value, "value")
        return value

    def _sanitize_key(self, value: str, kind: str | None) -> str:
        if kind is None:
            if _schema_key_is_safe(value):
                return value
            return self.anonymize_value(value, _key_alias_kind(value))
        if _safe_data_key(value, kind):
            return value
        if kind == "stage_status":
            stage, _sep, status = value.partition(":")
            safe_stage = stage if stage in _safe_stage_names() else self.anonymize_value(stage, "stage")
            safe_status = status if _safe_data_key(status, "status") else self.anonymize_value(status, "status")
            return f"{safe_stage}:{safe_status}" if status else safe_stage
        return self.anonymize_value(value, kind)

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

    investigations = _collect_recent_investigations(history_store)
    history_stats = history_store.stats()
    feedback_stats = feedback_store.get_aggregate_stats()
    feedback_analysis = feedback_store.analyze()
    signal_stats = signal_store.stats()
    dashboards = signal_store.list_ingested_dashboards(limit=EXPORT_ROW_LIMIT)
    alerts = signal_store.list_ingested_alerts(limit=EXPORT_ROW_LIMIT)
    learned_artifacts = signal_store.list_learned_artifacts(limit=EXPORT_ROW_LIMIT)
    collection = _collection_metadata(
        investigations=investigations,
        history_stats=history_stats,
        dashboards=dashboards,
        alerts=alerts,
        learned_artifacts=learned_artifacts,
        signal_stats=signal_stats,
    )

    report: AssessmentReport = {
        "metadata": _metadata(generated_at, anonymous=anonymous, collection=collection),
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
        validation_report = _pending_validation_report()
    else:
        validation_report = _skipped_validation_report()
    report["validation_report"] = validation_report

    output_path = output or default_report_path(anonymous=anonymous)
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="tacit-assessment-") as tmp:
        tmp_path = Path(tmp)
        files = _write_report_files(tmp_path, report, anonymous=anonymous)
        if anonymous:
            validation_report = validate_report_files_for_leakage(tmp_path, files)
            report["validation_report"] = validation_report
            _write_json(tmp_path / "validation_report.json", validation_report)
            validation_report = validate_report_files_for_leakage(tmp_path, files)
            report["validation_report"] = validation_report
            _write_json(tmp_path / "validation_report.json", validation_report)
            if validate and not validation_report["passed"]:
                findings = validation_report["findings_count"]
                raise ValueError(f"Anonymous report failed leakage validation with {findings} finding(s)")
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
    for path, value, location in _iter_strings(report):
        if _path_is_allowed_for_validation(path):
            continue
        for kind, pattern in _LEAKAGE_PATTERNS:
            match = pattern.search(value)
            if match:
                findings.append(
                    {
                        "path": path,
                        "location": location,
                        "kind": kind,
                        "sample": _sample(kind),
                    }
                )
    return {
        "passed": not findings,
        "findings_count": len(findings),
        "findings": findings[:100],
        "checks": [kind for kind, _pattern in _LEAKAGE_PATTERNS],
    }


def validate_report_files_for_leakage(root: Path, files: tuple[str, ...] | list[str]) -> dict[str, Any]:
    """Scan the exact staged files that will be added to the anonymous archive."""
    findings: list[dict[str, str]] = []
    for filename in files:
        path = root / filename
        text = path.read_text(encoding="utf-8")
        if filename.endswith(".json"):
            try:
                validation = validate_report_for_leakage(json.loads(text))
            except json.JSONDecodeError:
                findings.append(
                    {
                        "path": filename,
                        "location": "file",
                        "kind": "invalid_json",
                        "sample": "<redacted>",
                    }
                )
                continue
            for finding in validation["findings"]:
                finding = dict(finding)
                finding["path"] = f"{filename}:{finding['path']}"
                findings.append(finding)
        else:
            findings.extend(_scan_text(text, path=filename, location="file"))
    return {
        "passed": not findings,
        "findings_count": len(findings),
        "findings": findings[:100],
        "checks": [kind for kind, _pattern in _LEAKAGE_PATTERNS],
        "scope": "staged_archive_files",
    }


def _metadata(generated_at: str, *, anonymous: bool, collection: dict[str, Any]) -> dict[str, Any]:
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
        "collection": collection,
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


def _collect_recent_investigations(history_store: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while len(out) < EXPORT_ROW_LIMIT:
        limit = min(EXPORT_PAGE_SIZE, EXPORT_ROW_LIMIT - len(out))
        page = history_store.list_recent(limit=limit, offset=offset)
        if not page:
            break
        out.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
    return out


def _collection_metadata(
    *,
    investigations: list[dict[str, Any]],
    history_stats: dict[str, Any],
    dashboards: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    learned_artifacts: list[dict[str, Any]],
    signal_stats: dict[str, Any],
) -> dict[str, Any]:
    return {
        "investigations": _collection_entry(
            rows_exported=len(investigations),
            source_total=history_stats.get("total"),
            row_limit=None,
        ),
        "ingested_dashboards": _collection_entry(
            rows_exported=len(dashboards),
            source_total=signal_stats.get("ingested_dashboards"),
            row_limit=EXPORT_ROW_LIMIT,
        ),
        "ingested_alerts": _collection_entry(
            rows_exported=len(alerts),
            source_total=signal_stats.get("ingested_alerts"),
            row_limit=EXPORT_ROW_LIMIT,
        ),
        "learned_artifacts": _collection_entry(
            rows_exported=len(learned_artifacts),
            source_total=signal_stats.get("learned_artifacts"),
            row_limit=EXPORT_ROW_LIMIT,
        ),
    }


def _collection_entry(*, rows_exported: int, source_total: Any, row_limit: int | None) -> dict[str, Any]:
    total = int(source_total or rows_exported)
    return {
        "rows_exported": rows_exported,
        "source_total": total,
        "row_limit": row_limit,
        "truncated": rows_exported < total,
    }


def _pending_validation_report() -> dict[str, Any]:
    return {
        "passed": False,
        "findings_count": 0,
        "findings": [],
        "checks": [kind for kind, _pattern in _LEAKAGE_PATTERNS],
        "scope": "pending_staged_archive_files",
    }


def _skipped_validation_report() -> dict[str, Any]:
    return {
        "passed": None,
        "skipped": True,
        "reason": "raw_local_export",
        "findings_count": 0,
        "findings": [],
        "checks": [],
        "scope": "not_applicable",
    }


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
            yield f"{path}.<key>", str(key), "key"
            yield from _iter_strings(item, f"{path}.{key}" if _schema_key_is_safe(str(key)) else f"{path}.<key>")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _iter_strings(item, f"{path}[{index}]")
    elif isinstance(value, str):
        yield path, value, "value"


def _path_is_allowed_for_validation(path: str) -> bool:
    allowed = {
        "$.metadata.generated_at",
        "$.metadata.tacit_version",
        "$.metadata.assessment_version",
    }
    return path in allowed


def _scan_text(text: str, *, path: str, location: str) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for kind, pattern in _LEAKAGE_PATTERNS:
        if pattern.search(text):
            findings.append(
                {
                    "path": path,
                    "location": location,
                    "kind": kind,
                    "sample": _sample(kind),
                }
            )
    return findings


def _sample(kind: str) -> str:
    return f"<redacted_{kind}>"


def _schema_key_is_safe(key: str) -> bool:
    return key in SAFE_SCHEMA_KEYS


def _safe_data_key(value: str, kind: str) -> bool:
    normalized = value.lower()
    if normalized != value:
        return False
    safe_values = SAFE_DATA_KEYS.get(kind)
    if safe_values is not None:
        return normalized in safe_values
    if kind == "archetype":
        return bool(re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value)) and value in _safe_archetype_names()
    return False


def _contains_sensitive_shape(value: str) -> bool:
    return any(pattern.search(value) for _kind, pattern in _LEAKAGE_PATTERNS)


def _safe_stage_names() -> set[str]:
    return {
        "",
        "archetypes",
        "completion",
        "context",
        "discovery",
        "evidence",
        "freeform",
        "intent",
        "publish",
        "ranking",
        "validation",
    }


def _safe_archetype_names() -> set[str]:
    return {
        "database_slowdown",
        "deployment_regression",
        "downstream_outage",
        "error_spike",
        "general",
        "golden_signals",
        "latency_investigation",
        "network_instability",
        "pod_instability",
        "queue_lag",
        "resource_saturation",
    }


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


def _key_alias_kind(key: str) -> str:
    kind = _kind_for_key(key)
    return "key" if kind == "value" else kind
