"""Learning-index row construction and query helpers."""

from __future__ import annotations

import re
import time
from typing import Any

TOKEN_RE = re.compile(r"[a-zA-Z0-9_./:-]+")
LABEL_SERVICE_RE = re.compile(
    r"(?:service|service_name|app|application|component|container|job|pod)\s*=~?\s*['\"]([^'\"]+)['\"]"
)
GENERIC_METRIC_PREFIXES = {
    "http",
    "grpc",
    "rpc",
    "cpu",
    "memory",
    "mem",
    "redis",
    "kafka",
    "kube",
    "kubernetes",
    "request",
    "requests",
    "response",
    "container",
    "node",
    "process",
    "system",
    "jvm",
    "go",
    "python",
    "prometheus",
    "up",
}


def panel_metrics(panel: dict[str, Any], fallback_metrics: list[str]) -> list[str]:
    """Return explicit panel metrics or infer them from query text."""
    raw = panel.get("metrics", [])
    metrics: list[str] = []
    if isinstance(raw, list):
        metrics.extend(str(m) for m in raw if m)
    elif raw:
        metrics.append(str(raw))

    if metrics:
        return list(dict.fromkeys(metrics))

    queries = panel.get("queries", [])
    if not isinstance(queries, list):
        queries = [queries]
    query_text = "\n".join(str(q) for q in queries if q)
    if not query_text:
        return []
    return [metric for metric in fallback_metrics if metric and metric in query_text]


def infer_services_for_learning(
    *,
    metric: str,
    query_text: str,
    dashboard_title: str,
    panel_title: str,
    tags: list[str],
) -> list[str]:
    """Infer service tokens for learned dashboard retrieval."""
    candidates: list[str] = []

    def add(value: str) -> None:
        cleaned = value.strip().strip("*").strip()
        cleaned = re.sub(r"^\.\*", "", cleaned)
        cleaned = re.sub(r"\.\*$", "", cleaned)
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)

    for tag in tags:
        if ":" in tag:
            key, value = tag.split(":", 1)
            if key.lower() in {"service", "app", "application", "component", "team", "job", "pod"}:
                add(value)

    for match in LABEL_SERVICE_RE.findall(query_text):
        add(match)

    first = re.split(r"[_.:-]+", metric, maxsplit=1)[0].lower() if metric else ""
    if first and first not in GENERIC_METRIC_PREFIXES and len(first) > 2:
        add(first)

    text = f"{dashboard_title} {panel_title}".lower()
    for suffix in ("service", "api", "worker", "gateway"):
        for match in re.findall(rf"\b([a-z0-9][a-z0-9_-]+)[\s_-]+{suffix}\b", text):
            if match not in GENERIC_METRIC_PREFIXES:
                add(match)

    return candidates


def fts_query(text: str) -> str:
    """Build a conservative FTS5 OR query from free text."""
    terms = []
    for token in TOKEN_RE.findall(text.lower()):
        token = token.strip("-_.:/")
        if len(token) < 2:
            continue
        escaped = token.replace('"', '""')
        terms.append(f'"{escaped}"')
    return " OR ".join(dict.fromkeys(terms))


def learning_row_review_state(
    *,
    status: str,
    metric: str,
    signal_type: str,
    sig: dict[str, Any],
    activated_pairs: set[tuple[str, str]] | None = None,
) -> str:
    """Return the retrieval-index review state for one learned metric row."""
    if status in {"rejected", "ignored"}:
        return status
    if status != "approved":
        return "candidate"
    if activated_pairs is not None:
        return "approved" if (metric, signal_type) in activated_pairs else "candidate"
    if not metric or not signal_type:
        return "candidate"
    if sig.get("source") == "heuristic":
        return "approved" if sig.get("auto_teach_eligible") else "candidate"
    return "trusted" if sig.get("confidence", 0.0) >= 0.5 else "candidate"


def eligible_pairs_from_ingested_signals(signals: list[Any]) -> set[tuple[str, str]]:
    """Return metric/signal pairs that approval should activate."""
    pairs: set[tuple[str, str]] = set()
    for sig in signals:
        if not isinstance(sig, dict):
            continue
        metric = sig.get("metric", "")
        signal_type = sig.get("signal_type", "")
        if not metric or not signal_type:
            continue
        if sig.get("source") == "heuristic":
            if sig.get("auto_teach_eligible"):
                pairs.add((metric, signal_type))
        elif sig.get("confidence", 0.0) >= 0.5:
            pairs.add((metric, signal_type))
    return pairs


def build_learning_context_rows(
    *,
    dashboard_uid: str,
    backend_name: str,
    dashboard_title: str,
    dashboard_tags: list[str],
    panels: list[dict[str, Any]],
    metrics_found: list[str],
    signals_inferred: list[dict[str, Any]] | list[str],
    status: str,
    activated_pairs: set[tuple[str, str]] | None,
) -> list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str, float]]:
    """Build FTS rows for learned dashboard context."""
    tags = dashboard_tags or []
    metrics = list(dict.fromkeys(metrics_found or []))
    signal_by_metric: dict[str, list[dict[str, Any]]] = {}
    for sig in signals_inferred or []:
        if not isinstance(sig, dict):
            continue
        metric = sig.get("metric", "")
        if metric:
            signal_by_metric.setdefault(metric, []).append(sig)

    rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str, float]] = []
    indexed_at = time.time()
    panel_items = panels or []
    if not panel_items and metrics:
        panel_items = [{"title": "", "queries": [], "metrics": metrics}]

    for panel_index, panel in enumerate(panel_items):
        panel_title = str(panel.get("title", "") or "")
        query_values = panel.get("queries", [])
        if not isinstance(query_values, list):
            query_values = [str(query_values)]
        query_text = "\n".join(str(q) for q in query_values if q)
        metrics_for_panel = panel_metrics(panel, metrics)
        if not metrics_for_panel and metrics:
            metrics_for_panel = metrics

        for metric in metrics_for_panel:
            related_signals = signal_by_metric.get(metric) or [{}]
            for sig_index, sig in enumerate(related_signals):
                if not isinstance(sig, dict):
                    sig = {}
                services = infer_services_for_learning(
                    metric=metric,
                    query_text=query_text,
                    dashboard_title=dashboard_title,
                    panel_title=panel_title,
                    tags=tags,
                )
                signal_type = str(sig.get("signal_type", ""))
                review_state = learning_row_review_state(
                    status=status,
                    metric=metric,
                    signal_type=signal_type,
                    sig=sig,
                    activated_pairs=activated_pairs,
                )
                reason = str(sig.get("reason", ""))
                provenance = " ".join(
                    part
                    for part in (
                        f"source:{sig.get('source', '')}" if sig.get("source") else "",
                        f"family:{sig.get('signal_family', '')}" if sig.get("signal_family") else "",
                        f"confidence:{sig.get('confidence', '')}" if sig.get("confidence") else "",
                    )
                    if part
                )
                rows.append(
                    (
                        "dashboard_panel",
                        f"{backend_name}:{dashboard_uid}:{panel_index}:{metric}:{sig_index}",
                        backend_name,
                        dashboard_uid,
                        dashboard_title,
                        " ".join(tags),
                        panel_title,
                        metric,
                        query_text,
                        " ".join(services),
                        signal_type,
                        review_state,
                        reason,
                        provenance,
                        indexed_at,
                    )
                )
    return rows


def build_alert_context_rows(
    *,
    alert_uid: str,
    backend_name: str,
    alert_title: str,
    alert_tags: list[str],
    condition: str,
    metrics_found: list[str],
    query_transformations: list[str],
    service_hints: list[str],
    signals_inferred: list[dict[str, Any]] | list[str],
    status: str,
    activated_pairs: set[tuple[str, str]] | None,
) -> list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str, float]]:
    """Build FTS rows for learned alert-rule context."""
    alert_context_id = f"alert:{alert_uid}"
    tags = alert_tags or []
    metrics = list(dict.fromkeys(metrics_found or []))
    query_text = "\n".join(str(q) for q in query_transformations or [] if q)
    signal_by_metric: dict[str, list[dict[str, Any]]] = {}
    for sig in signals_inferred or []:
        if not isinstance(sig, dict):
            continue
        metric = sig.get("metric", "")
        if metric:
            signal_by_metric.setdefault(metric, []).append(sig)

    rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str, float]] = []
    indexed_at = time.time()
    for metric in metrics:
        related_signals = signal_by_metric.get(metric) or [{}]
        inferred_services = infer_services_for_learning(
            metric=metric,
            query_text=query_text,
            dashboard_title=alert_title,
            panel_title=condition,
            tags=tags,
        )
        services = " ".join(dict.fromkeys([*service_hints, *inferred_services]))
        for sig_index, sig in enumerate(related_signals):
            if not isinstance(sig, dict):
                sig = {}
            signal_type = str(sig.get("signal_type", ""))
            review_state = learning_row_review_state(
                status=status,
                metric=metric,
                signal_type=signal_type,
                sig=sig,
                activated_pairs=activated_pairs,
            )
            reason = str(sig.get("reason", ""))
            provenance = " ".join(
                part
                for part in (
                    "source:alert_ingest",
                    f"inference_source:{sig.get('source', '')}" if sig.get("source") else "",
                    f"family:{sig.get('signal_family', '')}" if sig.get("signal_family") else "",
                    f"confidence:{sig.get('confidence', '')}" if sig.get("confidence") else "",
                )
                if part
            )
            rows.append(
                (
                    "alert_rule",
                    f"{backend_name}:alert:{alert_uid}:{metric}:{sig_index}",
                    backend_name,
                    alert_context_id,
                    alert_title,
                    " ".join(tags),
                    condition,
                    metric,
                    query_text,
                    services,
                    signal_type,
                    review_state,
                    reason,
                    provenance,
                    indexed_at,
                )
            )
    return rows
