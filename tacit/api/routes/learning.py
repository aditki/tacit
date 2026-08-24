"""Dashboard learning routes."""

from __future__ import annotations

import hashlib
import inspect
from contextlib import nullcontext
from typing import Any, Protocol

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi import Path as PathParam

import tacit.signals as signals_mod
from tacit.api.dependencies import get_runtime_stores, get_signal_store, get_signal_store_factory
from tacit.api.security import (
    KnowledgeAction,
    assert_knowledge_action,
    knowledge_tenant,
    require_knowledge_action,
    require_knowledge_tenant,
    verify_api_key,
)
from tacit.errors import SemanticAuthorizationError
from tacit.models.schemas import (
    LearnAlertRequest,
    LearnDashboardRequest,
    LearnDashboardUploadRequest,
    LearnIncidentRequest,
    LearnRunbookRequest,
)
from tacit.pagination import MAX_COMPATIBILITY_OFFSET
from tacit.runtime_stores import RuntimeStores
from tacit.signals.store import ArtifactGenerationConflictError

logger = structlog.get_logger()
router = APIRouter(dependencies=[Depends(verify_api_key), Depends(require_knowledge_tenant)])


def _diagnostic_fingerprint(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:16]


def _log_learning_failure(event: str, exc: BaseException, **references: object) -> None:
    logger.error(
        event,
        reason_code=event,
        exception_class=type(exc).__name__[:64],
        error_fingerprint=_diagnostic_fingerprint(exc),
        **{f"{name}_fingerprint": _diagnostic_fingerprint(value) for name, value in references.items()},
    )


def _redact_bulk_learning_failures(result: Any, *, reason_code: str) -> Any:
    """Return only classified diagnostics for bulk failures."""
    if not isinstance(result, dict) or not isinstance(result.get("failures"), list):
        return result
    redacted = dict(result)
    failures: list[dict[str, str]] = []
    for value in result["failures"]:
        failure = {
            "error": "Learning item failed.",
            "reason_code": reason_code,
            "error_fingerprint": _diagnostic_fingerprint(value),
        }
        if isinstance(value, dict):
            source_identity = value.get("dashboard_uid") or value.get("alert_uid") or value.get("title")
            if source_identity:
                failure["item_fingerprint"] = _diagnostic_fingerprint(source_identity)
        failures.append(failure)
    redacted["failures"] = failures
    return redacted


class _ArtifactPayload(Protocol):
    title: str
    body_text: str
    external_id: str
    source_vendor: str
    source_instance: str
    provenance_url: str


def _authorize_signal_approval(request: Request, enabled: bool) -> None:
    if not enabled:
        return
    assert_knowledge_action(request, KnowledgeAction.TEACH_SIGNALS)


def _authorize_learning_read(request: Request) -> None:
    assert_knowledge_action(request, KnowledgeAction.READ)


def _authorize_learning_mutation(request: Request, enabled: bool = True) -> None:
    if enabled:
        assert_knowledge_action(request, KnowledgeAction.APPLY)


def _authorize_artifact_learning(request: Request, *, dry_run: bool) -> None:
    action = KnowledgeAction.READ if dry_run else KnowledgeAction.LEARN_ARTIFACTS
    assert_knowledge_action(request, action)


def _artifact_external_id(payload: _ArtifactPayload, artifact_type: str) -> str:
    if payload.external_id:
        return payload.external_id
    if payload.provenance_url:
        return payload.provenance_url
    source_vendor = payload.source_vendor or "api"
    source_instance = payload.source_instance or ""
    body_hash = hashlib.sha256(payload.body_text.encode()).hexdigest()[:16]
    return f"{artifact_type}:{source_vendor}:{source_instance}:{payload.title}:{body_hash}"


def _supported_kwargs(callable_obj: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Keep route adapters compatible with older integrations and test doubles."""
    try:
        parameters = inspect.signature(callable_obj).parameters
    except (TypeError, ValueError):
        return kwargs
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in parameters}


async def _call_ingest_dashboard(ingest_dashboard: Any, **kwargs: Any) -> Any:
    return await ingest_dashboard(**_supported_kwargs(ingest_dashboard, kwargs))


async def _call_learn_backend_dashboards(learn_backend_dashboards: Any, **kwargs: Any) -> Any:
    return await learn_backend_dashboards(**_supported_kwargs(learn_backend_dashboards, kwargs))


async def _call_ingest_alert(ingest_alert: Any, **kwargs: Any) -> Any:
    return await ingest_alert(**_supported_kwargs(ingest_alert, kwargs))


async def _call_learn_backend_alerts(learn_backend_alerts: Any, **kwargs: Any) -> Any:
    return await learn_backend_alerts(**_supported_kwargs(learn_backend_alerts, kwargs))


@router.post(
    "/api/v1/learn/dashboard",
    tags=["Learning"],
    summary="Learn from an existing Grafana dashboard",
    response_description="Extracted features, inferred signals, and optional quarantined archetype YAML",
)
async def learn_from_dashboard(
    request: Request,
    payload: LearnDashboardRequest,
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Ingest an existing dashboard to learn operational patterns."""
    from tacit.config import settings
    from tacit.dashboard_ingest import DashboardReviewConflictError, ingest_dashboard

    _authorize_learning_read(request)
    _authorize_signal_approval(request, payload.auto_approve)
    _authorize_learning_mutation(request)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory()
        return await _call_ingest_dashboard(
            ingest_dashboard,
            dashboard_uid=payload.dashboard_uid,
            backend_name=payload.backend,
            auto_approve=payload.auto_approve,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
            tenant_id=tenant_id,
        )
    except DashboardReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure(
            "dashboard_ingest_failed",
            exc,
            source=payload.dashboard_uid,
            backend=payload.backend,
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to ingest dashboard. Check that the source exists and the backend is accessible.",
        )


@router.post(
    "/api/v1/learn/alerts",
    tags=["Learning"],
    summary="Learn from an existing alert rule",
    response_description="Extracted alert features and inferred signals",
)
async def learn_from_alert(
    request: Request,
    payload: LearnAlertRequest,
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Ingest an existing alert rule/detector to learn operational patterns."""
    from tacit.alert_ingest import ingest_alert
    from tacit.config import settings

    _authorize_learning_read(request)
    _authorize_signal_approval(request, payload.auto_approve and not payload.dry_run)
    _authorize_learning_mutation(request, not payload.dry_run)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory() if not payload.dry_run else None
        return await _call_ingest_alert(
            ingest_alert,
            alert_uid=payload.alert_uid,
            backend_name=payload.backend,
            auto_approve=payload.auto_approve,
            dry_run=payload.dry_run,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
            tenant_id=tenant_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure(
            "alert_ingest_failed",
            exc,
            source=payload.alert_uid,
            backend=payload.backend,
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to ingest alert. Check that the source exists and the backend is accessible.",
        )


@router.post(
    "/api/v1/learn/runbooks",
    tags=["Learning"],
    summary="Learn from a runbook artifact",
    response_description="Extracted operational IR candidates with provenance",
)
async def learn_from_runbook(
    request: Request,
    payload: LearnRunbookRequest,
    stores: RuntimeStores = Depends(get_runtime_stores),
):
    """Learn operational candidates from a markdown/plain-text runbook."""
    from tacit.artifact_learning import RunbookExtractor, artifact_from_text, learn_artifact

    _authorize_artifact_learning(request, dry_run=payload.dry_run)
    tenant_id = knowledge_tenant(request)
    try:
        store = None if payload.dry_run else stores.signals()
        artifact = artifact_from_text(
            artifact_type="runbook",
            title=payload.title,
            body_text=payload.body_text,
            external_id=_artifact_external_id(payload, "runbook"),
            source_vendor=payload.source_vendor,
            source_instance=payload.source_instance,
            provenance_url=payload.provenance_url,
        )
        return learn_artifact(
            artifact,
            RunbookExtractor(),
            dry_run=payload.dry_run,
            runtime_settings=stores.settings,
            store=store,
            tenant_id=tenant_id,
        )
    except SemanticAuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure("runbook_artifact_learning_failed", exc, artifact=payload.title)
        raise HTTPException(status_code=500, detail="Failed to learn from runbook artifact.")


@router.post(
    "/api/v1/learn/incidents",
    tags=["Learning"],
    summary="Learn from an incident-history artifact",
    response_description="Extracted operational IR candidates with provenance",
)
async def learn_from_incident(
    request: Request,
    payload: LearnIncidentRequest,
    stores: RuntimeStores = Depends(get_runtime_stores),
):
    """Learn operational candidates from an incident-history record."""
    from tacit.artifact_learning import IncidentExtractor, artifact_from_text, learn_artifact

    _authorize_artifact_learning(request, dry_run=payload.dry_run)
    tenant_id = knowledge_tenant(request)
    try:
        store = None if payload.dry_run else stores.signals()
        artifact = artifact_from_text(
            artifact_type="incident",
            title=payload.title,
            body_text=payload.body_text,
            external_id=_artifact_external_id(payload, "incident"),
            source_vendor=payload.source_vendor,
            source_instance=payload.source_instance,
            provenance_url=payload.provenance_url,
        )
        return learn_artifact(
            artifact,
            IncidentExtractor(),
            dry_run=payload.dry_run,
            runtime_settings=stores.settings,
            store=store,
            tenant_id=tenant_id,
        )
    except SemanticAuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure("incident_artifact_learning_failed", exc, artifact=payload.title)
        raise HTTPException(status_code=500, detail="Failed to learn from incident artifact.")


@router.post(
    "/api/v1/learn/dashboard/json",
    tags=["Learning"],
    summary="Learn from uploaded dashboard JSON",
    response_description="Extracted features, inferred signals, and optional quarantined archetype YAML",
)
async def learn_from_dashboard_json(
    request: Request,
    payload: LearnDashboardUploadRequest,
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Ingest an uploaded dashboard JSON export without contacting the vendor."""
    from tacit.dashboard_ingest import DashboardReviewConflictError, ingest_dashboard_features
    from tacit.dashboard_uploads import parse_uploaded_dashboard

    _authorize_learning_read(request)
    _authorize_signal_approval(request, payload.auto_approve)
    _authorize_learning_mutation(request)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory()
        features = parse_uploaded_dashboard(
            payload.dashboard,
            vendor=payload.vendor,
            source_name=payload.source_name,
        )
        from tacit.config import settings

        return await ingest_dashboard_features(
            features,
            auto_approve=payload.auto_approve,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
            tenant_id=tenant_id,
        )
    except DashboardReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure(
            "dashboard_json_ingest_failed",
            exc,
            vendor=payload.vendor,
            source=payload.source_name,
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to ingest uploaded dashboard JSON. Check that the file is a supported dashboard export.",
        )


@router.post(
    "/api/v1/learn/{backend_name}",
    tags=["Learning"],
    summary="Crawl and learn from all dashboards in a backend",
    response_description="Bulk dashboard learning summary",
)
async def learn_backend(
    request: Request,
    backend_name: str = PathParam(description="Backend name: grafana or signalfx"),
    auto_approve: bool = Query(
        False,
        description="Request automated review for eligible signal mappings only; "
        "generated archetypes remain quarantined",
    ),
    limit: int = Query(500, ge=1, le=5000, description="Maximum dashboards to crawl"),
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Crawl a connected backend and persist learned dashboard context."""
    from tacit.config import settings
    from tacit.dashboard_ingest import DashboardReviewConflictError, learn_backend_dashboards

    _authorize_learning_read(request)
    _authorize_signal_approval(request, auto_approve)
    _authorize_learning_mutation(request)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory()
        result = await _call_learn_backend_dashboards(
            learn_backend_dashboards,
            backend_name=backend_name,
            auto_approve=auto_approve,
            limit=limit,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
            tenant_id=tenant_id,
        )
        return _redact_bulk_learning_failures(result, reason_code="dashboard_learning_item_failed")
    except DashboardReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure("backend_learning_failed", exc, backend=backend_name)
        raise HTTPException(
            status_code=500,
            detail="Failed to learn dashboards from the backend. Check backend connectivity.",
        )


@router.post(
    "/api/v1/learn/backends/{backend_name}/alerts",
    tags=["Learning"],
    summary="Crawl and learn from all alerts in a backend",
    response_description="Bulk alert learning summary",
)
async def learn_backend_alert_rules(
    request: Request,
    backend_name: str = PathParam(description="Backend name: grafana or signalfx"),
    auto_approve: bool = Query(
        False,
        description="Request automated review for eligible signal mappings only; "
        "generated archetypes remain quarantined",
    ),
    dry_run: bool = Query(False, description="Preview alert ingestion without persisting learned context"),
    limit: int = Query(500, ge=1, le=5000, description="Maximum alerts to crawl"),
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Crawl a connected backend and persist learned alert context."""
    from tacit.alert_ingest import learn_backend_alerts
    from tacit.config import settings

    _authorize_learning_read(request)
    _authorize_signal_approval(request, auto_approve and not dry_run)
    _authorize_learning_mutation(request, not dry_run)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory() if not dry_run else None
        result = await _call_learn_backend_alerts(
            learn_backend_alerts,
            backend_name=backend_name,
            auto_approve=auto_approve,
            dry_run=dry_run,
            limit=limit,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
            tenant_id=tenant_id,
        )
        return _redact_bulk_learning_failures(result, reason_code="alert_learning_item_failed")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        _log_learning_failure("backend_alert_learning_failed", exc, backend=backend_name)
        raise HTTPException(
            status_code=500,
            detail="Failed to learn alerts from the backend. Check backend connectivity.",
        )


@router.get(
    "/api/v1/learn/dashboards",
    tags=["Learning"],
    summary="List ingested dashboards",
    response_description="Ingested dashboards with extracted features and status",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def list_ingested_dashboards(
    request: Request,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    before_created_at: float | None = Query(default=None),
    before_id: int | None = Query(default=None),
    store: Any = Depends(get_signal_store),
):
    """List dashboards that have been ingested for learning."""
    from tacit.dashboard_ingest import build_learning_impact_report, build_signal_quality_report

    assert_knowledge_action(request, KnowledgeAction.READ)
    try:
        dashboards = store.list_ingested_dashboards(
            status=status,
            limit=limit,
            tenant_id=knowledge_tenant(request),
            before_created_at=before_created_at,
            before_id=before_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for dashboard in dashboards:
        metrics = dashboard.get("metrics_found", [])
        signals = dashboard.get("signals_inferred", [])
        if isinstance(metrics, list) and isinstance(signals, list):
            dashboard["signal_quality"] = build_signal_quality_report(metrics=metrics, signals=signals)
            dashboard["learning_impact"] = build_learning_impact_report(
                metrics=metrics,
                signals=signals,
                approved=dashboard.get("status") == "approved",
            )
    next_cursor = None
    if len(dashboards) == limit:
        next_cursor = {
            "before_created_at": dashboards[-1]["created_at"],
            "before_id": dashboards[-1]["id"],
        }
    return {"count": len(dashboards), "dashboards": dashboards, "next_cursor": next_cursor}


@router.get(
    "/api/v1/learn/alerts",
    tags=["Learning"],
    summary="List ingested alerts",
    response_description="Ingested alerts with extracted features and status",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def list_ingested_alerts(
    request: Request,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    before_created_at: float | None = Query(default=None),
    before_id: int | None = Query(default=None),
    store: Any = Depends(get_signal_store),
):
    """List alerts that have been ingested for learning."""
    from tacit.dashboard_ingest import build_learning_impact_report, build_signal_quality_report

    assert_knowledge_action(request, KnowledgeAction.READ)
    try:
        alerts = store.list_ingested_alerts(
            status=status,
            limit=limit,
            tenant_id=knowledge_tenant(request),
            before_created_at=before_created_at,
            before_id=before_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for alert in alerts:
        metrics = alert.get("metrics_found", [])
        signals = alert.get("signals_inferred", [])
        if isinstance(metrics, list) and isinstance(signals, list):
            alert["signal_quality"] = build_signal_quality_report(metrics=metrics, signals=signals)
            alert["learning_impact"] = build_learning_impact_report(
                metrics=metrics,
                signals=signals,
                approved=alert.get("status") == "approved",
            )
    next_cursor = None
    if len(alerts) == limit:
        next_cursor = {
            "before_created_at": alerts[-1]["created_at"],
            "before_id": alerts[-1]["id"],
        }
    return {"count": len(alerts), "alerts": alerts, "next_cursor": next_cursor}


@router.get(
    "/api/v1/learn/runbooks",
    tags=["Learning"],
    summary="List learned runbook artifacts",
    response_description="Learned runbooks and extracted operational IR candidates",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def list_learned_runbooks(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=1024),
    offset: int = Query(default=0, ge=0, le=MAX_COMPATIBILITY_OFFSET),
    store: Any = Depends(get_signal_store),
):
    """List bounded runbook summaries learned by Tacit Artifact Learning v1."""
    assert_knowledge_action(request, KnowledgeAction.READ)
    tenant_id = knowledge_tenant(request)
    read_transaction = getattr(store, "read_transaction", None)
    with read_transaction() if callable(read_transaction) else nullcontext():
        try:
            page = store.list_learned_artifacts_page(
                tenant_id=tenant_id,
                artifact_type="runbook",
                limit=limit,
                cursor=cursor,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        counts = store.artifact_extraction_counts_batch(
            [runbook["artifact_id"] for runbook in page.items],
            tenant_id=tenant_id,
        )
        for runbook in page.items:
            runbook["extraction_counts"] = counts[runbook["artifact_id"]]
    return {
        "count": len(page.items),
        "runbooks": page.items,
        "has_more": page.has_more,
        "next_cursor": page.next_cursor,
    }


@router.get(
    "/api/v1/learn/incidents",
    tags=["Learning"],
    summary="List learned incident artifacts",
    response_description="Learned incidents and extracted operational IR candidates",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def list_learned_incidents(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=1024),
    offset: int = Query(default=0, ge=0, le=MAX_COMPATIBILITY_OFFSET),
    store: Any = Depends(get_signal_store),
):
    """List bounded incident summaries learned by Tacit Artifact Learning v1."""
    assert_knowledge_action(request, KnowledgeAction.READ)
    tenant_id = knowledge_tenant(request)
    read_transaction = getattr(store, "read_transaction", None)
    with read_transaction() if callable(read_transaction) else nullcontext():
        try:
            page = store.list_learned_artifacts_page(
                tenant_id=tenant_id,
                artifact_type="incident",
                limit=limit,
                cursor=cursor,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        counts = store.artifact_extraction_counts_batch(
            [incident["artifact_id"] for incident in page.items],
            tenant_id=tenant_id,
        )
        for incident in page.items:
            incident["extraction_counts"] = counts[incident["artifact_id"]]
    return {
        "count": len(page.items),
        "incidents": page.items,
        "has_more": page.has_more,
        "next_cursor": page.next_cursor,
    }


async def _list_artifact_extraction_page(
    *,
    request: Request,
    artifact_id: str,
    artifact_type: str,
    kind: str,
    limit: int,
    cursor: str | None,
    store: Any,
) -> dict[str, Any]:
    assert_knowledge_action(request, KnowledgeAction.READ)
    tenant_id = knowledge_tenant(request)
    artifact = store.get_learned_artifact(artifact_id, tenant_id=tenant_id)
    if artifact is None or artifact.get("artifact_type") != artifact_type:
        raise HTTPException(status_code=404, detail="Learned artifact not found")
    try:
        page = store.list_artifact_extraction_page(
            artifact_id,
            extraction_kind=kind,
            tenant_id=tenant_id,
            limit=limit,
            cursor=cursor,
        )
    except ArtifactGenerationConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "artifact_id": artifact_id,
        "kind": kind,
        "count": len(page.items),
        "extractions": page.items,
        "has_more": page.has_more,
        "next_cursor": page.next_cursor,
    }


@router.get(
    "/api/v1/learn/runbooks/{artifact_id}/extractions",
    tags=["Learning"],
    summary="List one runbook extraction kind",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def list_runbook_extractions(
    artifact_id: str,
    request: Request,
    kind: str = Query(...),
    limit: int = Query(200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=1024),
    store: Any = Depends(get_signal_store),
):
    return await _list_artifact_extraction_page(
        request=request,
        artifact_id=artifact_id,
        artifact_type="runbook",
        kind=kind,
        limit=limit,
        cursor=cursor,
        store=store,
    )


@router.get(
    "/api/v1/learn/incidents/{artifact_id}/extractions",
    tags=["Learning"],
    summary="List one incident extraction kind",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def list_incident_extractions(
    artifact_id: str,
    request: Request,
    kind: str = Query(...),
    limit: int = Query(200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=1024),
    store: Any = Depends(get_signal_store),
):
    return await _list_artifact_extraction_page(
        request=request,
        artifact_id=artifact_id,
        artifact_type="incident",
        kind=kind,
        limit=limit,
        cursor=cursor,
        store=store,
    )


@router.get(
    "/api/v1/learning/search",
    tags=["Learning"],
    summary="Search learned operational context",
    response_description="FTS-ranked learned context rows",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def search_learning_context(
    request: Request,
    q: str = Query(..., min_length=1),
    service: str = "",
    include_candidates: bool = True,
    limit: int = Query(20, ge=1, le=100),
    store: Any = Depends(get_signal_store),
):
    """Search learned dashboard/panel/metric context."""
    assert_knowledge_action(request, KnowledgeAction.READ)
    try:
        rows = store.search_learning_context(
            q,
            service=service,
            include_candidates=include_candidates,
            limit=limit,
            tenant_id=knowledge_tenant(request),
        )
    except signals_mod.LearningIndexUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"query": q, "count": len(rows), "results": rows}


@router.get(
    "/api/v1/services/{service_name}",
    tags=["Learning"],
    summary="Describe a service from learned operational context",
    response_description="Service-level learned dashboards, metrics, panels, and signals",
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.READ))],
)
async def describe_service(
    request: Request,
    service_name: str = PathParam(description="Service/component name to describe"),
    include_candidates: bool = True,
    limit: int = Query(50, ge=1, le=200),
    store: Any = Depends(get_signal_store),
):
    """Answer what is known about this service from learned context."""
    assert_knowledge_action(request, KnowledgeAction.READ)
    try:
        return store.describe_service(
            service_name,
            include_candidates=include_candidates,
            limit=limit,
            tenant_id=knowledge_tenant(request),
        )
    except signals_mod.LearningIndexUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/approve",
    tags=["Learning"],
    summary="Approve an ingested dashboard",
    response_description="Approval status and signal mappings created",
)
async def approve_ingested_dashboard(
    request: Request,
    dashboard_uid: str,
    backend: str | None = None,
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Approve a pending ingested dashboard, activating its signal mappings."""
    from tacit.config import settings
    from tacit.dashboard_ingest import DashboardReviewConflictError, approve_ingested_dashboard_record

    _authorize_learning_read(request)
    _authorize_signal_approval(request, True)
    _authorize_learning_mutation(request)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory()
        return approve_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=store,
            runtime_settings=getattr(request.app.state, "settings", settings),
            tenant_id=tenant_id,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    except DashboardReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/reject",
    tags=["Learning"],
    summary="Reject an ingested dashboard",
    response_description="Rejection status; no signal mappings are created",
)
async def reject_ingested_dashboard(
    request: Request,
    dashboard_uid: str,
    backend: str | None = None,
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Reject a pending ingested dashboard."""
    from tacit.config import settings
    from tacit.dashboard_ingest import DashboardReviewConflictError, reject_ingested_dashboard_record

    _authorize_learning_read(request)
    assert_knowledge_action(request, KnowledgeAction.REJECT)
    _authorize_learning_mutation(request)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory()
        return reject_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=store,
            runtime_settings=getattr(request.app.state, "settings", settings),
            tenant_id=tenant_id,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    except DashboardReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/ignore",
    tags=["Learning"],
    summary="Ignore an ingested dashboard",
    response_description="Ignored status; no signal mappings or negative examples are created",
)
async def ignore_ingested_dashboard(
    request: Request,
    dashboard_uid: str,
    backend: str | None = None,
    store_factory: Any = Depends(get_signal_store_factory),
):
    """Ignore a dashboard without creating mappings or negative examples."""
    from tacit.config import settings
    from tacit.dashboard_ingest import DashboardReviewConflictError, ignore_ingested_dashboard_record

    _authorize_learning_read(request)
    assert_knowledge_action(request, KnowledgeAction.REJECT)
    _authorize_learning_mutation(request)
    tenant_id = knowledge_tenant(request)
    try:
        store = store_factory()
        return ignore_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=store,
            runtime_settings=getattr(request.app.state, "settings", settings),
            tenant_id=tenant_id,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    except DashboardReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
