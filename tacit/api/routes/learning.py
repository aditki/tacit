"""Dashboard learning routes."""

from __future__ import annotations

import hashlib
import inspect
from typing import Any, Protocol

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi import Path as PathParam

import tacit.signals as signals_mod
from tacit.api.dependencies import get_runtime_stores, get_signal_store
from tacit.api.security import verify_api_key
from tacit.models.schemas import (
    LearnAlertRequest,
    LearnDashboardRequest,
    LearnDashboardUploadRequest,
    LearnIncidentRequest,
    LearnRunbookRequest,
)
from tacit.runtime_stores import RuntimeStores

logger = structlog.get_logger()
router = APIRouter(dependencies=[Depends(verify_api_key)])


class _ArtifactPayload(Protocol):
    title: str
    body_text: str
    external_id: str
    source_vendor: str
    source_instance: str
    provenance_url: str


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
    store: Any = Depends(get_signal_store),
):
    """Ingest an existing dashboard to learn operational patterns."""
    from tacit.config import settings
    from tacit.dashboard_ingest import ingest_dashboard

    try:
        return await _call_ingest_dashboard(
            ingest_dashboard,
            dashboard_uid=payload.dashboard_uid,
            backend_name=payload.backend,
            auto_approve=payload.auto_approve,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("dashboard_ingest_failed", uid=payload.dashboard_uid, backend=payload.backend)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest dashboard '{payload.dashboard_uid}'. "
            "Check that the UID exists and the backend is accessible.",
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
    store: Any = Depends(get_signal_store),
):
    """Ingest an existing alert rule/detector to learn operational patterns."""
    from tacit.alert_ingest import ingest_alert
    from tacit.config import settings

    try:
        return await _call_ingest_alert(
            ingest_alert,
            alert_uid=payload.alert_uid,
            backend_name=payload.backend,
            auto_approve=payload.auto_approve,
            dry_run=payload.dry_run,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("alert_ingest_failed", uid=payload.alert_uid, backend=payload.backend)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest alert '{payload.alert_uid}'. "
            "Check that the alert exists and the backend is accessible.",
        )


@router.post(
    "/api/v1/learn/runbooks",
    tags=["Learning"],
    summary="Learn from a runbook artifact",
    response_description="Extracted operational IR candidates with provenance",
)
async def learn_from_runbook(
    payload: LearnRunbookRequest,
    stores: RuntimeStores = Depends(get_runtime_stores),
):
    """Learn operational candidates from a markdown/plain-text runbook."""
    from tacit.artifact_learning import RunbookExtractor, artifact_from_text, learn_artifact

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
        return learn_artifact(artifact, RunbookExtractor(), dry_run=payload.dry_run, store=store)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("runbook_artifact_learning_failed", title=payload.title)
        raise HTTPException(status_code=500, detail="Failed to learn from runbook artifact.")


@router.post(
    "/api/v1/learn/incidents",
    tags=["Learning"],
    summary="Learn from an incident-history artifact",
    response_description="Extracted operational IR candidates with provenance",
)
async def learn_from_incident(
    payload: LearnIncidentRequest,
    stores: RuntimeStores = Depends(get_runtime_stores),
):
    """Learn operational candidates from an incident-history record."""
    from tacit.artifact_learning import IncidentExtractor, artifact_from_text, learn_artifact

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
        return learn_artifact(artifact, IncidentExtractor(), dry_run=payload.dry_run, store=store)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("incident_artifact_learning_failed", title=payload.title)
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
    store: Any = Depends(get_signal_store),
):
    """Ingest an uploaded dashboard JSON export without contacting the vendor."""
    from tacit.dashboard_ingest import ingest_dashboard_features
    from tacit.dashboard_uploads import parse_uploaded_dashboard

    try:
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
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("dashboard_json_ingest_failed", vendor=payload.vendor, source_name=payload.source_name)
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
    store: Any = Depends(get_signal_store),
):
    """Crawl a connected backend and persist learned dashboard context."""
    from tacit.config import settings
    from tacit.dashboard_ingest import learn_backend_dashboards

    try:
        return await _call_learn_backend_dashboards(
            learn_backend_dashboards,
            backend_name=backend_name,
            auto_approve=auto_approve,
            limit=limit,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("backend_learning_failed", backend=backend_name)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to learn dashboards from backend '{backend_name}'. Check backend connectivity.",
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
    store: Any = Depends(get_signal_store),
):
    """Crawl a connected backend and persist learned alert context."""
    from tacit.alert_ingest import learn_backend_alerts
    from tacit.config import settings

    try:
        return await _call_learn_backend_alerts(
            learn_backend_alerts,
            backend_name=backend_name,
            auto_approve=auto_approve,
            dry_run=dry_run,
            limit=limit,
            runtime_settings=getattr(request.app.state, "settings", settings),
            store=store,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("backend_alert_learning_failed", backend=backend_name)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to learn alerts from backend '{backend_name}'. Check backend connectivity.",
        )


@router.get(
    "/api/v1/learn/dashboards",
    tags=["Learning"],
    summary="List ingested dashboards",
    response_description="Ingested dashboards with extracted features and status",
)
async def list_ingested_dashboards(
    status: str | None = None,
    limit: int = 50,
    store: Any = Depends(get_signal_store),
):
    """List dashboards that have been ingested for learning."""
    from tacit.dashboard_ingest import build_learning_impact_report, build_signal_quality_report

    dashboards = store.list_ingested_dashboards(status=status, limit=limit)
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
    return {"count": len(dashboards), "dashboards": dashboards}


@router.get(
    "/api/v1/learn/alerts",
    tags=["Learning"],
    summary="List ingested alerts",
    response_description="Ingested alerts with extracted features and status",
)
async def list_ingested_alerts(
    status: str | None = None,
    limit: int = 50,
    store: Any = Depends(get_signal_store),
):
    """List alerts that have been ingested for learning."""
    from tacit.dashboard_ingest import build_learning_impact_report, build_signal_quality_report

    alerts = store.list_ingested_alerts(status=status, limit=limit)
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
    return {"count": len(alerts), "alerts": alerts}


@router.get(
    "/api/v1/learn/runbooks",
    tags=["Learning"],
    summary="List learned runbook artifacts",
    response_description="Learned runbooks and extracted operational IR candidates",
)
async def list_learned_runbooks(
    limit: int = Query(50, ge=1, le=500),
    store: Any = Depends(get_signal_store),
):
    """List runbooks learned by Tacit Artifact Learning v1."""
    runbooks = store.list_learned_artifacts(artifact_type="runbook", limit=limit)
    for runbook in runbooks:
        runbook["extractions"] = store.list_artifact_extractions(runbook["artifact_id"])
    return {"count": len(runbooks), "runbooks": runbooks}


@router.get(
    "/api/v1/learn/incidents",
    tags=["Learning"],
    summary="List learned incident artifacts",
    response_description="Learned incidents and extracted operational IR candidates",
)
async def list_learned_incidents(
    limit: int = Query(50, ge=1, le=500),
    store: Any = Depends(get_signal_store),
):
    """List incident history learned by Tacit Artifact Learning v1."""
    incidents = store.list_learned_artifacts(artifact_type="incident", limit=limit)
    for incident in incidents:
        incident["extractions"] = store.list_artifact_extractions(incident["artifact_id"])
    return {"count": len(incidents), "incidents": incidents}


@router.get(
    "/api/v1/learning/search",
    tags=["Learning"],
    summary="Search learned operational context",
    response_description="FTS-ranked learned context rows",
)
async def search_learning_context(
    q: str = Query(..., min_length=1),
    service: str = "",
    include_candidates: bool = True,
    limit: int = Query(20, ge=1, le=100),
    store: Any = Depends(get_signal_store),
):
    """Search learned dashboard/panel/metric context."""
    try:
        rows = store.search_learning_context(
            q,
            service=service,
            include_candidates=include_candidates,
            limit=limit,
        )
    except signals_mod.LearningIndexUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"query": q, "count": len(rows), "results": rows}


@router.get(
    "/api/v1/services/{service_name}",
    tags=["Learning"],
    summary="Describe a service from learned operational context",
    response_description="Service-level learned dashboards, metrics, panels, and signals",
)
async def describe_service(
    service_name: str = PathParam(description="Service/component name to describe"),
    include_candidates: bool = True,
    limit: int = Query(50, ge=1, le=200),
    store: Any = Depends(get_signal_store),
):
    """Answer what is known about this service from learned context."""
    try:
        return store.describe_service(
            service_name,
            include_candidates=include_candidates,
            limit=limit,
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
    store: Any = Depends(get_signal_store),
):
    """Approve a pending ingested dashboard, activating its signal mappings."""
    from tacit.config import settings
    from tacit.dashboard_ingest import approve_ingested_dashboard_record

    try:
        return approve_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=store,
            runtime_settings=getattr(request.app.state, "settings", settings),
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")


@router.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/reject",
    tags=["Learning"],
    summary="Reject an ingested dashboard",
    response_description="Rejection status; no signal mappings are created",
)
async def reject_ingested_dashboard(
    dashboard_uid: str,
    backend: str | None = None,
    store: Any = Depends(get_signal_store),
):
    """Reject a pending ingested dashboard."""
    from tacit.dashboard_ingest import reject_ingested_dashboard_record

    try:
        return reject_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=store,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Dashboard is no longer pending")


@router.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/ignore",
    tags=["Learning"],
    summary="Ignore an ingested dashboard",
    response_description="Ignored status; no signal mappings or negative examples are created",
)
async def ignore_ingested_dashboard(
    dashboard_uid: str,
    backend: str | None = None,
    store: Any = Depends(get_signal_store),
):
    """Ignore a pending ingested dashboard without creating mappings or negative examples."""
    ingested = store.get_ingested_dashboard(dashboard_uid, backend_name=backend)
    if ingested is None:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    if ingested["status"] != "pending":
        return {"message": f"Dashboard already {ingested['status']}"}

    if not store.ignore_ingested_dashboard(dashboard_uid, backend_name=backend):
        raise HTTPException(status_code=409, detail="Dashboard is no longer pending")

    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "ignored",
        "message": "Dashboard ignored; no mappings created",
    }
