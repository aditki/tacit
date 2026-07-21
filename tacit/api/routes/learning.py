"""Dashboard learning routes."""

from __future__ import annotations

import hashlib
import inspect
from typing import Protocol

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi import Path as PathParam

import tacit.signals as signals_mod
from tacit.api.security import verify_api_key
from tacit.models.schemas import (
    LearnAlertRequest,
    LearnDashboardRequest,
    LearnDashboardUploadRequest,
    LearnIncidentRequest,
    LearnRunbookRequest,
)

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


async def _call_ingest_dashboard(ingest_dashboard, **kwargs):
    if "runtime_settings" not in inspect.signature(ingest_dashboard).parameters:
        kwargs.pop("runtime_settings", None)
    return await ingest_dashboard(**kwargs)


async def _call_learn_backend_dashboards(learn_backend_dashboards, **kwargs):
    if "runtime_settings" not in inspect.signature(learn_backend_dashboards).parameters:
        kwargs.pop("runtime_settings", None)
    return await learn_backend_dashboards(**kwargs)


async def _call_ingest_alert(ingest_alert, **kwargs):
    if "runtime_settings" not in inspect.signature(ingest_alert).parameters:
        kwargs.pop("runtime_settings", None)
    return await ingest_alert(**kwargs)


async def _call_learn_backend_alerts(learn_backend_alerts, **kwargs):
    if "runtime_settings" not in inspect.signature(learn_backend_alerts).parameters:
        kwargs.pop("runtime_settings", None)
    return await learn_backend_alerts(**kwargs)


@router.post(
    "/api/v1/learn/dashboard",
    tags=["Learning"],
    summary="Learn from an existing Grafana dashboard",
    response_description="Extracted features, inferred signals, and optional quarantined archetype YAML",
)
async def learn_from_dashboard(request: Request, payload: LearnDashboardRequest):
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
async def learn_from_alert(request: Request, payload: LearnAlertRequest):
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
async def learn_from_runbook(payload: LearnRunbookRequest):
    """Learn operational candidates from a markdown/plain-text runbook."""
    from tacit.artifact_learning import RunbookExtractor, artifact_from_text, learn_artifact

    try:
        artifact = artifact_from_text(
            artifact_type="runbook",
            title=payload.title,
            body_text=payload.body_text,
            external_id=_artifact_external_id(payload, "runbook"),
            source_vendor=payload.source_vendor,
            source_instance=payload.source_instance,
            provenance_url=payload.provenance_url,
        )
        return learn_artifact(artifact, RunbookExtractor(), dry_run=payload.dry_run)
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
async def learn_from_incident(payload: LearnIncidentRequest):
    """Learn operational candidates from an incident-history record."""
    from tacit.artifact_learning import IncidentExtractor, artifact_from_text, learn_artifact

    try:
        artifact = artifact_from_text(
            artifact_type="incident",
            title=payload.title,
            body_text=payload.body_text,
            external_id=_artifact_external_id(payload, "incident"),
            source_vendor=payload.source_vendor,
            source_instance=payload.source_instance,
            provenance_url=payload.provenance_url,
        )
        return learn_artifact(artifact, IncidentExtractor(), dry_run=payload.dry_run)
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
async def learn_from_dashboard_json(request: LearnDashboardUploadRequest):
    """Ingest an uploaded dashboard JSON export without contacting the vendor."""
    from tacit.dashboard_ingest import ingest_dashboard_features
    from tacit.dashboard_uploads import parse_uploaded_dashboard

    try:
        features = parse_uploaded_dashboard(
            request.dashboard,
            vendor=request.vendor,
            source_name=request.source_name,
        )
        return await ingest_dashboard_features(features, auto_approve=request.auto_approve)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("dashboard_json_ingest_failed", vendor=request.vendor, source_name=request.source_name)
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
):
    """List dashboards that have been ingested for learning."""
    from tacit.dashboard_ingest import build_learning_impact_report, build_signal_quality_report

    store = signals_mod.get_signal_store()
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
):
    """List alerts that have been ingested for learning."""
    from tacit.dashboard_ingest import build_learning_impact_report, build_signal_quality_report

    store = signals_mod.get_signal_store()
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
):
    """List runbooks learned by Tacit Artifact Learning v1."""
    store = signals_mod.get_signal_store()
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
):
    """List incident history learned by Tacit Artifact Learning v1."""
    store = signals_mod.get_signal_store()
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
):
    """Search learned dashboard/panel/metric context."""
    store = signals_mod.get_signal_store()
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
):
    """Answer what is known about this service from learned context."""
    store = signals_mod.get_signal_store()
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
async def approve_ingested_dashboard(dashboard_uid: str, backend: str | None = None):
    """Approve a pending ingested dashboard, activating its signal mappings."""
    from tacit.dashboard_ingest import approve_ingested_dashboard_record

    try:
        return approve_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=signals_mod.get_signal_store(),
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")


@router.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/reject",
    tags=["Learning"],
    summary="Reject an ingested dashboard",
    response_description="Rejection status; no signal mappings are created",
)
async def reject_ingested_dashboard(dashboard_uid: str, backend: str | None = None):
    """Reject a pending ingested dashboard."""
    from tacit.dashboard_ingest import reject_ingested_dashboard_record

    try:
        return reject_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=signals_mod.get_signal_store(),
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
async def ignore_ingested_dashboard(dashboard_uid: str, backend: str | None = None):
    """Ignore a pending ingested dashboard without creating mappings or negative examples."""
    store = signals_mod.get_signal_store()
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
