"""DashForge – FastAPI entrypoint + Slack bot startup."""

from __future__ import annotations

import asyncio
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi import Path as PathParam
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader

from dashforge import __version__
from dashforge.config import settings
from dashforge.feedback import get_feedback_store
from dashforge.models.schemas import (
    ArchetypeListResponse,
    ArchetypeReloadResponse,
    DashRequest,
    DashResponse,
    FeedbackRequest,
    FeedbackResponse,
    FeedbackStatsResponse,
    HealthResponse,
    LearnDashboardRequest,
    LearnDashboardUploadRequest,
    TeachSignalRequest,
    TeachSignalResponse,
)
from dashforge.pipeline import run_pipeline

_STATIC_DIR = Path(__file__).parent / "static"

logger = structlog.get_logger()

_slack_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure logging and start the Slack bot alongside the API server."""
    from dashforge.logging import configure_logging

    configure_logging(settings.log_level)

    global _slack_task
    if settings.slack_bot_token and settings.slack_app_token:
        from dashforge.integrations.slack import start_slack_bot

        _slack_task = asyncio.create_task(start_slack_bot())
        logger.info("slack_bot_scheduled")
    else:
        logger.warning("slack_not_configured", hint="Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN to enable Slack")
    yield
    if _slack_task and not _slack_task.done():
        _slack_task.cancel()


app = FastAPI(
    title="DashForge",
    description=(
        "## Natural Language → Grafana Dashboards\n\n"
        "DashForge is a multi-agent pipeline that converts plain-English incident descriptions "
        "into ready-to-use Grafana dashboards. It supports 10+ datasource types (Prometheus, "
        "CloudWatch, Loki, Elasticsearch, Graphite, InfluxDB, etc.) and uses LLM-powered "
        "intent classification, cross-datasource metric discovery, and deterministic query building.\n\n"
        "### Key capabilities\n"
        "- **Dashboard generation** — describe what you need, get a published Grafana dashboard\n"
        "- **Feedback loop** — rate dashboards, and the system automatically improves metric selection\n"
        "- **Archetype management** — edit investigation templates via YAML, hot-reload without restart\n\n"
        "### Authentication\n"
        "When `API_AUTH_ENABLED=true`, pass your key via the `X-API-Key` header. "
        "When disabled (default for development), all endpoints are open.\n\n"
        "### Interactive docs\n"
        "- **Swagger UI** — you are here (`/docs`)\n"
        "- **ReDoc** — alternative view at [`/redoc`](/redoc)\n"
        "- **Web UI** — interactive dashboard generator at [`/`](/)\n"
    ),
    version=__version__,
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "Dashboard Generation",
            "description": "Generate Grafana dashboards from natural-language prompts. "
            "The pipeline: Intent Classification → Metric Discovery → Query Building → Dashboard Publishing.",
        },
        {
            "name": "Feedback",
            "description": "Submit and retrieve human evaluation feedback for generated dashboards. "
            "Feedback drives the closed-loop improvement system — rated metrics influence future ranking.",
        },
        {
            "name": "Insights",
            "description": "Analyze collected feedback to surface actionable improvement signals: "
            "per-archetype quality, noisy dashboards, metric quality, archetype gaps, and recommendations.",
        },
        {
            "name": "Archetypes",
            "description": "View and manage investigation archetype templates. "
            "Archetypes are loaded from packaged data or `DASHFORGE_ARCHETYPES_PATH` "
            "and can be hot-reloaded without restart.",
        },
        {
            "name": "Signals",
            "description": "Semantic signal taxonomy — maps canonical observability concepts "
            "(e.g. 'request_latency', 'error_rate') to environment-specific metrics. "
            "Signals decouple archetypes from raw metric names for portability.",
        },
        {
            "name": "Learning",
            "description": "Learn operational patterns from existing Grafana dashboards. "
            "Ingests dashboards, extracts metric co-occurrence, panel groupings, "
            "and aggregation patterns, then infers signal mappings.",
        },
        {
            "name": "System",
            "description": "Health checks and system status.",
        },
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API key auth ─────────────────────────────────────────────────────────

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str | None = Security(_api_key_header)):
    """Verify API key if auth is enabled. No-op when disabled."""
    if not settings.api_auth_enabled:
        return
    if not api_key or not secrets.compare_digest(api_key, settings.api_auth_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ── Input sanitization ───────────────────────────────────────────────────

MAX_PROMPT_LENGTH = 2000


def _sanitize_prompt(prompt: str) -> str:
    """Basic prompt sanitization — length cap and control char removal."""
    # Strip control characters (except newlines)
    cleaned = "".join(c for c in prompt if c == "\n" or (c.isprintable() and ord(c) < 0x10000))
    return cleaned[:MAX_PROMPT_LENGTH].strip()


@app.get(
    "/healthz",
    tags=["System"],
    summary="Health check",
    response_model=HealthResponse,
    response_description="Server health status",
)
async def healthz():
    """Lightweight health check for load balancers and orchestrators."""
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
async def web_ui():
    return FileResponse(_STATIC_DIR / "index.html", media_type="text/html")


@app.post(
    "/api/v1/chart",
    response_model=DashResponse,
    dependencies=[Depends(verify_api_key)],
    tags=["Dashboard Generation"],
    summary="Generate a dashboard",
    response_description="Published dashboard URL, UID, panel count, and summary",
)
async def create_chart(request: DashRequest):
    """Generate a Grafana dashboard from a natural-language prompt.

    The pipeline steps:
    1. **Intent classification** — extracts services, keywords, archetypes
    2. **Metric discovery** — queries configured datasources for relevant metrics
    3. **Pre-ranking** — narrows metrics using keyword relevance + feedback quality scores
    4. **Query building** — LLM generates PromQL/LogQL/etc. expressions
    5. **Dashboard publishing** — creates the dashboard in Grafana via API
    6. **Provenance recording** — stores prompt → dashboard mapping for feedback loop
    """
    request.prompt = _sanitize_prompt(request.prompt)
    if not request.prompt:
        raise HTTPException(status_code=400, detail="prompt is required")
    try:
        return await run_pipeline(request)
    except Exception:
        logger.exception("api_pipeline_error")
        raise HTTPException(status_code=500, detail="Failed to generate dashboard")


# ── Feedback endpoints ───────────────────────────────────────────────────


@app.post(
    "/api/v1/feedback",
    response_model=FeedbackResponse,
    dependencies=[Depends(verify_api_key)],
    tags=["Feedback"],
    summary="Submit dashboard feedback",
    response_description="Confirmation with feedback ID",
)
async def submit_feedback(req: FeedbackRequest):
    """Submit human evaluation feedback for a generated dashboard.

    Rate a dashboard across four dimensions (1-5 scale) plus an overall
    usefulness boolean. All rating fields are optional — submit as many or
    as few as you like. Feedback is stored in SQLite and used to:

    - Adjust metric pre-ranking scores (metrics in well-rated dashboards get boosted)
    - Generate improvement recommendations via the analysis endpoint
    - Track per-archetype quality trends over time
    """
    store = get_feedback_store()
    feedback_id = store.submit_feedback(
        dashboard_uid=req.dashboard_uid,
        symptom_visibility=req.symptom_visibility,
        root_cause_support=req.root_cause_support,
        noise_level=req.noise_level,
        investigation_speed=req.investigation_speed,
        overall_useful=req.overall_useful,
        comment=req.comment,
        reviewer=req.reviewer,
    )
    return FeedbackResponse(feedback_id=feedback_id, dashboard_uid=req.dashboard_uid)


@app.get(
    "/api/v1/feedback/stats",
    dependencies=[Depends(verify_api_key)],
    tags=["Insights"],
    summary="Feedback statistics",
    response_model=FeedbackStatsResponse,
    response_description=(
        "Aggregate stats: total feedback, dashboards reviewed, useful rate, average dimensional scores"
    ),
)
async def get_feedback_stats():
    """Aggregate feedback statistics across all reviewed dashboards.

    Returns totals, averages, and the overall usefulness rate."""
    store = get_feedback_store()
    return store.get_aggregate_stats()


@app.get(
    "/api/v1/feedback/analysis",
    dependencies=[Depends(verify_api_key)],
    tags=["Insights"],
    summary="Feedback analysis & recommendations",
    response_description="Actionable improvement signals with prioritized recommendations",
)
async def get_feedback_analysis():
    """Analyze collected feedback to produce actionable improvement signals.

    Joins dashboard provenance with feedback data to identify:

    - **Per-archetype quality** — average scores by archetype
    - **Noisy dashboards** — dashboards with low signal clarity
    - **Low symptom visibility** — dashboards that didn't surface the problem
    - **Archetype gaps** — freeform-path dashboards rated useful (candidates for new archetypes)
    - **Metric quality** — which metrics correlate with good vs. bad dashboards
    - **Confidence calibration** — whether high-confidence archetypes actually perform better
    - **Recommendations** — prioritized list of concrete actions (PRUNE, ADD SIGNAL, NEW ARCHETYPE, etc.)
    """
    store = get_feedback_store()
    return store.analyze()


@app.get(
    "/api/v1/feedback/{dashboard_uid}",
    dependencies=[Depends(verify_api_key)],
    tags=["Feedback"],
    summary="Get feedback for a dashboard",
    response_description="Dashboard provenance metadata and all submitted feedback entries",
)
async def get_feedback(
    dashboard_uid: str = PathParam(..., pattern=r"^[a-zA-Z0-9_\-]{1,128}$", description="Dashboard UID"),
):
    """Retrieve provenance (how the dashboard was generated) and all submitted
    feedback entries for a specific dashboard UID."""
    store = get_feedback_store()
    provenance = store.get_provenance(dashboard_uid)
    feedback = store.get_feedback(dashboard_uid)
    if not provenance and not feedback:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"provenance": provenance, "feedback": feedback}


# ── Archetype management ─────────────────────────────────────────────────


@app.post(
    "/api/v1/archetypes/reload",
    dependencies=[Depends(verify_api_key)],
    tags=["Archetypes"],
    summary="Reload archetypes from YAML",
    response_model=ArchetypeReloadResponse,
    response_description="Confirmation with count and summary of loaded archetypes",
)
async def reload_archetypes_endpoint():
    """Hot-reload archetype templates without server restart.

    Workflow: set `DASHFORGE_ARCHETYPES_PATH`, edit that YAML, then call this endpoint.
    If the YAML file is missing or invalid, falls back to built-in Python definitions."""
    from dashforge.archetypes.templates import reload_archetypes

    reload_archetypes()
    from dashforge.archetypes.templates import ALL_ARCHETYPES as reloaded

    return {
        "message": "Archetypes reloaded",
        "count": len(reloaded),
        "archetypes": [{"id": a.id, "name": a.name, "panels": len(a.panels)} for a in reloaded],
    }


@app.get(
    "/api/v1/archetypes",
    dependencies=[Depends(verify_api_key)],
    tags=["Archetypes"],
    summary="List all archetypes",
    response_model=ArchetypeListResponse,
    response_description="All loaded investigation archetypes with their panels and problem types",
)
async def list_archetypes():
    """List all currently loaded investigation archetypes.

    Each archetype defines a known investigation pattern (e.g. latency investigation,
    error spike) with pre-defined panel templates and query patterns. Archetypes are
    loaded from packaged data or `DASHFORGE_ARCHETYPES_PATH`, otherwise from built-in Python definitions."""
    from dashforge.archetypes.templates import ALL_ARCHETYPES

    return {
        "count": len(ALL_ARCHETYPES),
        "archetypes": [
            {
                "id": a.id,
                "name": a.name,
                "description": a.description,
                "problem_types": a.problem_types,
                "panel_count": len(a.panels),
                "panels": [p.title for p in a.panels],
                "tags": a.tags,
            }
            for a in ALL_ARCHETYPES
        ],
    }


# ── Investigation history ─────────────────────────────────────────────────


@app.get(
    "/api/v1/investigations",
    dependencies=[Depends(verify_api_key)],
    tags=["History"],
    summary="List recent investigations",
    response_description="Investigation history with intent, metrics, queries, timings, and results",
)
async def list_investigations(
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
    user_id: str | None = None,
):
    """List recent investigation runs, newest first.

    Includes full pipeline telemetry: intent classification, datasource discovery,
    metric selection, generated queries, validation warnings, timings, and results."""
    from dashforge.history import get_investigation_store

    store = get_investigation_store()
    investigations = store.list_recent(limit=limit, offset=offset, status=status, user_id=user_id)
    return {"count": len(investigations), "investigations": investigations}


@app.get(
    "/api/v1/investigations/stats",
    dependencies=[Depends(verify_api_key)],
    tags=["History"],
    summary="Investigation aggregate stats",
    response_description="Aggregate statistics across all investigations",
)
async def investigation_stats():
    """Aggregate stats: success/failure rates, avg timings, path distribution."""
    from dashforge.history import get_investigation_store

    store = get_investigation_store()
    return store.stats()


@app.get(
    "/api/v1/investigations/{investigation_id}",
    dependencies=[Depends(verify_api_key)],
    tags=["History"],
    summary="Get investigation details",
    response_description="Full investigation record with all pipeline data",
)
async def get_investigation(investigation_id: str):
    """Get full details of a single investigation by ID.

    Returns the complete pipeline trace: prompt, intent, archetypes,
    datasources, metrics, queries, validation, timings, and result."""
    from dashforge.history import get_investigation_store

    store = get_investigation_store()
    inv = store.get(investigation_id)
    if inv is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return inv


# ── Signal taxonomy endpoints ─────────────────────────────────────────────


@app.get(
    "/api/v1/signals",
    dependencies=[Depends(verify_api_key)],
    tags=["Signals"],
    summary="List all signal types",
    response_description="All registered semantic signal types with categories",
)
async def list_signals():
    """List all registered semantic signal types.

    Signal types are canonical observability concepts (e.g. 'request_latency',
    'error_rate') that are independent of specific metric names. They bridge
    archetypes to environment-specific metrics."""
    from dashforge.signals import get_signal_store

    store = get_signal_store()
    return {"signal_types": store.list_signal_types()}


@app.get(
    "/api/v1/signals/stats",
    dependencies=[Depends(verify_api_key)],
    tags=["Signals"],
    summary="Signal store statistics",
    response_description="Summary stats: signal types, mappings, ingested dashboards",
)
async def signal_stats():
    """Summary statistics for the signal mapping store."""
    from dashforge.signals import get_signal_store

    store = get_signal_store()
    return store.stats()


@app.get(
    "/api/v1/signals/{signal_type}",
    dependencies=[Depends(verify_api_key)],
    tags=["Signals"],
    summary="Get signal type details",
    response_description="Signal type with all metric mappings, confidence scores, and provenance",
)
async def get_signal(signal_type: str):
    """Get a signal type with all its metric mappings.

    Each mapping includes: metric pattern, confidence, context filters,
    provenance (source type + references), and trust metrics."""
    from dashforge.signals import get_signal_store

    store = get_signal_store()
    result = store.get_signal_type(signal_type)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Signal type '{signal_type}' not found")
    return result


@app.post(
    "/api/v1/signals/teach",
    dependencies=[Depends(verify_api_key)],
    tags=["Signals"],
    summary="Teach DashForge a signal mapping",
    response_model=TeachSignalResponse,
    response_description="Confirmation of the created mapping",
)
async def teach_signal(request: TeachSignalRequest) -> TeachSignalResponse:
    """Teach DashForge an organization-specific signal mapping.

    Example: tell the system that for your org, 'queue_depth' means
    'kafka_consumer_lag' and 'inflight_messages'.

    The request body is validated against ``TeachSignalRequest`` — confidence
    bounds, non-empty identifiers, and unknown fields are all enforced before
    this handler runs (invalid input → 422).
    """
    from dashforge.signals import get_signal_store

    store = get_signal_store()

    # Register or update the signal type
    store.register_signal_type(
        signal_type=request.signal_type,
        description=request.description,
        category=request.category,
        unit=request.unit,
    )

    # Add metric mappings (each already validated: non-empty pattern, 0–1 confidence)
    mappings_created = 0
    for mp in request.metric_patterns:
        store.add_mapping(
            signal_type=request.signal_type,
            metric_pattern=mp.pattern,
            confidence=mp.confidence,
            context_services=request.services,
            context_datasource_types=request.datasource_types,
            context_environments=request.environments,
            source_type="teach",
            source_refs=[f"manual:{request.taught_by}"],
        )
        mappings_created += 1

    return TeachSignalResponse(
        signal_type=request.signal_type,
        mappings_created=mappings_created,
        message=f"Signal '{request.signal_type}' updated with {mappings_created} mapping(s)",
    )


# ── Dashboard learning endpoints ─────────────────────────────────────────


@app.post(
    "/api/v1/learn/dashboard",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Learn from an existing Grafana dashboard",
    response_description="Extracted features, inferred signals, and generated archetype YAML",
)
async def learn_from_dashboard(request: LearnDashboardRequest):
    """Ingest an existing Grafana dashboard to learn operational patterns.

    Extracts metric co-occurrence, panel groupings, aggregation patterns,
    query transformations, and infers semantic signal mappings.

    Optionally auto-generates an archetype YAML snippet for review.

    The request body is validated against ``LearnDashboardRequest``;
    ``auto_approve`` accepts real JSON booleans plus the explicit strings
    ``"true"`` / ``"false"``. Other truthy/falsy values are rejected (422)
    so accidental strings like ``"yes"`` cannot approve unreviewed mappings.

    The ``backend`` field selects which backend to fetch from: ``"grafana"``
    (default) or ``"signalfx"``. If omitted, uses the first active backend.

    When `auto_approve` is false (default), the ingested dashboard is stored
    as 'pending' for human review before signal mappings are activated."""
    from dashforge.dashboard_ingest import ingest_dashboard

    try:
        result = await ingest_dashboard(
            dashboard_uid=request.dashboard_uid,
            backend_name=request.backend,
            auto_approve=request.auto_approve,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("dashboard_ingest_failed", uid=request.dashboard_uid, backend=request.backend)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to ingest dashboard '{request.dashboard_uid}'. "
            "Check that the UID exists and the backend is accessible.",
        )


@app.post(
    "/api/v1/learn/dashboard/json",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Learn from uploaded dashboard JSON",
    response_description="Extracted features, inferred signals, and generated archetype YAML",
)
async def learn_from_dashboard_json(request: LearnDashboardUploadRequest):
    """Ingest an uploaded dashboard JSON export without contacting the vendor.

    The uploaded document is parsed through the dashboard upload parser registry
    and then follows the same inference, persistence, YAML generation, and
    approval workflow as backend-fetched dashboards.
    """
    from dashforge.dashboard_ingest import ingest_dashboard_features
    from dashforge.dashboard_uploads import parse_uploaded_dashboard

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


@app.post(
    "/api/v1/learn/{backend_name}",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Crawl and learn from all dashboards in a backend",
    response_description="Bulk dashboard learning summary",
)
async def learn_backend(
    backend_name: str = PathParam(description="Backend name: grafana or signalfx"),
    auto_approve: bool = Query(False, description="Immediately approve eligible inferred mappings"),
    limit: int = Query(500, ge=1, le=5000, description="Maximum dashboards to crawl"),
):
    """Crawl a connected backend and persist learned dashboard context."""
    from dashforge.dashboard_ingest import learn_backend_dashboards

    try:
        return await learn_backend_dashboards(
            backend_name=backend_name,
            auto_approve=auto_approve,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("backend_learning_failed", backend=backend_name)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to learn dashboards from backend '{backend_name}'. Check backend connectivity.",
        )


@app.get(
    "/api/v1/learn/dashboards",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="List ingested dashboards",
    response_description="Ingested dashboards with extracted features and status",
)
async def list_ingested_dashboards(
    status: str | None = None,
    limit: int = 50,
):
    """List dashboards that have been ingested for learning.

    Filter by status: 'pending', 'approved', or 'rejected'."""
    from dashforge.dashboard_ingest import build_learning_impact_report, build_signal_quality_report
    from dashforge.signals import get_signal_store

    store = get_signal_store()
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


@app.get(
    "/api/v1/learning/search",
    dependencies=[Depends(verify_api_key)],
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
    from dashforge.signals import LearningIndexUnavailable, get_signal_store

    store = get_signal_store()
    try:
        rows = store.search_learning_context(
            q,
            service=service,
            include_candidates=include_candidates,
            limit=limit,
        )
    except LearningIndexUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"query": q, "count": len(rows), "results": rows}


@app.get(
    "/api/v1/services/{service_name}",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Describe a service from learned operational context",
    response_description="Service-level learned dashboards, metrics, panels, and signals",
)
async def describe_service(
    service_name: str = PathParam(description="Service/component name to describe"),
    include_candidates: bool = True,
    limit: int = Query(50, ge=1, le=200),
):
    """Answer “what do we know about this service?” from learned context."""
    from dashforge.signals import LearningIndexUnavailable, get_signal_store

    store = get_signal_store()
    try:
        return store.describe_service(
            service_name,
            include_candidates=include_candidates,
            limit=limit,
        )
    except LearningIndexUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/approve",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Approve an ingested dashboard",
    response_description="Approval status and signal mappings created",
)
async def approve_ingested_dashboard(dashboard_uid: str, backend: str | None = None):
    """Approve a pending ingested dashboard, activating its signal mappings.

    This creates signal-to-metric mappings from the inferred signals with
    provenance tracking back to the source dashboard."""
    from dashforge.dashboard_ingest import approve_ingested_dashboard_record
    from dashforge.signals import get_signal_store

    try:
        return approve_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=get_signal_store(),
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")


@app.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/reject",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Reject an ingested dashboard",
    response_description="Rejection status; no signal mappings are created",
)
async def reject_ingested_dashboard(dashboard_uid: str, backend: str | None = None):
    """Reject a pending ingested dashboard.

    Rejecting does not create mappings. Heuristic candidates are retained as
    negative examples so the inference rules can be audited or tuned later.
    """
    from dashforge.dashboard_ingest import reject_ingested_dashboard_record
    from dashforge.signals import get_signal_store

    try:
        return reject_ingested_dashboard_record(
            dashboard_uid=dashboard_uid,
            backend_name=backend,
            store=get_signal_store(),
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    except RuntimeError:
        raise HTTPException(status_code=409, detail="Dashboard is no longer pending")


@app.post(
    "/api/v1/learn/dashboards/{dashboard_uid}/ignore",
    dependencies=[Depends(verify_api_key)],
    tags=["Learning"],
    summary="Ignore an ingested dashboard",
    response_description="Ignored status; no signal mappings or negative examples are created",
)
async def ignore_ingested_dashboard(dashboard_uid: str, backend: str | None = None):
    """Ignore a pending ingested dashboard without creating mappings or negative examples."""
    from dashforge.signals import get_signal_store

    store = get_signal_store()
    backend_name = backend
    ingested = store.get_ingested_dashboard(dashboard_uid, backend_name=backend_name)
    if ingested is None:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")
    if ingested["status"] != "pending":
        return {"message": f"Dashboard already {ingested['status']}"}

    if not store.ignore_ingested_dashboard(dashboard_uid, backend_name=backend_name):
        raise HTTPException(status_code=409, detail="Dashboard is no longer pending")

    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "ignored",
        "message": "Dashboard ignored; no mappings created",
    }


def main():
    uvicorn.run(
        "dashforge.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
