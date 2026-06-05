"""DashForge – FastAPI entrypoint + Slack bot startup."""

from __future__ import annotations

import asyncio
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi import Path as PathParam
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader

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
    version="0.2.0",
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
            "Archetypes are loaded from `archetypes.yaml` and can be hot-reloaded without restart.",
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
    """Hot-reload archetype templates from `archetypes.yaml` without server restart.

    Workflow: edit `archetypes.yaml` → call this endpoint → changes take effect immediately.
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
    loaded from `archetypes.yaml` if available, otherwise from built-in Python definitions."""
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
    ``auto_approve`` is a strict boolean, so a JSON serialization mistake like
    the string ``"false"`` is read correctly rather than treated as truthy
    (invalid input → 422).

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
    from dashforge.signals import get_signal_store

    store = get_signal_store()
    dashboards = store.list_ingested_dashboards(status=status, limit=limit)
    return {"count": len(dashboards), "dashboards": dashboards}


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
    from dashforge.signals import get_signal_store

    store = get_signal_store()
    backend_name = backend

    ingested = store.get_ingested_dashboard(dashboard_uid, backend_name=backend_name)
    if ingested is None:
        raise HTTPException(status_code=404, detail="Ingested dashboard not found")

    if ingested["status"] != "pending":
        return {"message": f"Dashboard already {ingested['status']}"}

    # Create signal mappings from inferred signals
    mappings_created = 0
    source_ref = f"{ingested['backend_name']}:{dashboard_uid}" if ingested.get("backend_name") else dashboard_uid
    for sig in ingested.get("signals_inferred", []):
        # Support both old format (plain signal type string) and new format
        # (dict with signal_type, metric, confidence)
        if isinstance(sig, dict):
            signal_type = sig["signal_type"]
            metric = sig.get("metric", "")
            confidence = sig.get("confidence", 0.6)
            if metric and confidence >= 0.5:
                store.add_mapping(
                    signal_type=signal_type,
                    metric_pattern=metric,
                    confidence=confidence,
                    source_type="dashboard_ingest",
                    source_refs=[source_ref],
                )
                mappings_created += 1
        else:
            # Legacy: plain string signal type — fall back to pattern matching
            signal_type = sig
            for metric in ingested.get("metrics_found", []):
                from dashforge.signals import _metric_matches_pattern

                signal_data = store.get_signal_type(signal_type)
                if signal_data:
                    for mapping in signal_data.get("mappings", []):
                        if _metric_matches_pattern(metric, mapping["metric_pattern"]):
                            store.add_mapping(
                                signal_type=signal_type,
                                metric_pattern=metric,
                                confidence=mapping.get("confidence", 0.6),
                                source_type="dashboard_ingest",
                                source_refs=[source_ref],
                            )
                            mappings_created += 1
                            break

    store.approve_ingested_dashboard(dashboard_uid, backend_name=backend_name)

    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "approved",
        "mappings_created": mappings_created,
        "message": f"Dashboard approved, {mappings_created} signal mapping(s) created",
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
