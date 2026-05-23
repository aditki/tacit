"""DashForge – FastAPI entrypoint + Slack bot startup."""
from __future__ import annotations

import asyncio
import logging
import secrets
from contextlib import asynccontextmanager

from pathlib import Path

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Path as PathParam, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader

from dashforge.config import settings

_STATIC_DIR = Path(__file__).parent / "static"
from dashforge.models.schemas import (
    DashRequest,
    DashResponse,
    FeedbackRequest,
    FeedbackResponse,
    FeedbackStatsResponse,
    HealthResponse,
    ArchetypeListResponse,
    ArchetypeReloadResponse,
)
from dashforge.feedback import get_feedback_store
from dashforge.pipeline import run_pipeline

logger = structlog.get_logger()

_slack_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the Slack bot alongside the API server."""
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
    response_description="Aggregate stats: total feedback, dashboards reviewed, useful rate, average dimensional scores",
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
    from dashforge.archetypes.templates import reload_archetypes, ALL_ARCHETYPES
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


def main():
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
    )
    uvicorn.run(
        "dashforge.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
