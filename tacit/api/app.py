"""FastAPI app factory and OpenAPI metadata."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from tacit import __version__
from tacit.config import Settings
from tacit.config import settings as default_settings

LifespanFactory = Any

OPENAPI_TAGS = [
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
        "Archetypes are loaded from packaged data or `TACIT_ARCHETYPES_PATH` "
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
]

DESCRIPTION = (
    "## Natural Language → Grafana Dashboards\n\n"
    "Tacit is a multi-agent pipeline that converts plain-English incident descriptions "
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
)


def create_app(
    *,
    runtime_settings: Settings = default_settings,
    lifespan: LifespanFactory | None = None,
    include_default_routes: bool = True,
) -> FastAPI:
    """Create the FastAPI app shell.

    Route modules can attach handlers to the returned app. Keeping app
    construction here separates app metadata/middleware from route business
    logic and gives tests a small factory to exercise.
    """
    app = FastAPI(
        title="Tacit",
        description=DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
        openapi_tags=OPENAPI_TAGS,
    )
    app.state.settings = runtime_settings
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    if include_default_routes:
        from tacit.api.routes import include_routes

        include_routes(app)
    return app
