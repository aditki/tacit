"""FastAPI app factory and OpenAPI metadata."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from tacit import __version__
from tacit.config import Settings
from tacit.config import settings as default_settings
from tacit.runtime_stores import RuntimeStores

LifespanFactory = Any

OPENAPI_TAGS = [
    {
        "name": "Investigation Generation",
        "description": "Generate evidence-grounded observability investigations from natural-language prompts. "
        "The pipeline: Intent Classification → Metric Discovery → Query Building → Artifact Publishing.",
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
        "Curated archetypes are loaded from packaged data or `TACIT_ARCHETYPES_PATH` "
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
        "description": "Learn operational patterns from trusted dashboards and alerts. "
        "Ingests dashboards, extracts metric co-occurrence, panel groupings, "
        "and aggregation patterns, then proposes governed signal mappings. "
        "Generated archetype output is quarantined and disabled by default.",
    },
    {
        "name": "System",
        "description": "Health checks and system status.",
    },
]

DESCRIPTION = (
    "## Evidence-Grounded Incident Investigation\n\n"
    "Tacit is a multi-agent pipeline that turns plain-English incident descriptions "
    "and trusted operational context into validated observability investigations. "
    "It supports Grafana and SignalFx outputs, works across common datasource types "
    "(Prometheus, CloudWatch, Loki, Elasticsearch, Graphite, InfluxDB, etc.), and uses "
    "LLM-powered intent classification, cross-datasource metric discovery, and "
    "deterministic query building.\n\n"
    "### Key capabilities\n"
    "- **Investigation generation** — describe the incident, get validated evidence artifacts\n"
    "- **Feedback loop** — rate dashboards, and the system automatically improves metric selection\n"
    "- **Curated archetype management** — edit operator-authored templates via YAML and hot-reload without restart; "
    "generated output remains quarantined\n\n"
    "### Authentication\n"
    "When `API_AUTH_ENABLED=true`, pass your key via the `X-API-Key` header. "
    "When disabled (default for development), all endpoints are open.\n\n"
    "### Interactive docs\n"
    "- **Swagger UI** — you are here (`/docs`)\n"
    "- **ReDoc** — alternative view at [`/redoc`](/redoc)\n"
    "- **Web UI** — interactive investigation workspace at [`/`](/)\n"
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
    app.state.runtime_stores = RuntimeStores(runtime_settings)
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
