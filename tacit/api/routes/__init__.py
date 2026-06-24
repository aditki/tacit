"""FastAPI route registration."""

from __future__ import annotations

from fastapi import FastAPI

from tacit.api.routes import archetypes, dashboard, feedback, history, learning, signals, system


def include_routes(app: FastAPI) -> None:
    """Attach all API routers to the application."""
    app.include_router(system.router)
    app.include_router(dashboard.router)
    app.include_router(feedback.router)
    app.include_router(archetypes.router)
    app.include_router(history.router)
    app.include_router(signals.router)
    app.include_router(learning.router)
