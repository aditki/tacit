"""System and static UI routes."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

from tacit.models.schemas import HealthResponse

router = APIRouter()
_STATIC_DIR = Path(__file__).resolve().parents[2] / "static"


@router.get(
    "/healthz",
    tags=["System"],
    summary="Health check",
    response_model=HealthResponse,
    response_description="Server health status",
)
async def healthz():
    """Lightweight health check for load balancers and orchestrators."""
    return {"status": "ok"}


@router.get("/", include_in_schema=False)
async def web_ui():
    return FileResponse(_STATIC_DIR / "index.html", media_type="text/html")
