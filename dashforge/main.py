"""DashForge – FastAPI entrypoint + Slack bot startup."""
from __future__ import annotations

import asyncio
import logging
import secrets
from contextlib import asynccontextmanager

from pathlib import Path

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader

from dashforge.config import settings

_STATIC_DIR = Path(__file__).parent / "static"
from dashforge.models.schemas import DashRequest, DashResponse
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
    description="Natural language → Grafana dashboards",
    version="0.1.0",
    lifespan=lifespan,
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


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/")
async def web_ui():
    return FileResponse(_STATIC_DIR / "index.html", media_type="text/html")


@app.post("/api/v1/chart", response_model=DashResponse, dependencies=[Depends(verify_api_key)])
async def create_chart(request: DashRequest):
    """Generate a Grafana dashboard from a natural-language prompt."""
    request.prompt = _sanitize_prompt(request.prompt)
    if not request.prompt:
        raise HTTPException(status_code=400, detail="prompt is required")
    try:
        return await run_pipeline(request)
    except Exception:
        logger.exception("api_pipeline_error")
        raise HTTPException(status_code=500, detail="Failed to generate dashboard")


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
