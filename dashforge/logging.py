"""Structured logging helpers for per-stage pipeline telemetry.

Every pipeline stage emits a ``stage_complete`` event with a consistent
schema: request_id, stage name, latency_ms, token_count, plus
stage-specific metadata.  This module provides:

- ``bind_request_id()``  — generates and binds a request_id to structlog
  context vars so all downstream log lines include it automatically.
- ``unbind_request_id()`` — clears the request_id binding.
- ``stage_log()``         — emits the canonical ``stage_complete`` event.
- ``configure_logging()`` — one-call structlog configuration for prod use.
"""

from __future__ import annotations

import logging
import uuid

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from dashforge.agents.providers.base import TokenUsage

logger = structlog.get_logger()


def generate_request_id() -> str:
    """Generate a short, URL-safe request id."""
    return uuid.uuid4().hex[:12]


def bind_request_id(request_id: str | None = None) -> str:
    """Bind a request_id into structlog context vars.

    If *request_id* is None a new one is generated.
    Returns the bound request_id.
    """
    rid = request_id or generate_request_id()
    bind_contextvars(request_id=rid)
    return rid


def unbind_request_id() -> None:
    """Remove request_id from structlog context vars."""
    unbind_contextvars("request_id")


def stage_log(
    stage: str,
    latency_ms: float,
    *,
    token_usage: TokenUsage | None = None,
    **extra,
) -> None:
    """Emit a canonical ``stage_complete`` structured log event.

    The ``request_id`` is pulled automatically from structlog context vars
    (set once at the start of ``run_pipeline``).

    Args:
        stage: Pipeline stage name, e.g. ``"intent"``, ``"metric_ranking"``.
        latency_ms: Wall-clock time for the stage in milliseconds.
        token_usage: LLM token counts (prompt + completion + total).
        **extra: Stage-specific key-value pairs (e.g.
            ``metrics_considered=284, metrics_selected=18``).
    """
    event_data: dict = {
        "stage": stage,
        "latency_ms": round(latency_ms, 1),
    }
    if token_usage is not None:
        event_data["prompt_tokens"] = token_usage.prompt_tokens
        event_data["completion_tokens"] = token_usage.completion_tokens
        event_data["token_count"] = token_usage.total_tokens

    event_data.update(extra)
    logger.info("stage_complete", **event_data)


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structlog for production JSON output with context vars.

    Call once at startup (``main.py``).  All log events will include:
    - ``timestamp``
    - ``level``
    - ``event``
    - ``request_id`` (when bound via ``bind_request_id``)
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
