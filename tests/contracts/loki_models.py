"""Pydantic contract models for the Loki HTTP API (via Grafana proxy).

Tacit reads:
  GET .../loki/api/v1/labels -> {status, data: [label names]}
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class LokiLabelsResponse(BaseModel):
    status: str = "success"
    data: list[str] = Field(default_factory=list)
