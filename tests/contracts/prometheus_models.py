"""Pydantic contract models for the Prometheus HTTP API (via Grafana proxy).

DashForge reads:
  GET .../api/v1/label/__name__/values  -> {status, data: [metric names]}
  GET .../api/v1/series?match[]=metric  -> {status, data: [{__name__, label: val}]}
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class PrometheusLabelValuesResponse(BaseModel):
    status: str = "success"
    data: list[str] = Field(default_factory=list)


class PrometheusSeriesResponse(BaseModel):
    status: str = "success"
    # Each series is a label set; __name__ carries the metric name.
    data: list[dict[str, str]] = Field(default_factory=list)
