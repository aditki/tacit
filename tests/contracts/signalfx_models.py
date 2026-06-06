"""Pydantic contract models for the SignalFx (Splunk Observability) v2 REST API.

DashForge reads:
  GET  /v2/metric?query=&limit=&offset=  -> {count, results: [{name, type, ...}]}
DashForge writes:
  POST /v2/chart       {name, programText, options}  -> {id, name, ...}
  POST /v2/dashboard   {name, charts}                 -> {id, name, ...}
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SignalFxMetric(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type: str = "GAUGE"  # GAUGE | COUNTER | CUMULATIVE_COUNTER


class SignalFxMetricSearchResponse(BaseModel):
    count: int = 0
    results: list[SignalFxMetric] = Field(default_factory=list)


class SignalFxChartCreate(BaseModel):
    """POST /v2/chart request body DashForge must send."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    programText: str
    options: dict[str, Any] = Field(default_factory=dict)


class SignalFxChartResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    name: str


class SignalFxDashboardChart(BaseModel):
    """Chart layout entry inside POST /v2/dashboard."""

    model_config = ConfigDict(extra="forbid")

    chartId: str
    column: int
    row: int
    height: int
    width: int


class SignalFxDashboardCreate(BaseModel):
    """POST /v2/dashboard request body DashForge must send."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    groupId: str
    charts: list[SignalFxDashboardChart] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class SignalFxDashboardResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    name: str
