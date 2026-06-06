"""Pydantic contract models for the InfluxDB v1 query API (via Grafana proxy).

DashForge reads:
  GET .../query?q=SHOW+MEASUREMENTS&db=...
    -> {results: [{series: [{name, columns: ["name"], values: [["cpu"], ...]}]}]}
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class InfluxSeries(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str = "measurements"
    columns: list[str] = Field(default_factory=lambda: ["name"])
    values: list[list[Any]] = Field(default_factory=list)


class InfluxResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    series: list[InfluxSeries] = Field(default_factory=list)


class InfluxQueryResponse(BaseModel):
    results: list[InfluxResult] = Field(default_factory=list)
