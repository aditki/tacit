"""Pydantic contract models for the Grafana HTTP API.

These capture the exact request/response shapes Tacit depends on
(`tacit/grafana/client.py`, `grafana/dashboard.py`, `grafana/datasource.py`).
They are the single source of truth for hermetic mocks: a factory builds a
payload *through* these models, so if Grafana renames a field we only update it
here and every dependent test breaks loudly.

Field names mirror the wire (camelCase) via aliases so the same model validates
both Grafana's real responses and Tacit's outgoing requests.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class GrafanaDatasource(BaseModel):
    """An element of GET /api/datasources."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    uid: str
    name: str
    type: str
    url: str = ""
    is_default: bool = Field(default=False, alias="isDefault")
    json_data: dict[str, Any] = Field(default_factory=dict, alias="jsonData")
    id: int | None = None


class GrafanaDashboardModel(BaseModel):
    """The `dashboard` object (GET response and POST body share this shape)."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    uid: str | None = None
    title: str
    tags: list[str] = Field(default_factory=list)
    panels: list[dict[str, Any]] = Field(default_factory=list)
    schema_version: int = Field(default=39, alias="schemaVersion")
    timezone: str = "browser"


class GrafanaDashboardMeta(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str = "db"
    slug: str = ""
    url: str = ""
    folder_uid: str = Field(default="", alias="folderUid")
    version: int = 1


class GrafanaDashboardEnvelope(BaseModel):
    """GET /api/dashboards/uid/{uid}."""

    meta: GrafanaDashboardMeta
    dashboard: GrafanaDashboardModel


class GrafanaFolder(BaseModel):
    """Element of GET /api/folders and POST /api/folders response."""

    model_config = ConfigDict(extra="allow")

    uid: str
    title: str
    id: int | None = None
    url: str = ""


class GrafanaDashboardSaveCommand(BaseModel):
    """POST /api/dashboards/db request body that Tacit must send.

    ``extra="forbid"`` so an unexpected/renamed key in our outgoing payload is
    caught by contract validation.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    dashboard: GrafanaDashboardModel
    folder_uid: str = Field(default="", alias="folderUid")
    overwrite: bool = False
    message: str | None = None


class GrafanaDashboardSaveResponse(BaseModel):
    """POST /api/dashboards/db response."""

    model_config = ConfigDict(extra="allow")

    id: int
    uid: str
    url: str
    status: str = "success"
    slug: str = ""
    version: int = 1
