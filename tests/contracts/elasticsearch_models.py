"""Pydantic contract models for the Elasticsearch / OpenSearch _mapping API.

Tacit reads (via Grafana proxy):
  GET .../{index}/_mapping -> {index: {mappings: {properties: {field: {type}}}}}
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, RootModel


class ESFieldType(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str = "keyword"


class ESMappings(BaseModel):
    model_config = ConfigDict(extra="allow")

    properties: dict[str, ESFieldType] = Field(default_factory=dict)


class ESIndexMapping(BaseModel):
    model_config = ConfigDict(extra="allow")

    mappings: ESMappings


class ESMappingResponse(RootModel[dict[str, ESIndexMapping]]):
    """Top level is keyed by index name."""
