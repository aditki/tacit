"""Pydantic contract models for the Grafana CloudWatch datasource resource API.

Tacit calls (POST, via /api/datasources/uid/{uid}/resources/...):
  namespaces       {region}                         -> [namespace, ...]
  metrics          {region, namespace}              -> [metricName, ...]
  dimension-keys   {region, namespace, metricName}  -> [dimensionKey, ...]
"""

from __future__ import annotations

from pydantic import BaseModel


class CloudWatchNamespacesRequest(BaseModel):
    region: str


class CloudWatchMetricsRequest(BaseModel):
    region: str
    namespace: str


class CloudWatchDimensionKeysRequest(BaseModel):
    region: str
    namespace: str
    metricName: str


# All three resource endpoints return a flat JSON array of strings.
CloudWatchNamespacesResponse = list[str]
CloudWatchMetricsResponse = list[str]
CloudWatchDimensionKeysResponse = list[str]
