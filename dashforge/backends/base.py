"""Base protocol and types for dashboard backend adapters.

Every vendor (Grafana, SignalFx, etc.) implements `DashboardBackend`.
The pipeline calls the same interface regardless of vendor.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from dashforge.models.schemas import DashboardSpec, Intent, MetricEntry


@dataclass
class PublishResult:
    """Outcome of publishing a dashboard to a backend."""
    url: str = ""
    uid: str = ""
    backend_name: str = ""


@runtime_checkable
class DashboardBackend(Protocol):
    """Common interface every dashboard vendor must implement."""

    @property
    def name(self) -> str:
        """Short identifier: 'grafana', 'signalfx', etc."""
        ...

    @property
    def query_language(self) -> str:
        """Target query language: 'promql', 'signalflow', etc."""
        ...

    async def discover_metrics(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        """Find metrics relevant to the investigation."""
        ...

    async def validate_queries(
        self,
        spec: DashboardSpec,
    ) -> tuple[DashboardSpec, list[str]]:
        """Drop panels whose queries return no data.

        Returns (filtered_spec, list_of_warnings).
        """
        ...

    async def publish(
        self,
        spec: DashboardSpec,
    ) -> PublishResult:
        """Create/update the dashboard on this backend."""
        ...

    async def close(self) -> None:
        """Release HTTP clients and other resources."""
        ...
