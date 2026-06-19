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


@dataclass
class DiscoveryStatus:
    """Last discovery attempt status for user-facing failure decisions."""

    available: bool = True
    error: str = ""
    datasource_count: int = 0
    searchable_datasource_count: int = 0


@dataclass
class DashboardFeatures:
    """Vendor-agnostic features extracted from an existing dashboard.

    Every backend returns this common structure from ``ingest_dashboard()``.
    The signal inference engine and archetype generator work against this
    dataclass — zero vendor-specific logic downstream.
    """

    dashboard_uid: str = ""
    dashboard_title: str = ""
    dashboard_tags: list[str] = field(default_factory=list)
    backend_name: str = ""  # 'grafana', 'signalfx', etc.
    query_language: str = ""  # 'promql', 'signalflow', etc.

    # Extracted features
    metrics_found: list[str] = field(default_factory=list)
    panel_count: int = 0
    panel_titles: list[str] = field(default_factory=list)
    row_groups: list[dict] = field(default_factory=list)  # [{"row": "Traffic", "panels": [...]}]
    metric_cooccurrence: dict[str, list[str]] = field(default_factory=dict)
    aggregation_patterns: list[dict] = field(default_factory=list)
    query_transformations: list[str] = field(default_factory=list)
    alert_links: list[str] = field(default_factory=list)
    drilldown_links: list[str] = field(default_factory=list)

    # Per-panel detail (for archetype generation)
    panels: list[dict] = field(default_factory=list)


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

    async def discover_datasource_targets(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        """Return datasource identities usable for query compilation.

        Implementations may return metric entries with an empty ``name`` when
        datasource metadata is available but metric discovery found no series.
        """
        ...

    async def validate_queries(
        self,
        spec: DashboardSpec,
        catalog: list[MetricEntry] | None = None,
    ) -> tuple[DashboardSpec, list[str]]:
        """Validate queries and drop the ones that fail.

        Each query is judged independently on existence, syntax, and data
        presence; failing queries are dropped and a panel survives if any of
        its queries returns data. ``catalog`` (the discovered metrics) enables
        the existence/UID checks; when omitted those checks are skipped.

        Returns (filtered_spec, list_of_warnings).
        """
        ...

    async def publish(
        self,
        spec: DashboardSpec,
    ) -> PublishResult:
        """Create/update the dashboard on this backend."""
        ...

    async def ingest_dashboard(self, uid: str) -> DashboardFeatures:
        """Fetch an existing dashboard and extract operational features.

        Each backend implements its own fetch + parse logic, but returns
        the same ``DashboardFeatures`` structure.  The signal inference
        engine and archetype generator are fully vendor-agnostic.
        """
        ...

    async def list_dashboards(self, limit: int = 500) -> list[dict]:
        """Return dashboard summaries that can be passed to ``ingest_dashboard``.

        Each item should include at least ``uid`` and may include ``title``.
        """
        ...

    async def close(self) -> None:
        """Release HTTP clients and other resources."""
        ...
