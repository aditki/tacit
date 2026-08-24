"""Investigation archetype schema.

An archetype defines a known investigation pattern (e.g. latency, error spike)
with pre-defined panel templates and deterministic query patterns.
The LLM classifies the problem → archetype, then fills parameters —
it does NOT invent the dashboard structure.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

MAX_ARCHETYPE_REQUIRED_METRICS = 64
MAX_ARCHETYPE_REQUIRED_SIGNALS = 64
MAX_ARCHETYPE_SIGNAL_BINDINGS = 64
MAX_ARCHETYPE_SIGNAL_REQUIREMENTS = 64
MAX_ARCHETYPE_TAGS = 64
MAX_ARCHETYPE_PANELS = 256
MAX_ARCHETYPE_QUERIES_PER_PANEL = 1_024
MAX_ARCHETYPE_TOTAL_QUERIES = 1_024


class QueryTemplate(BaseModel):
    """A parameterised query expression.

    Placeholders use {service}, {status_regex}, {rate_interval} etc.
    These are resolved at runtime from the intent + metric catalog.
    """

    expr: str = Field(description="Query template with {placeholders}")
    legend_format: str = ""
    query_language: str = "promql"
    datasource_type: str = "prometheus"
    cloudwatch_namespace: str = ""
    cloudwatch_stat: str = ""
    cloudwatch_dimensions: dict[str, str | list[str]] = Field(default_factory=dict)
    cloudwatch_region: str = ""


class PanelTemplate(BaseModel):
    """A pre-defined panel in an archetype."""

    title: str
    description: str = ""
    panel_type: str = "timeseries"
    row: str = Field(default="", description="Section/row grouping name")
    queries: list[QueryTemplate] = Field(max_length=MAX_ARCHETYPE_QUERIES_PER_PANEL)
    unit: str = ""


class InvestigationArchetype(BaseModel):
    """A complete investigation template.

    Defines what panels to create, what metrics are needed, and how
    to compile queries deterministically from known patterns.
    """

    id: str = Field(description="Unique archetype ID, e.g. 'latency_investigation'")
    name: str = Field(description="Human-readable name")
    description: str = ""
    problem_types: list[str] = Field(
        description="Intent problem_type values that map to this archetype, "
        "e.g. ['latency_investigation', 'slow_requests']"
    )
    required_metrics: list[str] = Field(
        default_factory=list,
        max_length=MAX_ARCHETYPE_REQUIRED_METRICS,
        description="Literal metric names this archetype needs, "
        "e.g. ['http_requests_total', 'http_request_duration_seconds']",
    )
    required_signals: list[str] = Field(
        default_factory=list,
        max_length=MAX_ARCHETYPE_REQUIRED_SIGNALS,
        description="Semantic signal types this archetype needs, "
        "e.g. ['request_latency', 'error_rate']. Resolved to actual "
        "metrics at compile time via the signal mapping store.",
    )
    signal_bindings: dict[str, str] = Field(
        default_factory=dict,
        max_length=MAX_ARCHETYPE_SIGNAL_BINDINGS,
        description="Maps signal_type → default_metric_name used in query templates. "
        "When the default metric is missing from the catalog, the signal "
        "resolution engine finds the best alternative. "
        "e.g. {'request_latency': 'http_request_duration_seconds', "
        "'error_rate': 'http_requests_total'}",
    )
    panels: list[PanelTemplate] = Field(max_length=MAX_ARCHETYPE_PANELS)
    tags: list[str] = Field(default_factory=list, max_length=MAX_ARCHETYPE_TAGS)
    default_timerange: str = "1h"

    @model_validator(mode="after")
    def bound_coverage_signal_requirements(self) -> InvestigationArchetype:
        signal_requirements = set(self.required_signals) | set(self.signal_bindings)
        if len(signal_requirements) > MAX_ARCHETYPE_SIGNAL_REQUIREMENTS:
            raise ValueError("combined required_signals and signal_bindings exceed the archetype limit")
        total_queries = sum(len(panel.queries) for panel in self.panels)
        if total_queries > MAX_ARCHETYPE_TOTAL_QUERIES:
            raise ValueError("combined panel queries exceed the archetype limit")
        return self
