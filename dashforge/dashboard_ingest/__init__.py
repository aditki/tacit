"""Dashboard ingestion package."""

from __future__ import annotations

from dashforge.dashboard_ingest import service as _service
from dashforge.dashboard_ingest.archetype_generation import (
    escape_literal_braces as escape_literal_braces,
)
from dashforge.dashboard_ingest.archetype_generation import (
    generate_archetype_yaml as generate_archetype_yaml,
)
from dashforge.dashboard_ingest.features import (
    extract_panel_data as extract_panel_data,
)
from dashforge.dashboard_ingest.features import (
    features_to_dict as features_to_dict,
)
from dashforge.dashboard_ingest.features import (
    parse_dashboard_json as parse_dashboard_json,
)
from dashforge.dashboard_ingest.promql import (
    extract_aggregation_patterns as extract_aggregation_patterns,
)
from dashforge.dashboard_ingest.promql import (
    extract_metrics_from_promql as extract_metrics_from_promql,
)
from dashforge.dashboard_ingest.reports import (
    build_learning_impact_report as build_learning_impact_report,
)
from dashforge.dashboard_ingest.reports import (
    build_signal_quality_report as build_signal_quality_report,
)
from dashforge.dashboard_ingest.service import *  # noqa: F403
from dashforge.signals import get_signal_store as get_signal_store

_escape_literal_braces = escape_literal_braces
_features_to_dict = _service._features_to_dict
_extract_panel_data = extract_panel_data

__all__ = [name for name in globals() if not name.startswith("__")]
