"""Metric discovery stage."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from tacit.backends.base import DashboardBackend
from tacit.logging import stage_log
from tacit.models.schemas import Intent
from tacit.pipeline.discovery import (
    DiscoveryResult,
    discover_catalogs,
    discovery_stage_status,
    semantic_mapping_diagnostics,
)
from tacit.pipeline.recording import PipelineRecorder


@dataclass(frozen=True)
class DiscoveryStageResult:
    discovery: DiscoveryResult
    confirmed_keywords: list[str]


async def run_discovery_stage(
    *,
    backends: list[DashboardBackend],
    primary: DashboardBackend,
    intent: Intent,
    timings: dict[str, float],
    recorder: PipelineRecorder,
    signal_store: Any | None = None,
) -> DiscoveryStageResult:
    """Discover catalogs and record discovery diagnostics."""
    from tacit.pipeline.discovery import confirm_colloquial_keywords

    t0 = time.monotonic()
    discovery = await discover_catalogs(backends, intent)
    timings["metrics_fetch"] = time.monotonic() - t0
    stage_log(
        "metrics_fetch",
        (time.monotonic() - t0) * 1000,
        backends_queried=len(backends),
        datasource_types=discovery.datasource_types,
        metrics_found=len(discovery.metric_catalog),
        datasource_targets_found=len(discovery.datasource_catalog),
    )
    recorder.discovery(discovery)

    status, reason, details = discovery_stage_status(discovery)
    recorder.stage("discovery", status, reason, **details)

    status, reason, details = semantic_mapping_diagnostics(discovery.metric_catalog)
    recorder.stage("semantic_mapping", status, reason, **details)

    confirmed_keywords = confirm_colloquial_keywords(
        intent,
        discovery.metric_catalog,
        primary.query_language,
        signal_store,
    )
    return DiscoveryStageResult(discovery=discovery, confirmed_keywords=confirmed_keywords)
