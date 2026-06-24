"""Typed state shared by pipeline orchestration stages."""

from __future__ import annotations

from dataclasses import dataclass, field

from tacit.agents.providers.base import TokenUsage
from tacit.backends.base import DashboardBackend
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies
from tacit.models.schemas import DashRequest
from tacit.pipeline.recording import PipelineRecorder


@dataclass
class PipelineRunContext:
    """Mutable runtime state for one pipeline request."""

    request: DashRequest
    deps: PipelineDependencies
    settings: Settings
    backends: list[DashboardBackend]
    primary: DashboardBackend
    history: object
    investigation_id: str
    recorder: PipelineRecorder
    started_at: float
    timings: dict[str, float] = field(default_factory=dict)
    token_usage: TokenUsage = field(default_factory=TokenUsage)

    def add_tokens(self, usage: TokenUsage) -> None:
        """Accumulate model token usage for the request."""
        self.token_usage = self.token_usage + usage
