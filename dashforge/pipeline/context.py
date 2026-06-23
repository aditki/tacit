"""Typed state shared by pipeline orchestration stages."""

from __future__ import annotations

from dataclasses import dataclass, field

from dashforge.agents.providers.base import TokenUsage
from dashforge.backends.base import DashboardBackend
from dashforge.config import Settings
from dashforge.dependencies import PipelineDependencies
from dashforge.models.schemas import DashRequest
from dashforge.pipeline.recording import PipelineRecorder


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
