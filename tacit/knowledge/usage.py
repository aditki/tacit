"""Typed, transient records of exact Operational Knowledge stage effects."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


@dataclass(frozen=True, order=True)
class KnowledgeRevisionRef:
    """Stable identity of the immutable revision consumed by a runtime stage."""

    knowledge_ref: str
    knowledge_revision: int

    def __post_init__(self) -> None:
        if not self.knowledge_ref:
            raise ValueError("knowledge_ref is required")
        if self.knowledge_revision < 1:
            raise ValueError("knowledge_revision must be positive")


class KnowledgeUsageStage(StrEnum):
    ARCHETYPE_SELECTION = "archetype_selection"


class KnowledgeUsageEffect(StrEnum):
    ARCHETYPE_SELECTED_BY_LIVE_COVERAGE = "archetype_selected_by_live_coverage"


@dataclass(frozen=True)
class KnowledgeStageUse:
    """One immutable knowledge revision confirmed to affect a stage output."""

    revision_ref: KnowledgeRevisionRef
    stage: KnowledgeUsageStage
    effect: KnowledgeUsageEffect
    target_ref: str

    @property
    def knowledge_ref(self) -> str:
        return self.revision_ref.knowledge_ref

    @property
    def knowledge_revision(self) -> int:
        return self.revision_ref.knowledge_revision
