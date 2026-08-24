"""Versioned, deterministic promotion policy interface."""

from __future__ import annotations

from typing import Protocol

from tacit.knowledge.enums import KnowledgeKind
from tacit.knowledge.models import KnowledgeCandidate, PromotionContext, PromotionDecision


class PromotionPolicy(Protocol):
    policy_id: str
    version: str
    knowledge_kind: KnowledgeKind

    def evaluate(self, candidate: KnowledgeCandidate, context: PromotionContext) -> PromotionDecision: ...
