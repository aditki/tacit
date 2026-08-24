"""Explicit state-machine rules for governed Operational Knowledge."""

from __future__ import annotations

from typing import Final

from tacit.knowledge.enums import KnowledgeEligibility, LifecycleStatus, ReviewState
from tacit.knowledge.models import KnowledgeState

REVIEW_TRANSITIONS: Final[dict[ReviewState, frozenset[ReviewState]]] = {
    ReviewState.CANDIDATE: frozenset({ReviewState.APPROVED, ReviewState.TRUSTED, ReviewState.REJECTED}),
    ReviewState.APPROVED: frozenset({ReviewState.TRUSTED, ReviewState.REJECTED}),
    ReviewState.TRUSTED: frozenset(),
    ReviewState.REJECTED: frozenset(),
}

LIFECYCLE_TRANSITIONS: Final[dict[LifecycleStatus, frozenset[LifecycleStatus]]] = {
    LifecycleStatus.ACTIVE: frozenset(
        {
            LifecycleStatus.STALE,
            LifecycleStatus.SUPERSEDED,
            LifecycleStatus.EXPIRED,
            LifecycleStatus.WITHDRAWN,
        }
    ),
    LifecycleStatus.STALE: frozenset({LifecycleStatus.ACTIVE}),
    LifecycleStatus.SUPERSEDED: frozenset(),
    LifecycleStatus.EXPIRED: frozenset(),
    LifecycleStatus.WITHDRAWN: frozenset(),
}


def transition_review_state(state: KnowledgeState, target: ReviewState) -> KnowledgeState:
    """Apply a legal review transition and reset policy-derived eligibility."""
    if target == state.review_state:
        return state
    if target not in REVIEW_TRANSITIONS[state.review_state]:
        raise ValueError(f"cannot transition review state from {state.review_state.value} to {target.value}")
    return KnowledgeState(
        review_state=target,
        lifecycle_status=state.lifecycle_status,
        eligibility=KnowledgeEligibility.INELIGIBLE,
    )


def transition_lifecycle_state(state: KnowledgeState, target: LifecycleStatus) -> KnowledgeState:
    """Apply a legal source lifecycle transition and require reevaluation."""
    if target == state.lifecycle_status:
        return state
    if target not in LIFECYCLE_TRANSITIONS[state.lifecycle_status]:
        raise ValueError(f"cannot transition lifecycle from {state.lifecycle_status.value} to {target.value}")
    return KnowledgeState(
        review_state=state.review_state,
        lifecycle_status=target,
        eligibility=KnowledgeEligibility.INELIGIBLE,
    )
