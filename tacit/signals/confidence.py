"""Signal trust and confidence policy helpers."""

from __future__ import annotations

TRUST_THRESHOLD = 0.15

REVIEW_STATE_RANK = {"candidate": 0, "approved": 1, "trusted": 2}


def stronger_review_state(existing: str, incoming: str) -> str:
    """Return the higher-trust review state without allowing downgrades."""
    existing_rank = REVIEW_STATE_RANK.get(existing, 0)
    incoming_rank = REVIEW_STATE_RANK.get(incoming, 0)
    return incoming if incoming_rank > existing_rank else existing
