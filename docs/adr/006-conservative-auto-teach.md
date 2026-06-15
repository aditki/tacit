# ADR-006: Conservative auto-teach prevents learned mapping poisoning

## Status

Accepted

## Context

Dashboard ingestion can infer many plausible metric-to-signal mappings. Some are strong; some are weak guesses. Bad
auto-teaching would poison future metric resolution and make generated dashboards less trustworthy.

The current implementation distinguishes bootstrap/trusted, heuristic candidate, approved, rejected, and manually taught
mappings. Heuristic signals must pass the `auto_teach_eligible` gate before they are persisted as approved mappings.

## Decision

Auto-teach should remain conservative. Strong candidates may be auto-approved in controlled flows; weak candidates should
stay reviewable and should not appear as approved learned context.

## Consequences

- Auto-teach eligibility must be based on score, margin, evidence diversity, or explicit strong metric names.
- Review state must be visible and queryable.
- Rejection should retain useful negative examples for later tuning.

## Implementation Notes

Implementation status: implemented.

Validated against:

- `dashforge/signal_inference.py`: computes score, confidence, margin, evidence, and `auto_teach_eligible`.
- `dashforge/dashboard_ingest.py`: `persist_inferred_signal_review` only teaches eligible heuristic candidates.
- `dashforge/signals.py`: stores `review_state`, rejected candidates, feedback counters, and confidence decay.
- `dashforge/main.py`: exposes approve/reject/ignore endpoints.
- Tests cover auto-teach gates, rejection, and review-state behavior.

TODO:

- Continue adding regression tests around held candidates so approved-only retrieval never treats refused candidates as
  trusted mappings.

