# ADR-006: Conservative auto-teach prevents learned mapping poisoning

## Status

Accepted

Amended by [ADR-019](019-governed-knowledge-authority.md). "Auto-teach" is a
legacy name for automatic candidate qualification. It is not permission to
activate learned behavior directly, and it never approves or registers a
generated archetype.

## Context

Dashboard ingestion can infer many plausible metric-to-signal mappings. Some are strong; some are weak guesses. Direct
automatic activation would poison future metric resolution and make generated dashboards less trustworthy.

The inference layer distinguishes bootstrap/trusted, heuristic candidate,
approved, rejected, and manually taught mappings. Heuristic signals must pass the
`auto_teach_eligible` gate before a controlled flow may submit them for governed
evaluation. Passing the gate alone does not make a mapping active.

## Decision

Automatic candidate qualification should remain conservative. Controlled flows
may request automated approval for eligible signal mappings, but Operational
Knowledge policy remains the authority for promotion and runtime projection. Weak
candidates stay reviewable and must not appear as trusted learned context.

Generated archetypes are outside this mechanism. Their creation is disabled by
default, and explicitly generated output remains quarantined with no auto-approval
or normal-retrieval path.

## Consequences

- Candidate eligibility must be based on score, margin, evidence diversity, or explicit strong metric names.
- Review state must be visible and queryable.
- Rejection should retain useful negative examples for later tuning.
- Runtime activation must require a governed, eligible knowledge revision.

## Implementation Notes

Implementation status: the conservative inference gate is implemented. The
governed-authority and projection consolidation is tracked by ADR-019.

Validated against:

- `tacit/signal_inference.py`: computes score, confidence, margin, evidence, and `auto_teach_eligible`.
- `tacit/dashboard_ingest`: submits eligible heuristic candidates through the learning workflow.
- `tacit/signals`: stores review state, rejected candidates, feedback counters, and confidence decay.
- `tacit/api/routes/learning.py`: exposes approve/reject/ignore endpoints.
- Tests cover candidate-eligibility gates, rejection, and review-state behavior.

TODO:

- Continue adding regression tests around held candidates so approved-only retrieval never treats refused candidates as
  trusted mappings.
