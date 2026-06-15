# ADR-002: Operational intelligence before stateful investigation sessions

## Status

Accepted

## Context

DashForge's current moat is learning operational language: dashboard ingestion, signal inference, signal mappings,
approval/teaching, archetypes, validation, and feedback. The repository does not contain an `InvestigationSession`,
mutable investigation graph, persistent hypothesis state machine, or evidence graph runtime.

## Decision

Near-term work should prioritize operational intelligence quality before full stateful investigation sessions. Avoid
building mutable incident sessions, investigation graphs, or state machines unless they are explicitly marked as future
work.

## Consequences

- New learning features should improve ingestion quality, mapping confidence, provenance, retrieval, and generated
  dashboard usefulness.
- Investigation state should remain lightweight: request history, selected archetypes, metrics, panels, validation
  results, and feedback.
- V2 concepts such as evidence graphs and mutable hypothesis updates should stay out of the core runtime until the
  learning loop is strong.

## Implementation Notes

Implementation status: implemented as a near-term guardrail.

Validated against:

- `dashforge/history.py`: stores request lifecycle and generated dashboard metadata, not mutable investigation sessions.
- `dashforge/archetypes/schema.py`: defines reusable investigation archetypes and panel templates.
- `dashforge/pipeline.py`: has archetype/freeform paths but no stateful investigation graph.
- Repository search finds no `InvestigationSession`, `InvestigationNode`, `HypothesisState`, or persistent evidence graph.

TODO:

- If richer investigation objects are introduced, document them as additive artifacts and keep operational-language
  learning as the near-term priority.

