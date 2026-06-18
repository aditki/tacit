# ADR-007: Investigation plans are useful, but should remain lightweight for now

## Status

Accepted

## Context

DashForge has investigation archetypes, intent classification, panel templates, selected metrics, history, validation,
and dashboard provenance. It does not currently expose a standalone `InvestigationPlan`, `EvidenceItem`, hypothesis
state, coverage model, or mutable investigation session object in `dashforge/models/schemas.py`.

## Decision

Investigation planning should remain lightweight for now. The product should use archetypes, selected signals, missing
metrics, validation warnings, history, and feedback before adding a full mutable session engine.

## Consequences

- Generated dashboards should reflect an investigation path.
- Plan-like information should be explainable, but not overrepresented as autonomous reasoning.
- Future richer objects should be introduced incrementally and validated by usefulness, not architectural novelty.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `dashforge/archetypes/schema.py`: reusable investigation archetypes and panels.
- `dashforge/archetypes/engine.py`: deterministic compilation of archetypes into dashboard specs.
- `dashforge/pipeline.py`: selected archetypes and path information guide generated dashboards.
- `dashforge/history.py`: persists investigation lifecycle details.
- No first-class `InvestigationPlan` model exists in `dashforge/models/schemas.py`.

TODO:

- If API responses add investigation-plan output, label it as selected investigation context unless it is backed by real
  model reasoning/evidence evaluation.

