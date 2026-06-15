# ADR-008: Dashboards are evidence artifacts, not the sole product

## Status

Accepted

## Context

Dashboards are the current concrete output of DashForge. They are valuable because operators can inspect and share them.
However, the repo already treats dashboards as products of intent, archetypes, learned signals, metric discovery,
validation, provenance, and feedback.

## Decision

Dashboards should be modeled and documented as evidence artifacts generated from investigation context. They are an
important output, not the entire product.

## Consequences

- Dashboard specs should preserve why panels were selected through provenance/history where possible.
- API and CLI surfaces should gradually expose more investigation context alongside URLs.
- Docs should avoid reducing DashForge to only "prompt to dashboard."

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `dashforge/models/schemas.py`: `DashResponse` primarily returns dashboard URL, UID, panel count, summary, backend URLs,
  path, and archetypes.
- `dashforge/pipeline.py`: records provenance and validation decisions.
- `dashforge/feedback.py`: connects generated dashboards to human usefulness feedback.
- `README.md` and `docs/operational-cognition.md`: both frame dashboards as part of an investigation workflow.

TODO:

- Consider adding explicit selected-signal/missing-evidence fields to response models when those are stable.

