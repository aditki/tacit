# ADR-001: DashForge is investigation-first, not dashboard-only

## Status

Accepted

## Context

DashForge began as natural language to Grafana dashboards, but the repository now frames the problem as reducing
incident-investigation cognitive load. `README.md` describes the "where should I look next?" problem, and
`docs/operational-cognition.md` explicitly says dashboards are necessary but not sufficient.

The current product output is still primarily a dashboard URL and generated dashboard spec. Investigation structure is
represented through intent classification, archetype selection, deterministic panel ordering, validation, provenance,
history, and feedback rather than a separate investigation artifact.

## Decision

DashForge should be positioned as an investigation-first observability system. Grafana and SignalFx dashboards remain
core artifacts, but they should be described as generated investigation views, not as the whole product.

## Consequences

- README and docs should keep emphasizing investigation path, operational semantics, query validation, provenance, and
  feedback.
- Dashboard generation should be judged by whether it helps an operator make progress during an incident.
- Future APIs may add explicit investigation-plan artifacts, but they should build on current archetype/history/feedback
  foundations rather than replacing them with vague agent behavior.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `README.md`: strongly frames on-call navigation and investigation, but the headline remains "Natural language →
  observability dashboards."
- `docs/operational-cognition.md`: directly supports the investigation-first framing.
- `dashforge/pipeline.py`: records intent, path used, selected metrics, validation, dashboard URLs, and provenance.
- `dashforge/history.py`: persists investigation lifecycle details.
- `dashforge/models/schemas.py`: `DashResponse` remains dashboard-centered.

TODO:

- Add an explicit lightweight investigation context/plan field to API responses if the product wants the API contract to
  fully match the investigation-first positioning.
- Keep README wording honest: DashForge is demoable/experimental and not production-ready.

