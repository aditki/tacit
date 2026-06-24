# ADR-015: Evidence has a lightweight lifecycle

## Status

Accepted

## Context

The GAMMA pre-evidence-model baseline showed that Tacit can semantically map and bind present CPU/memory signals,
while still losing application symptom evidence before it survives into validated panels. The important missing model is
not an investigation graph or an agent state machine. It is explicit evidence.

The current pipeline largely reasons through metric to query to panel. Large-dataset evaluation needs finer accounting:
which evidence was required, which live signal resolved it, and whether the resulting query survived validation.

## Decision

Represent evidence as a lightweight lifecycle:

1. `EvidenceRequirement`: the investigation needs a signal or metric.
2. `EvidenceResolution`: the requirement resolved to a live metric/datasource, or abstained with a reason.
3. `EvidenceObservation`: the resolved evidence appeared in a validated, non-empty query/panel.

Fallback must produce evidence candidates, not panels directly. Culprit ranking must consume observed evidence, not raw
metric names alone.

## Consequences

- Evidence survival can be measured independently from dashboard creation.
- Metric meaning, ownership, and observation become separable diagnostics.
- Guarded fallback has a trust boundary: unresolved requirement to candidate resolution to validation.
- The design deliberately avoids `InvestigationSession`, `HypothesisGraph`, and agent orchestration until evidence
  retrieval is reliable.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `tacit/models/schemas.py`: defines requirement, resolution, and observation models.
- `tacit/evidence.py`: derives evidence needs from selected archetypes and records resolution/observation counts.
- `tacit/pipeline.py`: records a reason-coded `evidence` diagnostic stage after validation.
- `docs/results/gamma-pre-evidence-model-baseline-2026-06-22.md`: freezes the pre-model target that evidence survival
  should improve next.
