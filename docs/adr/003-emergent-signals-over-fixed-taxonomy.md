# ADR-003: Signals should emerge from observed usage, not a fixed vendor taxonomy

## Status

Accepted

## Context

The repository currently ships `tacit/data/signals.yaml` with canonical signal categories and bootstrap metric
patterns. That is useful for demos and common Prometheus-style telemetry, but it is not enough for organization-specific
metrics such as custom checkout latency, Envoy, JVM, Redis, Kafka, or vendor-specific naming conventions.

Tacit also has a SQLite signal store, dashboard ingestion, manual teach APIs, feedback adjustment, confidence decay,
context filters, and rejected-candidate storage. That means the shipped taxonomy is already only one source of learning,
not the only source of truth.

## Decision

Tacit should treat the packaged taxonomy as bootstrap guidance, not a rigid universal ontology. The system should
learn operational semantics from observed usage: existing dashboards, explicit teaching, feedback, and review outcomes.

## Consequences

- Bootstrap signals are acceptable, but docs should call them seed patterns.
- Learned mappings need provenance, review state, confidence, and context scope.
- Unknown or weakly inferred signals should remain candidates until reviewed.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `tacit/data/signals.yaml`: provides bootstrap categories and patterns.
- `tacit/signals.py`: persists mappings, confidence, context filters, feedback counts, review state, rejected
  candidates, and SQLite-backed learning state.
- `tacit/dashboard_ingest.py`: infers signals from ingested dashboards and records candidates.
- `tacit/main.py`: exposes teach, learn, approve/reject/ignore endpoints.
- `README.md`: still describes a "Signal Taxonomy" prominently, which is accurate but should be read as bootstrap data.

TODO:

- Keep README wording clear that the packaged taxonomy is a bootstrap layer, not a universal truth.
- Continue improving learned mapping retrieval and review UX so organization-specific mappings become the operational
  source of truth.

