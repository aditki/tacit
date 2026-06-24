# ADR-004: Dashboard ingestion is the primary onboarding path for custom telemetry

## Status

Accepted

## Context

The highest-leverage adoption story is: point Tacit at existing dashboards and let it learn how the organization
already investigates. The repo supports dashboard ingestion through backend adapters and uploaded JSON parsing, and the
Web UI includes a Learning tab.

The CLI in the current `main` branch does not yet expose `tacit learn dashboard <uid>` or bulk `tacit learn
grafana/signalfx`; ingestion is available through API and UI.

## Decision

Dashboard ingestion should be the primary onboarding path for custom telemetry. It should extract metrics, panel
groupings, query patterns, inferred signals, provenance, and reviewable mappings.

## Consequences

- Ingestion output should be explainable and reviewable.
- Approval should activate trusted mappings; rejection should preserve negative training data where applicable.
- Bulk dashboard crawling is a natural next step for the adoption/demo story, but it should not be overclaimed until it
  exists on the target branch.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `tacit/backends/base.py`: `DashboardFeatures` normalizes dashboard ingestion features.
- `tacit/backends/grafana.py` and `tacit/backends/signalfx.py`: implement `ingest_dashboard`.
- `tacit/dashboard_uploads.py`: parses uploaded dashboard JSON exports.
- `tacit/dashboard_ingest.py`: performs extraction, signal inference, YAML generation, persistence, and approval
  support.
- `tacit/main.py`: exposes `/api/v1/learn/dashboard` and `/api/v1/learn/dashboard/json`.
- `tacit/static/index.html`: includes a Learning tab.
- `README.md`: documents API/UI ingestion.

TODO:

- Add and document first-class CLI commands such as `tacit learn dashboard <uid>` and bulk backend learning if this
  branch is intended to support the "connect Grafana, learn everything" demo.
- Add pagination/concurrency/backoff considerations before claiming large-enterprise crawl readiness.

