# ADR-004: Dashboard ingestion is the primary onboarding path for custom telemetry

## Status

Accepted

## Context

The highest-leverage adoption story is: point DashForge at existing dashboards and let it learn how the organization
already investigates. The repo supports dashboard ingestion through backend adapters and uploaded JSON parsing, and the
Web UI includes a Learning tab.

The CLI in the current `main` branch does not yet expose `dashforge learn dashboard <uid>` or bulk `dashforge learn
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

- `dashforge/backends/base.py`: `DashboardFeatures` normalizes dashboard ingestion features.
- `dashforge/backends/grafana.py` and `dashforge/backends/signalfx.py`: implement `ingest_dashboard`.
- `dashforge/dashboard_uploads.py`: parses uploaded dashboard JSON exports.
- `dashforge/dashboard_ingest.py`: performs extraction, signal inference, YAML generation, persistence, and approval
  support.
- `dashforge/main.py`: exposes `/api/v1/learn/dashboard` and `/api/v1/learn/dashboard/json`.
- `dashforge/static/index.html`: includes a Learning tab.
- `README.md`: documents API/UI ingestion.

TODO:

- Add and document first-class CLI commands such as `dashforge learn dashboard <uid>` and bulk backend learning if this
  branch is intended to support the "connect Grafana, learn everything" demo.
- Add pagination/concurrency/backoff considerations before claiming large-enterprise crawl readiness.

