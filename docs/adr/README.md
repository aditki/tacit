# Architecture Decision Records

Tacit ADRs capture the architecture and product decisions that should guide near-term work.
They are intentionally lightweight and validated against the current repository state.

Tacit is an experimental, public-beta infrastructure project. These records should not be read as
production-readiness claims.

| ADR | Title | Status | Implementation Status |
|---|---|---|---|
| [ADR-001](001-investigation-first.md) | Tacit is investigation-first, not dashboard-only | Accepted | Partially implemented |
| [ADR-002](002-operational-intelligence-before-stateful-sessions.md) | Operational intelligence before stateful investigation sessions | Accepted | Implemented as a guardrail |
| [ADR-003](003-emergent-signals-over-fixed-taxonomy.md) | Signals should emerge from observed usage, not a fixed vendor taxonomy | Accepted | Partially implemented |
| [ADR-004](004-dashboard-ingestion-onboarding.md) | Dashboard ingestion is the primary onboarding path for custom telemetry | Accepted | Partially implemented |
| [ADR-005](005-deterministic-candidate-signal-inference.md) | Deterministic candidate signal inference is the default | Accepted | Implemented |
| [ADR-006](006-conservative-auto-teach.md) | Conservative auto-teach prevents learned mapping poisoning | Accepted | Implemented |
| [ADR-007](007-lightweight-investigation-plans.md) | Investigation plans are useful, but should remain lightweight for now | Accepted | Partially implemented |
| [ADR-008](008-dashboards-as-evidence-artifacts.md) | Dashboards are evidence artifacts, not the sole product | Accepted | Partially implemented |
| [ADR-009](009-operational-usefulness-evaluation.md) | Evaluation should prioritize operational usefulness over label purity | Accepted | Partially implemented |
| [ADR-010](010-incident-management-integrations-downstream.md) | Incident management integrations are downstream distribution | Accepted | Implemented as a roadmap guardrail |
| [ADR-011](011-logs-traces-as-evidence-types.md) | Logs and traces should be introduced as evidence types | Deferred | Future work |
| [ADR-012](012-lightweight-service-context.md) | Service context is a lightweight bridge toward operational memory | Proposed | Not implemented |
| [ADR-013](013-adoption-time-to-value-and-trust.md) | Adoption depends on time-to-value and trust, not more agents | Accepted | Partially implemented |
| [ADR-014](014-tacit-is-not-a-system-of-record.md) | Tacit Is Not a System of Record | Accepted | Partially implemented |
| [ADR-015](015-evidence-lifecycle.md) | Evidence has a lightweight lifecycle | Accepted | Partially implemented |
| [ADR-016](016-contextual-vs-telemetry-evidenced-ranking.md) | Culprit ranking has contextual and telemetry-evidenced tiers | Accepted | Partially implemented |
| [ADR-017](017-artifact-learning-framework.md) | Artifact learning compiles operational artifacts into Operational IR | Proposed | Partially implemented |
