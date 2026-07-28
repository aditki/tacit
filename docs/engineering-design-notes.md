# Engineering Design Notes

Status: living implementation guidance

Last reviewed: 2026-07-28

This document records recurring engineering lessons and refactor signals found
while building Tacit's Investigation Contract, Operational Knowledge, learning,
and generated-archetype workflows. It is a memory aid for future changes, not a
substitute for an Architecture Decision Record. Accepted ADRs are authoritative;
tests remain the executable specification for fixed regressions.

## How to use this document

- Read the relevant sections before changing a cross-cutting workflow.
- Apply the invariants to API, CLI, direct Python, replay, refresh, background,
  and web UI entry points.
- Add a regression test when an invariant fixes a concrete defect.
- Update this document when work reveals reusable design pressure.
- Promote a note into an ADR when it requires a durable product or architecture
  choice.

## Core invariants

### One runtime composition root

Application, CLI, learning, replay, refresh, benchmark, and direct pipeline
paths must use the same resolved runtime settings and store graph.

- App-scoped paths must never fall back silently to process-global stores.
- Distinguish "not injected" from "injected but unavailable" so an unavailable
  scoped store cannot trigger a global fallback.
- Optional stores should fail best-effort only where the feature is genuinely
  optional. Their failure must not select data from a different database.
- Store caches must be scoped to the store identity, tenant, and relevant
  configuration.

The app and CLI must not develop separate heads. A capability available through
both surfaces should share the same service method, policy checks, tenant
resolution, transaction semantics, and projections.

### Tenant isolation is end to end

Tenant selection is a data boundary, not request decoration.

- Resolve and validate the tenant at every public boundary.
- A pinned deployment rejects another tenant. A wildcard deployment requires a
  concrete tenant. Internal bootstrap tenants are never valid user tenants.
- Propagate the resolved tenant through contracts, runs, revisions, feedback,
  corrections, raw learned artifacts, extractions, signal definitions, signal
  mappings, FTS indexes, provenance, exports, benchmarks, and caches.
- Filter reads, aggregates, list endpoints, and mutations. Authorization after a
  lookup is not enough if the lookup itself leaks another tenant's data.
- Browser requests must send the selected tenant consistently across Generate,
  Learning, Knowledge, Signals, and History views.
- Legacy migrations inherit the configured pinned tenant. Wildcard migrations
  require an explicit, documented assignment policy.

Every tenant-aware feature should be tested across this matrix:

| Configuration | Entry point | Expected behavior |
|---|---|---|
| Pinned tenant | API, CLI, direct call | Missing tenant may resolve to the pin; a mismatch fails |
| Wildcard | API, CLI, direct call | Concrete tenant is required |
| Any | Read and aggregate | Only the selected tenant is visible |
| Any | Mutation | Permission and tenant are checked before state changes |

### Governed knowledge is authoritative

Operational Knowledge is the authority for learned organizational behavior.
Mutable signal tables, indexes, and caches are projections for efficient runtime
selection.

- Do not activate a resolver projection before its governed revision is
  eligible.
- Projection writes must preserve exact runtime values such as metric patterns;
  normalized proposition identities are not resolver patterns.
- Projection identity must retain tenant, scope, and governed support. One row
  must not make independent scoped revisions retire each other.
- Signal-to-metric semantics are many-to-many unless a predicate is explicitly
  exclusive.
- Bootstrap mappings remain globally identifiable and cannot be mutated or
  retired by a tenant named `default`.
- Lifecycle changes and source disappearance must reconcile both governed
  revisions and their runtime projections.
- Reappearing sources may preserve review history, but they need an explicit
  transition out of stale or ineligible state.

### Usage is confirmed by the consuming stage

Selection does not mean application. Knowledge starts as considered and becomes
applied only after a stage confirms that it changed or selected an output.

- Record the exact knowledge ID and revision, target, stage, effect, reason, and
  contribution.
- A zero score delta can still be a real effect, such as query compilation or
  candidate exclusion, but it must name that effect.
- Do not assign `used_for` from knowledge kind alone.
- Rebuild the final knowledge snapshot after live-evidence and stage-use
  reconciliation.
- Counterfactuals must downgrade usage when they remove the output that knowledge
  generated or changed.
- Impact analysis includes only applied usage. Considered and rejected items
  remain audit records, not affected investigations.

Current design pressure: compilation returns the governed mapping references it
actually selected, and the pipeline reconciles those references into usage. If
ownership, investigation patterns, evidence requirements, or other knowledge
types begin affecting more stages, introduce a typed stage-effect ledger instead
of adding more per-stage reference sets. A likely record contains:

- stage and effect type
- knowledge ID and revision
- target artifact or candidate
- before/after value or score delta
- reason and provenance references

### Investigation revisions are immutable and replayable

- Schema versions are explicit and unsupported versions fail closed.
- Exact replay either reproduces the stored output fingerprint or fails/returns
  the stored contract according to the documented mode. It must not return a
  divergent result as a successful exact replay.
- Legacy fingerprint compatibility omits fields that were absent when the old
  fingerprint was created.
- Normalize volatile timestamps in every provenance source before output
  fingerprinting.
- Current-engine and counterfactual replay require captured inputs; they do not
  silently fall back to an exact load.
- Refresh, replay, and correction writes are pinned to the revision that supplied
  their inputs and use an expected-parent check.
- Assessment bundles for an old revision contain no future revisions.

### Evidence controls conclusions

- Observation references point to the matching query and requirement.
- Missing-observation fields contain observation IDs, not requirement IDs.
- A requirement is missing only when none of its observations are supported.
- Missing and contradicted observations participate in grounding even when the
  requirement itself resolved successfully.
- Explicit negative telemetry appears as contradiction and cannot be hidden by
  contextual support.
- Abstention suppresses leading-suspect language and causal conclusions.
- Knowledge may add contextual candidates, but it does not turn context into
  telemetry evidence.
- Safety gates count any unsafe suspect conclusion in expected-abstention cases,
  not only claims labeled proven.

### Review and lifecycle transitions are state machines

- Authorization is semantic: read, review, reject, trust, correct, apply, and
  policy override are separate capabilities.
- Policy override inputs require their own privileged permission in API and CLI.
- Review transitions use compare-and-swap predicates on tenant, identity, and
  expected state.
- Candidate state and provenance review state change together.
- Terminal states are idempotent and are not overwritten by expiry handling.
- Corrections bind to the exact investigation revision and knowledge revision
  that the reviewer saw.
- Stale and incorrect corrections require and validate a target before approval.
- Rejection or source retirement recomputes surviving support before retiring an
  active revision.
- Rejected, pending, stale, unresolved, copied, or entity-unresolved candidates
  cannot manufacture corroboration or active conflicts.

### Concurrency-sensitive writes are atomic

- Acquire a SQLite write lock before reading a parent/current revision used by a
  compare-and-swap decision.
- Keep parent verification, immutable revision insert, projection update, and
  terminal candidate transition in one transaction when they form one operation.
- Candidate evaluation must not overwrite a concurrent rejection or lifecycle
  transition.
- Correction application must verify and supersede the pinned target revision in
  the same atomic operation.
- Table-rebuild migrations must not allow an implicit commit between rename,
  create, copy, and validation.

Raw SQL remains acceptable while repositories own these invariants clearly. See
"Data access refactor triggers" below for the point where a stronger persistence
layer is warranted.

### Persistence semantics match the user operation

- A successfully published dashboard remains a chart success if later optional
  history, provenance, or contract persistence fails. Record the degraded audit
  state visibly.
- Refresh, replay, and correction endpoints explicitly request an authoritative
  new revision. Persistence failure or a stale-parent race must be surfaced to
  those callers rather than returned as HTTP 200 success.
- A failed refresh must not overwrite the last successful investigation summary
  or dashboard state.
- Terminal run events occur after all successful reconstruction and revision
  events.
- Client cancellation is cancellation, not timeout.

### Entity and scope normalization is symmetric

- Canonicalize entity IDs, aliases, service references, and scope lists at write
  boundaries with the same normalizer used for lookup.
- IDs are kind-safe and entity kind is immutable.
- Exact-ID resolution still checks active status, expected kind, tenant, and
  scope.
- Alias scope is enforced before accepting an exact alias match.
- Scope lists are canonicalized before proposition fingerprinting.
- Applicability and conflict overlap use the same dimensions: tenant, service,
  environment, region, cluster, namespace, archetype, and version constraints.
- Normalize timezone-naive validity values before comparing them with UTC times.

## Generated archetype containment and growth

Generated archetypes remain quarantined and shadow-only under ADR-019 through
ADR-021. An experimental retrieval mode must not blend generated output into the
authoritative dashboard. File mutation is not a promotion mechanism, and signal
mapping approval is independent from generated-archetype persistence.

The long-lived-state benchmark showed that accumulated learned archetypes can
reduce retrieval recall and signal-to-noise. Before any generated archetype can
be promoted or used authoritatively, the lifecycle needs controls for:

- content and semantic deduplication before creation
- deterministic scope-aware identity
- per-tenant and per-scope quotas
- source lifecycle reconciliation and tombstones
- retention windows and expiry
- clustering or consolidation of near-duplicate programs
- novelty and marginal-utility scoring against curated behavior
- shadow comparisons against clean and long-lived state
- explicit review, revision, rollback, and retirement

Do not solve growth only by increasing retrieval limits. The system should stop
redundant state from being created, consolidate existing state, and prove that a
candidate adds durable value before investing in an authoritative lifecycle.

## Data access refactor triggers

Do not adopt an ORM merely to hide SQL. Preserve SQLite-specific behavior such as
`BEGIN IMMEDIATE`, explicit uniqueness constraints, and compare-and-swap checks.
Introduce a stronger repository/unit-of-work layer, query builder, or ORM when
several of these conditions are present:

- one aggregate mutation repeatedly spans three or more tables
- transaction boundaries are duplicated across services
- tenant predicates or lifecycle predicates are repeatedly omitted and repaired
- schema rebuilds and compatibility migrations dominate feature work
- parent revision and candidate version checks are hand-coded in many places
- pagination, filtering, and aggregate queries are duplicated across API, CLI,
  export, and benchmark paths
- tests cannot inject one store graph without patching globals
- projection and authority updates need coordinated rollback

The likely first step is a shared unit-of-work and typed repository API, not a
wholesale ORM rewrite. Keep domain state transitions in services; keep storage
mechanics, tenant predicates, and transaction ownership in repositories.

## Observability expectations

Best-effort behavior must be visible. Silent fallback is not resilience.

- Emit structured diagnostics for store initialization failure, blocked global
  fallback, persistence degradation, stale-parent races, replay mismatch,
  projection mismatch, and resolver/snapshot usage mismatch.
- Time intent classification, discovery, semantic resolution, archetype
  selection, compilation, validation, publication, and persistence separately.
- Record knowledge counts by disposition and reason, selected knowledge IDs and
  revisions, stage effects, lifecycle backlog, stale sources, conflicts, and
  projection health.
- Separate timeout, cancellation, validation failure, backend failure, and audit
  persistence failure in run status and metrics.
- Benchmark reports identify clean versus long-lived state and include latency
  percentiles, recall, signal-to-noise, and zero-match cases.
- When debugging exposes missing telemetry, call it out and add focused
  instrumentation when it is in scope.

The current compilation reconciliation emits a structured
`signal_mapping_usage_disposition_mismatch` warning when the resolver reports a
governed mapping that the selected snapshot cannot safely mark as applied.

## Validation expectations

Cross-cutting work should use focused regression tests plus the relevant matrix:

- tenant: pinned, wildcard, missing, mismatch, reserved, and legacy migration
- permissions: read, review, reject, trust, correct, apply, and override
- entry point: API, browser UI, CLI, direct Python, refresh, and replay
- lifecycle: candidate, approved, trusted, active, stale, reactivated,
  superseded, withdrawn, expired, and rejected
- concurrency: duplicate review, evaluation versus rejection, refresh/replay
  races, and stale correction application
- persistence: clean database, upgraded database, unavailable optional store,
  and interrupted multi-table write
- packaging: resources included in wheels and installed-package smoke tests

Run the deterministic Operational Learning and Grounding gates for knowledge,
ranking, evidence, replay, and contract changes. Run the 100-prompt harness when
intent extraction, retrieval, signal resolution, archetype compilation, ranking,
or learned-state selection changes. Compare both clean and representative
long-lived state; a clean-state pass alone does not establish operational
usefulness.

## Related decisions

- [ADR-015: Evidence lifecycle](adr/015-evidence-lifecycle.md)
- [ADR-016: Contextual versus telemetry-evidenced ranking](adr/016-contextual-vs-telemetry-evidenced-ranking.md)
- [ADR-018: Investigation Contract](adr/018-investigation-contract.md)
- [ADR-019: Governed knowledge authority](adr/019-governed-knowledge-authority.md)
- [ADR-020: Generated archetypes stay in shadow](adr/020-generated-archetypes-shadow-before-lifecycle.md)
- [ADR-021: Generated archetype scope](adr/021-generated-archetype-scope-context.md)
- [ADR-022: Operational Knowledge lifecycle](adr/022-operational-knowledge-lifecycle.md)
- [Generated archetype evaluation roadmap](generated-archetype-evaluation-roadmap.md)

