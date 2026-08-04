# Engineering Design Notes

Status: living implementation guidance

Last reviewed: 2026-07-29

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
- Legacy migrations inherit the configured pinned tenant. A wildcard runtime
  must fail before schema mutation when pre-tenant user data has no explicit
  owner; migrate once under a pinned owner before enabling wildcard tenancy.

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
- Authority revisions and resolver projections that share a database commit in
  one unit of work. A retry may repair or verify an idempotent projection, but
  it must not report an active revision whose projection failed to commit. A
  service receiving explicit authority and resolver stores verifies their
  canonical database paths at construction; two individually valid stores do
  not form a valid dependency graph when they point at different files.
- External dashboard or alert IDs establish provenance, not independence.
  Corroboration groups copied sources by stable operational-content lineage.

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
- Once a governed mapping changed compilation or evidence resolution, later
  negative telemetry may annotate that effect but cannot erase it from usage.
  Marking it unapplied requires rebuilding and validating the output without
  the mapping.
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
- Offline current-engine replay must fail when the recorded engine, contract
  policy, ranking, or vocabulary version changed and the capture cannot rerun
  discovery, compilation, and validation. A refresh is the authoritative path
  for those changes; replay must not relabel captured stage output as current.
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

## Recovery, replay, and selection invariants

- A source-refresh checkpoint is committed only after candidate lifecycle and
  authority transitions succeed against state re-read under the write lock.
  Compare-and-swap loss is a failed reconciliation, not a successful skip.
- Resolver projections are disposable views of immutable knowledge revisions.
  Startup repair is bidirectional: quarantine unauthorized rows and recreate
  missing or deactivated rows from current eligible authority. Version the audit
  marker whenever the repair contract changes. If active immutable authority
  lacks enough exact resolver data to rebuild safely, leave the audit dirty and
  fail that knowledge store closed instead of certifying a partial graph. The
  pipeline may continue only through its explicit knowledge-unavailable path.
- Current-engine replay fails closed when historical knowledge changed a stage
  that captured inputs cannot rebuild. Applied non-ranking usage must be pinned
  to an exact historical snapshot before replay can compare current authority.
- Snapshot result caps are applied after every scope dimension, version range,
  validity window, and dependency-subject predicate. Page authority rows with a
  stable keyset so an early non-applicable row cannot hide a later match. Read
  every page plus conflict and active-support eligibility in one SQLite snapshot,
  then persist the deduplicated snapshot after releasing that read view. Initial
  archetype scope contains only concrete templates resolved from classifier
  matches plus their legacy labels. Catalog-driven widening requires an explicit
  staged pin; never approximate it by selecting the entire curated universe.
- Schema alterations and their required backfills share an explicit rollback
  boundary. Derived-index migrations may commit bounded, idempotent batches and
  set their completion marker only after the final page so interruption is
  resumable without one long write lock. Tenant-owner migration treats the
  remaining legacy rows as durable progress: governed rows are archived and
  removed in the same batch transaction, retargetable rows move in bounded
  batches, the intended owner is pinned in a progress marker across restarts,
  and the owner marker is terminal rather than aspirational.
- In-memory caches have capacity bounds as well as TTLs; ordinary writes prune
  expired keys. Metric caches also carry a total item-weight budget and refuse
  to retain one oversized catalog; a key-count limit alone does not bound
  memory. Datasource cache identities include endpoint, organization, and a
  non-secret credential fingerprint so app-scoped runtimes cannot share catalog
  values accidentally. LLM caches belong to one runtime dependency graph and
  include provider, model, endpoint, engine version, and prompt identity. Exact
  aliases and non-authoritative fuzzy entity suggestions both use bounded
  indexed candidate buckets; truncation is ambiguous and fails closed.
- Immutable knowledge usage is an authority projection, not a free-form audit
  API. Persisting it requires apply permission and an exact semantic match to
  the usage stored in the tenant-scoped investigation contract.
- Dirty resolver projection repair commits bounded batches while the audit stays
  dirty. A read-only validation captures a generation token, and only an
  unchanged token may be marked clean. This avoids a database-wide write lock
  without certifying a concurrent or partial repair. Another initializer may
  complete the same repair concurrently; observing a clean marker is success,
  while a missing marker remains an invalid state. Validation pages immutable
  authorities and resolver rows, joins mappings to authority in each page, and
  restarts if the generation changes; never validate a large graph with one
  long read snapshot or one authority query per mapping.
- History, signal, and feedback ownership backfills use the same restartability
  rule: pin the intended owner before the first data batch, release the SQLite
  writer lock between batches, persist a per-table keyset cursor, and publish
  the terminal owner marker only after an under-lock sweep finds no legacy rows.
  A batch limit without a cursor is still quadratic on unindexed FTS state.
- Bulk learning creates one tenant-scoped service/repository graph per crawl and
  reuses it across records and stale-source reconciliation. If more ingestion
  paths need this behavior, replace the private store-to-service helper with a
  public dependency factory rather than adding new process-global fallbacks.
- Candidate review priority is persisted as a base priority plus an indexed
  unresolved-conflict bit. Conflict writes adjust that bit set-wise instead of
  deserializing and rewriting every candidate while holding the writer lock.

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
- Failed chart runs should expose a run identifier and terminal status even when
  validation drops every panel and no investigation revision is produced. The
  audit record should retain per-datasource catalog counts and truncation,
  intended versus compiled archetypes, signal-binding disposition, and rejected
  query reasons so a safe abstention remains externally explainable.
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
