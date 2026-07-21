# Generated Archetype Evaluation Roadmap

## Status

Proposed implementation roadmap, 2026-07-21.

This roadmap implements [ADR-020](adr/020-generated-archetypes-shadow-before-lifecycle.md) and
[ADR-021](adr/021-generated-archetype-scope-context.md). It does not authorize generated archetypes in normal or
experimental investigation output.

## Milestone Statement

> Generated archetypes are quarantined hypotheses. Tacit validates and evaluates them against frozen investigations in
> shadow mode, proves whether they add unique value beyond curated archetypes and Operational Knowledge, and only then
> decides whether a runtime lifecycle should exist.

## Why This Sequence

C0 established containment. It did not establish governance, and it did not establish that generated archetypes deserve
governance.

The observed long-lived-state degradation came from global registration and shared retrieval. Clean-state quality
remained broadly stable, and Operational Knowledge added little latency while preserving scoped provenance. The next
investment should therefore test the unproven abstraction rather than immediately surrounding it with a large lifecycle.

```text
proven product path

Artifacts -> Operational Knowledge -> Investigation

experimental path

Artifacts -> Generated archetype -> Quarantine -> Validation -> Shadow evidence
                                                              |
                                                              v
                                                    architecture decision
```

## Non-Goals

This roadmap does not deliver:

- generated-archetype approval for runtime use
- automatic promotion
- replacement or supplementation of curated archetypes
- normal generated-archetype retrieval
- fuzzy, global, or partially overlapping scope matching
- a review queue for promotion
- applied-artifact expiry, rollback, or retirement
- a second knowledge authority beside Operational Knowledge

## Current Baseline

Implemented now:

- generated output is disabled by default
- generated YAML is written to a separate quarantine namespace when explicitly enabled
- curated registry loading rejects or filters generated entries
- quarantined artifacts are excluded from retrieval
- an exact-scope experimental loader and containment tests exist
- retrieval diagnostics distinguish curated and generated candidates

Known gaps:

- file content is still the only generated-artifact persistence model
- manual YAML mutation is the only way to move from `quarantined` to `experimental`
- no validator, immutable registry revision, or reason-coded transition exists
- no shadow evaluator exists
- request tenant and environment are not propagated to selection end to end
- selection can fall back to a configured tenant and an empty environment set
- no frozen benchmark measures unique generated-archetype contribution

The existing exact-scope runtime loader remains disabled during this roadmap. It is removed after registry-backed shadow
evaluation is available.

## Delivery Order

| Phase | Outcome | Runtime influence |
|---|---|---|
| A | Stabilize the proven Operational Knowledge and storage foundation | None from generated archetypes |
| B | Freeze explicit archetype selection context | None |
| C | Import and validate immutable generated revisions | None |
| D | Evaluate generated revisions in offline shadow replay | None |
| E | Run frozen and long-lived evidence programs | None |
| F | Choose lifecycle, decomposition, or retirement in a new ADR | None until a later ADR |

Phases are ordered. A later phase must not compensate for an unmet earlier invariant.

## Phase A: Stabilize The Proven Foundation

### Objective

Keep generated-archetype work from masking unresolved authority, storage, tenancy, or benchmark problems in the normal
learning path.

### Deliverables

- Complete Operational Knowledge authority and projection reconciliation defined by ADR-019.
- Route repositories through one request/application-scoped storage context rather than module-global database paths.
- Keep signal and learning search projections tenant-native and rebuildable from governing records.
- Preserve candidate, revision, correction, and source lifecycle invariants under retries and concurrent writes.
- Instrument selection, projection, persistence, and benchmark stages with stable reason codes and durations.
- Freeze clean and governed long-lived benchmark state manifests.

### Exit Criteria

- The 100-prompt suite is reproducible from a clean manifest and a governed long-lived manifest.
- Each report identifies state fingerprint, tenant, corpus version, code version, and source contribution.
- Tenant, permission, lifecycle, and concurrency matrices pass.
- Operational Knowledge selection can be measured independently from curated and generated archetypes.
- Generated retrieval remains zero in every normal benchmark.

Phase A is a dependency, not an invitation to refactor all persistence inside the generated-archetype changeset.

## Phase B: Request-Scoped Selection Context

### Objective

Implement ADR-021 before any generated artifact is evaluated.

### Deliverables

- Add immutable `ArchetypeSelectionContext` and a shared canonical scope normalizer.
- Derive tenant from authenticated or explicitly supplied operation context.
- Derive the complete resolved service set and environment resolution from frozen investigation inputs.
- Carry archetype kind, generator-version policy, and intent reference explicitly.
- Persist the context or a stable reference in captured replay inputs without changing legacy contract fingerprints.
- Remove configured tenant and empty-environment fallbacks from generated evaluation.
- Emit per-dimension applicability and fail-closed reason codes.

### Exit Criteria

- Missing or ambiguous tenant, service, or environment produces `not_applicable`.
- Same service in another tenant does not match.
- Same tenant and service in another environment does not match.
- Partial multi-service overlap does not match.
- Replay uses captured scope and cannot silently switch to current process configuration.
- Curated investigation output remains unchanged when generated scope is unavailable.

## Phase C: Minimal Shadow Registry And Validation

### Objective

Replace file mutation with immutable registry records, but implement no promotion machinery.

### Minimal States

```text
quarantined -> validated -> shadow
```

An invalid artifact remains quarantined with validation reason codes. Changed content creates a new revision.

### Persistence Boundary

Use repository interfaces and explicit transactions. Routes, CLI commands, and pipeline stages must not issue ad hoc SQL.
The initial schema needs only:

`generated_archetypes`

- tenant ID
- stable generated-archetype ID
- current revision
- created and updated timestamps

`generated_archetype_revisions`

- tenant ID, generated-archetype ID, revision, and optional parent revision
- immutable definition and semantic fingerprint
- exact scope, archetype kind, and generator version
- generation run and source lineage
- state (`quarantined`, `validated`, or `shadow`)
- created actor and timestamp

`generated_archetype_validations`

- revision reference
- validator ID and version
- result, reason codes, and bounded diagnostics
- validated timestamp

`generated_archetype_transitions`

- revision reference, source state, target state, actor, command ID, and reason codes

The existing quarantine YAML may be retained as an attached artifact, but editing it cannot update registry state.

### Validator

Add a versioned `GeneratedArchetypeValidator` covering:

- identity and stable fingerprint
- exact tenant, service, and environment scope
- entity resolution and ambiguity
- generation run, source existence, same-tenant lineage, and source fingerprint
- structural bounds and duplicate or contradictory templates
- causal-claim and executable-remediation suppression
- hard-safety preservation

Every failure has a stable reason code suitable for UI, CLI, audit, and benchmark aggregation.

### Exit Criteria

- Every generated revision starts quarantined.
- No configuration or YAML field can skip validation.
- Validation is deterministic for a frozen validator version and input revision.
- Registry writes and transition audit are atomic and idempotent.
- Cross-tenant source provenance fails validation without leaking source details.
- No validated or shadow record appears in normal archetype retrieval.

### Persistence Decision Trigger

The minimal registry may use the repository and migration conventions already adopted by Tacit. Before choosing the full
runtime lifecycle option in Phase F, reassess the database layer. Cross-table state transitions, immutable revisions,
concurrent review, expiry, and rollback are the point where a migration library and transaction-aware ORM or query layer
become preferable to expanding hand-written SQL.

## Phase D: Offline-First Shadow Evaluation

### Objective

Measure counterfactual value without adding a request-path dependency or changing investigation output.

### D1: Replay-Based Shadow Runner

Start with captured investigation snapshots. For each frozen investigation:

1. Rebuild the authoritative result from curated archetypes and its pinned Operational Knowledge snapshot.
2. Resolve exact generated-archetype applicability from the captured selection context.
3. Compute projected selection and compilation in an isolated shadow workspace.
4. Compare coverage, evidence requirements, queries, ranking, grounding, safety, and latency.
5. Persist a sidecar shadow result linked to the investigation revision and generated revision.

The authoritative contract is loaded, not overwritten. Shadow data must not participate in its normalized output
fingerprint.

### D2: Optional Online Sidecar

Consider online shadowing only after replay-based shadowing is deterministic. If added, it runs after authoritative
persistence or through a bounded background worker. Failure, cancellation, or timeout cannot change the investigation
status or response.

### Shadow Result Contract

Persist at least:

- investigation and generated-revision references
- selection-context fingerprint
- applicability by scope dimension and reason codes
- would-select disposition and projected rank
- curated and projected coverage
- query, evidence, ranking, grounding, and warning deltas
- unsupported-assertion and negative-case findings
- duplicate-curated and decomposable-to-Operational-Knowledge classifications
- evaluator version, input fingerprint, duration, status, and error class
- `output_applied: false`

### Non-Interference Invariants

- Investigation Contract content and output fingerprint are identical with shadowing on or off.
- Current investigation revision does not advance because of shadow work.
- Dashboard, ranking, grounding, and renderers are unchanged.
- Shadow failure cannot fail or delay the authoritative run beyond a separately configured observation budget.
- Shadow results cannot be selected as context by replay, refresh, or correction.

### Exit Criteria

- Golden non-interference tests compare full authoritative contracts byte-for-byte after normalization.
- Shadow results are reproducible from the same captured inputs.
- Every skip and failure is reason-coded and observable.
- The online path, if implemented, is independently disableable and best-effort.

## Phase E: Evidence Program

### Evaluation States

Run all of the following with frozen manifests:

| State | Curated | Operational Knowledge | Generated archetypes |
|---|---:|---:|---:|
| Clean | Yes | No learned state | Off |
| Governed long-lived | Yes | Eligible long-lived state | Off |
| Shadow | Yes | Same pinned long-lived state | Evaluated, never applied |
| Adversarial mismatch | Yes | Same pinned state | Wrong tenant/service/environment/kind/version seeded |

Do not call shadow output an experimental investigation result. It is a counterfactual measurement.

### Required Corpora

- the frozen 100-prompt investigation suite
- clean and accumulated long-lived state
- at least two untouched telemetry or incident holdouts
- deliberate same-vocabulary cross-service cases
- cross-tenant and environment mismatch matrices
- missing and ambiguous scope cases
- negative and expected-abstention cases
- duplicate-curated and Operational-Knowledge-decomposable examples

### Required Metrics

- critical-signal recall
- negative correctness
- signal-to-noise ratio
- unsupported assertion rate
- cross-service and cross-tenant would-select rates
- scope rejection and missing-scope rates
- shadow would-select rate
- unique contribution rate
- duplicate-curated rate
- decomposable-to-Operational-Knowledge rate
- projected ranking and grounding deltas
- evaluation error rate and p50/p95 latency
- state-size and artifact-count sensitivity

### Observability

Record counters and timings for:

- artifacts generated, quarantined, validated, invalid, and shadow-enrolled
- applicability accepted and rejected by reason
- files or revisions scanned
- projected selections and unique contributions
- shadow failures, timeouts, and cancellations
- registry and evaluator latency
- artifact count and bytes by tenant/service/generator version

Diagnostics must make growth visible before it becomes retrieval degradation. Reports include the state fingerprint and
artifact cardinality so clean and long-lived results cannot be accidentally compared as equivalent environments.

### Gate Discipline

Before running the evidence program, freeze:

- corpus and dataset hashes
- minimum case counts and observation duration
- acceptable recall, SNR, negative correctness, and latency deltas
- definitions of unique contribution and decomposability
- generator, validator, and evaluator versions

Hard safety requirements are fixed now:

- cross-tenant would-select rate equals zero
- cross-service would-select rate equals zero
- authoritative output delta equals zero
- unsafe assertion increase equals zero
- normal generated-archetype contribution equals zero

Utility must be repeatable on untouched holdouts and materially exceed the governed long-lived baseline. A gain that is
fully expressible as Operational Knowledge is evidence for decomposition, not for a second runtime lifecycle.

## Phase F: Architecture Decision Gate

Produce an evidence packet containing:

- frozen manifests and reproducible commands
- aggregate and per-family metrics
- representative positive, duplicate, noisy, and leakage cases
- long-lived growth curves
- latency and storage costs
- classification of useful deltas by whether Operational Knowledge can represent them
- unresolved risks and proposed controls

Then write a new ADR selecting one option.

### Option 1: Build A Governed Runtime Lifecycle

Choose only when generated programs show repeatable, non-decomposable benefit. The future ADR must define:

```text
quarantined
    -> validated
    -> reviewed
    -> shadow_evaluation
    -> experimental_exact_scope
    -> expired | retired | rejected
```

It must also define authorization, immutable promotion revisions, benchmark policy, validity intervals, automatic expiry,
rollback, retirement, cache invalidation, usage attribution, historical replay, and transaction boundaries. Every applied
revision must be exact-scope, expiring, and reversible. Normal retrieval remains a separate future decision.

### Option 2: Compile Discoveries Into Operational Knowledge

Choose when useful behavior decomposes into existing governed concepts such as signal mappings, evidence requirements,
dependencies, ownership, or candidate roles. Retain the generator as an offline candidate extractor only if it improves
those candidates.

### Option 3: Retire The Runtime Abstraction

Choose when generated programs are redundant, noisy, unstable, too expensive, or unsafe. Remove the experimental loader
and generation controls after preserving benchmark evidence and any useful candidate-extraction logic.

## Suggested Pull Request Slices

| Slice | Scope | Required proof |
|---|---|---|
| GA-1 | Selection context and fail-closed scope | Tenant/service/environment/replay matrix |
| GA-2 | Immutable shadow registry and import | Atomicity, idempotency, provenance, no YAML authority |
| GA-3 | Validator and reason codes | Safety, ambiguity, lineage, structural corpus |
| GA-4 | Offline replay shadow evaluator | Contract non-interference and deterministic replay |
| GA-5 | Benchmark corpus, reports, and growth instrumentation | Clean/long-lived/mismatch reproducibility |
| GA-6 | Evidence review and final architecture ADR | Explicit option 1, 2, or 3 decision |

Each slice should be independently releasable with generated runtime retrieval disabled.

## Test Matrix

### Scope And Isolation

- exact tenant/service/environment/kind/version matches
- every one-dimension mismatch rejects
- partial multi-service overlap rejects
- missing, unresolved, and ambiguous scope rejects
- request and captured replay context cannot be replaced by global settings

### Registry And Validation

- imports are idempotent by semantic fingerprint
- changed content creates an immutable revision
- invalid provenance, wildcard scope, causal claims, and unsafe definitions fail with reason codes
- concurrent validation cannot create duplicate current revisions
- YAML edits cannot change registry state

### Shadow Non-Interference

- selected archetypes, queries, dashboard, ranking, grounding, warnings, and fingerprints remain unchanged
- shadow timeout, exception, cancellation, or persistence failure does not alter authoritative success
- shadow records never appear as applied context

### Long-Lived Behavior

- increasing artifact counts do not change authoritative output
- lookup work remains bounded by exact scope
- stale or replaced sources stop participating in future shadow runs
- repeated runs do not duplicate artifacts or shadow results

## Completion Criteria

This roadmap is complete when:

- generated YAML has no lifecycle authority
- every generated revision is immutable, scoped, provenance-bearing, and quarantined by default
- only validated, exact-scope revisions enter shadow evaluation
- shadow evaluation cannot affect authoritative output or fingerprints
- request tenant, service, and environment are propagated and captured without configuration fallback
- clean, governed long-lived, shadow, and adversarial benchmark states are reproducible
- growth, applicability, utility, safety, and latency are observable
- an evidence-backed ADR chooses lifecycle, Operational Knowledge decomposition, or retirement
- no runtime promotion code is merged before that decision
