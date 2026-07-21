# ADR-019: Operational Knowledge is the authority for learned organizational behavior

## Status

Accepted

Generated-archetype containment is implemented. The broader authority and projection consolidation is in progress.

[ADR-020](020-generated-archetypes-shadow-before-lifecycle.md) narrows the generated-archetype follow-up: Tacit will
prove unique value in shadow mode before committing to a promotion lifecycle. [ADR-021](021-generated-archetype-scope-context.md)
defines the fail-closed request scope required for any generated-archetype evaluation.

## Context

Tacit learns from dashboards, alerts, runbooks, incidents, ownership records, and other operational artifacts. These
sources can improve investigations, but they can also introduce stale, conflicting, overly broad, or tenant-specific
behavior.

The earlier generated-archetype path allowed an approved dashboard to produce YAML, append that YAML to the active
archetype registry, and hot-reload it into future investigations. That path made generated templates a second knowledge
authority. It bypassed the controls expected of reusable organizational knowledge:

- candidate review and explicit eligibility
- tenant and service scope
- immutable revisions and conflict handling
- investigation knowledge snapshots
- usage attribution, correction, and rollback

Long-lived development-state evaluation showed that accumulated generated archetypes could reduce investigation recall
and signal-to-noise ratio. This did not demonstrate that operational learning is harmful. It demonstrated that generated
investigation programs cannot safely enter a global runtime registry. It also did not establish that those programs are
valuable enough to justify building a permanent lifecycle.

ADR-004, ADR-006, and ADR-017 remain directionally correct: artifact ingestion is an important onboarding path,
inference must be conservative, and heterogeneous artifacts should compile into typed candidates. This ADR adds the
missing authority boundary governing when those candidates may affect an investigation.

## Decision

Operational Knowledge is the sole governed authority for reusable organization-specific learned knowledge.

The governing invariant is:

> No learned organizational behavior may affect a normal investigation unless it is represented by an eligible
> Operational Knowledge revision selected into that investigation's knowledge snapshot.

System roles are separated as follows:

| System | Role |
|---|---|
| Curated archetype registry | Immutable product-defined investigation templates |
| Operational Knowledge | Candidates, review, scope, revisions, eligibility, conflicts, corrections, snapshots, and usage |
| Dashboard, alert, and artifact learning | Candidate producers |
| Signal store | Tenant-scoped runtime projection and resolution index |
| Learning search index | Tenant-scoped retrieval projection |
| Investigation history | Audit, replay, and candidate-extraction source |
| Generated archetype YAML | Quarantined experimental output |

Indexes, stores, caches, and history records do not independently establish trust or eligibility. A projection must not
make an item active when its governing revision is absent, stale, superseded, conflicted, or otherwise ineligible.

### Generated archetypes

Generated archetypes are investigation programs, not ordinary facts or mappings. Until Tacit has a dedicated lifecycle
for reviewing, consolidating, versioning, evaluating, promoting, and rolling back those programs, they remain outside
normal runtime behavior.

Generation, quarantine persistence, and retrieval are independent controls. All are disabled by default.

Generated artifacts:

- are written only to a separate quarantine namespace
- cannot be appended to or loaded from the curated registry
- carry origin, lifecycle status, tenant, service, generation version, run, source, and creation metadata
- are invalid for persistence or retrieval without explicit tenant and service scope
- are excluded from normal investigation retrieval

The repository currently contains an explicitly selected exact-scope experimental retrieval mode. It is a development
escape hatch, not a supported promotion path: file mutation is still required to make a quarantined artifact eligible,
and request scope is not yet propagated end to end. ADR-020 replaces this escape hatch with shadow-only evaluation.
Until that work lands, it must remain disabled and must not be used as evidence of a governed lifecycle.

### Artifact and signal learning

Dashboards, alerts, runbooks, incidents, and other artifacts may produce typed candidates such as evidence requirements,
signal mappings, ownership, and dependencies. Candidate production does not itself make learned behavior eligible.

Signal and search stores are projections for runtime efficiency. They are not peer sources of truth. Existing legacy
paths that activate mappings without an eligible Operational Knowledge revision are migration debt covered by the
authority-consolidation changeset; this ADR does not misrepresent them as already complete.

### Investigation history

Investigation history records what happened and supports replay, comparison, audit, and correction. Historical evidence
may produce a new candidate through an explicit extraction path, but history is never queried as unconditional reusable
truth.

## Consequences

- Normal investigations become stable against unbounded generated-archetype accumulation.
- Cross-service and cross-tenant leakage become explicit invariant failures rather than ranking-quality anecdotes.
- Learning can still improve investigations, but only after governance and scoped snapshot selection.
- Generated-archetype research remains possible without silently changing production behavior.
- Runtime projections must be rebuildable from governing revisions and must preserve tenant boundaries.
- Every investigation must attribute contributions by source so benchmark regressions can be assigned to curated
  archetypes, Operational Knowledge, or experimental generated artifacts.
- Learned-archetype enablement is deferred until shadow evaluation demonstrates repeatable value beyond Operational
  Knowledge and curated archetypes. Only then will Tacit decide whether to build a promotion lifecycle, compile useful
  discoveries into Operational Knowledge, or retire generated archetypes as a runtime abstraction.

This decision intentionally favors trustworthy abstention and slower promotion over immediate compounding. Tacit may
know less temporarily, but it must be able to explain why each reusable learned item was eligible to contribute.

## Alternatives Considered

### Keep auto-registration with a confidence threshold

Rejected. Confidence does not provide tenant isolation, conflict resolution, revision history, rollback, or usage audit.

### Treat generated archetypes as ordinary Operational Knowledge candidates immediately

Rejected for now. Facts and mappings can be scoped and reviewed independently; an archetype is an executable
investigation program whose panels, queries, blending behavior, and interactions require a separate evaluation model.

### Delete generated-archetype generation entirely

Rejected. Generation remains useful for research and future consolidation work when its output is quarantined.

### Permit fuzzy experimental scope matching

Rejected. Similar service names and overlapping metric vocabulary are common, making fuzzy scope unsafe for executable
investigation behavior.

## Implementation and Follow-up

The containment changeset implements:

- disabled generation, automatic quarantine persistence, and generated retrieval defaults
- a separate generated-archetype schema and quarantine store
- curated-registry rejection and filtering of legacy generated entries
- exact-scope experimental retrieval
- source-attribution and cross-service regression instrumentation
- cross-tenant, cross-service, lifecycle, version, and Investigation Contract tests

Follow-up changesets must separately deliver:

1. Clean, governed long-lived, and adversarial benchmark states with frozen tolerances.
2. Operational Knowledge authority consolidation and a projection-rebuild contract.
3. Tenant-bound repositories and shared transaction boundaries where needed.
4. Tenant-native signal and search projections.
5. A shadow-only generated-archetype experiment and evidence review. A promotion-lifecycle RFC is conditional on that
   review selecting generated archetypes as a durable runtime abstraction.
