# ADR-020: Generated archetypes must prove value in shadow mode before lifecycle investment

## Status

Accepted

Containment is implemented. Validation, governed shadow evaluation, and the evidence review are not yet implemented.

## Context

Tacit's earlier dashboard-learning path generated archetype YAML and could register it into the same global registry as
curated product templates. Long-lived development state then reduced recall and signal-to-noise ratio because a generated
program could match broadly and compete with curated behavior.

ADR-019 removed that authority: generated output is quarantined, excluded from normal retrieval, and disabled by default.
That protects investigations, but it leaves a design question unanswered:

> Are generated investigation programs a useful durable abstraction, or are their useful parts better represented as
> Operational Knowledge such as signal mappings, evidence requirements, dependencies, and candidate roles?

Operational Knowledge has demonstrated value through scoped ranking lift, evidence survival, immutable revisions,
snapshots, replay, and provenance. Generated archetypes have not demonstrated equivalent value. Building review queues,
promotion policy, expiry, rollback, and retirement before answering that question would make the abstraction expensive
before it is proven.

The current file-based exact-scope retrieval mode does not answer the question safely. It requires manual mutation from
`quarantined` to `experimental`, has no governed transition record, and can influence runtime output before a shadow
comparison exists.

## Decision

Generated archetypes are experimental hypotheses, not Operational Knowledge revisions and not trusted runtime programs.

Tacit will implement only the minimum lifecycle needed to measure them:

```text
generated
    -> quarantined
    -> validated
    -> shadow
```

There is no promotion state in this decision. In particular, this ADR does not authorize:

- automatic approval
- human approval for runtime use
- exact-scope runtime application
- replacement or supplementation of curated archetypes
- expiry, rollback, or retirement workflows for an applied artifact
- global or fuzzy generated-archetype retrieval

Normal investigations continue to use curated archetypes and eligible Operational Knowledge only.

### State meanings

`quarantined` means the generated artifact is stored with complete origin metadata but has no evaluation authority.

`validated` means a specific immutable artifact revision passed structural, identity, provenance, scope, and safety
checks. Validation is not approval and does not permit runtime influence.

`shadow` means the revision may be evaluated against an investigation after exact-scope applicability succeeds. Its
projected selection and output are recorded separately, but the authoritative investigation remains unchanged.

A rejected or invalid artifact remains quarantined with reason codes. Changed content creates a new immutable artifact
revision and must be validated again.

## Authority And Persistence

YAML is an import/export and inspection format, not lifecycle authority. Editing `retrieval_status` in a file must not
change eligibility.

The minimal shadow registry must persist:

- stable generated-archetype ID and immutable revision
- semantic fingerprint and parent revision where applicable
- tenant, exact service set, explicit environment state, archetype kind, and generator version
- generation run and source lineage
- validation result and reason codes
- shadow enrollment metadata
- creation actor and timestamps

The registry may share the Operational Knowledge database and transaction infrastructure, but a generated archetype
must remain a distinct object type. An executable investigation program must not be smuggled through a fact or mapping
revision merely to reuse an existing promotion path.

## Validation Boundary

Validation must be deterministic, versioned, and reason-coded. At minimum it checks:

- tenant, service, environment-state, kind, generator version, and stable identity are present
- every entity resolves exactly in the same tenant
- the generation run and all source artifacts exist and preserve lineage
- no wildcard tenant or service scope exists
- no executable remediation instruction is treated as evidence
- no historical causal conclusion is encoded as a reusable fact
- evidence templates and candidate roles are bounded, non-duplicative, and structurally valid
- hard safety constraints cannot be removed or overridden

Validation failure cannot be bypassed by configuration or file mutation.

## Shadow Evaluation

For an applicable validated revision, the evaluator computes two paths from the same frozen inputs:

```text
authoritative: curated archetypes + eligible Operational Knowledge
shadow:        authoritative inputs + one generated archetype revision
```

Only the authoritative path produces the Investigation Contract and user-visible renderings. Shadow evaluation must not
change:

- selected archetypes or candidate rankings
- evidence requirements, observations, or grounding
- queries, panels, warnings, summaries, or conclusions
- the Investigation Contract output fingerprint
- persisted current revision or replay result

Shadow results are sidecar evaluation records linked to the investigation run and immutable generated revision. They
record applicability, would-select disposition, projected rank, coverage delta, output delta, safety findings, latency,
and reason codes. They may be exposed through diagnostics or benchmark reports, but cannot be labeled as applied
context.

## Evidence Gate

Shadow data must be evaluated against frozen, reproducible states:

- clean curated state
- governed long-lived state with Operational Knowledge but no generated archetypes
- shadow state
- deliberately mismatched tenant, service, environment, kind, and generator-version states

The decision package must report:

- critical-signal recall and negative correctness
- signal-to-noise ratio and unsupported assertion rate
- cross-service and cross-tenant would-select rates
- exact-scope rejection rate
- would-select and unique-contribution rates
- duplicate-curated and decomposable-to-Operational-Knowledge rates
- projected output and latency deltas
- results on untouched holdouts and long-lived replay state

Safety gates are absolute: cross-tenant selection, cross-service selection, or an unsafe assertion increase blocks any
future promotion proposal. Utility thresholds, case counts, observation duration, and dataset hashes must be frozen
before reviewing results rather than selected after seeing them.

## Decision After Shadow Evidence

Shadow evaluation culminates in a new ADR choosing exactly one direction:

1. **Build a governed generated-archetype lifecycle.** Choose this only if generated programs provide repeatable,
   non-decomposable value beyond curated archetypes and Operational Knowledge.
2. **Compile useful discoveries into Operational Knowledge.** Choose this when the value is explained by mappings,
   evidence requirements, dependencies, ownership, or candidate roles.
3. **Retire generated archetypes as a runtime abstraction.** Choose this when value is absent, redundant, or too risky.

No runtime promotion implementation begins before that decision.

If option 1 wins, a separate ADR must define review authorization, immutable promotion revisions, benchmark policy,
validity intervals, expiry, rollback, retirement, cache invalidation, historical replay, and exact-scope runtime
application. The fuller lifecycle proposal is retained as input to that future decision, not accepted by this ADR.

## Consequences

- Tacit gathers evidence about generated archetypes without risking investigation quality.
- Engineering effort remains focused on Operational Knowledge, the learned abstraction that has demonstrated value.
- Shadow storage and instrumentation are built before review or promotion UX.
- The existing manual experimental escape hatch remains unsupported and is removed when registry-backed shadowing lands.
- A useful generated pattern may ultimately strengthen Operational Knowledge instead of creating a second governance
  system.
- Time-to-runtime-use is intentionally longer because runtime use is not yet justified.

## Alternatives Considered

### Build the complete promotion lifecycle now

Rejected for now. It presumes generated archetypes are a permanent product concept before shadow data establishes value.

### Keep manual exact-scope YAML activation

Rejected. Manual file mutation bypasses authority, audit, immutable transitions, and reproducible evaluation.

### Delete generation immediately

Rejected. Quarantined shadow evaluation is cheap enough to test whether the abstraction has unique value.

### Treat generated archetypes as ordinary Operational Knowledge revisions

Rejected. An executable investigation program has interactions and safety properties that cannot be reviewed as an
independent fact or mapping.

## Implementation And Follow-up

Implementation is sequenced by the
[Generated Archetype Evaluation Roadmap](../generated-archetype-evaluation-roadmap.md).

ADR-019 remains the authority boundary. ADR-021 defines the request-scoped applicability contract required before
shadow evaluation can run.
