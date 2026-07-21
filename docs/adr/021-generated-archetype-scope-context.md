# ADR-021: Generated-archetype evaluation uses explicit request scope and fails closed

## Status

Accepted

Implementation pending. Current exact-scope selection accepts optional tenant and environment arguments, while the
pipeline runner does not supply them end to end.

## Context

Generated archetypes are organization-specific investigation programs. A broad or incorrectly inferred match can alter
metric selection, query construction, evidence requirements, and ranking. Tenant and service isolation therefore cannot
be treated as a ranking preference.

The containment implementation introduced exact-scope comparison, but the runtime selector still permits two unsafe
ambiguities:

- when no tenant is passed, it falls back to `learned_archetypes_tenant_id` from process configuration
- when no environment is passed, it constructs an empty environment set

The pipeline runner currently calls selection without either value. A configured tenant is not proof of which tenant
owns a request, and an absent environment is not an empty exact scope. This makes the development retrieval escape hatch
insufficient as a foundation for shadow evaluation.

## Decision

Every generated-archetype validation, shadow evaluation, and any future applicability decision must consume one frozen,
request-scoped `ArchetypeSelectionContext`. The selector must not reconstruct identity from process configuration.

The target contract is:

```python
@dataclass(frozen=True)
class ArchetypeSelectionContext:
    tenant_id: str
    service_refs: frozenset[str]
    environment_refs: frozenset[str]
    environment_resolution: ScopeResolution
    archetype_kind: str
    generation_version_policy: str
    investigation_intent_ref: str
```

`ScopeResolution` distinguishes at least `resolved`, `not_provided`, `unresolved`, and `ambiguous`. An empty collection
does not carry that distinction by itself.

### Sources Of Scope

- Tenant comes from the authenticated request boundary or an explicit tenant supplied by a local CLI operation.
- Services come from the frozen, entity-resolved Investigation Intent.
- Environment comes from the frozen execution scope and retains its resolution state.
- Archetype kind comes from the invoking investigation stage, not from the generated artifact.
- Generator-version policy comes from an explicit evaluation policy.
- The intent reference binds the context to captured replay inputs.

Configuration may enable shadow evaluation, select a registry, and choose a generator-version policy. It may not provide
a missing runtime tenant, service, or environment identity.

### Applicability

A generated revision is shadow-applicable only when:

- the tenant is concrete, non-wildcard, and equal
- the request service set is non-empty, fully resolved, and exactly equal
- environment resolution is `resolved`, the set is non-empty, and exactly equal
- archetype kind is exactly equal
- the immutable generator version is allowed by the frozen policy
- the revision is validated and enrolled for shadow evaluation
- the revision is inside any evaluation validity interval

There is no subset match, overlap match, fuzzy alias match, global fallback, default-tenant fallback, or "best available"
generated archetype.

When a required dimension is missing or ambiguous, the result is `not_applicable` with a reason code. It is not an error
that should fail a normal investigation, and it is never interpreted as broad scope.

### Invariants

`ARCHETYPE-SCOPE-001`: The request tenant is derived from authenticated or explicitly supplied operation context.

`ARCHETYPE-SCOPE-002`: The complete resolved service set comes from the frozen Investigation Intent.

`ARCHETYPE-SCOPE-003`: Missing, unresolved, and ambiguous environment states are distinct from an exact environment.

`ARCHETYPE-SCOPE-004`: Process configuration cannot fill missing runtime identity.

`ARCHETYPE-SCOPE-005`: Applicability fails closed with a stable reason code when required context is unavailable.

`ARCHETYPE-SCOPE-006`: Replay uses the captured selection context, unless a replay mode explicitly requests a
current-context comparison and records that distinction.

`ARCHETYPE-SCOPE-007`: Scope normalization is shared by generation, persistence, and selection; each layer must not
invent its own representation.

## Diagnostics And Audit

Every generated-archetype evaluation records:

- a hash or immutable reference to the selection context
- candidate revision reference
- per-dimension match result
- applicability disposition and reason codes
- evaluation mode (`shadow` only under ADR-020)
- whether output was applied, which must be `false`

Raw cross-tenant candidate details must not be exposed to a caller. Aggregate rejection counters may be logged without
revealing another tenant's identifiers.

## Consequences

- Tenant and service isolation become validity conditions rather than ranking heuristics.
- Environment-free prompts cannot evaluate generated archetypes until environment is resolved explicitly.
- Some potentially useful shadow matches are intentionally skipped; the diagnostic explains why.
- Replay remains attributable because the exact selection context is captured.
- Pipeline APIs must thread one context object instead of optional scalar arguments.
- Curated archetype classification remains available when generated evaluation is not applicable.

## Alternatives Considered

### Use configured tenant and empty environment defaults

Rejected. Configuration describes deployment policy, not request ownership, and an empty set erases whether scope was
unknown.

### Allow partial service overlap

Rejected. Multi-service investigations and shared metric vocabulary make overlap an unsafe authority boundary.

### Normalize and infer missing scope inside the selector

Rejected. Scope resolution belongs upstream and must be frozen for replay. Re-resolving inside selection creates hidden,
time-dependent behavior.

### Fail the whole investigation when generated scope is incomplete

Rejected. Generated evaluation is optional and shadow-only. It should fail closed locally while curated investigation
continues.

## Implementation And Follow-up

The first slice of the
[Generated Archetype Evaluation Roadmap](../generated-archetype-evaluation-roadmap.md) must:

1. Introduce the immutable context and shared normalizer.
2. Build it from request-scoped pipeline dependencies and the frozen Investigation Intent.
3. Capture it in replay inputs without changing legacy output fingerprints.
4. Remove selector fallbacks to `learned_archetypes_tenant_id` and empty environment scope.
5. Add tenant, service, environment, kind, version, missing-scope, and replay tests.

No shadow evaluator should be merged until these invariants pass end to end.
