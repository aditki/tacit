# ADR-019: Operational Knowledge is governed, scoped, and revisioned

## Status

Accepted

## Context

Tacit extracts useful operational propositions from dashboards, alerts, runbooks, incidents, and human
corrections. Treating an extraction as truth would let ambiguous entities, copied sources, stale artifacts,
or malicious content influence investigations. The existing artifact-specific records also lacked one shared
way to explain review state, lifecycle, eligibility, corroboration, conflicts, and use.

## Decision

Reusable propositions enter a shared `KnowledgeCandidate` envelope while retaining their typed semantic
payload. Review state, lifecycle status, and investigation eligibility are independent dimensions. Candidates
must carry tenant scope, stable proposition identity, entity-resolution results, evidence lineage, and
provenance.

Entity binding uses exact stable IDs, canonical names, and approved scoped aliases. Fuzzy matches are
suggestions only. Proposition identity includes kind, direction, subject, object or concept, and normalized
scope. Corroboration counts independent lineage groups and source families, not row count.

Promotion is deterministic and policy-versioned by knowledge kind. Eligible candidates become immutable
`KnowledgeRevision` records. Investigations consume immutable snapshots of exact revisions and record every
item considered, including scope, review, lifecycle, conflict, and eligibility rejections. Contextual knowledge
may adjust bounded ranking support but never becomes live telemetry or root-cause proof.

Corrections create candidates. Approval or trust is an authorization-controlled transition that produces a
new revision or supersedes an old item; it never mutates an investigation or knowledge revision. Historical
replay keeps the historical knowledge snapshot. Current-engine replay explicitly selects current knowledge.

Artifact content is untrusted. It cannot alter policy, review state, permissions, or ranking weights. Suspicious
instruction-like content is flagged. Every table and lookup is tenant-scoped, and sensitive source content is
represented by bounded excerpts, hashes, and references.

## Consequences

- Learning is inspectable through the REST API, CLI, review queue, events, promotion decisions, and revisions.
- Duplicate artifacts cannot manufacture independent corroboration.
- Unresolved, ambiguous, rejected, stale-disallowed, expired, withdrawn, and superseded knowledge cannot
  contribute to new investigations.
- The signal store and artifact IR remain compatible through migration adapters; they are not rewritten.
- Policy changes require benchmark and replay evaluation because they can change promotion and investigation
  output.
- Real-environment scale validation remains a separate milestone and is not implied by this implementation.

## Implementation

The implementation lives under `tacit/knowledge/`, with the frozen learning corpus in
`tacit/data/operational_learning_v1.json`. `tacit operational-learning-benchmark` behavior is exposed by the
benchmark runner and included in anonymous assessment bundles. The Investigation Contract records
`knowledge_snapshot_ref` and detailed `knowledge_usage` entries.
