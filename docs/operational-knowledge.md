# Operational Knowledge

Operational Knowledge is Tacit's governed layer for reusable dependency, ownership, signal-mapping,
evidence-requirement, artifact-quality, and investigation-pattern propositions.

## Lifecycle

1. Artifact extraction or a structured correction creates a provenance-bearing candidate.
2. Entity resolution binds exact, scoped entities or leaves the candidate ambiguous or unresolved.
3. Proposition normalization assigns a stable, direction- and scope-preserving key.
4. Corroboration groups evidence by lineage and source family; conflict analysis remains visible.
5. A versioned kind-specific policy records a promotion, retention, rejection, expiry, or supersession decision.
6. Promotion creates an immutable knowledge revision.
7. Investigations select an immutable snapshot and record every applied or rejected usage disposition.
8. Corrections produce new candidates and revisions; affected investigations can be replayed.

## Operator Surface

Use `tacit learn status` for grouped discovery, resolution, governance, lifecycle, quality, and usage counts.
Use `tacit knowledge candidates`, `list`, `show`, `explain`, `conflicts`, `history`, and `usage` for inspection.
Review a queued candidate with:

```bash
tacit knowledge review <candidate-id> --approve --reviewer <actor>
tacit knowledge review <candidate-id> --reject --reviewer <actor>
```

Trust is a separate privileged action using `--trust`. The web workspace contains the same focused review
queue. REST clients use `/api/v1/knowledge`; configured permissions are `knowledge.read`, `knowledge.review`,
`knowledge.trust`, `knowledge.reject`, `knowledge.correct`, `knowledge.export`, and `knowledge.override`.
The override permission is required to assert authoritative-source or live-verification policy inputs during
review, including authoritative correction reviews. When `knowledge_tenant_id` is `*`, artifact-learning CLI
commands require `--tenant <tenant-id>` so governed candidates cannot be written to a wildcard scope.

## Evaluation

The packaged Operational Learning v1 corpus evaluates entity binding, proposition normalization,
corroboration, conflict analysis, promotion safety, and prompt-injection resistance independently from culprit
ranking. Anonymous assessment bundles include `learning_evaluation_summary.json` without raw artifact text.
