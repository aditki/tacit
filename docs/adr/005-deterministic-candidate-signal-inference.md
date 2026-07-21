# ADR-005: Deterministic candidate signal inference is the default

## Status

Accepted

Amended by [ADR-019](019-governed-knowledge-authority.md). The legacy
`auto_teach_eligible` field describes inference strength only. It does not grant
runtime authority, activate a signal mapping, or approve/register a generated
archetype.

## Context

Signal inference affects trust. If Tacit silently learns bad mappings, future dashboards become worse. The current
repo uses deterministic inference over metric names, panel titles, units, query shape, dashboard grouping, and bootstrap
patterns. The inference code records evidence, score, confidence, margin, and
legacy auto-teach eligibility as candidate-quality evidence for governance.

## Decision

Default signal inference should be deterministic, explainable, and conservative. LLM-assisted clustering or inference may
be useful later, but should be opt-in due to cost, non-determinism, and trust risk.

## Consequences

- Candidate inference should expose why a mapping was suggested.
- Review output should include enough evidence for humans to approve or reject.
- Candidate eligibility must remain separate from governed promotion and runtime projection.
- Generated archetypes are never approved through signal-inference eligibility.
- LLMs can help summarize or cluster later, but deterministic gates should remain the default learning path.

## Implementation Notes

Implementation status: implemented.

Validated against:

- `tacit/signal_inference.py`: deterministic rules and `auto_teach_eligible` logic.
- `tacit/dashboard_ingest.py`: combines taxonomy matches and heuristic candidates, preserves evidence, and records
  rejected candidates.
- `tests/unit/test_signal_inference.py`: verifies explainable heuristic behavior and conservative gates.
- `tests/unit/test_signals.py`: covers dashboard ingestion and candidate persistence.

TODO:

- If LLM-assisted signal inference is added, require an explicit config flag and record the model/source in provenance.
