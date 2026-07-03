# ADR-018: Investigation is the primary product, exposed as a versioned contract

## Status

Proposed

## Context

Tacit's public surface today is effectively prompt → dashboard (`DashResponse`: URL, UID, panel count,
prose summary). Internally the pipeline already produces a much richer object: intent, evidence
requirements, evidence resolutions, observations, validation outcomes, contextual/telemetry-evidenced
culprit ranking with reason codes, abstention, warnings, and provenance. The API exposes the least
interesting artifact and discards the most interesting one at the boundary.

Meanwhile the intended consumers are widening beyond humans: SRE agents, self-healing loops, and other
automation need a machine-consumable answer to "what evidence exists for this operational claim?" —
with abstention as a first-class outcome. Prose summaries and dashboard URLs cannot serve that audience.

ADR-001 (investigation-first), ADR-008 (dashboards are evidence artifacts), ADR-015 (evidence
lifecycle), and ADR-017 (Operational IR) all point at the same conclusion without stating it.

## Decision

Investigation is the primary product. Dashboards, the CLI, Slack, the REST API, MCP, and future
agent protocols (A2A) are renderers or consumers of a versioned **Investigation Contract**.

The contract is a contract, not merely a schema: it carries guarantees consumers may rely on.
An Investigation MUST contain:

- Intent
- Evidence Requirements
- Observations
- Ranked Suspects (or an explicit Abstention with reason codes)
- Provenance
- Warnings

The contract is versioned (`investigation.v1`). It never changes silently; breaking changes require a
new version published alongside the old one.

**Abstention is not an exceptional outcome. It is a valid investigation result.** Investigation
contracts MUST represent abstention explicitly, and evaluation MUST measure abstention quality
independently from ranking accuracy. The contract therefore carries a first-class grounding block
alongside ranked suspects:

```json
{
  "ranked_suspects": [],
  "grounding": {
    "status": "supported | partial | insufficient",
    "abstained": true,
    "reason": "missing_runtime_evidence",
    "missing_observations": ["redis_miss_rate", "db_latency"]
  }
}
```

Downstream consumers — human or agent — must always be able to see not only what Tacit concluded but
whether the conclusion is evidence-supported, and if not, exactly which observations were missing.
A grounding layer answers one question: *can this claim be supported with evidence?* The valid answers
are yes, no, and not yet — and "not yet" must be machine-readable.

Positioning follows from the architecture: Tacit is an **evidence grounding layer** for operational
investigations. It does not produce truth; it produces evidence, observations, ranked suspects, and
abstention, with provenance for every claim.

Sequencing:

1. Define and freeze `investigation.v1` (contract document + Pydantic models + JSON Schema export).
2. `tacit investigate "<prompt>" --json` becomes the first renderer, emitting the full contract object.
   The existing REST/Web/Slack surfaces migrate to render the same object.
3. MCP is the first *external* consumer of the contract — it consumes `investigation.v1`, it does not
   define it. A2A and other protocols follow the same rule.
4. Alert-triggered investigations: an alert firing may start an investigation so the result already
   exists when a human or agent opens the incident. Push, not only pull.

Scope guardrail (reaffirming ADR-002 and ADR-015): the Investigation Contract stays narrow —
evidence → observations → suspects. It is not memory, planning, remediation, actions, or conversation.
Consumers that need those build them *on top of* investigations; Tacit does not absorb them.

Taken together with the prior ADRs, this completes Tacit's operating philosophy:

1. Evidence before conclusions (ADR-015).
2. Provenance before confidence (ADR-008, ADR-014).
3. Context before telemetry (ADR-016).
4. Abstention before unsupported certainty (this ADR).

## Consequences

- The pipeline architecture becomes: Operational Artifacts → Operational IR → Investigation Contract →
  renderers (Dashboard, Slack, CLI, REST, MCP, A2A).
- `DashResponse` becomes a rendering of an investigation, retained for backward compatibility; the
  dashboard URL is one field of the investigation, not the product.
- Agents can build against Tacit without scraping prose.
- Evaluation gains a **Grounding Quality** benchmark family, separate from ranking accuracy:
  abstention precision (when Tacit abstained, was abstention correct?), abstention recall (when
  evidence was insufficient, did Tacit abstain?), unsafe assertion rate (evidence insufficient but a
  culprit was asserted anyway — must trend to zero), and confidence calibration (does stated
  confidence track evidence completeness?). The optimization objective is maximum *trustworthy*
  answer rate, not maximum answer rate: a self-healing agent recovers from "I don't know"; it does
  not recover from "restart Redis" when Redis wasn't the problem.
- Measuring abstention recall requires benchmark cases where abstention is the correct answer:
  scenarios with deliberately withheld or absent evidence. The existing 100-prompt suite contains
  none, so the Grounding Quality family requires a new adversarial-insufficiency case family.
- Contract versioning discipline is a permanent obligation: every field added to `investigation.v1`
  is additive; removals or semantic changes require `investigation.v2`.
- Per-consumer identity, audit ("which agent asked, what was it told"), and bounded-latency answer
  modes become roadmap requirements rather than nice-to-haves.
- The README repositioning follows: "Tacit produces evidence-grounded operational investigations that
  any human or agent can consume. Dashboards are one visualization of an investigation."

## Implementation Notes

Implementation status: not implemented (decision record).

Existing building blocks validated against the current repository:

- `tacit/models/schemas.py`: Intent, EvidenceRequirement/Resolution/Observation, CulpritRanking
  (with abstention reason codes) already model most contract fields.
- `tacit/history.py` + `tacit/pipeline/recording.py`: investigation records already persist intent,
  stages, queries, validation, and provenance per run.
- `tacit/pipeline/progress.py` + `/api/v1/chart/stream`: stage events already stream; the final SSE
  `result` event is the natural place to emit the contract object.
- `tacit/evidence.py`, `tacit/culprit_ranking.py`: produce the observations and ranked suspects the
  contract requires.

TODO:

- Author the `investigation.v1` contract document (guarantees, field semantics, versioning policy)
  and generate JSON Schema from the Pydantic model.
- Add `tacit investigate --json` as the first renderer; keep `tacit test` as the human-friendly view.
- Return the investigation object (or a link to it) from `/api/v1/chart` alongside `DashResponse`.
- Implement Grounding Benchmark v1 as specified in
  [docs/evaluation-grounding-benchmark-v1.md](../evaluation-grounding-benchmark-v1.md):
  five adversarial-insufficiency case families scored against expected grounding status, with
  unsafe assertion rate as a release gate.
- Add the `grounding` block to the contract model, populated from the existing evidence
  observation and abstention-reason machinery (`tacit/evidence.py`, `tacit/culprit_ranking.py`).
- Specify per-consumer auth/audit requirements before exposing the contract to autonomous agents.
