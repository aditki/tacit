# ADR-018: Investigation is the primary product, exposed as a versioned contract

## Status

Accepted

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

Canonical vocabulary:

- **Investigation**: the persistent logical investigation, identified by a stable ID such as `inv_123`.
- **Investigation Contract**: the versioned, machine-consumable product document for one revision.
- **Investigation Record**: the durable storage record that links the logical investigation to its current revision.
- **Investigation Revision**: an immutable version of the contract, identified by `(investigation_id, revision)`.
- **Investigation Run**: one execution attempt; it may fail before producing a valid revision.
- **Replay**: recomputation from captured historical inputs without contacting external systems.
- **Refresh**: rerun resolution against current or newly available external data.
- **Correction**: human feedback scoped to an investigation revision.
- **Knowledge Candidate**: a provenance-bearing correction candidate awaiting review; it is not truth by default.
- **Assessment Bundle**: a portable package containing an investigation, captured inputs, expected outcomes, and comparison metadata.
- **Grounding**: the explicit classification of observed, inferred, contradicted, missing, and unsafe claims.
- **Decision Log**: ordered runtime decisions with reason codes, inputs, outputs, and mechanisms.
- **Artifact Contribution**: an explicit additive or negative contribution from an operational artifact to a contract field.

Canonical lifecycle states:

- `created`
- `resolving`
- `observing`
- `ranking`
- `grounding`
- `completed`
- `failed_resolution`
- `failed_observation`
- `failed_ranking`
- `failed_validation`
- `cancelled`

Completion does not imply root cause proof. Completed investigations classify their grounding as:

- `supported`
- `partially_supported`
- `insufficient_evidence`
- `contradicted`
- `indeterminate`

Run types are:

- `initial`
- `replay`
- `refresh`
- `correction_application`
- `migration`

These values are represented as typed enums in `tacit/investigation_contract.py`.

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

Implemented:

- `tacit/investigation_contract.py` defines `tacit.investigation` schema version `1.0`, typed lifecycle enums,
  structural grounding, decision log, normalized provenance, queries, corrections, renderings, and runtime
  fingerprints.
- `tacit/schemas/investigation/v1.0.schema.json` is generated from the Pydantic model and packaged as a
  runtime resource. Future contract families and versions follow `tacit/schemas/{family}/v{version}.schema.json`.
- `tacit investigate --json` is the first contract renderer; `tacit test` remains the human-friendly dashboard path.
- `/api/v1/chart` remains backward compatible while returning `investigation_id` and `investigation_revision`.
- `/api/v1/investigations/{id}/contract`, `/revisions`, `/compare`, `/replay`, and `/corrections` expose inspect,
  compare, exact replay, and correction-candidate workflows.
- `InvestigationStore` persists append-only revisions, replay runs, events, and knowledge candidates.

Remaining:

- Implement Grounding Benchmark v1 as specified in
  [docs/evaluation-grounding-benchmark-v1.md](../evaluation-grounding-benchmark-v1.md):
  five adversarial-insufficiency case families scored against expected grounding status, with
  unsafe assertion rate as a release gate.
- Specify per-consumer auth/audit requirements before exposing the contract to autonomous agents.
