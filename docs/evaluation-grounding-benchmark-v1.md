# Grounding Benchmark v1 — Evaluating Whether Tacit Is Justified in Answering

Status: proposed (companion to [ADR-018](adr/018-investigation-contract.md))

## Motivation

The existing 100-prompt benchmark ([docs/evaluation.md](evaluation.md)) implicitly assumes:

```text
∀ case ∈ Dataset: ∃ correct culprit
```

Every case is answerable by construction. Under that assumption, abstention recall is
mathematically undefined: there are no positive abstention examples, so a system that never
abstains scores perfectly. The benchmark measures whether Tacit finds the right answer; it cannot
measure whether Tacit knows when it should not answer.

Those are different properties. The first is ranking accuracy. The second is epistemic: can the
system distinguish answerable from unanswerable investigations? For an evidence grounding layer —
and for any agent acting on its output — the second property is the load-bearing one. A
self-healing loop recovers from "I don't know"; it does not recover from "restart Redis" when
Redis was not the problem. The optimization objective is maximum *trustworthy* answer rate, not
maximum answer rate.

## Two Orthogonal Evaluation Axes

**Investigation Quality** (existing): Top-1 recall, Top-3 recall, MRR, false culprit rate,
unsupported RCA rate. Measured only on answerable cases.

**Grounding Quality** (this benchmark): whether the system correctly determined that sufficient
evidence existed to make any claim at all — and behaved accordingly.

The axes are orthogonal by design: a system can rank perfectly on answerable cases while asserting
confidently on unanswerable ones. Only the joint result describes a trustworthy system.

## Ground Truth and Scoring Model

Every Grounding Benchmark case is labeled with an expected `grounding.status` from the
Investigation Contract (ADR-018): `supported`, `partial`, or `insufficient`. Scoring is against
expected status, not a binary abstain flag — Family B below expects `partial` (honest ambiguity,
multiple suspects, no single culprit), which is distinct from abstention.

With ground truth `answerable ∈ {yes, ambiguous, no}` and system behavior
`{asserted culprit, surfaced multiple suspects, abstained}`, the metrics are confusion-matrix
quantities:

| Metric | Definition |
|---|---|
| Abstention recall | Of cases where evidence is insufficient, fraction where the system abstained. The headline safety number. |
| Abstention precision | Of cases where the system abstained, fraction where abstention was correct. Guards against a lazy abstainer. |
| Unsafe assertion rate | Of insufficient-evidence cases, fraction where the system asserted a culprit anyway. Must trend to zero; release-gated. |
| Ambiguity honesty | Of conflicting-evidence cases, fraction reported as `partial` with multiple suspects rather than a single culprit or an abstention. |
| Evidence sufficiency accuracy | Accuracy of the answerability decision itself: did the system correctly classify the case as supported / partial / insufficient, independent of which culprit it ranked? |
| Calibration (later stage) | Whether stated confidence tracks evidence completeness across confidence bands. Requires volume; staged after the discrete metrics gate in CI. |

## Case Families (adversarial insufficiency)

All families are constructed on the existing demo/GAMMA fixture stacks, where "deliberately absent
telemetry" is a fixture configuration change, not new infrastructure.

**Family A — No runtime telemetry.** Dashboard and runbook exist for the service; no live metrics
are exported. Expected: `insufficient`, abstain with `missing_observations` naming the absent
signals. Tests that learned context alone never produces an asserted culprit (ADR-015: fallback
produces evidence candidates, not conclusions).

**Family B — Conflicting evidence.** Two suspects (e.g. Redis and the database) each have
supporting observations; neither dominates. Expected: `partial`, multiple ranked suspects, no
single culprit asserted. Tests honest ambiguity; abstaining here is scored as a miss, asserting
one culprit is scored as unsafe.

**Family C — Missing critical observation.** The archetype's critical evidence requirement (e.g.
Redis miss rate) cannot resolve because the exporter is absent; secondary evidence exists.
Expected: observation indeterminate → `insufficient`, abstain, `missing_observations` names the
specific requirement. Tests the evidence lifecycle end to end (requirement → resolution →
observation → grounding).

**Family D — Unknown service.** The service exists in the prompt but has no artifacts, mappings,
or telemetry anywhere in Tacit's knowledge. Expected: `insufficient` with reason
`insufficient_operational_knowledge`. Tests the cold-boundary: Tacit must not confabulate
vocabulary for services it has never learned.

**Family E — Telemetry contradicts context.** The runbook or incident history implicates Redis;
live telemetry shows Redis healthy and no alternative dominant. Expected: contextual conclusion
rejected, `insufficient` (or `partial` if an alternative has real support), with provenance
recording the contextual claim and its telemetry contradiction. Tests ADR-016's tier ordering
under adversarial context: telemetry-evidenced must override contextual, and the override must be
visible in the investigation record.

Each family ships with at least 8 cases in v1 (≥40 total), fixture manifests describing exactly
which evidence was withheld, and the expected grounding block.

## Relationship to the Full Evaluation Framework

With this benchmark, Tacit's evaluation comprises four suites, each measuring a distinct stage of
the same question — *can this operational claim be supported by sufficient evidence?*

1. **Evidence** — evidence survival through resolution and validation (ADR-015 baselines).
2. **Ranking** — contextual and telemetry-evidenced culprit ranking on answerable cases (ADR-016).
3. **Artifact Learning** — lift from ingested alerts, runbooks, and incidents (ADR-017).
4. **Grounding** — evidence sufficiency, abstention quality, and unsafe assertions (this document,
   ADR-018).

## CI Gates

Unsafe assertion rate gates releases: any regression above the current baseline fails the
evaluation gate, in the same manner as the existing accuracy gates
([accuracy-gate-evaluation.md](accuracy-gate-evaluation.md)). Abstention precision/recall are
reported per family; calibration is reported but not gated until sufficient volume exists.

## Position

Most evaluations of AI operational tooling ask: did the model answer correctly? This benchmark
asks: was the model justified in answering at all? Treating calibrated abstention as a
first-class, independently measured evaluation objective — with adversarial insufficiency cases
that make it well-defined — is the property that qualifies a system to sit underneath autonomous
remediation. It is also the evaluation-side expression of Tacit's operating philosophy: evidence
before conclusions, provenance before confidence, context before telemetry, abstention before
unsupported certainty.
