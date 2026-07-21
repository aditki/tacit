# ADR-017: Artifact learning compiles operational artifacts into Operational IR

## Status

Proposed

ADR-019 constrains this decision: Operational IR outputs are candidate inputs. They may affect a normal investigation
only through an eligible Operational Knowledge revision selected into the investigation's knowledge snapshot.

## Context

Tacit learns from operational artifacts such as dashboards, alerts, runbooks, incidents, documentation, deployments,
service catalogs, and ownership metadata. Those sources are heterogeneous, but downstream investigation should not need
vendor-specific or artifact-specific logic.

Earlier ingestion surfaces risked becoming separate product paths: dashboard learning, alert learning, runbook
ingestion, incident ingestion, and documentation ingestion. That would make ranking and evidence resolution depend on
the source system rather than on the operational knowledge Tacit learned.

Tacit also has an explicit contextual-versus-telemetry boundary. Artifact learning must preserve that split: artifacts
produce operational knowledge and evidence requirements, not root causes or telemetry-backed conclusions.

## Decision

Tacit will implement Artifact Learning as a vendor-neutral compiler from operational artifacts into a small
Operational Intermediate Representation.

```text
Artifact
-> Extractor
-> Operational IR candidates
-> Existing learning pipelines
-> Contextual Ranking
-> Optional Telemetry-Evidenced Ranking
```

Extractors do not emit culprits, RCA claims, hypotheses, investigation sessions, or mitigation actions. A new IR
primitive is introduced only when an existing downstream stage consumes it.

## Operational IR

Artifact extractors emit only these first-class outputs:

- `EvidenceRequirement`: a check that evidence resolution can later resolve into an observation.
- `OwnershipHint`: ownership, escalation, or routing context.
- `DependencyHint`: dependency context usable by contextual ranking.
- Signal mapping candidates: candidate signal mappings routed through the existing signal store lifecycle.

Extractors preserve raw strings and provenance. Entity resolution is a separate stage with `resolved`, `unresolved`, and
`ambiguous` states. Unresolved values remain indeterminate; they are not guessed, silently dropped, or converted into
causal conclusions.

## Review State

Artifact learning uses the same review vocabulary as the rest of Tacit:

- `candidate`
- `approved`
- `trusted`
- `rejected`

Staleness is lifecycle metadata, not a review state.

## Artifact Lifecycle

Learned artifacts follow the same lifecycle semantics as alert ingestion:

- same content: skip extraction refresh, update `last_seen_at`
- changed content: update fingerprint and `updated_at`
- missing from a complete crawl: mark stale and set `missing_since`
- reappears: clear stale state and preserve `first_seen_at`

Artifacts are not hard-deleted by default. Stale artifacts may have reduced contribution or be removed from active search
indexes while the structured learned record remains preserved.

## Provenance

Every learned artifact and extracted IR row carries provenance:

- artifact ID
- artifact type
- source vendor and instance where applicable
- source excerpt
- fingerprint or extraction hash
- review state

Tacit centralizes operational knowledge without losing where it came from.

## Ranking Rules

Contextual ranking may consume ownership hints, dependency hints, signal candidates, evidence requirement metadata,
dashboards, alerts, runbooks, incidents, and documentation. It must not require production telemetry.

Only a resolved `EvidenceRequirement -> Observation` path can strengthen evidence-backed ranking. Ownership alone cannot
create a causal suspect. Dependency alone can create a plausible candidate but cannot assert cause. Runbooks suggest
checks; they do not produce RCA.

Ranked outputs remain `causal_status="suspect_not_proven"` unless a future telemetry-evidenced layer explicitly
corroborates them.

## Runbook Extraction V1

The initial runbook extractor supports conservative patterns:

- `Check X`, `Verify X`, `Observe X`, `Look at X` -> `EvidenceRequirement`
- `Escalate to X`, `Contact X`, ownership labels -> `OwnershipHint`
- `Depends on X`, `Calls X`, `Downstream X` -> `DependencyHint`
- metric-like names -> signal mapping candidates

Mitigations such as restart, rollback, scale, or flush are ignored as non-evidential until Tacit has a runbook execution
or mitigation planning model.

## Incident History Extraction V1

Incident history is learned through the same Operational IR. Incident records may produce:

- observed symptoms and evidence as `EvidenceRequirement` rows with observed state
- metric-like names as signal mapping candidates
- dependency and ownership hints when the record states them
- investigation references as artifact provenance and searchable source text

Incident records must not produce learned root causes. Lines such as root cause, culprit, caused by, or resolved by are
treated as ignored causal claims. A historical incident can suggest what evidence was observed and where investigators
looked, but it cannot become a causal assertion without separate evidence resolution.

## Benchmarking

The frozen contextual culprit ranking baseline remains unchanged. Runbook lift is measured by running the same cases
with only runbook context added, then reporting deltas for:

- Top-1 recall
- Top-3 recall
- MRR
- false culprits
- unsupported RCA
- runbook contribution rate
- runbook tie-break rate
- runbook noise rate
- indeterminate rate

Top-3 should remain preserved, and false culprit or unsupported RCA regressions fail the benchmark.

Benchmark reports must label denominators per metric. In the current frozen fixture, the suite has 47 total cases: 38
scorable culprit-bearing cases and 9 negative/noise cases. Top-1 recall, Top-3 recall, and MRR are computed over the 38
scorable cases. Source contribution and source noise rates are computed over the full 47-case benchmark unless a report
explicitly states otherwise.

Because the disclosed synthetic candidate set has size 5, reports also include random-ranker baselines:

- random Top-1: 0.20
- random Top-3: 0.60
- random MRR: 0.4567

MRR is computed over the full candidate set, not truncated at Top-3. This keeps observed MRR and random MRR comparable
by construction. In the current fixture Top-3 is 1.0, so truncation would not change the reported observed MRR, but the
convention is explicit for future fixtures.

Top-3 at 1.0 therefore confirms candidate preservation in the top 3 of 5. The discriminating lift story is the Top-1
and MRR ablation across artifact sources.

The combined contextual ranking, alert context, and runbook artifact benchmark is frozen as
`Contextual Ranking + Alerts + Runbooks Baseline v1`. Incident-history evaluation starts after that baseline and must
measure lift through Operational IR rather than through direct learned-root-cause labels.

Incident-history lift must report:

- incident contribution rate
- incident tie-break rate
- incident noise rate
- ignored causal-claim count
- false culprit rate
- unsupported RCA rate

The critical regression guard is that an incident record can say "Root cause was Redis", but if the extracted observed
evidence points elsewhere or is absent, Redis must not be promoted from that causal claim. This proves Tacit can learn
from past investigations without institutionalizing past RCA mistakes.

Artifact-learning robustness is a separate gate from lift. It must include:

- an adversarial RCA phrase corpus of at least 50 causal phrasings
- a precision corpus of legitimate observe/check/evidence phrases that must not be causally suppressed
- noise injection across artifact-count and noise-ratio levels
- contradictory artifact checks where runbooks, alerts, and incidents point at different suspects

The expected result is not higher ranking at all costs. The expected result is that the same denominator contract remains
stable, causal claims are ignored as evidence, Top-3 and safety metrics do not regress, and irrelevant enterprise noise
does not create false culprits.

The precision corpus should grow toward parity with the suppression corpus over time. Precision is the side that catches
over-aggressive suppression of real checks, so it should not remain materially smaller than the causal-phrase corpus.

This robustness gate is hardened evidence, not proof of large-scale validation. Large-dataset live ingestion remains the
gate for retiring noise-at-scale risk.

## Consequences

Tacit is not a document ingestion system, knowledge graph, or RAG platform. Tacit is an operational compiler: it
translates fragmented operational artifacts into a common Operational IR that evidence resolution and ranking can
consume consistently.

Future integrations, such as incident history and documentation adapters, should be thin artifact adapters that emit the
same IR rather than new core concepts.
