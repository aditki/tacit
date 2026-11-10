# GAMMA Architecture Readiness Review

Status: architecture review, not a rewrite proposal

Reviewed: 2026-08-24

## Executive summary

Tacit's current pipeline is ready for incremental GAMMA work. It does not need
a V2 workflow engine or stateful investigation-session rewrite first. Planning,
catalog discovery, archetype selection, evidence declaration and resolution,
query generation, validation, evidence rescue, suspect ranking, publication,
and immutable investigation contracts have distinct models or module
boundaries.

The architecture has advanced beyond the historical context in issue 23:

- evidence is first-class through `EvidenceRequirement`,
  `EvidenceResolution`, `EvidenceObservation`, and `EvidenceRecord`;
- symptom and evidence-gap rescue run through normal query validation before a
  panel can survive;
- `rank_culprits()` emits contextual or telemetry-evidenced suspect rankings
  and abstains when runtime support is absent;
- discovery, selection, binding, validation, evidence, ranking, and publication
  record reason-coded stage outcomes;
- input, catalog, resolver, observation, dashboard-composition, and concurrent
  pipeline work now have explicit bounds.

Three blockers remain before the larger GAMMA claims are credible:

1. Raw heterogeneous metrics still abstain when service ownership cannot be
   resolved uniquely. Embedded service names are not yet a measured ownership
   signal with a safe ambiguity policy.
2. The GAMMA diagnostic scorer predates the current evidence and culprit models.
   It does not establish top-1/top-3 recall, MRR, requirement survival, rescue
   activation, or critical-evidence recall from the authoritative contract.
3. Dashboard and alert crawls are bounded, but they materialize source lists
   and schedule one coroutine per item. That is not a streaming ingestion design
   for a large corpus.

The recommended path is to update measurement first, add one explicit ownership
resolution result, and introduce a bounded ingestion iterator/worker queue. Keep
the existing pipeline and contract architecture.

## Current stage boundaries

| Roadmap stage | Current owner | Assessment |
|---|---|---|
| Planning and intent | `tacit/pipeline/stages/intent.py`, `tacit/agents/intent.py` | Separated and reason-coded |
| Catalog discovery | `tacit/pipeline/stages/discovery.py`, `tacit/pipeline/discovery.py`, backend adapters | Separated; provider results are normalized as `MetricEntry` |
| Archetype selection | `tacit/pipeline/stages/archetypes.py`, `tacit/archetypes/engine.py` | Curated selection is authoritative; generated candidates remain shadow-only |
| Evidence declaration and resolution | `tacit/pipeline/stages/evidence.py`, `tacit/evidence.py` | First-class lifecycle with bounded work |
| Signal resolution | `tacit/signals/resolution.py`, `tacit/signals/store.py` | Scoped and provenance-aware; ambiguity remains conservative |
| Query and panel generation | `tacit/archetypes/engine.py`, `tacit/pipeline/stages/freeform.py`, `tacit/agents/query_builder.py` | Archetype and freeform paths are explicit |
| Query validation and rescue | `tacit/pipeline/validation.py`, `tacit/validation.py`, `tacit/evidence_artifacts.py` | Rescue cannot bypass validation |
| Suspect ranking | `tacit/culprit_ranking.py` | Implemented conservatively; quality needs live top-k measurement |
| Publication | `tacit/pipeline/stages/publish.py` | Explicit commit phase behind validation |
| Evaluation and audit | `tacit/investigation_contract.py`, `tacit/history.py`, `tests/eval/` | Contract is rich; GAMMA scorer needs to consume it directly |

`tacit/pipeline/runner.py` remains the orchestrator. It is long, but the domain
work is already delegated to stages. Splitting it further is not a GAMMA
prerequisite unless a new feature again embeds domain logic directly in the
orchestrator.

## Blocking architectural risks

### P0: Freeze a current-model GAMMA evaluation contract

The existing `tests/eval/gamma_diagnostic_harness.py` still detects suspect
language from summaries and panel titles, and its historical documentation says
culprit ranking is unavailable. That scorer can pass while the structured
ranking is wrong, or fail because wording changed while the contract is safe.

Before changing binding or fallback behavior, freeze a case manifest and score
the persisted investigation contract directly:

- requirement count and critical requirement count;
- resolved, query-built, query-valid, non-empty, and surviving counts;
- symptom-rescue and gap-rescue activation;
- panel survival and irrelevant-panel rate;
- top-1, top-3, and untruncated MRR for scorable culprit cases;
- abstention and unsupported-cause rates for healthy or evidence-absent cases;
- critical evidence recall for CPU, memory, network, and mixed incidents;
- cold-state and representative long-lived-state results separately.

This is a measurement correction, not a production-model refactor.

Run that scorer through `cold_isolation()`'s explicit `state.dependencies`.
The evaluation runtime owns fresh settings, stores, caches, and admission
control as one graph; patching global store accessors into a process-default
pipeline is not an isolation boundary and is intentionally unsupported.

### P1: Make ownership ambiguity an explicit resolution result

`tacit/evidence.py` deliberately returns `ambiguous_default_metric_owner` when
the same metric name has more than one datasource owner. Semantic resolution
also abstains on tied best owners. This is safer than guessing, but it explains
why raw service-prefixed metrics can be discovered yet produce no dashboard.

Add a small ownership-resolution component that returns candidates, features,
score, selected owner when unique, and a stable abstention reason. Features may
include exact service dimensions, datasource identity, query language,
namespace, and a bounded normalized service-name token match. Embedded names
must never silently override contradictory dimensions. Freeze precision,
coverage, and ambiguity denominators before tuning thresholds.

Keep `MetricEntry` as the catalog object. A new stateful investigation model is
not needed; evidence resolution can consume this richer result.

### P1: Stream corpus ingestion with bounded backpressure

`learn_backend_dashboards()` and `learn_backend_alerts()` cap a crawl and bound
concurrent adapter calls, but both first materialize the list and then construct
all per-source coroutines for `asyncio.gather()`. Memory and task count therefore
grow with the crawl limit, and backend pagination is hidden behind a list API.

Introduce a backend page or async-iterator contract plus a fixed-size worker
queue. Persist per-source checkpoints as work completes and retain the current
per-source authority transaction. Do not wrap an entire corpus in one SQLite
writer lock. Record queue depth, source latency, bytes/rows parsed, candidate
fan-out, and checkpoint progress.

Benchmark at the limit and limit-plus-one for catalog size, label cardinality,
resident memory, ingestion throughput, database growth, and discovery latency.

## Non-blocking risks

### P2: Evidence lifecycle projection needs stable evaluation identities

The runtime models can express required, resolved, query-valid, non-empty, and
survived states. `EvidenceObservation` carries the query and panel title, while
the persisted contract assigns stable query and observation references. The
GAMMA scorer should use those contract identities rather than infer survival
from generated query text. A separate pipeline state machine is unnecessary.

### P2: Ranking is intentionally heuristic

`tacit/culprit_ranking.py` maps evidence signals to broad suspect classes and
orders telemetry-supported candidates before contextual candidates. This is a
sound safety baseline, not proof of causal diagnosis. Validate its current
top-k behavior before adding service-graph, temporal, or trace features. Any
future feature must preserve abstention and keep contextual reasons separate
from runtime evidence.

### P2: Guarded fallback needs live scenario coverage

The current fallback location is correct: `tacit/pipeline/validation.py` detects
missing critical symptom or gap evidence, builds a bounded candidate dashboard,
runs it through the backend validator, and appends only surviving panels.
Do not move fallback ahead of resolution or after publication. The remaining
work is live coverage and thresholds, especially untouched CPU, memory,
network, mixed, healthy, and evidence-absent cases.

### P2: Freeform generation is less inspectable than archetype compilation

The freeform path has tenant-scoped cache identity, metric pre-ranking, and
query validation, but evidence requirements currently originate from
contributing archetypes. GAMMA evaluation should report archetype and freeform
paths separately. Do not hide a raw-binding failure by allowing unconstrained
freeform generation to count as equivalent evidence success.

## Answers to the review questions

1. **Are stages separated?** Yes at the domain-module level. The runner owns
   orchestration and lifecycle, while intent, discovery, selection, compilation,
   evidence, validation, ranking, and publication have separate owners.
2. **Is evidence first-class enough?** Yes for the requested lifecycle. The
   missing piece is an evaluation projection over contract IDs, not new runtime
   state.
3. **Where does ownership ambiguity drop metrics?** Primarily in
   `resolve_declared_requirements_for_archetype()` when `_unique_owner()` cannot
   select one datasource, and when equally scored live-signal matches have
   different owners. Validation later also rejects an incorrect datasource UID
   or empty query, as it should.
4. **Where should guarded fallback live?** Where it now lives: inside the
   validation/evidence-preservation stage, after the first validation result and
   before ranking/publication, with rescued queries passing the same validator.
5. **Can ranking layer on existing outputs?** It already does. Improve the
   scorer and candidate identity incrementally; do not introduce a full RCA
   session model.
6. **What are the ingestion scale risks?** Materialized listing, one task per
   source, parser memory, SQLite single-writer contention, FTS/database growth,
   and catalog/resolver scans. Existing limits prevent unbounded work but do not
   prove target-corpus throughput.
7. **What minimal refactors help?** A contract-based GAMMA scorer, typed
   ownership-resolution result, and paged producer/fixed-worker ingestion path.

## Refactor priorities

### P0

- Replace wording-derived GAMMA outcomes with contract-derived evidence and
  ranking metrics.
- Freeze manifests, denominators, scorer version, corpus checksum, and clean vs
  long-lived state before tuning production behavior.

### P1

- Add explicit ownership candidates and ambiguity reasons at signal/evidence
  resolution.
- Add bounded paged ingestion and a reproducible scale harness.
- Run the frozen CPU, memory, network, mixed, healthy, and evidence-absent
  controls after each binding, rescue, or ranking change.

### P2

- Add panel/query survival projection helpers for evaluation.
- Add ranking calibration only after baseline top-k metrics are recorded.
- Add service graph or trace features only when a corpus demonstrates that
  evidence-signal classes are the limiting factor.

## What not to refactor yet

- Do not replace the pipeline with a workflow engine.
- Do not build full stateful investigation sessions for this roadmap.
- Do not replace repository SQL with an ORM as a prerequisite; the scale risk is
  ingestion shape and query plans, not object mapping.
- Do not merge all adapters into one vendor abstraction beyond the existing
  `DashboardBackend` contract.
- Do not make generated archetypes authoritative; keep them shadow-only until
  their separate lifecycle roadmap is complete.
- Do not relax owner ambiguity or query validation to improve dashboard counts.
- Do not add causal language to a suspect ranking without stronger evidence and
  an explicit safety gate.

## Suggested 2-3 week sequence

### Days 1-3: Measurement baseline

- Update the GAMMA harness to read investigation contracts.
- Add requirement survival, rescue, top-k, MRR, and abstention metrics.
- Freeze checksums and record clean plus long-lived baselines without tuning.

### Days 4-7: Heterogeneous ownership

- Introduce the typed ownership-resolution result.
- Add canonical, prefixed, embedded-service, multi-owner, and contradiction
  fixtures.
- Tune only against frozen precision/coverage gates; preserve ambiguous
  abstention.

### Days 8-10: Guarded fallback validation

- Run untouched CPU, memory, network, mixed, healthy, and evidence-absent cases.
- Measure panel survival, critical evidence recall, irrelevant panels, and
  fallback activation.
- Fix only failures attributable to the validated rescue path.

### Days 11-13: Culprit ranking

- Establish top-1/top-3/MRR and unsupported-cause baselines.
- Add the smallest evidence feature needed for demonstrated ranking errors.
- Keep telemetry-first ordering and abstention invariant tests.

### Days 14-15: Ingestion scale proof

- Add paged producer/fixed-worker ingestion.
- Benchmark representative catalog and corpus sizes with memory, latency,
  throughput, storage, and restart checkpoints.
- Publish the machine-readable result and remaining target-scale gaps.

At the end of each slice, run the focused tests, Operational Learning and
Grounding gates where relevant, the 100-prompt clean and long-lived harness for
retrieval/binding/ranking changes, and a whole-diff architecture/security review.
