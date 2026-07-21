# Tacit Accuracy Gates — Current Evaluation

This document defines the accuracy gates, records measured results, and tracks
remaining risks before large-dataset ingestion. Curated offline slices and repeated
LLM runs now provide reproducible baselines, but they are not substitutes for
catalog-level and incident-level evaluation on the real source datasets.

## Verdict

The original implementation gaps in signal vocabulary, metadata scoring, query
validation, archetype ranking, panel caps, and summary provenance are closed and
unit-tested. Curated ClickStack, LO2, and GAMMA slices score 100% on offline semantic
mapping and critical-signal resolvability.

The system has **not** passed the overall accuracy gate. The frozen prompt holdout
failed positive usefulness (65.71%) and negative correctness (83.33%), and the
live-stack gates for routing, data-in-window, hallucination, and panel relevance
have not been measured against complete real datasets. The next meaningful step is
large-dataset ingestion and end-to-end evaluation, not additional tuning against
the development corpus.

## Formal metric definitions

The final gates are defined against a **fully labeled gold set** that must exist before a catalog-level number is reported. For each scenario, that gold set contains:

- a metric-to-signal table: every metric in the routed datasource catalog labeled with its correct signal family (or `none` for metadata/info metrics);
- the set of **critical signals** the incident requires (the signals a competent engineer would insist on);
- the **expected datasource UID** for each critical signal (ground-truth routing);
- the scenario time window.

### Metric-to-signal mapping (precision, coverage, recall)

For the inference/resolution output over the gold-set metrics:

- **TP** — metric mapped to a signal, and the mapping equals the gold label.
- **FP** — metric mapped to a signal, but the mapping ≠ gold label (includes mapping a `none` metric to any signal).
- **FN** — metric whose gold label is a real signal, but inference produced no mapping (or `none`).
- **TN** — `none` metric correctly left unmapped.

Then:

- **Semantic mapping precision** = TP / (TP + FP). *Gate ≥90%.*
- **Semantic mapping recall** = TP / (TP + FN).
- **Semantic mapping coverage** = (metrics that received any mapping) / (metrics whose gold label is a real signal). *Gate ≥80%.* Coverage and precision are deliberately separate: `signal_inference.coverage()` measures only whether *a* candidate was produced, never whether it was *correct* — so it speaks to coverage, never to precision. Precision needs the gold labels above.

### Critical-signal recall (cold vs learned)

- **Critical-signal recall** = (critical signals correctly resolved to a real, routed, data-returning metric) / (critical signals in the gold set). Reported separately for cold and learned runs. *Gates: cold ≥75%, learned ≥90%.*

A critical signal counts as "resolved" only if it passes all three validation checks below (exists, valid syntax, returns data in-window) — not merely if a mapping was produced.

### Routing

- **Correct datasource routing** = (surviving queries whose `datasource_uid` equals the gold-set expected UID for that metric/signal) / (surviving queries). *Gate = 100%.* UID *existence* is necessary but not sufficient (see gate section); the gold set pins the *expected* UID.

### Panels

- **Irrelevant surviving panel** = a published panel whose primary signal is not in the gold-set critical or supporting set for the scenario. *Gate: irrelevant / total surviving < 15%.*
- **Useful dashboard** = a published dashboard that covers ≥ a threshold fraction of critical signals with zero hallucinated metrics. *Gate: supported prompt variants producing useful dashboards ≥85%.*

### Hallucination

- **Hallucinated published metric** = a metric in a published query that does not exist in the routed datasource catalog. *Gate = 0.* This is defined on catalog existence, **not** on no-data (a sparse real metric can return no data; see gate section).

## Measurement protocol

### Cold vs learned isolation

Cold and learned recall are different experiments and must not share state. Before every **cold** run the harness must reset, to a known baseline:

- the signal mapping store (`signals.py` SQLite) back to the packaged `signals.yaml` only — no learned mappings;
- the curated archetype registry back to the packaged set;
- the generated-archetype quarantine to an empty test directory, with generation and experimental retrieval disabled;
- the LLM/discovery cache (`cache.py`) and metric catalog cache;
- the investigation history and feedback/provenance stores (`history.py`, `feedback.py`), since ranking reads metric quality from feedback.

During ClickStack testing, repeated runs contaminated the baseline precisely because these stores accumulate across runs. Learned runs then start from a defined teaching step (a specified set of approved dashboards) so the learned baseline is reproducible rather than path-dependent.
Generated archetypes are not a learned-run input. They may be evaluated only as a
side-effect-free shadow comparison and cannot contribute to the normal cold or
learned release gates. See [ADR-020](adr/020-generated-archetypes-shadow-before-lifecycle.md)
and the [Generated Archetype Evaluation Roadmap](generated-archetype-evaluation-roadmap.md).

### Repeated trials and variance

The pipeline is LLM-driven at intent classification (and at freeform discovery/query building). Every gate that depends on those stages must be run **N times per prompt** (N ≥ 5, matching the roadmap's "at least five LLM runs") and reported as mean ± standard deviation, with the worst-case run called out. A single run is not a measurement.

### Holdout against overfitting

Reserve LO2/GAMMA convention slices as morphology holdouts while tuning ClickStack.
For prompt behavior, the original ClickStack corpus is a frozen development set and
`clickstack_prompts_holdout.json` is a one-time frozen holdout. Once a holdout result
is inspected, carry its failures forward without tuning that holdout. Real-dataset
evaluation must introduce new incident-level holdouts before changes are made.

## Root-cause map

### Open risks

| Observed issue | Current location | Current cause / status |
|---|---|---|
| Novel metaphors generalize poorly | Intent classification and prompt normalization | Frozen holdout novel-metaphor usefulness is 3/15 (20%); do not tune the frozen holdout |
| Contextual cache false positive | Intent classification | Qwen interpreted a test-double `in-memory tier` as cache in 5/5 trials |
| Colloquial confirmation is not service-scoped | `pipeline.py` post-discovery confirmation | Signal resolution is semantic and query-language-aware, but runs against the combined metric catalog rather than a service-scoped subset |
| Present-but-empty default metric can block a better alias | `SignalStore.resolve_signals_for_archetype` | An exact default metric short-circuits alternative signal resolution before data-window validation |
| Live routing, hallucination, and relevance numbers are unknown | Pipeline validation and publishing path | Controls exist, but the gates have not been measured against real datasource UIDs and incident windows |
| Cross-dataset generalization is not established | Dataset ingestion roadmap | LO2/GAMMA fixtures are convention-faithful synthetic slices, not complete real incidents |

### Closed implementation gaps

| Historical issue | Resolution |
|---|---|
| Cache counters fell through to generic traffic | Cache morphology and caching-family rules precede generic counter rules in `signal_inference.py` |
| Cache size, eviction, memory, and client pressure were unmapped | Explicit semantic signals and mappings exist in both packaged and repository `signals.yaml` |
| Resolution used metric names only | `MetricEntry` carries unit/type/namespace/dimensions; `resolve_signal` applies metadata compatibility |
| Archetype blending produced 20+ panels | Live-coverage ranking, secondary coverage threshold, archetype cap, panel cap, and query-signature dedup are enforced |
| Generic templates outranked a generated learned match | Historical pre-containment fix; generated archetypes are now excluded from normal retrieval by ADR-019 |
| One valid query kept fabricated siblings | Validation now evaluates and drops each query independently |
| Datasource summaries described discarded candidates | Summary datasource names are derived only from surviving panel queries |

## Current metadata capabilities and limits

- `MetricEntry` carries `unit`, `metric_type`, `namespace`, and label/dimension names.
- The Prometheus adapter reads `/api/v1/metadata` and handles histogram/counter suffixes.
- Signal resolution uses unit class, metric type, namespace, labels, and OTel-style
  dimensions as conservative ranking evidence after a name-pattern match.
- Cold metric discovery still lacks panel titles and query-shape context; those are
  available only when learning from an existing dashboard.
- Prometheus discovery generally has label names, not all label values, so service
  and OTel instrumentation-scope confirmation remains incomplete.

## Gate-by-gate

### Semantic mapping precision ≥90% and coverage ≥80%

**Implemented and passing on curated slices.** Morphology plus catalog metadata
scores 100% precision, recall, and coverage on the current ClickStack, LO2, and
GAMMA fixtures. This is not a catalog-level result; the complete real datasets
remain unlabeled and unmeasured.

### Critical-signal recall — cold ≥75%, learned ≥90%

**Offline resolvability proxy passing; end-to-end gate pending.** Cold and separately
taught runtimes map all fixture critical signals (ClickStack 7/7, LO2 5/5, GAMMA
4/4). Learned-mode evaluation now measures governed signal mappings while holding
the curated archetype registry constant. Generated archetypes are excluded from
this gate. The remaining code risk is exact-default short-circuiting: a catalog
metric can exist but return no data while a better semantic alias is never
attempted. Because the offline harness does not prove expected-UID routing or data
in-window, it does not by itself satisfy the formal critical-signal recall gate.

### Correct datasource routing 100%

**Implemented, live gate pending.** Query targets carry datasource UID, type, and
query language together. Validation rejects undiscovered UIDs and verifies every
referenced metric belongs to the routed UID. `signalfx-direct` is an intentional
direct-backend target and is represented in its discovered catalog. The 100% gate
still requires scenario ground truth with expected UIDs in a running multi-datasource
stack, especially where the same metric name exists in multiple datasources.

### Hallucinated published metrics = 0

**Implemented, live gate pending.** Each query is checked against its routed
datasource catalog before probing, and absent metrics are dropped independently.
The zero-hallucination gate has unit coverage but has not been measured through a
complete live generation-and-publish run.

### Validation: separate existence, syntax, and data

Validation now treats three conditions independently for every query:

1. **Exists** — metric is in the routed datasource catalog. Failure ⇒ hallucination; drop the query and fail the hallucination gate.
2. **Syntax valid** — query parses/compiles for the target language. Failure ⇒ drop the query; it is a generation defect.
3. **Returns data in-window** — query returns series during the scenario window. Failure ⇒ the query may be a valid-but-sparse real metric; drop the *query* (not necessarily the panel) and record it as no-data, distinct from hallucination.

The implementation preserves valid siblings, drops a panel only when no query
survives, and reports separate warnings. Live backend behavior remains to be measured.

### Irrelevant surviving panels <15%

**Controls implemented, live gate pending.** Archetypes are ranked by classifier
confidence × live signal coverage, weak secondaries are dropped, blending is capped
at three archetypes and ten panels, and duplicate query signatures collapse. The
actual irrelevant-panel ratio still requires labeled incident expectations and the
full live pipeline.

### Dashboard size: cap the maximum, do not floor it

**Implemented.** Dashboard blending has a configurable maximum of ten panels and
no production minimum. Critical-signal coverage, rather than panel count, remains
the evaluation criterion. The current E2E quality helper still scores panel count
for test diagnostics; it is not a production generation floor.

### Supported prompt variants producing useful dashboards ≥85%

An **intent-level proxy** is now measurable: `tests/eval/prompt_variation_harness.py` runs a
30-prompt corpus (`fixtures/clickstack_prompts.json`) across five phrasing classes,
N=5 trials each, cold-isolated, scoring an intent "useful" when it retains both the
cache hypothesis and a latency/request path. It does not generate, validate, or
publish a dashboard, so the formal useful-dashboard gate remains a live-stack gate.

**First measured run (Qwen3 Coder 30B, local Ollama): 125/150 = 83.33%** — a narrow
miss. By class: precise 100%, noisy 100%, reworded 80%, vague 70%, misleading 66.7%.
The misses concentrated where cache context is implicit or colloquial ("key churn",
"fast-data layer", "ran out of headroom"); two misleading failures were arguable
scorer false-negatives (correct cache hypothesis, but the scorer also required a
latency token the prompt never used).

**First fix and its overfit (recorded honestly).** A flat operational-synonym
table was added and the Qwen run rose to **135/150 = 90%**. That number is **valid
post-tuning development accuracy, not evidence of generalization**: the table was
extended while observing failures on these exact prompts (phrases like `key churn`,
`fast-data layer`, `reuse efficiency`, `discarded entries`, `connection demand`, and
the verb forms `slowed`/`response times` were added after seeing them fail). An audit
found **48 of 115 phrases (42%) appeared near-verbatim in the 30-prompt corpus**, and
the synonyms-only deterministic floor hit reworded 6/6 — a textbook test-leakage
signal, since the table was both tuned and measured on the same prompts.

**Corrected architecture — two tiers with provenance** (`tacit/agents/synonyms.py`):

- *Conventional* (high precision, dataset-independent): standard SRE terms, vendor
  aliases, abbreviations (`redis`/`memcached` → cache, `oom` → memory, `5xx` →
  errors, `rps` → throughput). Injected directly as intent keywords.
- *Colloquial* (metaphor/ambiguous): emitted only as **scored evidence with
  provenance** (`Intent.keyword_evidence` = `[{keyword, score, tier, source}]`),
  never injected directly. A metaphor is promoted to a keyword only when one of
  its mapped semantic signals resolves against the discovered catalog
  (`confirm_colloquial`, wired into the pipeline's post-discovery step). This is
  signal-aware but not yet service-scoped.

This removes corpus-shaped metaphors from deterministic keyword injection. Those
phrases remain low-confidence evidence for post-discovery confirmation, so this is
not a claim that all corpus influence disappeared from the pipeline. The
synonyms-only floor on the **frozen dev set** drops accordingly — reworded 6/6 →
2/6, overall 0.77 → **0.60** — because metaphor-only intent scoring now depends on
the LLM, not direct table injection:

| Class | Floor before (leaky) | Floor after (de-leaked) |
|---|---|---|
| precise | 6/6 | 6/6 |
| noisy | 6/6 | 6/6 |
| reworded | 6/6 | 2/6 |
| misleading | 4/6 | 4/6 |
| vague | 1/6 | 0/6 |
| overall | 23/30 (0.77) | 18/30 (0.60) |

**Independent evaluation set.** `fixtures/clickstack_prompts.json` is now frozen and
labeled `role: development`; results on it are reported as post-tuning dev accuracy.
A new untouched holdout (`fixtures/clickstack_prompts_holdout.json`) contains fresh
paraphrases, novel-metaphor positives, and **negative prompts** where trigger phrases
mean something else (`key churn` = key rotation, `in-memory tier` = a test double,
`reuse efficiency` = meeting rooms). Offline checks on it: negatives produce **zero**
cache false-positives, and novel-metaphor prompts are caught **0/3** deterministically
(by design — they rely on the LLM; if they started passing, the table is chasing the
holdout again).

**Corrections applied before the one-time holdout run.**

1. *Prompt leakage removed.* The intent system prompt no longer lists the corpus
   metaphors as worked examples; it carries only generic "map colloquial language to
   the canonical signal" guidance, so the eval phrases are not fed to the model.
2. *Semantic-signal confirmation.* Colloquial evidence is no longer confirmed
   by a global substring scan of the catalog. `confirm_colloquial` now maps each
   keyword to the specific signal types it implies (`KEYWORD_SIGNALS`) and promotes it
   only when that signal actually resolves against the live catalog via the signal
   store (`pipeline.py`). The evidence `score` is load-bearing: ≥ auto-inject threshold
   injects directly, below it requires this confirmation. Resolution still spans the
   combined catalog; service-level scoping is an open large-dataset risk.
3. *Expectation-aware, corpus-selectable harness.* `prompt_variation_harness.py` takes
   `--corpus dev|holdout|<path>` and scores each prompt against its own labels
   (`expects_cache`/`expects_latency`/`forbidden_keywords`); negative prompts pass only
   when cache is **not** asserted. The gate now requires both a ≥85% positive useful
   rate and a ≥85% negative-correct (no-false-positive) rate. Scoring logic lives in
   `tests/eval/prompt_scoring.py` and is unit-tested LLM-free.

**Frozen holdout result (one-time run, Qwen3 Coder 30B, N=5).** The corrected
layer was run once against the holdout on 2026-06-18 and failed both 85% gates:

| Holdout measure | Result |
|---|---:|
| Positive useful rate | 23/35 (65.71%) |
| Negative correct rate | 25/30 (83.33%) |
| Literal paraphrases | 20/20 (100%) |
| Novel metaphors | 3/15 (20%) |

The only negative failure was the test-double `in-memory tier` prompt (0/5).
The historical tuned 90% result and current 92% development rerun are development
evidence, not robustness claims. Per the
agreed protocol, the synonym layer is **not** to be tuned from these holdout
failures; they are carried forward as baseline evidence for the large-dataset phase.

An apples-to-apples rerun of the frozen development corpus after de-leaking and
pruning scored **138/150 (92%)**, versus 135/150 (90%) before pruning. Therefore
there is no observed development-set regression. The lower holdout score is a
generalization gap exposed by independent data, not a regression on the original
baseline. The 92% remains development evidence only.

**Model comparison — development set, current strict scorer (N=5).** Both runs use
the same expectation-aware scorer (`tests/eval/prompt_scoring.py`, phrase-based; this
is stricter than the lenient scorer behind the 92% figure above, so compare within
this table only). Qwen3 Coder 30B is local; Claude Opus 4.8 was run on 2026-06-19 via
`prompt_variation_harness.py --provider anthropic --model claude-opus-4-8 --corpus dev
--trials 5` (Opus 4.8 rejects `temperature`, so it ran at default sampling — higher
trial-to-trial variance).

| Class | Qwen3 Coder 30B | Claude Opus 4.8 | Δ |
|---|---:|---:|---:|
| Precise | 100% | 100% | — |
| Noisy | 100% | 100% | — |
| Misleading | 83% | **100%** | +17 |
| Reworded | 67% | **90%** | +23 |
| Vague | 50% | 50% | 0 |
| **Positive useful rate** | **120/150 (80%)** | **132/150 (88%)** | **+8** |
| Negative correct rate | — | 30/30 (100%) | — |

Opus clears the ≥85% gate (Qwen does not under this scorer). The entire +8 is
concentrated in the two classes that measure paraphrase and distractor comprehension
(reworded, misleading); negatives stayed at 100%, so the gain is real comprehension,
not leakage returning under a stronger model.

**Vague is flat because it is a labeling artifact, not a model limit.** All three
failing vague prompts went 0/5 because the prompt contains no cache cue, yet the gold
label expects cache (hindsight knowledge of the Redis incident):

- *"Checkout is slow. Show me what is under pressure."* — Opus returned a correct broad
  investigation (latency/saturation/cpu/memory) and did not invent cache.
- *"Why did response times get bad during the traffic spike?"* — no cache cue; correct
  latency/throughput intent.
- *"Investigate the checkout slowdown without assuming the cause."* — the prompt
  explicitly forbids assuming a cause; staying broad is the correct behavior, penalized
  by an `expects_cache=true` label.

The lone reworded miss (*"Did key churn and connection load make the request path take
longer?"*, 2/5) is a genuinely ambiguous metaphor — Opus split between a DB-connection
and a cache-eviction reading. This is exactly the case the live-coverage confirmation
step resolves end-to-end but the intent-only harness cannot. Net: the 88% ceiling is
now set mostly by eval labels, not by Opus. Recommended follow-ups (not yet applied):
re-examine the three vague labels on principle, and run Opus on the *intent step only*
in production where its comprehension gain is cheap (~1.6K-token call). The frozen
holdout was **not** rerun for this comparison.

## Baseline results — offline morphology harness (cold-isolated)

Measured by `tests/eval/gate_harness.py` under `cold_isolation()`, against labeled
curated fixtures in `tests/eval/fixtures/`. ClickStack is a 34-metric slice derived
from the real OTLP sample, with critical metrics represented in their Prometheus-
exported form; it is **not** a label-complete score over the 321/368 metric catalog.
LO2 and GAMMA are convention-faithful
**holdout** slices (the real archives aren't fetchable in this environment), used to
catch gains that only fit ClickStack naming. These are the *datasource-free* gates
only — see "Still needs the live stack" below.

**Semantic mapping — metric → signal family (morphology plus catalog metadata):**

| Dataset | Role | TP/FP/FN | Precision | Recall | Coverage |
|---|---|---|---|---|---|
| ClickStack | primary | 32/0/0 | 1.00 | 1.00 | 1.00 |
| LO2 | holdout | 13/0/0 | 1.00 | 1.00 | 1.00 |
| GAMMA | holdout | 12/0/0 | 1.00 | 1.00 | 1.00 |
| **Holdout aggregate** | | 25/0/0 | **1.00** | **1.00** | — |

All three curated slices clear the precision, recall, and coverage gates. The
generalized rules added for active connection pressure, Redis key-count capacity,
and observability self-metrics also improved both holdouts. This is a slice-level
result, not evidence that the complete source datasets are perfectly classified.

**Critical-signal offline resolvability (bootstrap taxonomy only):**

| Dataset | Role | Resolved / Critical | Recall | Target |
|---|---|---|---|---|
| ClickStack | primary | 7/7 | 1.00 | cold ≥75% |
| LO2 | holdout | 5/5 | 1.00 | cold ≥75% |
| GAMMA | holdout | 4/4 | 1.00 | cold ≥75% |

The same critical signals are also 7/7, 5/5, and 4/4 in separately created taught
runtimes. A legacy pre-containment selection check also preferred a strongly
covered generated archetype over a higher raw-confidence generic template for all
three slices. That result is retained as historical evidence only: ADR-019 removed
generated archetypes from normal retrieval, so it no longer contributes to the
learned release gate.

**What this run caught (the overfitting guard working).** The first run scored
ClickStack offline critical-signal resolvability at **3/7** while both synthetic holdouts passed —
because the original cache/client patterns were written in Redis-INFO word order
(`evicted_keys`, `connected_clients`, `used_memory`) but the real ClickStack OTLP
names use noun-verb order (`redis_keys_evicted`, `redis_clients_connected`,
`redis_memory_used`). The fix broadened both layers (`signals.yaml` patterns and
`signal_inference.py` rules) to accept both conventions plus the standard
`*request_duration*` latency base name. After the fix all three datasets resolve at
100%; the final semantic classification run also reached 100% precision, recall,
and coverage on all three slices. The holdouts did not regress, so the convention
fix generalized across these fixtures.

**Gates still requiring the live stack (not measured here):** expected-UID routing,
hallucinated published metrics, returns-data-in-window behavior, irrelevant surviving
panels, critical-signal recall of the final dashboard, and useful-dashboard rate
across prompt variants. The intent proxy has been measured separately: development
accuracy is 92%, while the frozen holdout failed at 65.71% positive usefulness and
83.33% negative correctness. Per-query validation, blending caps, and dedup are
unit-verified, but their gate numbers require the running stack and real ingested
datasets (starting with real GAMMA in roadmap M2).

To reproduce: `python -m tests.eval.gate_harness --json report.json` (runs cold-isolated).

## Implementation status

1. **Metadata prerequisite** — `unit`/`metric_type` added to `MetricEntry`; Prometheus adapter fetches `/api/v1/metadata`; resolution consumes unit, metric type, labels, namespace, and OTel scope hints conservatively. *Done.*
2. **Vocabulary + morphology** — cache/saturation/resource families in `signals.yaml`; cache morphology in `signal_inference.py` above the `_total`→traffic rule; both conventions (Redis-INFO + OTLP word order). *Done; offline critical-signal resolvability is 100% across all three fixtures.*
3. **Three-way per-query validation + UID assertion** (`validation.py`, backends, `pipeline.py`). *Done; unit-verified. Gate numbers pending live stack.*
4. **Coverage-ranked, capped blending; max-only sizing** (`engine.py`, `config.py`, `pipeline.py`). *Done; unit-verified (23-panel explosion → 5; zero-coverage archetypes dropped).*
5. **Summary from surviving panels** (`pipeline.py`). *Done.*
6. **Evaluation harness** — cold and taught runtimes are separate; ClickStack plus LO2/GAMMA slices measure semantic and critical-signal behavior; the expectation-aware prompt harness supports frozen development and holdout corpora. *Done.*
7. **Prompt normalization** — conventional terms auto-inject; colloquial terms carry scored provenance and require semantic-signal confirmation. *Implemented; frozen holdout failed, and service scoping remains open.*

## Bottom line

The implemented controls and curated offline semantic checks pass with
precision, recall, coverage, and cold/learned critical-signal resolvability at 100% on the
ClickStack slice **and** both synthetic holdouts. This is
evidence against an obvious naming-convention regression, not a catalog-level or
incident-level accuracy result. The harness already
proved its worth by catching a real convention-overfit (Redis-INFO vs OTLP naming)
that a ClickStack-only run would have hidden. What remains before a full go/no-go:
(a) ingest and label the real GAMMA dataset, with LO2 retained for later negative-control and log-scale testing; (b) run live-stack
routing, hallucination, relevance, and data-window gates; (c) measure service-scoped
signal confirmation at catalog scale; and (d) carry the frozen prompt-holdout failures
forward without tuning them. The current system is ready for the large-dataset
evaluation phase, not for a general accuracy claim.
