# Evaluation

Tacit includes a public validation suite for measuring whether generated investigations are directionally useful, not just whether the code runs.

The benchmark lives in [`tests/tacit_validation_prompts.csv`](../tests/tacit_validation_prompts.csv). The runner is [`tests/validate.py`](../tests/validate.py), with usage notes in [`tests/README.md`](../tests/README.md).

## 100 Prompt Benchmark

The current benchmark contains 100 incident prompts across three simulated services:

- `checkout-service`
- `payment-api`
- `inventory-db`

Each row defines:

- prompt text
- expected archetype
- expected metrics
- expected datasource
- difficulty
- validation goal
- critical metrics

The benchmark covers common operational situations:

- high latency
- 5xx/error spikes
- CPU and memory saturation
- database slowdowns
- Kubernetes pod instability
- network instability
- deployment regressions
- queue/consumer lag
- general service health and SLO tracking

The validation suite has two main modes:

- `archetype`: evaluates intent classification and investigation-type selection
- `pipeline`: runs the full prompt-to-dashboard pipeline and evaluates retrieved metrics

## Reported Results

The current validation report in `tests/README.md` records:

| Metric | Result |
|---|---:|
| Archetype strict accuracy (top-1) | 90/100 (90.0%) |
| Archetype soft accuracy (any match) | 95/100 (95.0%) |
| Average top confidence | 0.89 |
| Average archetype latency | 1108 ms |
| Average metric recall | 78.9% |
| Average critical metric recall | 85.2% |
| Average weighted recall | 82.1% |
| Average signal-to-noise ratio | 71.3% |
| Pipeline success count | 98/100 |
| Average panels per dashboard | 6.2 |
| Average pipeline latency | 3421 ms |

These results should be read as a public-beta benchmark, not as a production guarantee. The dataset is synthetic and demo-oriented, but it is intentionally concrete enough to catch regressions in investigation planning, metric retrieval, and dashboard density.

## Archetype Accuracy

Archetype accuracy measures whether Tacit chooses the right investigation path.

The suite reports two scores:

- **Strict accuracy**: the top-ranked archetype must match the expected archetype.
- **Soft accuracy**: the expected archetype can appear anywhere in the returned multi-label list.

Soft accuracy matters because real incidents are often mixed. A prompt about latency caused by resource saturation may reasonably return both `latency_investigation` and `resource_saturation`. Strict accuracy still matters because the first archetype drives dashboard emphasis.

The current strict/soft gap, 90% versus 95%, suggests the system usually identifies the right investigation family, but sometimes ranks overlapping hypotheses differently than the benchmark expects.

## Metric Recall

Metric recall measures whether the generated dashboard contains the expected observability signals.

The validation suite reports:

- **Metric recall**: fraction of expected metrics found in the generated dashboard.
- **Critical recall**: recall over must-have metrics such as latency histograms for latency incidents.
- **Weighted recall**: critical metrics count more than supporting metrics.
- **Signal-to-noise ratio**: relevant metrics divided by total metrics found.

This tier is the strongest public signal because it tests whether Tacit retrieves operationally meaningful telemetry, not only whether the prompt classification sounds right.

The current results show good critical recall, but lower overall metric recall and SNR. That is the right failure mode for the current beta: Tacit often finds the most important signal, but still needs work on reducing noisy supporting panels and improving secondary metric selection.

## Lessons Learned

Evaluation changed the architecture.

The benchmark pushed Tacit away from a pure "LLM generates a dashboard" approach and toward a more constrained system:

- deterministic archetypes for common investigations
- multi-label archetype blending for overlapping incidents
- metric pre-ranking before LLM selection
- live catalog validation to drop hallucinated metrics
- critical metric weighting, because not all missing metrics are equal
- signal-to-noise measurement, because large dashboards can hide the right answer

The most useful metric is not generic accuracy. It is whether a generated dashboard preserves the operator's investigation path while including the critical evidence needed to make progress.

## Known Weaknesses

The public benchmark is useful, but incomplete.

Current limitations:

- The 100-prompt dataset is synthetic and centered on the local fake-services demo.
- Prometheus paths are better covered than other datasources.
- The benchmark does not yet measure real human usefulness during incidents.
- Query cost and cardinality risk are not fully scored.
- "General" prompts can hide ambiguous expectations.
- Vendor-specific query quality is uneven across CloudWatch, Loki, Elasticsearch, Graphite, InfluxDB, and SignalFx.
- The pipeline still depends on live datasource shape, label quality, and metric naming conventions.
- The benchmark does not yet evaluate RBAC, tenant boundaries, or production deployment safety.

The contextual culprit-ranking benchmark evaluates suspect quality from normalized operational context. It should remain
separate from future telemetry-backed ranking evaluation:

- **Contextual suspect quality**: whether Tacit ranks plausible suspects from operational artifacts such as service
  context, runbooks, dashboards, incidents, deployments, pull requests, and historical investigations without querying
  live production telemetry.
- **Telemetry-evidenced ranking accuracy**: whether runtime metrics, logs, or traces corroborate, demote, or reorder the
  contextual suspects correctly.

These are different products and should not share one headline accuracy number. Contextual ranking measures operational
knowledge retrieval and reasoning. Telemetry-evidenced ranking measures runtime evidence verification.

Phase 1 contextual culprit ranking is implemented as a deterministic, provenance-preserving suspect ranking surface. It
ranks plausible suspects from operational context without asserting causality or requiring live telemetry access.

Near-term evaluation work:

- expand datasource-specific benchmarks
- add golden dashboard snapshots
- add query cost and cardinality scoring
- track dashboard usefulness feedback by archetype
- add regression tests for learned signal mappings
- expand contextual culprit-ranking fixtures before telemetry-backed ranking fixtures
- add more adversarial and ambiguous prompts
- separate demo-service results from external-vendor contract results

The evaluation goal is not to prove Tacit is done. It is to make progress measurable.

## Acquisition implementation evidence

The acquisition workstream retains admitted catalog facts independently from the
configured prompt projection (300 by default, with the existing freeform ranking
ceiling of 60). Stricter configured prompt limits also apply after ranking.
The shared selector requires every exact operand at
one datasource and preserves conflicts and ambiguity; defaults and sampled
service labels do not establish ownership. Source completeness describes the
search, not absence of telemetry or a measured incident condition. External
list-only integrations remain explicitly `legacy_unknown` because their hidden
transport cannot be accounted for by first-party post-return guards.

One investigation work budget bounds first-party calls, identity response reads,
retained entries, enrichment and resolution. Effective defaults are 32 attempts,
8 MiB response allowance and 5,000 catalog identities. Local retained facts still
undergo text/tree/physical-fact admission. Names generated from provider prefixes
must pass derived-text admission as well as raw-response limits. Typed discovery
uses existing runtime-owned operations and bypasses legacy catalog caches.
Acquisition adds no worker, controller, capacity-release owner or cleanup owner.

Before publication, copied assembly inputs and base identifiers admit the bounded
capture and its existing replay extensions. The completed contract and snapshot
use the same pinned facts and knowledge reference. Historical snapshots do not
gain new defaults. Exact replay uses the persisted extensions and makes no
remote or model calls. Admission failures finish a failed run before publishing
or committing a successful revision.

The existing SignalFlow translator is also the publisher's translation boundary.
It rejects regex widening, bucket-base inference, unparsed fallbacks, expressions
with unconsumed operators and unsupported literal syntax. Exact matchers cannot
silently become wildcard filters: Splunk documents `filter('host', 'kafka*')` as
a prefix filter, so such PromQL literals are rejected. See the
[vendor filter limits](https://help.splunk.com/en/splunk-observability-cloud/administer/org-reference-info/per-product-system-limits-in-splunk-observability-cloud/infrastructure-system-limits-details).
Every backend is preflighted before the first publication. A selected unsupported
slot fails the entire investigation, including otherwise supported panels.
Native SignalFlow support is not expanded by this change.

The source-bound inventory fixture contains 228 slots (198 YAML and 30 Python),
including 103 affected slots. The committed gate binds slot identities and
expression/source hashes to unchanged template definitions, reaches every
affected compiler slot and checks its bounded rejection reason. Supported YAML
and Python baseline paths survive. Public mixed-backend pipeline tests assert
zero publication and no successful contract on rejection; admission tests also
verify failed-run audit. A successful public investigation verifies persisted
capture, extension equality and exact replay, including mutation of the original
knowledge snapshot after preparation.

The frozen 588-entry diagnostic keeps six requested categories separate from
seven historical critical metrics. Availability alone earns no observation
credit. A supported observation requires correct datasource ownership, a valid
non-empty query, survival and the existing typed observation outcome. The old
first-300 projection loses all six requested categories in this fixture. These
synthetic checks measure this regression; they do not establish production
incident usefulness or classifier quality.

The foundation matrix records the test-first cases and cross-runtime lifecycle
gate. Narrow implementation inventory amendments cover the shared
`tacit/acquisition.py` transport, rescue consumers, publication preflight and
their integration/inventory fixtures. Two existing stabilization fixtures now
use valid metric names present in their catalog so they continue testing safe
store-failure logging rather than accidental owner fallback. ADR-025 records the
proposed compatibility decision; ADR-024 is reserved by separate work. No
archetype recipes, dependency framework, LangGraph, grounding/ranking arithmetic,
tenant policy or database schema change is included.

The duplicated publisher translator and consumer-local owner policies were
removed. Bounded transport, metadata and replay admission add maintained code;
this workstream does not claim a net code reduction. Offline Jev/Tev evaluation
is delivered separately and has no production imports or decision authority.

The measured allocation worksheet and isolated adversarial evidence are in
the engineering notes and foundation matrix. Typed activation is restricted to
the documented five-pipeline configuration before I/O; the generic Settings
concurrency range remains available for unrelated operations. Protected release
evidence still requires the existing authorized clean/long-lived-state
100-prompt workflow at the release's exact main tip. No live vendor mutation,
model spending or protected release evidence was performed for this branch.

Verification on 2026-11-18: repository default suite 4,708 passed / 12 skipped;
hermetic vendor integration 25 passed; public upload/learning/acquisition
pipelines 17 passed. Ruff, Black (357 files), mypy (170 source files) and diff
checks passed. Grounding passed all 40 cases (trustworthy rate 1.0, unsafe rate
0.0); Operational Learning passed 18/18. The actual typed 588-entry pipeline
fixture scored six of six requested categories and seven of seven historical
critical metrics, with two complete sources, no ambiguous selections, zero
routing errors and zero unsafe assertions under its finite query-result oracle.
These counts remain separate from the 100-prompt release evidence.

The response/allocation gate passed 45 tests, including an isolated five-graph
high-water measurement of 183,612,775 traced bytes / 408,190,976 RSS bytes after
the strict parser and derived-text guards. Independent acquisition transport
security and integration architecture reviews cleared all reported findings;
the final repository run followed the dependency repairs.

After Docker became available, the local container build of implementation
commit `4d296f9` passed on Linux ARM64 as `tacit:acquisition-local`. Disposable
network-disabled, read-only containers passed the installed CLI version check,
acquisition imports, packaged resource checks and UID-999 assertion. The image
uses the configured `tacit` user. This completes the local container gate;
protected release evidence remains outstanding.

### PR review follow-up, 2026-11-18

PR #105's first CI run reached 4,719 passing tests and two skips, then failed
because the isolated five-graph tracemalloc workload exceeded its 60-second
child guard on shared Linux. The fixed workload and memory assertions remain
unchanged; its bounded measurement guard is now 180 seconds. This is not a
production query or pipeline timeout change.

Automated review also exposed two gaps in the original test coverage. Ordinary
unrelated Prometheus names could displace a lexically late keyword match at the
entry limit; the prior noise-suffix fixture did not exercise that case. Native
keyword priority now survives both selection and entry admission with bounded
backfill and an actual exhaustion witness. Separate inventory and enrichment
phases now share one absolute adapter deadline, including direct SignalFx and
standalone native adapters through the same acquisition helper. Metadata timeout
retains admitted names and records incomplete provider metadata; cancellation
continues to propagate after the owned operation settles.

The ordinary-name and phased-deadline matrix first reproduced 12 failures; its
14 focused cases then passed. A direct SignalFx deadline case separately ran
red before the shared-boundary repair. The final acquisition/capture/replay
focused suite passed 118 tests in 47.01 seconds, including the unchanged
allocation workload. Ruff, Black, mypy (170 files), Grounding (40 cases) and
Operational Learning (18 cases) passed. CI revalidates the complete final merge.

## Real Telemetry Roadmap

The synthetic benchmark remains the fast regression layer. The staged plan for evaluating Tacit with public metrics, logs, and traces is documented in [Real Telemetry Dataset Testing Roadmap](telemetry-dataset-testing-roadmap.md). Its first milestone is attached to the checkout demo; later datasets are isolated in separate branches and evaluated against source-specific ground truth.
