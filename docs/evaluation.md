# Evaluation

DashForge includes a public validation suite for measuring whether generated investigations are directionally useful, not just whether the code runs.

The benchmark lives in [`tests/dashforge_validation_prompts.csv`](../tests/dashforge_validation_prompts.csv). The runner is [`tests/validate.py`](../tests/validate.py), with usage notes in [`tests/README.md`](../tests/README.md).

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

Archetype accuracy measures whether DashForge chooses the right investigation path.

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

This tier is the strongest public signal because it tests whether DashForge retrieves operationally meaningful telemetry, not only whether the prompt classification sounds right.

The current results show good critical recall, but lower overall metric recall and SNR. That is the right failure mode for the current beta: DashForge often finds the most important signal, but still needs work on reducing noisy supporting panels and improving secondary metric selection.

## Lessons Learned

Evaluation changed the architecture.

The benchmark pushed DashForge away from a pure "LLM generates a dashboard" approach and toward a more constrained system:

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

Near-term evaluation work:

- expand datasource-specific benchmarks
- add golden dashboard snapshots
- add query cost and cardinality scoring
- track dashboard usefulness feedback by archetype
- add regression tests for learned signal mappings
- add more adversarial and ambiguous prompts
- separate demo-service results from external-vendor contract results

The evaluation goal is not to prove DashForge is done. It is to make progress measurable.

## Real Telemetry Roadmap

The synthetic benchmark remains the fast regression layer. The staged plan for evaluating DashForge with public metrics, logs, and traces is documented in [Real Telemetry Dataset Testing Roadmap](telemetry-dataset-testing-roadmap.md). Its first milestone is attached to the checkout demo; later datasets are isolated in separate branches and evaluated against source-specific ground truth.
