# GAMMA Pilot Baseline — 2026-06-19

> **Historical architecture note:** This baseline records the pre-containment behavior
> that auto-registered generated archetypes for replay. ADR-019 retired that runtime
> path. Generated archetype creation is now disabled by default; explicitly generated
> output is quarantined and cannot enter normal investigation retrieval. The result
> below remains useful evidence about metric binding, not a description of current
> learning behavior.

## Scope

- Source: Kaggle GAMMA archive, 9.4 GB compressed and 129,510 entries.
- Scenario: one CPU-interference run, hidden from Tacit as opaque ID `gamma-0001`.
- Ground truth: three bottlenecked VMs at 75%, 99%, and 65% interference; VM placement and fault labels remained scorer-only.
- Model: local Ollama `qwen3-coder:30b-a3b-q4_K_M`.
- Backend: isolated Grafana on port 3001 with only the `gamma-telemetry` VictoriaMetrics datasource.
- Execution repeats: three API calls per primary evidence mode, using identical prompts with the Tacit LLM cache enabled.
- Independent Qwen samples: one per distinct prompt. The repeated calls measure pipeline replay consistency, not model robustness.

The converter did not expose the source scenario name, interference type, VM placement, processed graph labels, or fault intensities. Application metrics were derived only from RPC latency/start columns. Infrastructure metrics came from raw Prometheus files. The processed multimodal graph rows were not ingested.

A preflight run against the ordinary demo Grafana appeared successful but routed every panel to its synthetic Prometheus datasource. That result is excluded from the scores and is the reason the pilot now provisions an isolated Grafana/Tacit pair containing only `gamma-telemetry`.

## Input

| Mode | Samples | Metrics |
|---|---:|---|
| Application only | 3,672 | RPC p95 latency and request rate by service |
| Infrastructure only | 16,571 | CPU, memory, filesystem, and network by service |
| Combined | 20,243 | Both sets |

## Results

| Mode | Independent Qwen result | Cached replay consistency | Datasource routing | Result |
|---|---:|---:|---:|---|
| Application only, cold | 0/1 | 3/3 identical failures | No dashboard published | Failed |
| Infrastructure only, cold, service-neutral prompt | 0/1 | 3/3 identical failures | No dashboard published | Failed |
| Combined, cold | 0/1 | 3/3 identical failures | No dashboard published | Failed |
| Combined, learned, initial 1m rate windows | 1/1 | 3/3 identical partial results | 100% GAMMA datasource | Partial: 3/5 panels survived |
| Combined, learned, corrected 5m rate windows | 1/1 | 3/3 identical successes | 100% GAMMA datasource | Passed: 5/5 panels survived |

An additional infrastructure wording probe used “social-network services.” Qwen interpreted that phrase as a literal service name and Tacit added a nonexistent service selector. Its three cached API replays failed identically. The service-neutral control also failed, proving that wording was a separate issue rather than the sole cause.

## Findings

1. **Application metrics tested the engine more sharply than the synthetic benchmark.** Tacit discovered `gamma_request_latency_seconds` and `gamma_request_rate`, but the selected latency archetype compiled `http_request_duration_seconds_bucket` and `http_requests_total`. Per-query validation correctly rejected every empty panel.
2. **The cold-start gap is compilation, not catalog discovery.** Infrastructure and combined catalogs were visible, but packaged archetypes emitted canonical hard-coded metric names instead of resolving the live signal mappings or falling back to freeform generation.
3. **The retired same-vocabulary template replay worked in this historical run.** Uploading one representative five-panel dashboard produced an automatically generated archetype with the six real GAMMA metrics under the pre-containment architecture. One independent Qwen generation produced all five valid panels, and two cached replays reproduced it. Current Tacit does not auto-register or normally retrieve that output, and this result does not establish learned generalization.
4. **Sampling-aware query windows matter.** A 1-minute `rate()` window was too narrow for irregular source samples and dropped CPU/network panels. Five minutes returned data consistently.
5. **Root-cause ranking is not yet measured.** The learned dashboard presents latency and resources by service, but the current artifact does not rank culprit services or state a root cause. Dashboard success must not be reported as culprit-service accuracy.

## Frozen Baseline

Do not tune the synthetic GAMMA fixture to make these results pass. Carry forward the cold failures and evaluate fixes against additional untouched GAMMA scenarios, including memory, network, and simultaneous CPU-memory interference. The next implementation target is live-signal substitution during archetype compilation, followed by service-ranking evaluation over replayed interference windows.

## Pre-Fix Diagnostic Protocol

Instrument these stages before changing binding or fallback behavior:

1. Catalog discovery.
2. Semantic mapping.
3. Archetype coverage and metric binding.
4. Query compilation.
5. Per-query validation.
6. Culprit ranking or explicit abstention.

Run naming arms against identical underlying samples and prompts with all LLM and discovery caches disabled:

| Arm | Representation | Predicted pre-fix outcome |
|---|---|---|
| A | Exact packaged canonical metric names | Discovery and mapping pass; exact binding succeeds |
| B | Canonical stems with a `gamma_` vendor prefix | Discovery and mapping remain constant; exact binding fails |
| C | Raw GAMMA service-prefixed metric filenames | Discovery should pass; mapping may weaken; exact binding fails |

If discovery or semantic mapping changes unexpectedly between A and B, the prefix-only experiment has not isolated binding and must not be used to attribute the failure.

Add two negative controls before enabling freeform fallback:

- a healthy pre-interference window, where Tacit should not assert a culprit;
- an application-symptom window with resource evidence removed, where Tacit should present the symptom and explicitly abstain from a resource root cause.

Report false-positive culprit rate and unsupported-cause rate in addition to dashboard validity. Query validation prevents nonexistent metrics, but it does not prevent confidently selecting the wrong existing metric or service.

Split learned transfer into two experiments:

- **Vocabulary transfer:** teach mappings for all resource families, use only a CPU incident example, then test untouched memory/network incidents. This tests reasoning with vocabulary held constant.
- **Template transfer:** teach only the CPU dashboard and CPU metric vocabulary, then test memory/network. This tests whether the system generalizes beyond an exact learned template and is expected to be substantially harder.

The first instrumented naming diagnostic and live-signal binding fix are recorded in
`docs/results/gamma-naming-diagnostic-2026-06-20.md`. The frozen baseline above remains unchanged.
