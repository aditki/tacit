# GAMMA Naming and Binding Diagnostic — 2026-06-20

## Scope

- Dataset: the real GAMMA CPU-interference scenario frozen as `gamma-0001`.
- Model: local Ollama `qwen3-coder:30b-a3b-q4_K_M`.
- Backend: isolated Grafana and VictoriaMetrics containing only GAMMA telemetry.
- Execution: three different prompts per naming arm; metric and LLM caches were cleared before every prompt.
- Measurement: reason-coded discovery, semantic mapping, binding, compilation, validation, and ranking stages.

This diagnostic changes metric names while holding the source samples constant. It tests the boundary between semantic
mapping and query binding; it does not score fault type or culprit service.

## Naming Matrix

| Arm | Pre-fix dashboards | Post-fix dashboards | Semantic coverage | Interpretation |
|---|---:|---:|---:|---|
| Canonical packaged names | 3/3 | 3/3 | 100% | Positive control |
| Canonical names with `gamma_` prefix | 0/3 | 3/3 | 100% | Live-signal binding now tolerates a vendor prefix |
| Raw service-prefixed GAMMA names | 0/3 | 0/3 | 100% | Correctly abstains because many services are equally plausible owners |

The first post-fix run briefly produced raw dashboards by selecting `compose_post_redis` from 140 semantically matching
series. That was a valid query but unjustified service attribution. The binder now requires a unique best compatible
metric and abstains on an unresolved tie. The post-fix gate requires this raw-arm abstention.

Canonical and prefix-only dashboards retained two of six compiled panels: CPU and memory. Validation rejected four
generic panels whose metrics were absent. Therefore `3/3` means useful resource panels survived; it does not mean the
injected CPU culprit was identified.

## Negative Controls

| Control | Semantic discovery | Surviving panels | Unsupported culprit asserted |
|---|---:|---:|---:|
| Healthy pre-interference telemetry | 100% | 0 | No |
| Application symptom with resource metrics removed | 100% | 0 | No |

Neither control asserted a cause without evidence, but `0/2` false culprits is a provisional observation, not an
acceptance gate. The frozen protocol requires at least 20 control scenarios. A cause-assertion detector now scans the
generated artifact independently of the unimplemented ranking stage, so fallback changes can fail this check before
full top-k ranking exists. The controls also expose two frozen capability gaps:

1. The application-only symptom is discovered but no symptom panel survives.
2. Culprit ranking and explicit evidence-backed abstention are not implemented; the ranking stage is recorded as skipped.

Silence is not investigation accuracy. Future freeform binding and ranking work must preserve the zero unsupported-cause
result while making the evidence-absent control present the observed symptom and explicitly decline a resource cause.

Positive naming arms now require at least 80% evidence recall over six expected signal instances per arm (CPU and memory
across three fixed prompts). The protocol records dashboard, signal, query, and scenario numerators/denominators, hashes
the frozen prompt/protocol file and scorer harness, and fails if either the metric or LLM cache records a hit. The same
prompt set is applied to every naming arm.

The stricter rerun scored canonical evidence recall at `6/6` and prefix-only evidence recall at `6/6`, with zero metric
or LLM cache hits in all nine arm trials. At that point only two of the required 20 controls had been executed, so the
control gate remained pending data rather than failed. The complete controls must also run immediately after guarded
fallback is introduced, before full top-1/top-3 culprit ranking is added.

Clarification: the observed control result is `0/2` false culprits and `2/2` abstentions. It is pending data, not a red
gate. The raw GAMMA arm was run: semantic mapping remained 100%, but it produced `0/3` dashboards because 140
service-prefixed candidates did not provide unique service ownership. Raw evidence recall is therefore not counted as a
positive-arm pass; this is a frozen, bounded limitation.

An expanded pre-fallback control matrix is now frozen with 20 distinct scenarios: ten healthy pre-interference windows
and ten application-symptom windows with resource evidence removed, spanning CPU, memory, network, and mixed CPU-memory
runs. The arm-prompt and control-matrix hashes are recorded separately so control expansion cannot invalidate the
`6/6 + 6/6` naming baseline. The 20-case live run must pass before guarded fallback begins.

## Decision

Keep the prefix fix and six-stage instrumentation. Do not normalize raw GAMMA service names into one arbitrary service,
and do not claim root-cause accuracy from dashboard creation. Before scaling the corpus, implement symptom-preserving
fallback and evidence-backed culprit ranking, then test untouched memory and network scenarios with the same controls.
