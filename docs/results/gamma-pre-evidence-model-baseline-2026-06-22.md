# GAMMA Pre-Evidence-Model Baseline — 2026-06-22

## Scope

- Repository base: merged `main` at `200bbcf` plus the phase-file normalization recorded with this result.
- Model: local Ollama `qwen3-coder:30b-a3b-q4_K_M`.
- Backend: GAMMA-only Grafana on port 3001 and VictoriaMetrics on port 8428.
- Execution: 29 independent runs with metric and LLM caches cleared before every prompt.
- Isolation: every VictoriaMetrics series was deleted before each arm/control import.
- Dataset families: CPU, memory, network, and mixed CPU-memory.

The run used three fixed prompts for each naming arm and 20 distinct controls: ten healthy pre-interference windows and
ten application-symptom windows with resource evidence removed.

## Results

| Gate | Result |
|---|---:|
| Canonical evidence recall | 6/6 (100%) |
| `gamma_`-prefixed evidence recall | 6/6 (100%) |
| Canonical dashboards | 3/3 |
| Prefixed dashboards | 3/3 |
| Raw neutral dashboards | 0/3, expected ownership abstention |
| False culprits across controls | 0/20 |
| Abstention across controls | 20/20 |
| Evidence-absent symptom discovery | 10/10 |
| Evidence-absent symptom panel survival | 0/10 |
| Metric/LLM cache hits | 0 |
| Culprit ranking | unavailable |

The positive evidence and control-safety gates passed at their frozen denominators. This does not make the system ready
for fallback: symptom evidence was recognized but never survived into a panel in all ten evidence-absent controls.
That `0/10` is the primary pre-evidence-model regression target. Root-cause accuracy remains unmeasured.

## Frozen Hashes

| Artifact | SHA-256 |
|---|---|
| Arm prompt set | `d1b5054d986a7aaab139fc1d3fc1df0d36e02fe09a02bc84270fb223079c496f` |
| Control matrix | `b4b8adb1f41c06ebe2cba1ae643803611de59cfc98b0e56b042e6f2be585c514` |
| Protocol | `1706277c3de5f6d8cb9c5afde22940c64365d639f74346b1d984a2c93f8365e3` |
| Harness | `04ee6f89416709df5d96ced0d9e185ddc84275c427f8f7209ccef93c26ca57ff` |
| GAMMA converter | `b37d0cc4b932fa21aa5fb58bed44032fcc6a465f5fb056a88809f89fa4a5445f` |
| Full local JSON report | `f60256fc66c13a11c9001118d7ec035bbc7184628651abf1e6591e02e2d27dc6` |

The detailed JSON remains local at `data/gamma/diagnostic/pre-evidence-model.json` and is intentionally ignored because
it contains generated dashboard details and expanded per-run diagnostics.

## Decision

Proceed to the first-class evidence model, not guarded fallback. Preserve canonical/prefixed `6/6`, raw neutral
ownership abstention, and `0/20` false culprits. Make the first usefulness gate convert the ten discovered application
symptoms into validated observations and surviving symptom evidence without asserting a resource cause.
