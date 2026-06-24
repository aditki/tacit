# Tacit Validation Suite

Automated evaluation of Tacit's archetype classification and pipeline metric retrieval accuracy.

## Quick Start

```bash
# Archetype classification only (needs LLM, no running stack)
uv run python tests/validate.py tests/tacit_validation_prompts.csv --mode archetype

# Full pipeline (requires running Tacit + Grafana + Prometheus stack)
uv run python tests/validate.py tests/tacit_validation_prompts.csv --mode pipeline

# Upload-learning E2E framework (hermetic: fake backend + fake intent)
uv run pytest tests/e2e -m e2e -q

# Both
uv run python tests/validate.py tests/tacit_validation_prompts.csv --mode all --output results.json

# Limit to first N prompts (useful for quick iteration)
uv run python tests/validate.py tests/tacit_validation_prompts.csv --mode archetype --limit 10
```

## Evaluation Philosophy

Observability is **investigation-first**, not ontology-first. Multiple dashboards and archetypes may be valid for the same prompt. The evaluation prioritizes **signal quality** over taxonomy purity.

The validation suite uses a **tiered evaluation framework**:

| Tier | What it measures | Why it matters |
|------|-----------------|----------------|
| **Tier 1 — Retrieval Accuracy** | Metric recall, critical recall, weighted recall, SNR | Can the system retrieve relevant signals? **Most important.** |
| **Tier 2 — Operational Utility** | Panels/dashboard, error rate, latency | Would an SRE actually use this dashboard? |
| **Tier 3 — Investigation Coverage** | Archetype match (strict + soft) | Does the dashboard support the right investigation path? |

## Upload-Learning E2E Framework

`tests/e2e` validates the workflows that matter before dashboard creation:

1. Upload a representative dashboard JSON.
2. Infer semantic signals and keep the dashboard pending.
3. Approve it, which creates signal mappings and registers the generated learned archetype.
4. Run a combinatorial prompt matrix against the learned telemetry language.
5. Score the generated dashboard spec for metric recall, critical evidence, signal-to-noise, and an incident usefulness score.

It also covers manual signal teaching, rejecting uploaded dashboards without activating mappings, and the no-metrics failure path.
The API-surface E2E tests additionally cover health, auth, archetype reload/listing, signal detail/stats, chart generation, investigation history, feedback, feedback insights, learning lists, ignore flows, and upload validation failures.

The first scenario is `tests/e2e/scenarios/checkout_upload_incident.yaml`. Add more scenarios by defining dashboard panels, noise metrics, prompt styles, perturbations, failure modes, and utility thresholds. This keeps the scenario reviewable while still generating many prompt variants.

## Modes

### `archetype` — Intent Classification

Tests the LLM intent agent's ability to classify prompts into investigation archetypes.

**Requires:** LLM API key (`LLM_API_KEY` env var)
**Does not require:** Running stack

**Metrics reported:**
- **Strict accuracy (top-1)** — does the highest-confidence archetype match expected?
- **Soft accuracy (any-match)** — does the expected archetype appear *anywhere* in the returned list?
- **Avg top confidence** — mean confidence of the highest-ranked archetype
- Per-archetype and per-difficulty breakdowns

**Multi-label evaluation:** The intent agent returns multiple archetypes with confidence scores. A prompt like *"high latency and OOM kills in checkout-service"* might return:
```json
[
  {"type": "latency_investigation", "confidence": 0.88},
  {"type": "resource_saturation", "confidence": 0.75}
]
```
If the expected archetype is `resource_saturation`, the strict check fails (top-1 is `latency_investigation`) but the soft check passes (it appears in the list).

### `pipeline` — Full Pipeline Metric Retrieval

Tests the complete pipeline: prompt → API → dashboard → metric extraction from Grafana.

**Requires:** Running Tacit API + Grafana + Prometheus (via `docker compose -f docker-compose.dev.yml up -d`)

**Metrics reported:**

- **Metric recall** — what fraction of expected metrics appear in the generated dashboard?
- **Critical metric recall** — recall over only the most important metrics (e.g. `http_request_duration_seconds` for latency investigations). Missing a critical metric is worse than missing a supporting one.
- **Weighted recall** — critical metrics weighted at 1.0, supporting metrics at 0.4
- **Signal-to-noise ratio (SNR)** — `relevant_found / total_found`. A dashboard with 90% recall but 50 irrelevant panels scores low on SNR.
- **Panels/dashboard** — average panel count (indicates dashboard density)

## CSV Format

The test dataset (`tacit_validation_prompts.csv`) has these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `prompt_id` | Yes | Unique ID (e.g. `DF-001`) |
| `prompt` | Yes | Natural language prompt sent to Tacit |
| `expected_archetype` | Yes | Expected investigation type (e.g. `latency_investigation`, `error_spike`, `resource_saturation`, `golden_signals`, `general`) |
| `expected_metrics` | Yes | Comma-separated list of metric names that should appear in the dashboard |
| `expected_datasources` | Yes | Comma-separated datasource types (e.g. `Prometheus`) |
| `difficulty` | Yes | `easy`, `medium`, or `hard` |
| `validation_goal` | Yes | What the prompt primarily tests (e.g. `signal prioritization`, `query correctness`) |
| `critical_metrics` | No | Semicolon-separated list of **must-have** metrics. When present, enables weighted recall and critical recall scoring. |

### Adding new test cases

Add rows to the CSV following the same format. The validation script works with any CSV that has the required columns — you can create separate datasets for different scenarios.

### Critical metrics

Not all metrics matter equally. Missing `http_request_duration_seconds` during a latency investigation is catastrophic. Missing `container_memory_working_set_bytes` may be acceptable.

The `critical_metrics` column uses **semicolons** as delimiters (to avoid conflicts with the comma-separated `expected_metrics`):

```csv
...,critical_metrics
...,http_request_duration_seconds
...,container_cpu_usage_seconds_total; container_memory_working_set_bytes
...,
```

When the column is empty or absent, the validation falls back to unweighted recall.

**Weight scheme:**
| Metric type | Weight | Example |
|-------------|--------|---------|
| Critical (primary symptom) | 1.0 | `http_request_duration_seconds` for latency |
| Supporting (context) | 0.4 | `container_cpu_usage_seconds_total` for latency |

## Output

### Console report

```
========================================================================
  ARCHETYPE CLASSIFICATION REPORT
========================================================================
  Strict accuracy (top-1) : 90/100 (90.0%)
  Soft accuracy (any-match): 95/100 (95.0%)
  Avg top confidence       : 0.89
  Avg latency              : 1108ms

========================================================================
  TIERED PIPELINE EVALUATION REPORT
========================================================================

  ── Tier 1: Retrieval Accuracy ──
  Avg metric recall    : 78.9%
  Avg critical recall  : 85.2%
  Avg weighted recall  : 82.1%
  Avg signal-to-noise  : 71.3%

  ── Tier 2: Operational Utility ──
  Total prompts        : 100
  Succeeded            : 98
  Avg panels/dashboard : 6.2
  Avg latency          : 3421ms

========================================================================
  VALIDATION SUMMARY
========================================================================
  Archetype strict   : 90/100 (90.0%)
  Archetype soft     : 95/100 (95.0%)
  Metric recall      : 78.9%  (98 succeeded, 2 errors)
  Critical recall    : 85.2%
  Signal-to-noise    : 71.3%
========================================================================
```

### JSON output (`--output results.json`)

Detailed per-prompt results including:
- Archetype: expected, actual, all returned archetypes with confidence, strict/soft pass
- Pipeline: found/missing/extra metrics, critical metrics found/missing, recall/weighted recall/SNR, panel count, dashboard URL

## Architecture

```
validate.py
├── load_test_cases()           # CSV → TestCase dataclasses
├── run_archetype_validation()  # Calls classify_intent() per prompt
│   └── Multi-label eval: strict (top-1) + soft (any-match)
├── run_pipeline_validation()   # Calls /api/v1/chart + fetches dashboard from Grafana
│   ├── extract_metrics_from_expr()  # PromQL → metric names
│   ├── fuzzy_metric_match()         # Handles histogram suffixes
│   └── Weighted recall computation  # Critical vs supporting
├── print_archetype_report()    # Tiered console output
├── print_pipeline_report()     # Tiered console output
└── JSON serialization          # Detailed per-prompt results
```

## Interpreting Results

| Signal | What it means |
|--------|--------------|
| High strict accuracy, low soft | Model is wrong, not just differently ordered |
| Low strict, high soft | Model finds the right archetype but ranks another higher — often valid |
| High metric recall, low SNR | Dashboard has the right signals but also too much noise |
| Low critical recall | System is missing the most important signals — **investigate these** |
| High weighted recall | Good balance of critical + supporting metrics |

## Files

| File | Description |
|------|-------------|
| `validate.py` | Validation suite (run with `uv run python tests/validate.py`) |
| `tacit_validation_prompts.csv` | 100-prompt test dataset across 3 fake services |
| `test_unit.py` | Core unit tests: schemas, LLM JSON repair, SignalFx cache |
| `test_bedrock.py` | Bedrock provider: session auth, converse, model resolution, inference profile retry, Mistral |
| `test_azure.py` | Azure OpenAI provider: init validation, deployment resolution |
| `test_registry.py` | Provider registry: routing, error messages |
| `test_providers.py` | CloudWatch schema/rendering, CLI doctor checks |
| `test_backends.py` | Backend adapters: Grafana, SignalFx protocol implementations |
| `test_signalfx_unit.py` | SignalFx discovery, validation, SignalFlow query handling |

Run all tests with:
```bash
uv run pytest
```
