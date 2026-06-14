# Test layout

Tests are split into two tiers so the fast, deterministic ones gate the rest.

```
tests/
├── unit/            # pure logic, no network — the default `pytest` run
├── integration/     # hermetic vendor contract tests (mocked HTTP via respx)
└── contracts/       # Pydantic models + factories describing each vendor's API
```

## Running

```bash
uv run pytest tests/unit -q                         # unit only
uv run pytest tests/integration -m integration -q   # integration (vendor contracts)
uv run pytest tests/e2e -m e2e -q                   # upload-learning workflow E2E
uv run pytest -m "not integration"                  # explicit non-integration filter
```

CI runs `unit` first, then `integration` only if unit passes (see
`.github/workflows/ci.yml`).

## How the contract layer works

`tests/contracts/*_models.py` hold Pydantic models for each third-party API
(Grafana, Prometheus, Loki, CloudWatch, Elasticsearch/OpenSearch, Graphite,
InfluxDB, SignalFx) — both the responses we read and the request bodies we send.

`tests/contracts/factories.py` builds every mock payload *through* those models,
so a fixture can never drift from its schema. If a vendor renames a field, you
update the model once and every dependent test fails loudly.

Integration tests mount [respx](https://lundberg.github.io/respx/) over httpx,
feed factory-built responses, drive the real client/adapter code, and — for
writes — validate DashForge's *outgoing* request body against the vendor's
request-contract model (e.g. `GrafanaDashboardSaveCommand`,
`SignalFxChartCreate`). This is what catches "we send the wrong shape" and
"we mis-parse their response" without a live service.

## Coverage (reads + writes)

| Vendor | Read (GET) | Write (POST/PUT) |
|---|---|---|
| Grafana | datasources, dashboard ingest | dashboard publish |
| SignalFx | metric search | chart + dashboard create |
| Prometheus | label values + series | — |
| Loki | labels | — |
| CloudWatch | namespaces + metrics (resource API) | — |
| Elasticsearch/OpenSearch | `_mapping` | — |
| Graphite | `metrics/find` | — |
| InfluxDB | `SHOW MEASUREMENTS` | — |
| DashForge API | `/healthz`, `/api/v1/signals` | `/signals/teach`, `/learn/dashboard` |

## E2E Workflow Tests

`tests/e2e` covers upload -> signal learning -> prompt -> dashboard-spec, manual signal teaching, reject/ignore-style learning control, empty telemetry outcomes, and the broader API surface: system, auth, archetypes, signals, chart generation, history, feedback, insights, and learning validation. These tests are opt-in via `-m e2e` and are intended for release gates or nightly runs where usefulness metrics matter as much as technical success.

> The DashForge API contract test requires Python 3.12 (the app imports
> `agents.llm`, which uses 3.12 generic syntax); it auto-skips on older runtimes.
