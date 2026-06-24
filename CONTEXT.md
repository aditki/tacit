# Tacit — Project Context & Knowledge Transfer

> This file captures the full context of the Tacit project: architecture, design
> decisions, pivots, tradeoffs, bugs encountered, and current state. It is intended to
> onboard a new AI assistant or developer on a different machine.
>
> Last updated: 2026-05-28

---

## 1. What Is Tacit

**Natural language → Grafana dashboards.** An AI-powered observability navigation layer
that lets on-call engineers describe a problem in plain English (via Slack, Web UI, or
HTTP API) and instantly get a purpose-built Grafana dashboard.

Inspired by [Uber's QueryGPT](https://www.uber.com/us/en/blog/query-gpt/) multi-agent
decomposition pattern.

### Stack

- **Language**: Python 3.12
- **Web framework**: FastAPI + Uvicorn
- **LLM**: Provider-agnostic — Anthropic (default), OpenAI/Azure, Ollama
- **Data validation**: Pydantic v2
- **HTTP client**: httpx
- **Logging**: structlog (structured JSON)
- **Slack**: slack-bolt + slack-sdk (socket mode)
- **CLI**: Click + Rich — `tacit init/doctor/connect/test/serve`
- **Config**: Layered YAML (`tacit.yaml` or `~/.tacit/config.yaml`) + env vars + Pydantic Settings
- **Persistence**: SQLite (feedback store) — in-memory TTLCache for metrics/LLM cache
- **Dependency management**: `uv` with `pyproject.toml` + `uv.lock`
- **SSL**: `truststore` package for system certificate store integration
- **Binary distribution**: PyInstaller spec for single-binary builds

### Key Env Vars

| Variable | Purpose |
|---|---|
| `LLM_PROVIDER` | `anthropic`, `openai`, `azure`, `ollama` |
| `LLM_API_KEY` | API key for chosen provider |
| `LLM_MODEL` | Model name (e.g. `gpt-4o`, `claude-sonnet-4-20250514`) |
| `GRAFANA_URL` | Grafana instance URL (default `http://localhost:3000`) |
| `GRAFANA_API_KEY` | Grafana service account token (Editor role) |
| `SLACK_BOT_TOKEN` | Slack bot token (`xoxb-...`) |
| `SLACK_APP_TOKEN` | Slack app-level token (`xapp-...`) |
| `API_AUTH_ENABLED` | Enable API key auth (`true`/`false`) |
| `API_AUTH_KEY` | API key for `X-API-Key` header |
| `TACIT_ARCHETYPES_PATH` | Custom path to `archetypes.yaml` |
| `TACIT_CONFIG` | Custom path to `tacit.yaml` |

---

## 2. Architecture & Pipeline

```
Prompt → Sanitizer → Intent Agent → Context Enrichment → Datasource Discovery
  → Metric Catalog Fetch → Pre-Ranking → [Archetype Engine | Freeform LLM path]
  → Query Validation → Dashboard Publisher → Provenance Recording
```

### Pipeline Steps (tacit/pipeline.py)

1. **Prompt Sanitizer** — length cap (2000 chars), control-char removal, injection guard
2. **Intent Agent** (`agents/intent.py`) — LLM classifies: domain, services, keywords,
   signals, timerange, multi-label archetypes with confidence scores
3. **Context Enrichment** (`context/`) — pluggable RAG/MCP/A2A knowledge base lookup
   (disabled by default)
4. **Datasource Discovery** (`grafana/datasource.py`) — finds all Grafana datasources,
   filters by signal type
5. **Metric Catalog Fetch** — per-datasource adapters query metrics + per-metric labels
6. **Pre-Ranking** (`ranking.py`) — keyword/service relevance scoring + feedback-driven
   quality boost/penalty. Narrows catalog (e.g. 288 → 60 metrics) before LLM
7. **Routing Decision**: if archetypes matched with confidence > 0.3 → Archetype Engine,
   else → Freeform LLM path
8. **Archetype Engine** (`archetypes/engine.py`) — deterministic compilation, no LLM
   needed, ~75% faster, zero hallucination. Blends panels from multiple archetypes
9. **Freeform path** — Metrics Discovery LLM → Post-Validation → Query Builder LLM
10. **Query Validation** (`validation.py`) — tests every query against live datasource,
    drops panels with no data, blocks empty dashboards
11. **Dashboard Publisher** (`grafana/dashboard.py`) — assembles Grafana JSON, publishes
12. **Provenance Recording** — stores prompt → dashboard mapping in SQLite for feedback

### Two Dashboard Generation Paths

| | Archetype Path | Freeform LLM Path |
|---|---|---|
| **When** | Intent matches known investigation pattern (confidence > 0.3) | No archetype match |
| **Speed** | ~75% faster (no LLM for query generation) | Slower (2 LLM calls: discovery + query builder) |
| **Hallucination risk** | Zero (deterministic templates) | Non-zero (mitigated by post-validation) |
| **Query quality** | Template-based, reliable | LLM-generated, variable |
| **Multi-label** | Blends panels from multiple archetypes | N/A |

---

## 3. Key Architectural Decisions & Pivots

### Decision: Multi-Label Archetypes (Pivot from Single-Label)

**Problem**: Incidents are inherently overlapping. A latency spike may be caused by
resource saturation, which also triggers errors. Single-label classification forced a
false choice.

**Solution**: Intent agent returns multiple archetypes with confidence scores:
```json
[{"type": "latency_investigation", "confidence": 0.91},
 {"type": "resource_saturation", "confidence": 0.62}]
```

**Implementation**:
- `ArchetypeMatch` model: `{type: str, confidence: float}`
- `Intent.archetypes`: list ordered by confidence (highest first)
- `Intent.problem_type`: kept for backward compatibility, synced from top archetype
- Confidence guidelines in intent agent prompt: 0.9+ primary, 0.6-0.9 implied, 0.3-0.6 secondary
- `get_archetypes_by_confidence()` resolves to templates above threshold
- `blend_archetypes()` in engine: primary contributes all panels, secondaries add non-overlapping panels
- Pipeline routing: multi-archetype → blend, single → compile, none → freeform LLM

**Tradeoff**: More panels per dashboard (broader coverage) vs. potential noise. Mitigated
by feedback loop — noisy dashboards get flagged and archetypes get PRUNE recommendations.

**Core insight**: Observability is investigation-first, not ontology-first. Multiple
dashboards/archetypes may be valid for the same prompt. Evaluation should prioritize
signal quality over taxonomy purity.

### Decision: YAML Archetype Templates (Pivot from Hardcoded Python)

**Problem**: Editing investigation templates required Python code changes — high friction
for SRE teams.

**Solution**: `archetypes.yaml` with hot-reload API endpoint (`POST /api/v1/archetypes/reload`).

**Implementation**:
- `archetypes.yaml` at project root (or `TACIT_ARCHETYPES_PATH` env var)
- `templates.py` loads YAML first, falls back to hardcoded Python definitions
- Uses `yaml.safe_load` (security: no arbitrary code execution)
- Template placeholders: `{service_filter}`, `{container_filter}`, `{rate_interval}`
- Panel types: timeseries, stat, gauge, table, logs
- Hot reload: `reload_archetypes()` function, exposed via API
- **Current count**: 41 archetypes, 176 panels, 153 problem_types

**Tradeoff**: YAML is less expressive than Python for complex query logic — but
investigation templates are fundamentally declarative (title + expression + unit), so
YAML is the right level of abstraction.

### Decision: Closed-Loop Feedback System

**Problem**: No way to know if generated dashboards were actually useful. No mechanism
to improve over time.

**Solution**: Three-layer feedback system:

1. **Collection** — dimensional SRE ratings (symptom visibility, root cause support,
   noise level, investigation speed, overall usefulness) + free-text comments
2. **Analysis** — joins provenance + feedback to surface: per-archetype quality, noisy
   dashboards, archetype gaps, metric quality, confidence calibration
3. **Action** — feedback-driven metric ranking (quality >= 0.7 → 1.3x boost,
   quality <= 0.3 → 0.7x penalty) + auto-generated recommendations

**Implementation**:
- SQLite at `data/tacit_feedback.db` (auto-created)
- Two tables: `dashboard_provenance` (prompt → dashboard mapping) and `feedback`
  (dimensional ratings per dashboard)
- `ranking.py` loads metric quality scores, cached 10min, applied as multipliers
- Recommendation types: PRUNE, ADD SIGNAL, NEW ARCHETYPE, DEPRIORITIZE METRICS,
  RECALIBRATE

**Tradeoff**: SQLite is single-writer — adequate for personal/team use, swap for
Postgres via SQLAlchemy for enterprise. Metric quality scoring is simple good/bad ratio —
works for small datasets, needs more sophisticated modeling at scale.

### Decision: Pre-Ranking Before LLM

**Problem**: Sending 288+ metrics to the LLM was expensive and slow. Token cost scaled
linearly with catalog size.

**Solution**: Lightweight scoring narrows catalog to ~60 candidates before LLM.

**Implementation** (`ranking.py`):
- Keyword relevance: metrics matching intent keywords get boosted
- Service relevance: metrics with service name in labels get boosted
- Feedback multiplier: quality scores from human reviews (see above)
- `MAX_LLM_CANDIDATES = 60`

**Tradeoff**: Keyword-based ranking may miss semantically relevant metrics that don't
share vocabulary. Future: BM25 + embedding hybrid search. But keyword matching is
surprisingly effective for metric names (they're descriptive by convention).

### Decision: Per-Metric Label Discovery

**Problem**: LLMs hallucinate label names and values when writing PromQL.

**Solution**: Prometheus adapter fetches actual label names/values per metric via
`/api/v1/series`, providing ground truth to the LLM.

**Tradeoff**: Extra API calls per metric — mitigated by TTL cache (5 min). Label
discovery is batched and cached at the datasource level.

### Decision: Layered Configuration (YAML + Env Vars)

**Problem**: Flat `.env` files don't scale — no nesting, no validation, secrets mixed
with config.

**Solution**: Schema-validated `tacit.yaml` with env var overrides. Secrets stay in
env vars, non-sensitive config in YAML.

**Implementation** (`config.py`):
- Config file discovery: `TACIT_CONFIG` env var → `./tacit.yaml` → `./tacit.yml` → `~/.tacit/config.yaml`
- Secrets loaded from `.env` and `~/.tacit/.env`
- Pydantic Settings with `SettingsConfigDict` for validation
- YAML sections flattened: `{llm: {provider: x}}` → `{llm_provider: x}`

### Decision: truststore for SSL

**Problem**: Python's bundled certificate store doesn't include all enterprise CA
certificates, causing `CERTIFICATE_VERIFY_FAILED` errors on corporate networks.

**Solution**: `truststore` package injects into Python's SSL layer to use the OS
certificate store. Conditional import — falls back gracefully if not installed.

### Decision: FastAPI Route Ordering for Path Parameters

**Lesson learned**: FastAPI matches routes in registration order. Static routes
(`/feedback/stats`, `/feedback/analysis`) MUST be registered before dynamic routes
(`/feedback/{dashboard_uid}`) — otherwise "stats" and "analysis" get matched as
dashboard_uid values.

---

## 4. API Surface

| Method | Path | Tag | Purpose |
|---|---|---|---|
| `GET` | `/healthz` | System | Health check |
| `GET` | `/` | — | Web UI (not in OpenAPI schema) |
| `POST` | `/api/v1/chart` | Dashboard Generation | Generate dashboard from prompt |
| `POST` | `/api/v1/feedback` | Feedback | Submit dimensional feedback |
| `GET` | `/api/v1/feedback/stats` | Insights | Aggregate feedback statistics |
| `GET` | `/api/v1/feedback/analysis` | Insights | Analysis & recommendations |
| `GET` | `/api/v1/feedback/{dashboard_uid}` | Feedback | Provenance + feedback for a dashboard |
| `POST` | `/api/v1/archetypes/reload` | Archetypes | Hot-reload from YAML |
| `GET` | `/api/v1/archetypes` | Archetypes | List loaded archetypes |
| `GET` | `/api/v1/investigations` | History | List recent investigations (filter by status, user) |
| `GET` | `/api/v1/investigations/stats` | History | Aggregate investigation stats |
| `GET` | `/api/v1/investigations/{id}` | History | Full investigation detail |
| `GET` | `/api/v1/signals` | Signals | List all semantic signal types |
| `GET` | `/api/v1/signals/stats` | Signals | Signal store statistics |
| `GET` | `/api/v1/signals/{signal_type}` | Signals | Signal details + all metric mappings |
| `POST` | `/api/v1/signals/teach` | Signals | Teach org-specific signal mapping |
| `POST` | `/api/v1/learn/dashboard` | Learning | Ingest existing Grafana dashboard |
| `GET` | `/api/v1/learn/dashboards` | Learning | List ingested dashboards |
| `POST` | `/api/v1/learn/dashboards/{uid}/approve` | Learning | Approve ingested dashboard |

**Auth**: Optional `X-API-Key` header (when `API_AUTH_ENABLED=true`).

**Docs**: Swagger at `/docs`, ReDoc at `/redoc`, OpenAPI JSON at `/openapi.json`.

---

## 5. Data Model (Pydantic Schemas — tacit/models/schemas.py)

### Core Pipeline Models

- **`Intent`** — LLM output: summary, domain, services, signals, keywords, timerange,
  problem_type, archetypes (list of `ArchetypeMatch`)
- **`ArchetypeMatch`** — `{type: str, confidence: float}` with 0.0-1.0 range
- **`MetricEntry`** — normalized metric from any datasource: name, datasource info,
  query language, namespace, dimensions
- **`DiscoveredMetric`** — LLM-selected metric with relevance reason
- **`PanelSpec`** — title, description, panel_type, queries, unit, row
- **`DashboardSpec`** — title, tags, timerange, panels
- **`DashRequest`** — prompt, channel_id, user_id, thread_ts
- **`DashResponse`** — dashboard_url, dashboard_uid, panel_count, summary

### Feedback Models

- **`FeedbackRequest`** — dashboard_uid, symptom_visibility (1-5), root_cause_support
  (1-5), noise_level (1-5), investigation_speed (1-5), overall_useful (bool), comment,
  reviewer
- **`FeedbackResponse`** — feedback_id, dashboard_uid, message
- **`FeedbackStatsResponse`** — totals, averages, useful_rate
- **`HealthResponse`**, **`ArchetypeListResponse`**, **`ArchetypeReloadResponse`**

### Archetype Schema (tacit/archetypes/schema.py)

- **`InvestigationArchetype`** — id, name, description, problem_types, required_metrics,
  required_signals, signal_bindings, panels, tags, default_timerange
- **`PanelTemplate`** — title, description, panel_type, row, queries, unit
- **`QueryTemplate`** — expr (with placeholders), legend_format, datasource_type

### Signal Schema (tacit/signals.py)

- **`signal_types` table** — signal_type (PK), description, category, unit, timestamps
- **`signal_metric_mappings` table** — many-to-many signal ↔ metric with context filters
  (services, datasource_types, environments, archetypes), provenance (source_type,
  source_refs), trust metrics (use_count, positive/negative_feedback), confidence decay
- **`ingested_dashboards` table** — dashboard features extracted during learning:
  metrics_found, row_groups, metric_cooccurrence, aggregation_patterns, panel_titles,
  alert_links, drilldown_links, inferred signals, approval status

---

## 6. Database Schema (SQLite — tacit/feedback.py)

### Table: dashboard_provenance

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| dashboard_uid | TEXT UNIQUE | Grafana UID |
| prompt | TEXT | Original user prompt |
| problem_type | TEXT | Primary archetype |
| archetypes | TEXT | JSON: `[{"type": "...", "confidence": 0.9}]` |
| metrics_used | TEXT | JSON: `["metric_name", ...]` |
| panel_count | INTEGER | |
| path_used | TEXT | "archetype" or "freeform" |
| dashboard_url | TEXT | Full Grafana URL |
| user_id | TEXT | |
| channel_id | TEXT | |
| created_at | REAL | Unix timestamp |

### Table: feedback

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PK | Auto-increment |
| dashboard_uid | TEXT FK | References provenance |
| reviewer | TEXT | User who submitted feedback |
| symptom_visibility | INTEGER | 1-5 scale |
| root_cause_support | INTEGER | 1-5 scale |
| noise_level | INTEGER | 1-5 scale (1=noisy, 5=clean) |
| investigation_speed | INTEGER | 1-5 scale |
| overall_useful | INTEGER | 0 or 1 |
| comment | TEXT | Free-text |
| created_at | REAL | Unix timestamp |

---

## 7. Security Hardening

- **Prompt injection**: All three agent system prompts include explicit guardrails.
  User messages treated as untrusted data, not instructions
- **Prompt sanitization**: Length cap (2000 chars), control-char removal
- **SQL injection**: All queries use parameterized `?` placeholders. `_sanitize_uid()`
  validates dashboard UIDs (regex: `^[a-zA-Z0-9_\-]{1,128}$`). FastAPI `PathParam`
  with regex pattern on `/{dashboard_uid}` route
- **YAML loading**: Uses `yaml.safe_load` everywhere (no arbitrary code execution)
- **API auth**: Optional `X-API-Key` header with timing-safe comparison
  (`secrets.compare_digest`)
- **SSL**: `truststore` for OS certificate store integration

---

## 8. Testing & Validation (tests/)

### Validation Suite (tests/validate.py)

Two validation modes:
1. **Archetype validation** — tests intent classification accuracy
   - Strict accuracy: top-1 archetype matches expected
   - Soft accuracy: any archetype matches expected
   - Confidence distribution analysis
2. **Pipeline validation** — end-to-end prompt → dashboard
   - **Tier 1**: metric recall, critical recall, weighted recall, signal-to-noise ratio
   - **Tier 2**: operational (panels/dashboard, latency, errors)
   - Weighted recall: critical metrics weight 1.0, supporting metrics weight 0.4

### Test Dataset

`tests/tacit_validation_prompts.csv` — 100 prompts with:
- prompt_id, prompt, expected_archetype, expected_metrics, expected_datasources,
  difficulty, validation_goal
- `critical_metrics` column: header added but not yet populated (semicolon-delimited)

### Interactive Human Review

`--review` flag triggers terminal-based review after pipeline validation:
- 5 dimensions + comment per dashboard
- Results stored in JSON output
- Feeds back into the feedback store

---

## 9. Bugs Fixed & Lessons Learned

### Bug: SSL Certificate Verify Failed
- **Symptom**: `httpx.ConnectError: [SSL: CERTIFICATE_VERIFY_FAILED]` when making LLM
  API calls
- **Root cause**: `truststore` was imported in `config.py` but not installed in the venv
- **Fix**: `pip install truststore` — enables Python to use OS certificate store
- **Lesson**: Always check that conditional imports have their packages actually installed

### Bug: sqlite3.ProgrammingError — Incorrect Number of Bindings
- **Symptom**: "The current statement uses 9, and there are 8 supplied" on feedback submit
- **Root cause**: `investigation_speed` was missing from the INSERT value tuple in
  `submit_feedback()`. 9 columns in the SQL but only 8 values in the Python tuple
- **Fix**: Added the missing `investigation_speed` parameter to the tuple
- **Lesson**: When constructing SQL tuples, always verify 1:1 correspondence between
  column list and values tuple. Count them.

### Bug: 404 on /feedback/stats and /feedback/analysis
- **Symptom**: Static routes returning 404
- **Root cause**: FastAPI route registration order — `/{dashboard_uid}` was registered
  before `/stats` and `/analysis`, so "stats" and "analysis" were matched as UID values
- **Fix**: Moved static routes above the dynamic `{dashboard_uid}` route
- **Lesson**: In FastAPI, static routes MUST be registered before parameterized routes
  at the same path level

### Bug: pathlib.Path vs fastapi.Path Name Collision
- **Symptom**: Import error when both `pathlib.Path` and `fastapi.Path` are needed
- **Fix**: `from fastapi import Path as PathParam`
- **Lesson**: Be aware of name collisions between stdlib and framework imports

---

## 10. Caching Strategy

| Cache | Location | TTL | What |
|---|---|---|---|
| Metric catalog | `cache.py` (`metric_cache`) | 5 min | Per-datasource metric names + labels |
| LLM responses | `cache.py` (`llm_cache`) | 10 min | Intent + metrics discovery results |
| Metric quality | `ranking.py` (module-level) | 10 min | Feedback-derived quality scores |

All caches are **in-memory TTLCache** — they die on restart. SQLite is the only
persistent store. No Redis/Postgres yet.

---

## 11. Supported Datasources

Each datasource type has a dedicated adapter in `tacit/grafana/adapters/`:

| Datasource | Adapter | Query Language |
|---|---|---|
| Prometheus / Mimir / Cortex / Thanos | `prometheus.py` | PromQL |
| CloudWatch | `cloudwatch.py` | CloudWatch JSON |
| Loki | `loki.py` | LogQL |
| Elasticsearch / OpenSearch | `elasticsearch.py` | Lucene |
| Graphite | `graphite.py` | Graphite functions |
| InfluxDB | `influxdb.py` | InfluxQL / Flux |
| Splunk SignalFx | `signalfx.py` | SignalFlow |

Adapters implement a common interface (`base.py`) for metric discovery through Grafana's
proxy/resource APIs.

---

## 12. Project File Structure

```
tacit/
├── tacit/
│   ├── cli.py               # CLI: init, doctor, connect, test, serve, history (Click + Rich)
│   ├── main.py              # FastAPI entrypoint (routes, auth, lifespan)
│   ├── config.py            # Layered config: YAML + env vars + Pydantic
│   ├── pipeline.py          # Orchestration: prompt → dashboard (8 steps)
│   ├── validation.py        # Pre-publish query validation
│   ├── cache.py             # In-memory TTL cache (metric_cache + llm_cache)
│   ├── ranking.py           # Pre-ranking + feedback-driven quality scoring
│   ├── feedback.py          # SQLite feedback store + analysis engine
│   ├── history.py           # SQLite investigation history (full pipeline telemetry)
│   ├── signals.py           # Semantic signal store + resolution engine (SQLite)
│   ├── dashboard_ingest.py  # Dashboard ingestion: extract → infer → learn
│   ├── signalfx/            # Direct Splunk SignalFx integration
│   │   ├── client.py        # Async SignalFx v2 REST API client
│   │   ├── discovery.py     # Direct metric discovery (reuses adapter keyword map)
│   │   └── publisher.py     # DashboardSpec → native SignalFx charts + dashboards
│   ├── agents/
│   │   ├── llm.py           # Provider-agnostic LLM caller (structured output)
│   │   ├── intent.py        # Intent classification (multi-label archetypes)
│   │   ├── metrics_discovery.py  # Cross-datasource metric selection
│   │   ├── query_builder.py # Multi-language query generation
│   │   └── providers/       # LLM backends (anthropic, openai, ollama)
│   ├── archetypes/
│   │   ├── schema.py        # InvestigationArchetype, PanelTemplate, QueryTemplate
│   │   ├── templates.py     # YAML loader + hardcoded fallback + registry
│   │   └── engine.py        # Template compiler + multi-archetype blending
│   ├── grafana/
│   │   ├── client.py        # Grafana HTTP API client
│   │   ├── datasource.py    # Cross-datasource orchestration
│   │   ├── dashboard.py     # Dashboard JSON builder & publisher
│   │   └── adapters/        # Per-datasource metric discovery
│   ├── context/             # Knowledge base integration (RAG/MCP/A2A)
│   ├── integrations/
│   │   └── slack.py         # Slack Bolt bot (socket mode)
│   ├── models/
│   │   └── schemas.py       # All Pydantic models
│   └── static/
│       └── index.html       # Web UI (dark theme, feedback forms, archetype info)
├── tests/
│   ├── validate.py          # Tiered validation suite
│   ├── tacit_validation_prompts.csv  # 100-prompt test dataset
│   └── README.md            # Validation documentation
├── dev/                     # Docker dev environment (Grafana, Prometheus, fake app)
├── archetypes.yaml          # Editable investigation templates (41 archetypes)
├── signals.yaml             # Bootstrap signal taxonomy (semantic signals → metric patterns)
├── tacit.yaml.example   # Reference config
├── tacit.spec           # PyInstaller spec for single-binary builds
├── scripts/
│   └── build.sh             # Build single binary
├── docker-compose.yml
├── pyproject.toml           # uv-managed deps
└── uv.lock                  # Reproducible lockfile
```

---

## 13. Running the Project

### Option A: CLI (Recommended)

```bash
# Install
pip install -e .

# Interactive setup — creates ~/.tacit/config.yaml + .env
tacit init

# Validate your setup
tacit doctor

# Connect to Grafana (interactive)
tacit connect grafana

# Run a sample investigation (opens dashboard in browser)
tacit test

# Start the API server
tacit serve
tacit serve --port 9000 --reload  # dev mode
tacit serve --no-slack             # disable Slack
```

### Option B: Docker

```bash
# Start supporting services + Tacit
docker compose up -d

# Create Grafana service account token (Editor role)
# Set GRAFANA_API_KEY in .env
```

### Access Points

| URL | Purpose |
|---|---|
| http://localhost:8000 | Web UI |
| http://localhost:8000/docs | Swagger UI |
| http://localhost:8000/redoc | ReDoc |
| http://localhost:3000 | Grafana (admin/admin) |

### CLI Commands Reference

| Command | Purpose |
|---|---|
| `tacit init` | Interactive setup wizard → `~/.tacit/config.yaml` + `.env` |
| `tacit doctor` | Validate Grafana, datasources, LLM, archetypes, cache |
| `tacit connect grafana` | Test and persist Grafana connection |
| `tacit test [-p "prompt"]` | Run sample investigation, open dashboard in browser |
| `tacit serve [--port --reload --no-slack]` | Start API + Slack server |
| `tacit history list [-n --status --user]` | List recent investigations (Rich table) |
| `tacit history show <id>` | Full investigation detail (intent, metrics, queries, timings) |
| `tacit history stats` | Aggregate stats (success/fail rates, avg time, path distribution) |

### Single Binary Distribution

```bash
./scripts/build.sh              # builds dist/tacit
sudo cp dist/tacit /usr/local/bin/
tacit init && tacit serve
```

### Validation

```bash
# Archetype-only validation
python tests/validate.py --mode archetype --api-url http://localhost:8000

# Full pipeline validation
python tests/validate.py --mode pipeline --api-url http://localhost:8000

# With interactive human review
python tests/validate.py --mode pipeline --api-url http://localhost:8000 --review
```

---

## 14. Current State & Open Items

### What Works
- Full pipeline: prompt → intent → metrics → query → dashboard → Grafana
- Multi-label archetype blending (41 archetypes, 176 panels, 153 problem types)
- **Semantic signal layer** — decouples archetypes from raw metric names via signal
  taxonomy. Archetypes declare `required_signals` + `signal_bindings`; resolution engine
  maps signals to actual metrics at compile time. Many-to-many signal ↔ metric with
  context-aware resolution (service, datasource, archetype, environment), confidence
  decay, feedback adjustment, and full provenance tracking.
- **Dashboard learning** — ingest existing Grafana dashboards to extract metric
  co-occurrence, row groupings, aggregation patterns, panel ordering, and auto-infer
  signal mappings. Human approval workflow for anti-drift protection.
- Pre-ranking with feedback-driven quality scoring
- Feedback collection, analysis, and recommendations
- YAML archetype templates with hot-reload
- Web UI with feedback forms and archetype info
- Swagger/ReDoc API documentation
- Query validation (drops panels with no data)
- 100-prompt validation suite with tiered metrics
- **CLI** — `tacit init/doctor/connect/test/serve/history` with Rich terminal UI
- **Config discovery** — `~/.tacit/config.yaml` + `~/.tacit/.env`
- **Single-binary distribution** — PyInstaller spec for macOS/Linux/Windows
- **Investigation history** — full pipeline telemetry persisted in SQLite (`data/tacit_history.db`).
  Stores: prompt, intent, archetypes, datasources, metrics catalog, selected metrics,
  generated queries, validation warnings, per-step timings, failures, dashboard URLs.
- **Splunk SignalFx direct integration** — dual-publish to Splunk Observability Cloud.
  Creates native SignalFx charts (SignalFlow) + dashboards via v2 REST API.
  Config: `signalfx_enabled`, `signalfx_realm`, `signalfx_api_token`.
  Reuses keyword→metric mapping from the Grafana adapter.

### Known Gaps / TODOs
- `critical_metrics` column in test CSV is empty (validation falls back to unweighted)
- `FeedbackStatsResponse` has required fields that the `get_aggregate_stats()` method
  doesn't always return (when `total_feedback == 0`, only returns `{"total_feedback": 0}`)
- Dashboard garbage collection not implemented (dashboards accumulate in Grafana)
- Caches are in-memory only — lost on restart
- No Prometheus `/metrics` endpoint for self-observability

### Roadmap (see README.md for full list)
- **Product boundary**: Tacit should consume organizational knowledge, not custody it.
  Enterprise runbooks, service catalogs, ownership data, postmortems, and policy knowledge
  should come through customer-owned RAG/A2A/MCP systems. Tacit owns observability
  outcomes: investigation history, dashboard provenance, feedback-derived metric quality,
  archetype gaps, and what worked in prior incidents.
- **Near term**: Ephemeral dashboard TTL, Loki/Tempo support, conversational refinement,
  alert context ingestion, dashboard versioning
- **Personal/demo memory**: Optional local memory mode using SQLite FTS over Tacit
  history/feedback, with Qdrant as an optional Docker-backed semantic demo backend.
  This is a convenience path, not the enterprise knowledge strategy.
- **Highest-leverage**: Grafana App Plugin — native "Investigate with Tacit" side panel
  inside Grafana. Shifts from external service to native workflow. Zero context switch.
- **Enterprise**: Hardened context provider contract for RAG/A2A/MCP, Tacit-native
  observability memory, metadata indexing, semantic metric retrieval, Observability IR,
  deterministic query compiler, query cost planner, RBAC, circuit breakers,
  self-observability
