# DashForge

**Natural language → Grafana dashboards.**

DashForge is an AI-powered observability navigation layer that lets on-call engineers describe a problem in plain English (via Slack or HTTP API) and instantly get a purpose-built Grafana dashboard populated with the most relevant metrics. No more hunting through static dashboards during an incident.

> *"High latency on the checkout service in the last hour"*
> → a dashboard with request rate, error rate, p99 latency, CPU, memory, and pod restarts — all wired up and ready.

---

## Why?

### The Core Reality

Most observability vendors are currently optimizing **signal surfacing** — things like:

- Anomaly detection
- Root cause hints
- Topology maps
- AI summaries
- Automatic correlations
- Natural language querying

These are useful. But on-call pain is often **not** "I cannot see metrics."

It's:

> **I don't know where to look next at 3AM.**

Even advanced systems today are mostly doing one thing well:

| System | Behavior |
|---|---|
| Datadog AI | Summarize |
| New Relic AI | Correlate |
| Grafana Assistant | Query |
| CloudWatch Investigations | Suggest |
| Dynatrace Davis | Infer |
| Splunk AI | Explain |

But the operator still performs **navigation**, **prioritization**, **hypothesis sequencing**, and **drilldown orchestration** — and that cognitive load is enormous during incidents.

DashForge closes this gap by turning a problem statement into a ready-made investigation dashboard in seconds.

## How it works

```
 Slack / Web UI / API
     │
     ▼
┌───────────────────┐
│ Prompt Sanitizer  │  Length cap, control-char removal, injection guard
└──────┬────────────┘
       ▼
┌──────────────────┐
│ Intent Agent     │  LLM classifies domain, services, signals, multi-label archetypes
└──────┬───────────┘
       ▼
┌───────────────────────┐
│ Context Enrichment    │  Optional: RAG / MCP / A2A knowledge base lookup
└──────┬────────────────┘
       ▼
┌───────────────────────┐
│ Datasource Discovery  │  Finds all Grafana datasources, filters by signal type
└──────┬────────────────┘
       ▼
┌───────────────────────┐
│ Metric Catalog Fetch  │  Per-datasource adapter queries metrics + per-metric labels
└──────┬────────────────┘
       ▼
       ├─────────────────────────────────────┐
       │ Archetype confidence > 0.3?         │ No match
       ▼                                     ▼
┌─────────────────────┐           ┌───────────────────────┐
│ Archetype Engine    │           │ Metrics Discovery LLM │
│ (blend if multi)    │           └──────┬────────────────┘
└──────┬──────────────┘                  ▼
       │                          ┌───────────────────────┐
       │                          │ Post-Validation       │
       │                          └──────┬────────────────┘
       │                                 ▼
       │                          ┌───────────────────────┐
       │                          │ Query Builder LLM     │
       │                          └──────┬────────────────┘
       │                                 │
       ├─────────────────────────────────┘
       ▼
┌───────────────────────┐
│ Query Validation      │  Verifies queries return real series data
└──────┬────────────────┘
       ▼
┌───────────────────────┐
│ Dashboard Publisher   │  Builds Grafana JSON, creates/updates via API
└──────┬────────────────┘
       ▼
 Dashboard URL → Slack / Web UI / API response
```

Inspired by [Uber's QueryGPT](https://www.uber.com/us/en/blog/query-gpt/) multi-agent decomposition pattern.

## Quick Start

### Prerequisites

- Docker & Docker Compose
- An LLM API key (Anthropic, OpenAI, or a local Ollama instance)
- (Optional) Slack Bot & App tokens for the Slack integration

### 1. Clone & configure

```bash
cd dashforge

# Option A: YAML config (recommended) + env vars for secrets
cp dashforge.yaml.example dashforge.yaml
# Edit dashforge.yaml for non-sensitive settings

# Option B: Flat .env file (still supported)
cp .env.example .env

# Either way, set secrets as env vars or in .env:
#   LLM_API_KEY, GRAFANA_API_KEY, SLACK_BOT_TOKEN, etc.
```

### 2. Start the stack

```bash
docker compose up -d
```

This starts:
- **Grafana** on [http://localhost:3000](http://localhost:3000) (admin/admin)
- **Prometheus** on [http://localhost:9090](http://localhost:9090)
- **Fake Metrics App** on [http://localhost:9091](http://localhost:9091) — generates realistic microservice metrics
- **DashForge API + Web UI** on [http://localhost:8000](http://localhost:8000)

Grafana is auto-provisioned with a Prometheus datasource. The fake app simulates
three services (`checkout-service`, `payment-api`, `inventory-db`) with HTTP, CPU,
memory, database, and pod metrics.

### API Documentation

Once the stack is running, interactive API docs are available at:

| URL | Format |
|-----|--------|
| [http://localhost:8000/docs](http://localhost:8000/docs) | **Swagger UI** — interactive, try-it-out |
| [http://localhost:8000/redoc](http://localhost:8000/redoc) | **ReDoc** — clean reference docs |
| [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json) | Raw OpenAPI 3.1 JSON schema |

Endpoints are grouped into:
- **Dashboard Generation** — `POST /api/v1/chart`
- **Feedback** — submit and retrieve human evaluation ratings
- **Insights** — aggregate stats and actionable analysis/recommendations
- **Archetypes** — list and hot-reload investigation templates
- **System** — health check

### 3. Create a Grafana service account token

1. Open Grafana → Administration → Service Accounts
2. Create a service account with **Editor** role
3. Generate a token and put it in `.env` as `GRAFANA_API_KEY`
4. Restart: `docker compose restart dashforge`

### 4. Try it via the HTTP API

```bash
curl -X POST http://localhost:8000/api/v1/chart \
  -H "Content-Type: application/json" \
  -d '{"prompt": "high CPU usage on prometheus server in the last 30 minutes"}'
```

Response:
```json
{
  "dashboard_url": "http://localhost:3000/d/abc123/...",
  "dashboard_uid": "abc123",
  "panel_count": 6,
  "summary": "Created dashboard **CPU Investigation — prometheus** with 6 panels."
}
```

## Slack Integration

### Setup

1. Create a Slack App at [api.slack.com/apps](https://api.slack.com/apps)
2. Enable **Socket Mode** and generate an App-Level Token (`xapp-...`)
3. Add Bot Token Scopes: `app_mentions:read`, `chat:write`, `commands`
4. Install the app to your workspace
5. (Optional) Create a `/dashforge` slash command
6. Set the three Slack env vars in `.env`:
   ```
   SLACK_BOT_TOKEN=xoxb-...
   SLACK_APP_TOKEN=xapp-...
   SLACK_SIGNING_SECRET=...
   ```
7. Restart: `docker compose restart dashforge`

### Usage

Mention the bot in any channel:

```
@DashForge high error rate on the payments API since 2pm
```

Or use the slash command:

```
/dashforge disk almost full on the database nodes
```

The bot will reply with a link to the freshly created Grafana dashboard.

## Architecture

| Component | Description |
|---|---|
| **Prompt Sanitizer** | Length caps, control-char removal, prompt injection guardrails |
| **Intent Agent** | LLM classifies domain, services, keywords, signal types, timerange, and multi-label archetypes with confidence scores |
| **Context Enrichment** | Pluggable knowledge base lookup (MCP, A2A, RAG API) — disabled by default |
| **Datasource Discovery** | Auto-discovers all Grafana datasources, filters by signal type |
| **Metric Catalog Fetch** | Per-datasource adapters query metric names + per-metric label names/values |
| **Archetype Engine** | Deterministic dashboard compilation for known investigation patterns. Multi-label: blends panels from multiple archetypes based on confidence (e.g. latency primary + saturation secondary). Skips LLM query generation entirely |
| **Metrics Discovery LLM** | *(freeform fallback)* Selects the most relevant metrics from the full catalog |
| **Post-Validation** | Drops hallucinated datasource UIDs, verifies metrics exist in catalog |
| **Query Builder LLM** | *(freeform fallback)* Generates PromQL/LogQL with accurate label selectors |
| **Query Validation** | Verifies all panel queries return real series data; drops empty panels, blocks empty dashboards |
| **Dashboard Publisher** | Assembles Grafana JSON model, creates/updates dashboards via API |
| **Web UI** | Simple browser interface at `/` for testing prompts |

All agents use structured JSON output with Pydantic validation. The LLM layer is
provider-agnostic — set `LLM_PROVIDER` to `anthropic`, `openai`, `azure`, or `ollama`.

### Key design decisions

- **Multi-label investigation archetypes** — incidents are inherently overlapping. The intent agent returns multiple archetypes with confidence scores (e.g. `latency_investigation: 0.91, resource_saturation: 0.62`). The archetype engine blends panels from multiple templates, giving broader investigation coverage. Known patterns are compiled deterministically — no LLM needed for query generation, ~75% faster, zero hallucination risk.
- **Query validation** — before publishing, every panel query is tested against the live datasource. Panels with no matching series are dropped. If all panels are empty, no dashboard is created and the user gets a clear error.
- **Per-metric label discovery** — the Prometheus adapter fetches actual label names and values for each metric via `/api/v1/series`, so the LLM writes queries with correct selectors instead of guessing.
- **Hallucination post-validation** — after the Metrics Discovery LLM runs, any metric referencing a datasource UID not in the real catalog is silently dropped.
- **Layered configuration** — schema-validated YAML config file with env var overrides. Secrets stay in env vars, non-sensitive config in `dashforge.yaml`.
- **Concurrency & timeout guards** — pipeline runs are bounded by a semaphore and a configurable timeout to prevent runaway LLM calls.
- **Security hardening** — all three agent system prompts include injection guardrails; API key auth is optional but built-in.

## Supported Datasources

DashForge searches **all** datasources registered in Grafana, not just Prometheus.
When you say "5xx on checkout", it searches CloudWatch for ALB errors, Prometheus for
pod-level metrics, Elasticsearch for log-derived data — all at once.

| Datasource | Query Language | Examples |
|---|---|---|
| **Prometheus / Mimir / Cortex / Thanos** | PromQL | k8s workloads, node metrics |
| **CloudWatch** | CloudWatch JSON | ALB/ELB, EC2, RDS, Lambda, SQS |
| **Loki** | LogQL | Log streams, log-derived metrics |
| **Elasticsearch / OpenSearch** | Lucene | APM data, log fields |
| **Graphite** | Graphite functions | Legacy dot-path metrics |
| **InfluxDB** | InfluxQL / Flux | Time-series measurements |

Each datasource type has a dedicated adapter that knows how to discover metrics
through Grafana's proxy/resource APIs.  The LLM selects the best metrics across
*all* datasources and generates the correct query language for each.

## Project Structure

```
dashforge/
├── dashforge/
│   ├── main.py              # FastAPI + Slack bot startup
│   ├── config.py            # Layered config: YAML + env vars + Pydantic validation
│   ├── pipeline.py          # Orchestration: prompt → dashboard
│   ├── validation.py        # Pre-publish query validation
│   ├── cache.py             # TTL-based metadata & LLM response cache
│   ├── ranking.py           # Pre-ranking: narrows metric catalog before LLM
│   ├── agents/
│   │   ├── llm.py           # Provider-agnostic LLM helpers
│   │   ├── intent.py        # Intent classification agent
│   │   ├── metrics_discovery.py  # Cross-datasource metric selection
│   │   ├── query_builder.py # Multi-language query generation
│   │   └── providers/       # LLM backends
│   │       ├── base.py      #   Abstract interface
│   │       ├── anthropic.py #   Claude
│   │       ├── openai_provider.py  # GPT-4o / Azure
│   │       ├── ollama.py    #   Local models
│   │       └── registry.py  #   Provider factory
│   ├── archetypes/          # Investigation archetypes (deterministic path)
│   │   ├── schema.py        #   Archetype, PanelTemplate, QueryTemplate models
│   │   ├── templates.py     #   Built-in archetypes (latency, error, golden, saturation)
│   │   └── engine.py        #   Template compiler: resolves params → DashboardSpec
│   ├── grafana/
│   │   ├── client.py        # Grafana HTTP API client
│   │   ├── datasource.py    # Cross-datasource orchestration
│   │   ├── dashboard.py     # Dashboard JSON builder & publisher
│   │   └── adapters/        # Per-datasource-type metric discovery
│   │       ├── base.py      #   Abstract adapter interface
│   │       ├── prometheus.py #  Prometheus / Mimir / Cortex / Thanos
│   │       ├── cloudwatch.py #  AWS CloudWatch
│   │       ├── loki.py       #  Loki log streams
│   │       ├── elasticsearch.py # ES / OpenSearch
│   │       ├── graphite.py   #  Graphite
│   │       ├── influxdb.py   #  InfluxDB
│   │       └── registry.py   #  Adapter factory
│   ├── context/             # Knowledge base integration (pluggable)
│   │   ├── base.py          #   Abstract ContextProvider interface
│   │   ├── mcp_provider.py  #   MCP (Model Context Protocol)
│   │   ├── a2a_provider.py  #   A2A (Agent-to-Agent)
│   │   ├── rag_api_provider.py # RAG API gateway (REST)
│   │   ├── registry.py      #   Provider factory
│   │   └── enrichment.py    #   Orchestrator + prompt formatter
│   ├── integrations/
│   │   └── slack.py         # Slack Bolt bot
│   ├── models/
│   │   └── schemas.py       # Pydantic models (Intent, ArchetypeMatch, DashboardSpec)
│   └── static/
│       └── index.html       # Web UI for testing
├── tests/                   # Validation & testing
│   ├── validate.py          # Validation suite (archetype + pipeline accuracy)
│   ├── dashforge_validation_prompts.csv  # 100-prompt test dataset
│   ├── test_unit.py         # Unit tests
│   └── README.md            # Validation documentation
├── dev/                     # Local dev environment
│   ├── fake_app/           # Fake metrics exporter (checkout, payment, inventory)
│   ├── prometheus/         # Prometheus config
│   └── grafana/            # Grafana provisioning
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml           # Project metadata & deps (uv)
├── dashforge.yaml.example   # Reference YAML config (schema-validated)
└── .env.example             # Reference env vars (secrets go here)
```

## Roadmap

### Done

- [x] Knowledge base integration (RAG / A2A / MCP) — pluggable context enrichment
- [x] Prompt injection guardrails & security hardening
- [x] Per-metric label introspection for accurate queries
- [x] Hallucination post-validation (drop invalid datasource UIDs)
- [x] Web UI for testing
- [x] Fake metrics app for local dev/testing
- [x] TTL-based metadata cache (metric catalog cached per datasource, 5 min TTL)
- [x] Pre-ranking — lightweight scoring narrows catalog before LLM (reduces token cost)
- [x] LLM response caching (intent + discovery results, 10 min TTL)
- [x] Pipeline timing telemetry (per-step latency logged on every request)
- [x] Dynamic panel grouping (row-based sections driven by intent, e.g. golden signals)
- [x] Investigation archetypes — deterministic dashboard compilation for known problem types (latency, error spike, golden signals, resource saturation). ~75% faster, zero hallucination
- [x] Query validation — pre-publish verification that queries return real data. Empty panels dropped, empty dashboards blocked
- [x] Schema-validated YAML config — layered `dashforge.yaml` + env var overrides, replacing fragile flat `.env`
- [x] Multi-label archetypes with confidence scores — incidents span multiple domains; intent agent returns ranked archetypes, engine blends panels from multiple templates
- [x] Tiered validation suite — archetype accuracy (strict + soft), metric recall, critical metric recall, weighted recall, signal-to-noise ratio
- [x] `uv`-based dependency management — faster installs, reproducible lockfile
- [x] Production feedback system — dimensional SRE ratings (symptom visibility, root cause support, noise level, investigation speed, overall usefulness), SQLite-backed provenance tracking, aggregate stats
- [x] Closed-loop metric ranking — feedback-driven quality scores automatically boost/penalize metrics in pre-ranking. No model retraining needed
- [x] Feedback analysis & recommendations — per-archetype quality, noisy dashboard detection, archetype gap identification, metric quality scoring, confidence calibration, actionable recommendations (PRUNE, ADD SIGNAL, NEW ARCHETYPE, DEPRIORITIZE, RECALIBRATE)
- [x] YAML archetype templates — editable `archetypes.yaml` with hot-reload API endpoint. Engineers update investigation templates without touching Python code
- [x] Interactive API documentation — Swagger UI (`/docs`) and ReDoc (`/redoc`) with grouped endpoints, response schemas, and examples
- [x] Input validation & SQL injection hardening — parameterized queries, UID regex validation, path parameter constraints

### Personal Use — Near Term

- [ ] Ephemeral dashboard garbage collection (TTL-based cleanup)
- [ ] Loki (log panel) support
- [ ] Tempo (trace) support
- [ ] Conversational refinement — refine dashboards via follow-up messages (zoom, pivot, drill-down)
- [ ] Alert context ingestion — auto-read alert payload as prompt
- [ ] Dashboard versioning / history

### Enterprise — Architecture Evolution

- [ ] **Metadata indexing layer** — background indexer → vector/relational metadata store, replacing live datasource introspection per request. Store metric names, label keys, common values, descriptions, usage frequency, service ownership, embeddings. Moves system from O(all metrics) to O(relevant metrics).
- [ ] **Semantic retrieval before reasoning** — BM25 + embedding hybrid search to narrow 50k+ metrics → top 50 candidates before LLM. Mandatory for cost/latency/quality at scale. (Current pre-ranking uses keyword/service relevance; embeddings would improve recall.)
- [ ] **Deterministic query compiler** — extend archetype engine to full AST-based compilation for freeform path. LLM emits semantic intent AST, deterministic code generates validated PromQL/LogQL.
- [ ] **Canonical Observability IR** — intermediate representation (`signal_type`, `resource_type`, `aggregation`, `scope`) that each datasource adapter maps to native queries. Scales portability without accumulating datasource-specific prompt hacks.
- [ ] **Query cost planner** — cardinality estimation, time-range scoring, query complexity analysis, datasource load awareness. Like a database optimizer for observability queries.

### Enterprise — Security & Compliance

- [ ] **Structured trust boundaries** — all untrusted input (logs, labels, RAG, Slack) tagged with `trusted: false` in structured prompts. LLM aware of trust levels.
- [ ] **Query allowlists** — allowed metric families, labels, aggregations, functions. Compile from AST templates, not raw text.
- [ ] **Multi-tenant RBAC** — RBAC-aware retrieval filtering by user/team/org/datasource permissions at every step, not just dashboard publishing.
- [ ] **Slack hardening** — per-user/channel/workspace rate limits, approval workflows for production datasources, query cost estimation before execution.
- [ ] **Audit logging & compliance** — PII exposure controls, data residency, LLM vendor routing. (Dashboard provenance and prompt audit trail already implemented via feedback store.)

### Enterprise — Reliability & Cost

- [ ] **Circuit breakers & degradation** — retries with jitter, partial degradation, fallbacks, cancellation propagation for Grafana/Prometheus/CloudWatch/LLM failures.
- [ ] **LLM cost optimization** — classifier models + local embeddings for intent/ranking, reserve large LLMs only for layout reasoning. (Pre-ranking and LLM response caching already reduce token cost; next step is smaller models for classification.)
- [ ] **Self-observability** — DashForge exposes Prometheus metrics: prompt latency, query success rate, hallucination rate, dashboard usefulness, token cost, cache hit rate. (Per-step pipeline telemetry already logged; next step is Prometheus `/metrics` endpoint.)
- [ ] **Correctness validation** — heuristics for SRE best practices (counter vs gauge, correct aggregation, valid RED/USE metrics), golden dashboard templates, domain-specific validation rules.

### Enterprise — Integrations

- [ ] Webex / Zoom integrations
- [ ] Vendor-specific dashboards (Datadog, New Relic)

## License

MIT
