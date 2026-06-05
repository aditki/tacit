# DashForge

**Natural language → observability dashboards.**

DashForge is an AI-powered observability navigation layer that lets on-call engineers describe a problem in plain English (via Slack or HTTP API) and instantly get a purpose-built dashboard populated with the most relevant metrics. It publishes to **Grafana**, **Splunk Observability Cloud (SignalFx)**, or both simultaneously — with a pluggable backend adapter pattern that makes adding new vendors straightforward. No more hunting through static dashboards during an incident.

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

| System                    | Behavior  |
| ------------------------- | --------- |
| Datadog AI                | Summarize |
| New Relic AI              | Correlate |
| Grafana Assistant         | Query     |
| CloudWatch Investigations | Suggest   |
| Dynatrace Davis           | Infer     |
| Splunk AI                 | Explain   |

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
│ Backend Adapters      │  Each enabled backend (Grafana, SignalFx, …) contributes
│ discover_metrics()    │  metrics from its own datasources in parallel
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
│ Backend Adapters      │  Primary backend validates queries return real data
│ validate_queries()    │
└──────┬────────────────┘
       ▼
┌───────────────────────┐
│ Backend Adapters      │  Each backend publishes independently:
│ publish()             │  Grafana JSON, SignalFx charts, or both
└──────┬────────────────┘
       ▼
 Dashboard URLs → Slack / Web UI / API response
```

### Dashboard Learning Loop

```
 Existing Grafana / SignalFx dashboards
     │
     ▼
┌──────────────────────────┐
│ Dashboard Ingestion      │  Vendor-agnostic: backend.ingest_dashboard(uid)
│ (PromQL / SignalFlow)    │  Extracts metrics, panels, rows, aggregation patterns
└──────┬───────────────────┘
       ▼
┌──────────────────────────┐
│ Signal Inference Engine  │  Matches metrics → signal taxonomy (signals.yaml)
│                          │  12 categories, pattern-based with confidence scores
└──────┬───────────────────┘
       ▼
┌──────────────────────────┐
│ Signal Store (SQLite)    │  Persists metric→signal mappings, confidence decay,
│                          │  feedback adjustment, context-aware resolution
└──────┬───────────────────┘
       ▼
 Signal taxonomy feeds archetype engine + metric ranking
```

The pipeline is **vendor-agnostic**. Each backend (Grafana, SignalFx) implements
the same `DashboardBackend` protocol — `discover_metrics()`, `validate_queries()`,
`publish()`, `close()`. The pipeline iterates over enabled backends with zero
vendor-specific conditionals. Adding a new backend means implementing one adapter
class and registering it in the config.

Inspired by [Uber's QueryGPT](https://www.uber.com/us/en/blog/query-gpt/) multi-agent decomposition pattern.

## Quick Start

### Option A: CLI (Recommended)

```bash
pip install -e .

# Interactive setup — walks you through Grafana URL, API key, LLM provider
dashforge init

# Validate everything is connected
dashforge doctor

# Run a sample investigation (opens dashboard in your browser)
dashforge test

# Start the server
dashforge serve
```

That's it. Three commands from zero to dashboard.

### CLI Commands

| Command                               | What it does                                                                                        |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `dashforge init`                      | Interactive setup wizard → `~/.dashforge/config.yaml` + secrets in `~/.dashforge/.env`              |
| `dashforge doctor`                    | Validates Grafana + SignalFx connectivity, datasource permissions, LLM key, archetypes, cache state |
| `dashforge connect grafana`           | Test & persist a Grafana connection (interactive or `--url` / `--api-key` flags)                    |
| `dashforge connect signalfx`          | Test & persist a Splunk SignalFx connection (interactive or `--realm` / `--token` flags)            |
| `dashforge test [-p "custom prompt"]` | Runs a full investigation pipeline and opens the resulting dashboard                                |
| `dashforge serve`                     | Starts the API server (+ Slack if configured)                                                       |
| `dashforge history list`              | List recent investigations with status, timings, archetypes                                         |
| `dashforge history show <id>`         | Full investigation detail (intent → metrics → queries → result)                                     |
| `dashforge history stats`             | Aggregate stats: success rates, avg time, path distribution                                         |

`dashforge serve` options: `--host`, `--port`, `--reload` (dev mode), `--no-slack`.

### Option B: Docker

```bash
# Setup your .env file
cp .env.example .env

docker compose up -d

# Go to localhost:8000 for Dashforge
# generate a dashboard
# go to localhost:3000 for Grafana to see dashboard
```

This starts Grafana (`:3000`), Prometheus (`:9090`), a fake metrics app (`:9091`), and DashForge (`:8000`). Grafana is auto-provisioned with a Prometheus datasource and three simulated services.

### Single Binary (no Python required)

```bash
# Build
./scripts/build.sh

# Install
sudo cp dist/dashforge /usr/local/bin/

# Use
dashforge init
dashforge serve
```

### Grafana Service Account

1. Open Grafana → Administration → Service Accounts
2. Create a service account with **Editor** role
3. Generate a token — `dashforge init` will prompt for it, or set `GRAFANA_API_KEY` in your env

### Try the HTTP API

```bash
curl -X POST http://localhost:8000/api/v1/chart \
  -H "Content-Type: application/json" \
  -d '{"prompt": "high CPU usage on prometheus server in the last 30 minutes"}'
```

```json
{
  "dashboard_url": "http://localhost:3000/d/abc123/...",
  "dashboard_uid": "abc123",
  "panel_count": 6,
  "summary": "Created dashboard **CPU Investigation — prometheus** with 6 panels."
}
```

### API Documentation

| URL                                                               | Format                       |
| ----------------------------------------------------------------- | ---------------------------- |
| [localhost:8000/docs](http://localhost:8000/docs)                 | **Swagger UI** — interactive |
| [localhost:8000/redoc](http://localhost:8000/redoc)               | **ReDoc** — reference docs   |
| [localhost:8000/openapi.json](http://localhost:8000/openapi.json) | OpenAPI 3.1 JSON             |

## Slack Integration

### Setup

1. Create a Slack App at [api.slack.com/apps](https://api.slack.com/apps)
2. Enable **Socket Mode** and generate an App-Level Token (`xapp-...`)
3. Add Bot Token Scopes: `app_mentions:read`, `chat:write`, `commands`
4. Install the app to your workspace
5. (Optional) Create a `/dashforge` slash command
6. Add the Slack tokens to your config:

   **Option A: CLI** — `dashforge init` will prompt for Slack tokens during interactive setup. They are stored in `~/.dashforge/.env`.

   **Option B: Manual** — add to `~/.dashforge/.env` or your project `.env`:
   ```
   SLACK_BOT_TOKEN=xoxb-...
   SLACK_APP_TOKEN=xapp-...
   SLACK_SIGNING_SECRET=...
   ```

7. Start the server:
   ```bash
   dashforge serve              # Slack enabled by default
   dashforge serve --no-slack   # disable Slack integration
   ```
   Or with Docker: `docker compose restart dashforge`

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

## Splunk SignalFx (Direct Integration)

DashForge can publish dashboards **directly to Splunk Observability Cloud** (SignalFx),
in addition to Grafana. When enabled, each pipeline run creates both a Grafana dashboard
and a native SignalFx dashboard with SignalFlow charts.

### Setup

1. Get a SignalFx API access token from **Settings → Access Tokens** in Splunk Observability Cloud
2. Configure via `dashforge init` or add to `~/.dashforge/.env`:
   ```
   SIGNALFX_API_TOKEN=<your-token>
   ```
   And in `~/.dashforge/config.yaml`:
   ```yaml
   signalfx:
     enabled: true
     realm: us1       # us0, us1, us2, eu0, jp0, au0
     dashboard_group: DashForge
   ```
3. Run `dashforge doctor` to verify connectivity

When enabled, the API response includes `signalfx_url` and `signalfx_dashboard_id`
alongside the standard Grafana fields.

## Dashboard Learning & Signals

DashForge can **learn from existing dashboards** — ingest a Grafana or SignalFx dashboard
and automatically infer which observability signals (latency, error rate, saturation, etc.)
its metrics represent. Learned mappings feed back into the pipeline to improve metric
ranking and archetype selection.

### Ingest a Dashboard

Via the **Web UI** — go to the **Learning** tab, enter a dashboard UID, select the backend, and click "Ingest Dashboard".

Via the **API**:

```bash
curl -X POST http://localhost:8000/api/v1/learn/dashboard \
  -H "Content-Type: application/json" \
  -d '{"dashboard_uid": "my-service-overview", "backend": "grafana", "auto_approve": true}'
```

### Teach a Signal Mapping

Manually teach DashForge that a custom metric maps to a signal:

```bash
curl -X POST http://localhost:8000/api/v1/signals/teach \
  -H "Content-Type: application/json" \
  -d '{
    "signal_type": "request_latency",
    "metric_patterns": [{"pattern": "my_custom_latency_seconds", "confidence": 0.9}],
    "services": ["checkout"]
  }'
```

### Signal Taxonomy

The signal taxonomy (`signals.yaml`) defines 12 categories with metric patterns:

| Category | Signals |
|---|---|
| **Latency** | request_latency, db_query_latency, dns_latency |
| **Throughput** | request_rate, message_throughput |
| **Errors** | error_rate, tls_handshake_failures, dns_failures |
| **Saturation** | cpu_usage, memory_usage, disk_usage, db_connection_pool, in_flight_requests |
| **Stability** | pod_restarts |
| **Auth** | auth_failure_count, rate_limit_hits, tls_handshake_failures |
| **Caching** | cache_hit_ratio |
| **Network** | network_bytes, dns_failures, tls_handshake_failures |
| **Messaging** | consumer_lag, queue_depth |
| **Storage** | disk_usage, db_connection_pool |
| **Serverless** | cold_start_duration, concurrent_executions |
| **Traffic Management** | rate_limit_hits |

### API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/learn/dashboard` | Ingest a dashboard (requires `dashboard_uid`, optional `backend`, `auto_approve`) |
| `GET` | `/api/v1/learn/dashboards` | List ingested dashboards |
| `POST` | `/api/v1/learn/dashboards/{uid}/approve` | Approve a pending ingested dashboard |
| `GET` | `/api/v1/signals` | List all signal types with mapping counts |
| `GET` | `/api/v1/signals/{signal_type}` | Get signal detail with all metric mappings |
| `GET` | `/api/v1/signals/stats` | Signal store statistics |
| `POST` | `/api/v1/signals/teach` | Teach a new metric→signal mapping |

## AWS Bedrock (LLM Provider)

DashForge supports **AWS Bedrock** as an LLM provider for organizations that require
all AI calls to stay within their AWS account.

### Setup

1. Install the optional dependency:
   ```bash
   pip install 'dashforge[bedrock]'
   ```
2. Configure via `dashforge init` or manually in `~/.dashforge/config.yaml`:
   ```yaml
   llm:
     provider: bedrock
     model: anthropic.claude-sonnet-4-20250514-v1:0
     bedrock_region: us-east-1
     # bedrock_role_arn: arn:aws:iam::123456789012:role/DashForgeBedrock  # optional cross-account
   ```
3. Authentication (resolved in order):
   - **Explicit keys** — set `LLM_AWS_ACCESS_KEY_ID` + `LLM_AWS_SECRET_ACCESS_KEY` in `~/.dashforge/.env`
   - **Assume-role** — set `llm.bedrock_role_arn` in config (uses STS)
   - **Default boto3 chain** — env vars, `~/.aws/credentials`, EC2 instance profile, ECS task role
4. Run `dashforge doctor` to verify (calls `sts:GetCallerIdentity`)

No API key is needed — Bedrock uses IAM authentication.

## Architecture

| Component | Description |
|---|---|
| **Prompt Sanitizer** | Length caps, control-char removal, prompt injection guardrails |
| **Intent Agent** | LLM classifies domain, services, keywords, signal types, timerange, and multi-label archetypes with confidence scores |
| **Context Enrichment** | Pluggable knowledge base lookup (MCP, A2A, RAG API) — disabled by default |
| **Backend Adapters** | Pluggable `DashboardBackend` protocol (Grafana, SignalFx). Each backend discovers metrics, validates queries, and publishes dashboards independently. Pipeline iterates over enabled backends — zero vendor-specific branching |
| **Datasource Discovery** | Grafana: auto-discovers all datasources, filters by signal type. SignalFx: keyword search via v2 metadata API |
| **Metric Catalog Fetch** | Per-datasource adapters query metric names + per-metric label names/values |
| **Archetype Engine** | Deterministic dashboard compilation for known investigation patterns. Multi-label: blends panels from multiple archetypes based on confidence (e.g. latency primary + saturation secondary). Skips LLM query generation entirely |
| **Metrics Discovery LLM** | *(freeform fallback)* Selects the most relevant metrics from the full catalog |
| **Post-Validation** | Drops hallucinated datasource UIDs, verifies metrics exist in catalog |
| **Query Builder LLM** | *(freeform fallback)* Generates PromQL/LogQL with accurate label selectors |
| **Query Validation** | Primary backend verifies all panel queries return real data (PromQL via datasource proxy, SignalFlow via metric existence check); drops empty panels, blocks empty dashboards |
| **Dashboard Publisher** | Each enabled backend publishes independently — Grafana JSON via API, SignalFx charts via v2 REST API, or both |
| **Dashboard Ingestion** | Vendor-agnostic learning: ingests existing Grafana/SignalFx dashboards, extracts metrics & query patterns, infers signal mappings. `DashboardFeatures` dataclass normalizes across backends |
| **Signal Store** | SQLite-backed signal taxonomy: 12 categories, metric→signal mappings with confidence decay (90-day half-life), feedback adjustment (±30%), context-aware resolution (service, datasource, environment), trust threshold (0.15) |
| **Web UI** | Browser interface at `/` with tabs: Generate, Learning (dashboard ingestion), Signals (taxonomy & teach), Insights (feedback), Archetypes, History |

All agents use structured JSON output with Pydantic validation. The LLM layer is
provider-agnostic — set `LLM_PROVIDER` to `anthropic`, `openai`, `azure`, `bedrock`, or `ollama`.

### Key design decisions

- **Multi-label investigation archetypes** — incidents are inherently overlapping. The intent agent returns multiple archetypes with confidence scores (e.g. `latency_investigation: 0.91, resource_saturation: 0.62`). The archetype engine blends panels from multiple templates, giving broader investigation coverage. Known patterns are compiled deterministically — no LLM needed for query generation, ~75% faster, zero hallucination risk.
- **Query validation** — before publishing, every panel query is tested against the live datasource. Panels with no matching series are dropped. If all panels are empty, no dashboard is created and the user gets a clear error.
- **Per-metric label discovery** — the Prometheus adapter fetches actual label names and values for each metric via `/api/v1/series`, so the LLM writes queries with correct selectors instead of guessing.
- **Hallucination post-validation** — after the Metrics Discovery LLM runs, any metric referencing a datasource UID not in the real catalog is silently dropped.
- **Layered configuration** — schema-validated YAML config file with env var overrides. Secrets stay in env vars, non-sensitive config in `dashforge.yaml`.
- **Concurrency & timeout guards** — pipeline runs are bounded by a semaphore and a configurable timeout to prevent runaway LLM calls.
- **Security hardening** — all three agent system prompts include injection guardrails; API key auth is optional but built-in.

## Supported Backends & Datasources

DashForge publishes to multiple backends simultaneously. Each backend discovers
metrics from its own sources, validates queries, and publishes dashboards independently.

| Backend             | Discovery                                             | Query Language                                          | Publishing         |
| ------------------- | ----------------------------------------------------- | ------------------------------------------------------- | ------------------ |
| **Grafana**         | Searches all registered datasources (see table below) | PromQL, LogQL, CW JSON, Lucene, Graphite, InfluxQL/Flux | Grafana JSON API   |
| **Splunk SignalFx** | Keyword search via v2 metadata API                    | SignalFlow                                              | Native v2 REST API |

When both are enabled, a single prompt creates dashboards in **both** systems.

### Grafana Datasources

When Grafana is enabled, DashForge searches **all** registered datasources, not just Prometheus.
When you say "5xx on checkout", it searches CloudWatch for ALB errors, Prometheus for
pod-level metrics, Elasticsearch for log-derived data — all at once.

| Datasource                               | Query Language     | Examples                                                 |
| ---------------------------------------- | ------------------ | -------------------------------------------------------- |
| **Prometheus / Mimir / Cortex / Thanos** | PromQL             | k8s workloads, node metrics                              |
| **CloudWatch**                           | CloudWatch JSON    | ALB/ELB, EC2, RDS, Lambda, SQS                           |
| **Loki**                                 | LogQL              | Log streams, log-derived metrics                         |
| **Elasticsearch / OpenSearch**           | Lucene             | APM data, log fields                                     |
| **Graphite**                             | Graphite functions | Legacy dot-path metrics                                  |
| **InfluxDB**                             | InfluxQL / Flux    | Time-series measurements                                 |
| **Splunk SignalFx**                      | SignalFlow         | Splunk Observability Cloud, infrastructure & APM metrics |

Grafana datasource types have dedicated adapters that discover metrics through
Grafana's proxy/resource APIs. SignalFx uses its own v2 metadata API. The LLM
selects the best metrics across *all* backends and datasources and generates
the correct query language for each.

## Project Structure

```
dashforge/
├── dashforge/
│   ├── cli.py               # CLI: init, doctor, connect, test, serve, history
│   ├── main.py              # FastAPI + Slack bot startup
│   ├── config.py            # Layered config: YAML + env vars + Pydantic validation
│   ├── pipeline.py          # Orchestration: prompt → dashboard (vendor-agnostic)
│   ├── validation.py        # Pre-publish query validation (PromQL + SignalFlow)
│   ├── backends/            # Backend adapter pattern
│   │   ├── base.py          # DashboardBackend Protocol + PublishResult + DashboardFeatures
│   │   ├── grafana.py       # GrafanaBackend adapter (incl. ingest_dashboard)
│   │   ├── signalfx.py      # SignalFxBackend adapter (incl. ingest_dashboard)
│   │   └── __init__.py      # Registry: get_active_backends()
│   ├── signals.py           # Signal store: taxonomy, metric mappings, confidence (SQLite)
│   ├── dashboard_ingest.py  # Vendor-agnostic dashboard learning pipeline
│   ├── signals.yaml         # Signal taxonomy: 12 categories, metric patterns
│   ├── cache.py             # TTL-based metadata & LLM response cache
│   ├── ranking.py           # Pre-ranking: narrows metric catalog before LLM
│   ├── history.py           # Investigation history store (SQLite)
│   ├── signalfx/            # Direct Splunk SignalFx integration
│   │   ├── client.py        # Async SignalFx v2 REST API client
│   │   ├── discovery.py     # Direct metric discovery (reuses adapter's keyword map)
│   │   └── publisher.py     # DashboardSpec → native SignalFx charts + dashboards
│   ├── agents/
│   │   ├── llm.py           # Provider-agnostic LLM helpers
│   │   ├── intent.py        # Intent classification agent
│   │   ├── metrics_discovery.py  # Cross-datasource metric selection
│   │   ├── query_builder.py # Multi-language query generation
│   │   └── providers/       # LLM backends
│   │       ├── base.py      #   Abstract interface
│   │       ├── anthropic.py #   Claude (direct API)
│   │       ├── openai_provider.py  # GPT-4o / Azure OpenAI
│   │       ├── bedrock.py   #   AWS Bedrock (IAM auth, assume-role, Converse API)
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
│       └── index.html       # Web UI (Generate, Learning, Signals, Insights, Archetypes, History)
├── tests/                   # Validation & testing
│   ├── validate.py          # Validation suite (archetype + pipeline accuracy)
│   ├── dashforge_validation_prompts.csv  # 100-prompt test dataset
│   ├── test_unit.py         # Unit tests
│   ├── test_signals.py      # Signal store, inference, SignalFlow extraction tests
│   └── README.md            # Validation documentation
├── dev/                     # Local dev environment
│   ├── fake_app/           # Fake metrics exporter (checkout, payment, inventory)
│   ├── prometheus/         # Prometheus config
│   └── grafana/            # Grafana provisioning (incl. signal_coverage dashboard)
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml           # Project metadata & deps (uv)
├── dashforge.yaml.example   # Reference YAML config (schema-validated)
├── dashforge.spec           # PyInstaller spec for single-binary builds
├── scripts/
│   └── build.sh             # Build single binary
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
- [x] CLI (`dashforge init/doctor/connect grafana/connect signalfx/test/serve`) — Click + Rich, interactive setup wizard, connection validation, single-command startup
- [x] Backend adapter pattern — `DashboardBackend` protocol with pluggable adapters (Grafana, SignalFx). Pipeline iterates over enabled backends for discovery, validation, and publishing — zero vendor-specific branching
- [x] AWS Bedrock LLM provider — IAM auth (explicit keys, assume-role, default boto3 chain), Converse API for unified model access (Claude, Llama, Mistral on Bedrock)
- [x] Config discovery (`~/.dashforge/config.yaml` + `~/.dashforge/.env`) — secrets isolated at 0600 permissions
- [x] Single-binary distribution — PyInstaller spec for macOS/Linux/Windows, `./scripts/build.sh`
- [x] 41 investigation archetypes with 176 panels covering latency, errors, golden signals, Kubernetes, Kafka (5 archetypes), Redis, SQS, Lambda, DDoS, mTLS, capacity planning, and more
- [x] Investigation history — full pipeline telemetry persisted in SQLite: prompt, intent, archetypes, datasources, metrics, queries, validation, per-step timings, failures, dashboard URLs. API + CLI (`dashforge history list/show/stats`)
- [x] Structured logging — every pipeline stage emits `stage_complete` events with `request_id`, `stage`, `latency_ms`, token counts, and stage-specific fields via structlog
- [x] Vendor-agnostic dashboard learning — ingest existing Grafana/SignalFx dashboards, extract metrics & query patterns (PromQL + SignalFlow), infer signal mappings via pattern matching. `DashboardFeatures` dataclass normalizes across backends
- [x] Signal taxonomy & store — 12 signal categories (latency, throughput, errors, saturation, stability, auth, caching, network, messaging, storage, serverless, traffic management), SQLite-backed metric→signal mappings with confidence decay, feedback adjustment, context-aware resolution
- [x] Web UI: Learning tab — dashboard ingestion form (backend selector, UID, auto-approve toggle), ingested dashboard history with approval workflow
- [x] Web UI: Signals tab — signal taxonomy browser (grouped by category, mapping counts, drill-down), teach signal mapping form for manual metric→signal associations
- [x] XSS hardening — all server data escaped via `esc()` before `innerHTML` injection across all UI tabs
- [x] Data persistence — SQLite databases mounted via Docker volume (`./data:/app/data`) for signals, feedback, and history

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

- [ ] **Grafana App Plugin** *(highest-leverage UX move)* — native "Investigate with DashForge" side panel inside Grafana. Shifts DashForge from external AI service to native Grafana workflow. Engineers trust tools inside Grafana far more than external systems. Plugin surfaces a prompt input in Grafana's sidebar, calls DashForge API, and opens the generated dashboard in-place — zero context switch.
- [ ] Webex / Zoom integrations
- [ ] Vendor-specific dashboards (Datadog, New Relic)

## License

MIT
