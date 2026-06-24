[!WARNING]

Early Project Status

Tacit is an experimental infrastructure engineering project.

It is not production-ready and should currently be treated as a development/private-beta system only.

The primary goals of this repository are:

exploring LLM-assisted observability workflows
experimenting with multi-agent infrastructure tooling
demonstrating systems/infrastructure engineering ability
learning from real-world operational tradeoffs

APIs, configuration formats, internal architecture, and integrations may change significantly between versions.

# Tacit

**Natural language → operational investigation artifacts.**

Tacit is an experimental operational cognition layer for incident response. It lets on-call engineers describe a problem in plain English (via Slack, Web UI, or HTTP API), then turns that prompt into a structured investigation path and one or more evidence artifacts. Today the primary artifact is a purpose-built dashboard published to **Grafana**, **Splunk Observability Cloud (SignalFx)**, or both. The longer-term object is the investigation itself: selected signals, learned telemetry mappings, validation results, history, feedback, and generated dashboards that explain where to look next.

> **Public beta / early alpha:** Tacit is demoable and useful for controlled trials, but it is not production-ready software. Expect rough edges, incomplete vendor coverage, and breaking changes. Do not expose it to the public internet or production observability systems without enabling API auth, reviewing generated queries, and applying your own deployment controls.

> *"High latency on the checkout service in the last hour"*
> → an investigation artifact with request rate, error rate, p99 latency, CPU, memory, and pod restarts — published as a Grafana or SignalFx dashboard and recorded with provenance for review.

## Demo

Want the fastest way to see the idea? Run the [checkout incident demo](demo/README.md). It uploads a known-good Grafana dashboard, lets Tacit infer reusable observability signals, approves those signals, and then asks for a fresh investigation dashboard from one plain-English incident prompt.

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

Tacit closes this gap by turning a problem statement into an investigation path with concrete evidence artifacts. Dashboards are the first artifact because they are the fastest way to inspect evidence during a live incident; they are not the final product boundary.

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
 Investigation result → dashboard artifact URLs + history/provenance
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
│ Signal Inference Engine  │  Matches metrics → signal taxonomy (tacit/data/signals.yaml)
│                          │  12 categories, pattern-based with confidence scores
└──────┬───────────────────┘
       ▼
┌──────────────────────────┐
│ Signal Store (SQLite)    │  Persists metric→signal mappings, confidence decay,
│                          │  feedback adjustment, context-aware resolution
└──────┬───────────────────┘
       ▼
 Approved operational language feeds archetype engine + metric ranking
```

The pipeline is **vendor-agnostic**. Each backend (Grafana, SignalFx) implements
the same `DashboardBackend` protocol — `discover_metrics()`, `validate_queries()`,
`publish()`, `ingest_dashboard()`, `close()`. The pipeline iterates over enabled
backends with zero vendor-specific conditionals. Adding a new backend means
implementing one adapter class and registering it in the config.

Inspired by [Uber's QueryGPT](https://www.uber.com/us/en/blog/query-gpt/) multi-agent decomposition pattern.

## Quick Start

### Option A: CLI (Recommended)

```bash
pip install -e .

# Interactive setup — walks you through Grafana URL, API key, LLM provider
tacit init

# Validate everything is connected
tacit doctor

# Run a sample investigation (publishes a dashboard artifact)
tacit test

# Start the server
tacit serve
```

That's it. Three commands from zero to a generated investigation artifact.

### CLI Commands

| Command                               | What it does                                                                                        |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `tacit init`                      | Interactive setup wizard → `~/.tacit/config.yaml` + secrets in `~/.tacit/.env`              |
| `tacit doctor`                    | Validates Grafana + SignalFx connectivity, datasource permissions, LLM key, archetypes, cache state |
| `tacit connect grafana`           | Test & persist a Grafana connection (interactive or `--url` / `--api-key` flags)                    |
| `tacit connect signalfx`          | Test & persist a Splunk SignalFx connection (interactive or `--realm` / `--token` flags)            |
| `tacit test [-p "custom prompt"]` | Runs a full investigation pipeline and opens the resulting dashboard                                |
| `tacit learn dashboard <uid>`     | Ingests an existing dashboard and reports signal quality + before/after learning impact             |
| `tacit serve`                     | Starts the API server (+ Slack if configured)                                                       |
| `tacit history list`              | List recent investigations with status, timings, archetypes                                         |
| `tacit history show <id>`         | Full investigation detail (intent → metrics → queries → result)                                     |
| `tacit history stats`             | Aggregate stats: success rates, avg time, path distribution                                         |

`tacit serve` options: `--host`, `--port`, `--reload` (dev mode), `--no-slack`.

### Option B: Docker

```bash
# Setup your .env file
cp .env.example .env

docker compose up -d

# Go to localhost:8000 for Tacit
```

This starts only Tacit. Point `GRAFANA_URL` and `GRAFANA_API_KEY` at a Grafana instance you control.

For the full local demo stack with Grafana, Prometheus, and fake metrics:

```bash
docker compose -f docker-compose.dev.yml up -d
```

The dev stack is local-only. It intentionally uses `admin/admin` and anonymous Grafana Editor access so demos work without setup friction. Do not expose it outside your machine.

### Single Binary (no Python required)

```bash
# Build
./scripts/build.sh

# Install
sudo cp dist/tacit /usr/local/bin/

# Use
tacit init
tacit serve
```

### Grafana Service Account

1. Open Grafana → Administration → Service Accounts
2. Create a service account with **Editor** role
3. Generate a token — `tacit init` will prompt for it, or set `GRAFANA_API_KEY` in your env

### Try the HTTP API

For local demos, API auth is disabled by default. For public/private-beta deployments, set:

```bash
API_AUTH_ENABLED=true
API_AUTH_KEY=<strong random token>
```

Then pass the key on every request:

```bash
curl -X POST http://localhost:8000/api/v1/chart \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_AUTH_KEY" \
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

The current API response names the generated dashboard fields directly because dashboards are the implemented artifact type. Investigation history and feedback stores preserve the wider context around that artifact.

### API Documentation

| URL                                                               | Format                       |
| ----------------------------------------------------------------- | ---------------------------- |
| [localhost:8000/docs](http://localhost:8000/docs)                 | **Swagger UI** — interactive |
| [localhost:8000/redoc](http://localhost:8000/redoc)               | **ReDoc** — reference docs   |
| [localhost:8000/openapi.json](http://localhost:8000/openapi.json) | OpenAPI 3.1 JSON             |

## Slack Integration

### Setup

1. Create a Slack App at [api.slack.com/apps](https://api.slack.com/apps)
2. Enable **Socket Mode** and generate an App-Level Token
3. Add Bot Token Scopes: `app_mentions:read`, `chat:write`, `commands`
4. Install the app to your workspace
5. (Optional) Create a `/tacit` slash command
6. Add the Slack tokens to your config:

   **Option A: CLI** — `tacit init` will prompt for Slack tokens during interactive setup. They are stored in `~/.tacit/.env`.

   **Option B: Manual** — add to `~/.tacit/.env` or your project `.env`:
   ```
   SLACK_BOT_TOKEN=<slack-bot-token>
   SLACK_APP_TOKEN=<slack-app-token>
   SLACK_SIGNING_SECRET=<slack-signing-secret>
   ```

7. Start the server:
   ```bash
   tacit serve              # Slack enabled by default
   tacit serve --no-slack   # disable Slack integration
   ```
   Or with the local demo stack: `docker compose -f docker-compose.dev.yml restart tacit`

### Usage

Mention the bot in any channel:

```
@Tacit high error rate on the payments API since 2pm
```

Or use the slash command:

```
/tacit disk almost full on the database nodes
```

The bot will reply with a link to the freshly created investigation dashboard artifact.

## Splunk SignalFx (Direct Integration)

Tacit can publish investigation dashboard artifacts **directly to Splunk Observability Cloud** (SignalFx),
in addition to Grafana. When enabled, each pipeline run creates both a Grafana dashboard
and a native SignalFx dashboard with SignalFlow charts.

### Setup

1. Get a SignalFx API access token from **Settings → Access Tokens** in Splunk Observability Cloud
2. Configure via `tacit init` or add to `~/.tacit/.env`:
   ```
   SIGNALFX_API_TOKEN=<your-token>
   ```
   And in `~/.tacit/config.yaml`:
   ```yaml
   signalfx:
     enabled: true
     realm: us1       # us0, us1, us2, eu0, jp0, au0
     dashboard_group: Tacit
   ```
3. Run `tacit doctor` to verify connectivity

When enabled, the API response includes `signalfx_url` and `signalfx_dashboard_id`
alongside the standard Grafana fields.

## Dashboard Learning & Signals

Tacit can **learn from existing dashboards** — ingest a Grafana or SignalFx dashboard
and automatically infer which observability signals (latency, error rate, saturation, etc.)
its metrics represent. Learned mappings feed back into the pipeline to improve metric
ranking and archetype selection.

This is the operational intelligence layer: Tacit should learn an environment's
telemetry language from the dashboards operators already trust. The current implementation
keeps that learning reviewable:

- heuristic inferences start as candidates unless explicitly auto-approved
- approved mappings can participate in signal resolution and archetype compilation
- rejected dashboards record negative candidate examples for future tuning
- the API/UI expose pending, approved, rejected, and ignored dashboard states

### Ingest a Dashboard

Via the **Web UI** — go to the **Learning** tab, enter a dashboard UID, select the backend, and click "Ingest Dashboard".

Via the **CLI**:

```bash
tacit learn dashboard my-service-overview --backend grafana
```

Via the **API**:

```bash
curl -X POST http://localhost:8000/api/v1/learn/dashboard \
  -H "Content-Type: application/json" \
  -d '{"dashboard_uid": "my-service-overview", "backend": "grafana", "auto_approve": true}'
```

Ingestion responses include `signal_quality` and `learning_impact` so reviewers can see:

- which metrics already matched the taxonomy
- which heuristic mappings are candidates pending approval
- which mappings are active after approval/trust
- which candidates were held back and why
- how many metrics would be recognized before vs. after approval

### Teach a Signal Mapping

Manually teach Tacit that a custom metric maps to a signal:

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

The packaged signal taxonomy (`tacit/data/signals.yaml`) defines 12 categories with metric patterns:

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
| `POST` | `/api/v1/learn/dashboard/json` | Ingest an uploaded dashboard JSON export (Grafana now, parser registry ready for other vendors) |
| `GET` | `/api/v1/learn/dashboards` | List ingested dashboards |
| `POST` | `/api/v1/learn/dashboards/{uid}/approve` | Approve a pending ingested dashboard |
| `POST` | `/api/v1/learn/dashboards/{uid}/reject` | Reject a dashboard and preserve rejected candidate signal examples |
| `POST` | `/api/v1/learn/dashboards/{uid}/ignore` | Ignore a dashboard without creating mappings |
| `GET` | `/api/v1/signals` | List all signal types with mapping counts |
| `GET` | `/api/v1/signals/{signal_type}` | Get signal detail with all metric mappings |
| `GET` | `/api/v1/signals/stats` | Signal store statistics |
| `POST` | `/api/v1/signals/teach` | Teach a new metric→signal mapping |

## AWS Bedrock (LLM Provider)

Tacit supports **AWS Bedrock** as an LLM provider for organizations that require
all AI calls to stay within their AWS account.

### Setup

1. Install the optional dependency:
   ```bash
   pip install 'tacit[bedrock]'
   ```
2. Configure via `tacit init` or manually in `~/.tacit/config.yaml`:
   ```yaml
   llm:
     provider: bedrock
     model: anthropic.claude-sonnet-4-20250514-v1:0
     bedrock_region: us-east-1
     # bedrock_role_arn: arn:aws:iam::123456789012:role/TacitBedrock  # optional cross-account
   ```
3. Authentication (resolved in order):
   - **Explicit keys** — set `LLM_AWS_ACCESS_KEY_ID` + `LLM_AWS_SECRET_ACCESS_KEY` in `~/.tacit/.env`
   - **Assume-role** — set `llm.bedrock_role_arn` in config (uses STS)
   - **Default boto3 chain** — env vars, `~/.aws/credentials`, EC2 instance profile, ECS task role
4. Run `tacit doctor` to verify (calls `sts:GetCallerIdentity`)

No API key is needed — Bedrock uses IAM authentication.

## Architecture

| Component | Description |
|---|---|
| **Prompt Sanitizer** | Length caps, control-char removal, prompt injection guardrails |
| **Intent Agent** | LLM classifies domain, services, keywords, signal types, timerange, and multi-label archetypes with confidence scores |
| **Context Enrichment** | Pluggable knowledge base lookup (MCP, A2A, RAG API) — disabled by default |
| **Backend Adapters** | Pluggable `DashboardBackend` protocol (Grafana, SignalFx). Each backend discovers metrics, ingests existing dashboards, validates queries, and publishes artifacts independently. Pipeline iterates over enabled backends — zero vendor-specific branching |
| **Datasource Discovery** | Grafana: auto-discovers all datasources, filters by signal type. SignalFx: keyword search via v2 metadata API |
| **Metric Catalog Fetch** | Per-datasource adapters query metric names + per-metric label names/values |
| **Archetype Engine** | Deterministic dashboard compilation for known investigation patterns. Multi-label: blends panels from multiple archetypes based on confidence (e.g. latency primary + saturation secondary). Skips LLM query generation entirely |
| **Metrics Discovery LLM** | *(freeform fallback)* Selects the most relevant metrics from the full catalog |
| **Post-Validation** | Drops hallucinated datasource UIDs, verifies metrics exist in catalog |
| **Query Builder LLM** | *(freeform fallback)* Generates PromQL/LogQL with accurate label selectors |
| **Query Validation** | Primary backend verifies all panel queries return real data (PromQL via datasource proxy, SignalFlow via metric existence check); drops empty panels, blocks empty dashboards |
| **Artifact Publisher** | Each enabled backend publishes dashboard evidence artifacts independently — Grafana JSON via API, SignalFx charts via v2 REST API, or both |
| **Dashboard Ingestion** | Vendor-agnostic learning: ingests existing Grafana/SignalFx dashboards, extracts metrics & query patterns, infers candidate signal mappings. `DashboardFeatures` dataclass normalizes across backends |
| **Signal Store** | SQLite-backed signal taxonomy: 12 categories, metric→signal mappings with candidate/approved/trusted review states, confidence decay (90-day half-life), feedback adjustment (±30%), context-aware resolution (service, datasource, environment), trust threshold (0.15) |
| **Web UI** | Browser interface at `/` with tabs: Generate, Learning (dashboard ingestion), Signals (taxonomy & teach), Insights (feedback), Archetypes, History |

All agents use structured JSON output with Pydantic validation. The LLM layer is
provider-agnostic — set `LLM_PROVIDER` to `anthropic`, `openai`, `azure`, `bedrock`, or `ollama`.

### Key design decisions

- **Multi-label investigation archetypes** — incidents are inherently overlapping. The intent agent returns multiple archetypes with confidence scores (e.g. `latency_investigation: 0.91, resource_saturation: 0.62`). The archetype engine blends panels from multiple templates, giving broader investigation coverage. Known patterns are compiled deterministically — no LLM needed for query generation, ~75% faster, zero hallucination risk.
- **Dashboards as evidence artifacts** — dashboard URLs are the current user-visible output, but the architecture tracks the larger investigation context: intent, selected archetypes, metrics, queries, validation, history, and feedback.
- **Conservative operational learning** — dashboard ingestion produces explainable candidate signal mappings. Approval activates trusted mappings; rejection/ignore paths prevent noisy dashboards from silently becoming operational truth.
- **Query validation** — before publishing, every panel query is tested against the live datasource. Panels with no matching series are dropped. If all panels are empty, no dashboard is created and the user gets a clear error.
- **Per-metric label discovery** — the Prometheus adapter fetches actual label names and values for each metric via `/api/v1/series`, so the LLM writes queries with correct selectors instead of guessing.
- **Hallucination post-validation** — after the Metrics Discovery LLM runs, any metric referencing a datasource UID not in the real catalog is silently dropped.
- **Layered configuration** — schema-validated YAML config file with env var overrides. Secrets stay in env vars, non-sensitive config in `tacit.yaml`.
- **Concurrency & timeout guards** — pipeline runs are bounded by a semaphore and a configurable timeout to prevent runaway LLM calls.
- **Security hardening** — all three agent system prompts include injection guardrails; API key auth is optional but built-in.

## Public Beta Support Matrix

| Area | Status | Notes |
|---|---|---|
| Grafana publishing | Supported beta | Best demo path; requires a Grafana service-account token |
| Prometheus datasource discovery | Supported beta | Best-covered datasource path |
| Web UI + HTTP API | Supported beta | Enable `API_AUTH_ENABLED=true` outside local demos |
| CLI setup/doctor/test/serve | Supported beta | Good for demos and local trials |
| Investigation history + feedback | Supported beta | Stores prompt, intent, selected signals, queries, validation, timings, URLs, and SRE usefulness ratings |
| SignalFx publishing | Experimental | Works in controlled tests; use with non-production dashboards first |
| CloudWatch/Loki/Elasticsearch/Graphite/Influx discovery | Experimental | Adapters exist, contract coverage is still growing |
| Dashboard learning/signals | Experimental | Useful for operational-language learning; review mappings before relying on them |
| Slack integration | Experimental | Not yet hardened for production workspace controls |
| Docker Compose demo stack | Dev-only | Uses unsafe Grafana defaults by design |

## Supported Backends & Datasources

Tacit publishes dashboard artifacts to multiple backends simultaneously. Each backend discovers
metrics from its own sources, validates queries, ingests existing dashboards where supported, and publishes independently.

| Backend             | Discovery                                             | Query Language                                          | Publishing         |
| ------------------- | ----------------------------------------------------- | ------------------------------------------------------- | ------------------ |
| **Grafana**         | Searches all registered datasources (see table below) | PromQL, LogQL, CW JSON, Lucene, Graphite, InfluxQL/Flux | Grafana JSON API   |
| **Splunk SignalFx** | Keyword search via v2 metadata API                    | SignalFlow                                              | Native v2 REST API |

When both are enabled, a single prompt creates dashboard artifacts in **both** systems.

### Grafana Datasources

When Grafana is enabled, Tacit searches **all** registered datasources, not just Prometheus.
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
tacit/
├── tacit/
│   ├── cli.py               # CLI: init, doctor, connect, test, serve, history
│   ├── main.py              # FastAPI + Slack bot startup
│   ├── config.py            # Layered config: YAML + env vars + Pydantic validation
│   ├── pipeline.py          # Orchestration: prompt → investigation artifact (vendor-agnostic)
│   ├── validation.py        # Pre-publish query validation (PromQL + SignalFlow)
│   ├── backends/            # Backend adapter pattern
│   │   ├── base.py          # DashboardBackend Protocol + PublishResult + DashboardFeatures
│   │   ├── grafana.py       # GrafanaBackend adapter (incl. ingest_dashboard)
│   │   ├── signalfx.py      # SignalFxBackend adapter (incl. ingest_dashboard)
│   │   └── __init__.py      # Registry: get_active_backends()
│   ├── signals.py           # Signal store: taxonomy, metric mappings, confidence (SQLite)
│   ├── dashboard_ingest.py  # Vendor-agnostic dashboard learning pipeline
│   ├── data/                # Packaged runtime YAML resources
│   │   ├── archetypes.yaml  # Investigation templates
│   │   └── signals.yaml     # Signal taxonomy: 12 categories, metric patterns
│   ├── cache.py             # TTL-based metadata & LLM response cache
│   ├── ranking.py           # Pre-ranking: narrows metric catalog before LLM
│   ├── history.py           # Investigation history store (SQLite)
│   ├── signalfx/            # Direct Splunk SignalFx integration
│   │   ├── client.py        # Async SignalFx v2 REST API client
│   │   ├── discovery.py     # Direct metric discovery (reuses adapter's keyword map)
│   │   └── publisher.py     # DashboardSpec → native SignalFx charts + dashboard artifact
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
├── tests/                   # Hermetic unit/contract tests plus live scripts
│   ├── validate.py          # Validation suite (archetype + pipeline accuracy)
│   ├── tacit_validation_prompts.csv  # 100-prompt test dataset
│   ├── test_*.py            # Hermetic pytest suite
│   └── live/                # Opt-in scripts that can mutate real vendor accounts
├── dev/                     # Local dev environment
│   ├── fake_app/           # Fake metrics exporter (checkout, payment, inventory)
│   ├── prometheus/         # Prometheus config
│   └── grafana/            # Grafana provisioning (incl. signal_coverage dashboard)
├── docker-compose.yml       # App-only compose file
├── docker-compose.dev.yml   # Local demo stack with unsafe Grafana dev defaults
├── Dockerfile               # Hardened non-root runtime image
├── pyproject.toml           # Project metadata & deps (uv)
├── tacit.yaml.example   # Reference YAML config (schema-validated)
├── tacit.spec           # PyInstaller spec for single-binary builds
├── scripts/
│   └── build.sh             # Build single binary
└── .env.example             # Reference env vars (secrets go here)
```

## Roadmap

See also:

- [Operational Cognition design doc](docs/operational-cognition.md)
- [Evaluation results](docs/evaluation.md)
- [Architecture Decision Records](docs/adr/README.md)

### Product Principles

- Tacit should optimize investigation quality, not dashboard generation alone.
- Dashboards are evidence artifacts inside an investigation. The system should also preserve intent, selected signals, queries, validation, history, and feedback.
- Tacit should learn an organization's operational language from reviewed dashboards and feedback before attempting heavier stateful incident sessions.
- Enterprise context should come from customer-owned systems through pluggable RAG / A2A / MCP providers. Tacit should own observability outcomes and provenance, not become the system of record for all organizational knowledge.

### Implemented Foundation

- [x] Prompt sanitizer, intent agent, multi-label archetype classification, and structured Pydantic outputs
- [x] Deterministic investigation archetypes with YAML templates, hot reload, and multi-archetype blending
- [x] Backend adapter pattern for Grafana and Splunk Observability Cloud (SignalFx)
- [x] Multi-datasource Grafana discovery for Prometheus/Mimir/Cortex/Thanos, CloudWatch, Loki, Elasticsearch/OpenSearch, Graphite, InfluxDB, and SignalFx
- [x] Per-metric label introspection for Prometheus-family datasources
- [x] Query validation before publish; empty panels are dropped and empty dashboards are blocked
- [x] Dashboard artifact publishing to Grafana and native SignalFx
- [x] Dashboard ingestion through API/Web UI for existing backend dashboards and uploaded Grafana JSON
- [x] Deterministic signal inference from dashboard metrics, panel titles, rows, and query patterns
- [x] SQLite signal store with candidate/approved/trusted review states, confidence decay, feedback adjustment, and context-aware resolution
- [x] Manual signal teaching API and Web UI signal browser
- [x] Investigation history store with prompt, intent, archetypes, metrics, queries, validation, timings, failures, and artifact URLs
- [x] Feedback/provenance store with dimensional SRE ratings and aggregate analysis
- [x] Context enrichment provider interfaces for RAG API, MCP, and A2A
- [x] CLI setup/doctor/connect/test/serve/history flows
- [x] Web UI tabs for Generate, Learning, Signals, Insights, Archetypes, and History
- [x] AWS Bedrock provider alongside Anthropic, OpenAI/Azure OpenAI, and Ollama
- [x] Dev-only Docker Compose stack with fake services, Prometheus, and Grafana
- [x] Public-beta docs, API auth guidance, CI baseline, Docker build, secret scan, and binary release workflow
- [x] ADR set documenting the current architecture decisions and known gaps

### Current Focus

- [ ] **Before/after learning demo** — show a prompt that fails to resolve custom metrics, ingest an existing dashboard, approve mappings, then show the same prompt generating a better investigation artifact.
- [ ] **Dashboard ingestion excellence** — improve extraction quality for messy real dashboards, especially Envoy, JVM, Redis, Kafka, Calico, Kubernetes, and organization-specific product metrics.
- [ ] **Conservative learning UX** — make candidate vs approved/trusted mappings obvious in API/UI output, and keep rejected/ignored candidates out of trusted retrieval paths.
- [ ] **Explainable signal inference** — show why a metric was inferred as latency/errors/saturation, including confidence, source dashboard, panel title, query language, and review state.
- [ ] **Learning retrieval/indexing** — add a scalable metadata search layer for learned dashboard context and service descriptions, with explicit fallback behavior if the local SQLite/FTS capability is unavailable.
- [ ] **Bulk backend learning** — add first-class CLI flows such as `tacit learn dashboard <uid>` and `tacit learn grafana/signalfx`, including pagination, bounded concurrency, retry/backoff, and progress output.
- [ ] **Demo hardening** — ship a repeatable 5-minute demo path, README screenshots/GIFs, and fresh-clone Docker smoke tests.
- [ ] **Evaluation expansion** — add ingestion-quality benchmarks, before/after learned-mapping tests, dashboard usefulness snapshots, and failure examples.
- [ ] **Self-observability** — expose Prometheus metrics for prompt latency, query success rate, hallucination/drop rate, artifact usefulness, token cost, and cache hit rate.
- [ ] **Slack and API hardening** — rate limits, production datasource approval workflows, query cost estimation, and clearer failure messages.
- [ ] **Ephemeral artifact cleanup** — TTL-based cleanup for generated dashboard artifacts in demo and trial environments.

### Next Evidence Types

- [ ] Loki log panels as first-class evidence artifacts
- [ ] Tempo/Jaeger/Zipkin trace evidence once the investigation model can explain how traces discriminate hypotheses
- [ ] Alert payload ingestion so the initial prompt can include labels, annotations, severity, and firing context
- [ ] Lightweight service context config for owners, dependencies, runbooks, and criticality
- [ ] Canonical evidence requirements such as latency, error rate, traffic volume, deployment correlation, dependency latency, queue backlog, restarts, DNS, and certificate failures

### Research Directions

- [ ] **Evidence graph** — make selected signals, missing evidence, validation results, logs, traces, and dashboards part of one inspectable investigation object.
- [ ] **Stateful investigation sessions** — update hypotheses after evidence retrieval instead of producing a single static plan. This should wait until learning quality is strong.
- [ ] **Semantic metric retrieval** — combine lexical search, embeddings, usage frequency, and learned mappings to narrow 50k+ metrics before LLM reasoning.
- [ ] **Canonical Observability IR** — represent `signal_type`, `resource_type`, `aggregation`, `scope`, and query semantics once, then compile to datasource-specific query languages.
- [ ] **Deterministic query compiler** — move more freeform query generation into AST/template compilation with LLMs emitting constrained semantic intent.
- [ ] **Query cost planner** — estimate cardinality, time range cost, datasource load, and query complexity before execution.
- [ ] **Structured trust boundaries** — tag logs, labels, RAG content, Slack text, and datasource metadata with trust levels in prompts and outputs.
- [ ] **RBAC-aware retrieval** — filter metrics, context, history, and generated artifacts by user/team/org/datasource permissions.
- [ ] **Grafana App Plugin** — native "Investigate with Tacit" side panel inside Grafana, calling the Tacit API and opening artifacts in-place.
- [ ] **Incident-management integrations** — PagerDuty, incident.io, Rootly, Slack workflows, and timeline export should consume investigation artifacts after the core learning loop is reliable.

## License

MIT
