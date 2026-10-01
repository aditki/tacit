# Tacit

[![CI](https://github.com/aditki/tacit/actions/workflows/ci.yml/badge.svg)](https://github.com/aditki/tacit/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)

**Incident prompt to evidence-backed investigation artifact.**

![Tacit demo — incident prompt to validated Grafana dashboard](docs/media/demo.gif)

Tacit helps on-call engineers answer the hardest question in an incident:

> Where should I look next?

Give Tacit a plain-English symptom and it discovers relevant telemetry, validates
queries against live data, and publishes a focused investigation dashboard to
Grafana, Splunk Observability Cloud, or both.

Example:

> "High latency on checkout in the last hour"

Tacit builds an investigation artifact with request rate, error rate, p99
latency, saturation, restarts, selected signals, validation results, provenance,
and links to the generated dashboards.

## Status

Tacit is public beta / early alpha software. It is useful for demos, controlled
trials, and learning how LLM-assisted observability should behave, but it is not
production-ready.

Use API auth, least-privilege vendor credentials, non-production dashboards, and
human review before connecting Tacit to important systems.

## Why Tacit Exists

Most observability tools are good at surfacing signals. They can summarize,
correlate, query, and suggest.

During a real incident, the operator still has to decide:

- which signals matter first
- which dashboard or datasource to open
- which metrics are trustworthy
- which hypothesis to test next
- whether generated queries actually return evidence

Tacit is built around that missing navigation layer. It turns operational
language into a concrete investigation path, then records what it selected, what
it validated, and what it published.

Dashboards are the first artifact because they are the fastest way to inspect
evidence during a live incident. The larger product object is the investigation:
intent, signals, queries, validation, learning, feedback, history, and generated
artifacts.

## What Tacit Does

- Accepts prompts from the CLI, Web UI, HTTP API, or Slack.
- Classifies incident intent with an LLM.
- Uses deterministic archetypes for known investigation patterns.
- Discovers metrics across Grafana datasources and direct SignalFx.
- Generates or compiles datasource-specific queries.
- Validates queries before publishing so empty dashboards are blocked.
- Publishes dashboard artifacts to Grafana and SignalFx.
- Learns an environment's telemetry language from trusted dashboards.
- Keeps candidate signal mappings reviewable before they become trusted.
- Stores investigation history, feedback, validation warnings, and provenance.

## Quick Start

### One command, zero API keys

```bash
git clone https://github.com/aditki/tacit && cd tacit
uv sync
uv run tacit demo
```

`tacit demo` boots the local Grafana + Prometheus + fake checkout-metrics
stack, teaches Tacit from a known-good incident dashboard, generates a fresh
investigation dashboard from a plain-English incident prompt, and opens both
the authenticated Tacit UI and dashboard in your browser.

No LLM API key is required: in zero-key mode Tacit classifies the incident
deterministically and compiles the dashboard through the archetype engine.
Configure a key later to unlock LLM intent classification and the freeform
query path. Tear the stack down with `tacit demo --down`.

### Your own environment

```bash
uv sync
uv run tacit init
uv run tacit doctor    # readiness checklist: Grafana, LLM, learned knowledge
uv run tacit test
```

Start the API and Web UI:

```bash
uv run tacit serve
```

No uv? `pip install -e .` works too.
Only the Linux x86_64 frozen binary is published with each
[release](https://github.com/aditki/tacit/releases).
That generic binary supports glibc-based Linux distributions with glibc 2.35
or newer; it is built and smoke-tested on Ubuntu 22.04, the minimum supported
baseline. Alpine and other musl-based systems should use the wheel, source, or
container image instead.
macOS users install with `pip` or from source until a dedicated Developer ID
signing and notarization release gate lands. Windows binaries are not published
while protected runtime storage requires POSIX filesystem controls.

Open:

- Web UI: [localhost:8000](http://localhost:8000) — watch pipeline stages stream live as dashboards are built
- Swagger: [localhost:8000/docs](http://localhost:8000/docs)
- ReDoc: [localhost:8000/redoc](http://localhost:8000/redoc)

The [checkout incident demo](demo/README.md) documents the same flow
`tacit demo` automates, step by step.

## Assess Your Operational Knowledge

`tacit assess` is a deterministic, zero-key scorecard of what Tacit has
ingested, extracted, resolved, and failed to resolve:

```text
Operational Knowledge Assessment

  Services known:            64
  Dashboards ingested:       187
  Alerts ingested:           91
  Runbooks learned:          12
  Knowledge coverage:        74%
  Missing ownership:         11
  Duplicate dashboard groups: 6
  Alerts without owners:     23
  Investigation Readiness:   Medium (55/100)
```

Every number comes from local SQLite stores — no LLM calls, no vendor API
calls. Add `--llm` for an optional narrative (what this means, what to fix
first), `--json` for machine-readable output, and share results safely with
`tacit export-report --anonymous`.

## Local Demo Stack

Run Tacit with the local Grafana, Prometheus, and fake checkout metrics stack:

```bash
export API_AUTH_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose -f docker-compose.dev.yml up -d
```

The demo stack is local-only. It intentionally uses unsafe Grafana defaults so
the demo works without setup friction. Tacit itself is authenticated, its host
port is published on loopback, and `tacit demo` creates an ephemeral API key
automatically. The command hands that key to the loopback UI without printing it
or placing it in the browser URL; the UI keeps it only for that browser session.
Direct Compose users must provide `API_AUTH_KEY` and enter it in the Web UI.
Do not expose the stack outside your machine.

## Connect Grafana

Tacit talks to Grafana through the HTTP API with a service account token.
It does not perform browser SSO, SAML, OAuth, Duo, or cookie login.

Recommended setup:

1. Open Grafana.
2. Go to Administration, then Service Accounts.
3. Create a service account for Tacit.
4. Generate a token.
5. Run `tacit init` or set `GRAFANA_API_KEY`.

For enterprise permissions, SSO caveats, and per-command credential needs, see
[docs/vendor-permissions.md](docs/vendor-permissions.md).

## Learn From Existing Dashboards

Tacit can ingest existing Grafana or SignalFx dashboards and infer what their
metrics mean in operational terms.

Single dashboard:

```bash
tacit learn dashboard my-service-overview
```

Bulk learning:

```bash
tacit learn grafana
tacit learn signalfx
```

Bulk learning paginates backend dashboard listings and ingests dashboards with
bounded concurrency. Inferred mappings start as reviewable candidates. Controlled
flows can request automated approval for eligible **signal mappings only**; this
does not approve, register, or retrieve a generated archetype.

The learning loop is intentionally conservative:

- trusted dashboards propose Tacit's local telemetry vocabulary
- candidate mappings remain visible for review
- governed, eligible mappings improve future metric selection
- rejected dashboards preserve negative examples
- ignored dashboards do not create mappings

Generated archetypes are not part of this normal learning loop. Generation is
disabled by default. When explicitly enabled, output remains a quarantined
experimental artifact and cannot modify the curated archetype registry or normal
investigation retrieval. There is no auto-approval path for generated archetypes.
The governing authority and containment boundary are recorded in
[ADR-019](docs/adr/019-governed-knowledge-authority.md). Generated archetypes must
prove unique value through side-effect-free shadow evaluation before Tacit builds
any promotion lifecycle; see [ADR-020](docs/adr/020-generated-archetypes-shadow-before-lifecycle.md)
and the [evaluation roadmap](docs/generated-archetype-evaluation-roadmap.md).

### Learn PagerDuty Incident History

Incident history can also be learned from PagerDuty (read-only, metadata only —
no notes or causal narratives are ingested):

```bash
tacit learn pagerduty --since 2026-01-01T00:00:00Z --dry-run
```

`--since` is required: the PagerDuty list API otherwise returns only its
default recent window, not full history. Set `pagerduty_api_token` via env or
`.env`. See
[docs/research/opensre-integration-review.md](docs/research/opensre-integration-review.md)
for design notes.

## Supported Backends

Grafana:

- Publishes Grafana dashboard JSON through the Grafana API.
- Discovers datasources registered in Grafana.
- Supports Prometheus, Mimir, Cortex, Thanos, CloudWatch, Loki,
  Elasticsearch, OpenSearch, Graphite, InfluxDB, and the SignalFx Grafana plugin.

Splunk Observability Cloud, also known as SignalFx:

- Publishes native SignalFx dashboards and charts.
- Discovers metrics through the SignalFx v2 metadata API.
- Uses SignalFlow for native queries.

When both backends are enabled, one prompt can publish artifacts to both systems.

## HTTP API

Generate an investigation dashboard:

```bash
curl -X POST http://localhost:8000/api/v1/chart \
  -H "Content-Type: application/json" \
  -d '{"prompt": "high CPU on checkout in the last 30 minutes"}'
```

Typical response:

```json
{
  "dashboard_url": "http://localhost:3000/d/abc123/...",
  "dashboard_uid": "abc123",
  "panel_count": 6,
  "summary": "Created dashboard with 6 validated panels."
}
```

Watch the pipeline work in real time with the SSE streaming variant — it emits
a `stage` event per pipeline step (intent, discovery, compilation, validation,
ranking, publish) and a final `result` event:

```bash
curl -N -X POST http://localhost:8000/api/v1/chart/stream \
  -H "Content-Type: application/json" \
  -d '{"prompt": "high CPU on checkout in the last 30 minutes"}'
```

For non-local deployments, enable Tacit's API key auth:

```bash
API_AUTH_ENABLED=true
API_AUTH_KEY=<strong-token>
API_ALLOWED_HOSTS=tacit.example.com
tacit serve --host 0.0.0.0
```

`tacit serve` and `python -m tacit.main` bind to loopback by default. A
non-loopback `--host` is rejected unless authentication is enabled and
`API_ALLOWED_HOSTS` was explicitly configured with a compatible exact host or
subdomain pattern. The Docker image uses that same CLI boundary and fails safe
when its required runtime key or Host policy is missing.
Its readiness probe connects over loopback while sending a Host value selected
from `API_ALLOWED_HOSTS`, so an external-only allowlist does not require a
loopback exception.

All deployments deny cross-origin browser requests by default. If a separate
web origin must call Tacit, configure exact origins. Authenticated deployments
never permit `*`:

```bash
API_CORS_ALLOWED_ORIGINS=https://console.example,https://ops.example
```

The built-in UI needs no CORS entry when it is served by the Tacit API itself.
Its API-key field is session-scoped: the browser keeps the value in
`sessionStorage`, clears it when that browser session closes, and does not write
it to Tacit's persistent application storage.

Tacit rejects oversized HTTP bodies before JSON parsing. The default limit is
2 MiB and can be changed with `API_MAX_REQUEST_BODY_BYTES` (1 KiB to 64 MiB).
`API_REQUEST_BODY_MAX_CONCURRENT` (16 by default) and
`API_REQUEST_BODY_MAX_BUFFERED_BYTES` (512 MiB by default) bound aggregate
authenticated request and decode memory. Wildcard runtimes additionally use
`API_REQUEST_BODY_TENANT_MAX_CONCURRENT` and
`API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES`; pinned runtimes retain the full
global capacity. `API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR` and
`API_REQUEST_BODY_MEMORY_FLOOR_BYTES` control conservative memory accounting,
while `API_REQUEST_BODY_READ_TIMEOUT_SECONDS` (15 seconds by default) bounds
the complete body read. Invalid credentials and tenant headers are rejected
before body bytes are received or capacity is reserved.

Tacit prepares history, feedback, signals/bootstrap, and Operational Knowledge
SQLite schemas before the API accepts traffic. Protected read-only admission
uses a 1 GiB cap per physical database by default; within one database the cap
is shared across its main/WAL copies and retries. Set
`SQLITE_SNAPSHOT_MAX_BYTES` to a larger positive value for an admitted database
that has grown beyond it. A runtime with separate history, feedback, and signals
files can therefore copy at most three times this value during complete startup.

For an unauthenticated local deployment only, `API_CORS_ALLOWED_ORIGINS=*` is
an explicit insecure opt-in that lets any website read and invoke the API from
the browser. Do not use it on a shared or network-accessible deployment.

For a wildcard multi-tenant runtime, bind each API key to one tenant and send
that tenant in `X-Tacit-Tenant`:

```bash
API_AUTH_ENABLED=true
KNOWLEDGE_TENANT_ID=*
KNOWLEDGE_TENANT_API_KEYS='{"tenant-a":"strong-key-a","tenant-b":"strong-key-b"}'
```

Chart request bodies cannot select a different tenant from the authenticated
`X-Tacit-Tenant` boundary.

## Slack

Tacit can run as a Slack bot through Socket Mode. Mention it in a channel or use
a slash command, and it will reply with an investigation artifact link.

Minimum Slack setup:

- bot token with `app_mentions:read` and `chat:write`
- app-level token with `connections:write`
- `commands` scope if using a slash command

Store Slack tokens through `tacit init` or environment variables, then run
`tacit serve`.

## LLM Providers

Tacit supports:

- Anthropic
- OpenAI
- Azure OpenAI
- AWS Bedrock
- Ollama

AWS Bedrock uses IAM instead of an API key. See the configuration examples in
[tacit.yaml.example](tacit.yaml.example).

Install Tacit's pinned Bedrock SDK support with either:

```bash
pip install 'tacit-ai[bedrock]'
# Source checkout:
uv sync --extra bedrock
```

Bedrock currently runs through a temporary, operation-scoped Boto3 compatibility
bridge. It accepts:

- explicit access keys and optional session tokens from Tacit settings or the
  corresponding AWS environment variables;
- static AWS credential/config profiles;
- one-level assume-role profiles backed by a static source profile;
- a web-identity token that is read and frozen independently for each admitted
  operation; and
- one configured role assumption over an accepted base credential source.

Credential-source files are opened without following symlinks and must be
regular files. Symlink-backed Kubernetes or IRSA projected-token paths may
therefore fail closed under this temporary bridge. Tacit does not weaken that
containment boundary by following them.

Credential-process, SSO/login, ECS/container-metadata, and EC2
instance-metadata providers fail closed because their executable or remote
authority is not yet modeled in Tacit's runtime ownership contract. An unknown
`llm_model` value also fails unless `LLM_BEDROCK_MODEL_ID` explicitly names the
Bedrock model; Tacit does not silently route it to a default Claude model.

Each call creates and disposes its Boto3 session, Bedrock client, and any needed
STS clients. This avoids sharing blocking SDK resources across async runtimes,
but it also means extra setup work, no connection pooling, and lower throughput.
While this bridge is active, `PIPELINE_MAX_CONCURRENT` is capped at 32 per
runtime identity, so one runtime cannot create an unbounded population of
blocking SDK workers. A process-wide worker cardinality owner remains a
separate follow-up in
[`docs/sync-async-boundary-roadmap.md`](docs/sync-async-boundary-roadmap.md).
Botocore connect and read timeouts are best effort: Tacit rejects a result that
arrives after the operation deadline, but keeps admission charged until the
worker-owned SDK call and cleanup return. The cap bounds worker population, not
the lifetime of an uninterruptible call. It is a containment bridge rather than
the target high-throughput design.

The surrounding provider lifecycle is runtime-owned: independently constructed
API, Slack, CLI, and direct dependency bundles for one runtime share a single
provider manager and service loop. Explicit leases are reference counted, calls
drain before final close, application shutdown drains the manager, and cleanup
failure revokes the generation rather than leaving live capacity behind. This
does not make noncooperative third-party code force-cancellable or extend the
same guarantee to stores and backends.

The next changeset replaces the Bedrock bridge with `aiobotocore`, attaching its
session/client lifecycle to that runtime graph before or while validating
non-streaming `converse`, then adding `converse_stream`; its
acceptance criteria and the separately deferred generic-factory, SQLite,
filesystem/YAML/CPU, cross-loop, and observability/guardrail debt are recorded in
[ADR-023](docs/adr/023-contain-blocking-bedrock-before-native-async.md).

## How It Works

```text
Prompt
  |
  v
Intent classification
  |
  v
Optional context enrichment
  |
  v
Backend metric discovery
  |
  v
Archetype engine or freeform query planning
  |
  v
Query validation against live data
  |
  v
Dashboard artifact publishing
  |
  v
History, provenance, learning, and feedback
```

Known incident shapes use deterministic archetypes, which reduces hallucination
risk and avoids unnecessary query-generation calls. Freeform paths still use the
LLM, but selected metrics and generated queries are validated before publishing.

## Why Not Just Ask an LLM for PromQL?

A chat model will happily write a plausible query for a metric that does not
exist in your environment. Tacit is built around three things a raw LLM call
cannot do:

1. **Live validation.** Every generated query is executed against your real
   datasources before publishing. Panels with no data are dropped, and
   fully-empty dashboards are blocked, so the artifact you open during an
   incident contains evidence, not guesses.
2. **Your telemetry vocabulary.** Tacit learns signal mappings from the
   dashboards, alerts, and runbooks your team already trusts, so "checkout
   latency" resolves to *your* metric names — reviewable and approvable before
   they influence anything.
3. **Deterministic paths.** Known incident shapes compile through YAML
   archetype templates with zero LLM involvement — faster, reproducible, and
   immune to hallucination. In zero-key mode this path runs entirely without
   an LLM.

## Benchmark Results

Tacit ships a public 100-prompt validation suite
([docs/evaluation.md](docs/evaluation.md)) covering latency, error, saturation,
database, Kubernetes, and deployment incidents across three simulated services:

| Metric | Result |
|---|---:|
| Archetype accuracy (top-1 / any match) | 90% / 95% |
| Average critical metric recall | 85.2% |
| Average signal-to-noise ratio | 71.3% |
| Pipeline success | 98/100 |
| Average panels per dashboard | 6.2 |
| Average pipeline latency | 3.4s |

These are public-beta benchmarks on a synthetic dataset — read them as a
regression gate, not a production guarantee.

## Current Fit

Tacit is a good fit for:

- SRE and platform teams exploring LLM-assisted incident navigation
- demos and private trials with non-production observability systems
- teams with trusted Grafana or SignalFx dashboards that can seed learning
- experiments around operational language, signal taxonomies, and evidence
  validation

Tacit is not yet a good fit for:

- public internet exposure
- unsupervised production incident response
- environments where no machine credential can reach vendor APIs
- replacing existing incident management, runbooks, or observability systems

## Adoption Path

1. Run `tacit demo` (zero API keys needed).
2. Connect a non-production Grafana or SignalFx account.
3. Ingest a few dashboards your team already trusts.
4. Run `tacit assess` to see coverage, gaps, and investigation readiness.
5. Review and approve the inferred signal mappings.
6. Generate dashboards from real incident-style prompts.
7. Add Slack or API integration only after the generated evidence is useful.

## Share Feedback Safely

Adopters can export a local assessment bundle from the CLI. The shareable
anonymous mode preserves aggregate structure, counts, ranking diagnostics,
feedback summaries, validation warnings, and failure categories while excluding
raw dashboards, raw runbooks, raw incidents, raw alert bodies, logs, telemetry,
secrets, and anonymization mappings.

Anonymous bundles include an `evaluation_summary.json` when benchmark results
are available. This file preserves benchmark contracts, denominators,
anonymized per-case outcomes, and safety metrics without exporting raw prompts,
artifact text, or operational identifiers.

Use this when you want to send maintainers useful adoption feedback without
shipping operational details.

## Project Layout

Core code lives in [tacit](tacit):

- [tacit/cli.py](tacit/cli.py): CLI commands
- [tacit/api](tacit/api): FastAPI routes
- [tacit/pipeline](tacit/pipeline): investigation pipeline
- [tacit/backends](tacit/backends): Grafana and SignalFx backend adapters
- [tacit/grafana](tacit/grafana): Grafana client, dashboard publisher, datasource adapters
- [tacit/signalfx](tacit/signalfx): direct SignalFx client, discovery, publisher
- [tacit/dashboard_ingest](tacit/dashboard_ingest): dashboard learning
- [tacit/signals](tacit/signals): signal taxonomy and mapping store
- [tacit/agents](tacit/agents): LLM provider and agent logic

Useful docs:

- [demo/README.md](demo/README.md): checkout incident demo
- [docs/vendor-permissions.md](docs/vendor-permissions.md): least-privilege vendor permissions
- [docs/operational-cognition.md](docs/operational-cognition.md): product thesis
- [docs/evaluation.md](docs/evaluation.md): evaluation notes
- [docs/adr/README.md](docs/adr/README.md): architecture decision records
- [SECURITY.md](SECURITY.md): security policy and safe-usage expectations

## Roadmap

Near-term focus:

- improve dashboard ingestion quality for messy real-world dashboards
- make candidate, approved, trusted, rejected, and ignored mappings clearer
- add richer progress and retry behavior for bulk learning
- strengthen Slack and API hardening
- expose Tacit's own operational metrics
- add better demo assets and screenshots
- expand evaluation for usefulness, not just technical success

Longer-term research:

- evidence graphs that include metrics, logs, traces, alerts, and missing evidence
- stateful investigation sessions
- semantic metric retrieval for very large telemetry estates
- deterministic query compilation from a canonical observability IR
- query cost planning before execution
- RBAC-aware retrieval and artifact publishing
- native Grafana app plugin

## License

MIT
