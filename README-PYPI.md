# Tacit CLI

Tacit helps operators turn incident prompts and existing operational context into evidence-grounded investigations from the command line.

The `tacit` command can:

- configure local Grafana, SignalFx, and LLM settings
- run environment health checks
- run an investigation workflow from a natural-language prompt
- start the local API and browser UI
- run the demo stack
- ingest existing dashboards, alerts, runbooks, and incidents as reusable operational context
- export an anonymized assessment bundle for sharing

## Install

```bash
uvx --from tacit-ai tacit --help
```

Or install into your environment:

```bash
pip install tacit-ai
tacit --help
```

## Quick Start

Configure Tacit:

```bash
tacit init
```

Check your local environment:

```bash
tacit doctor
```

Run a test investigation:

```bash
tacit test --prompt "checkout-service p95 latency is high"
```

Run the local API and UI:

```bash
tacit serve --host 127.0.0.1 --port 8000 --no-slack
```

Then open `http://127.0.0.1:8000`.

## Common Commands

```bash
tacit init
tacit doctor
tacit test --prompt "5xx errors on checkout-service"
tacit serve --no-slack
tacit demo
tacit learn dashboard <dashboard_uid>
tacit learn approve <dashboard_uid>
tacit history list
tacit export-report --anonymous --validate
```

## Configuration

Tacit reads settings from environment variables, `.env`, and optional YAML config.
For local development, start from the repository's `.env.example` or `tacit.yaml.example`.

Useful settings include:

- `GRAFANA_URL`
- `GRAFANA_API_KEY`
- `LLM_PROVIDER`
- `LLM_MODEL`
- `LLM_API_KEY`
- `SIGNALFX_API_TOKEN`
- `HISTORY_DB_PATH`
- `FEEDBACK_DB_PATH`
- `SIGNALS_DB_PATH`
- `API_AUTH_ENABLED`
- `API_AUTH_KEY`
- `API_CORS_ALLOWED_ORIGINS`
- `API_MAX_REQUEST_BODY_BYTES`
- `KNOWLEDGE_TENANT_ID`
- `KNOWLEDGE_TENANT_API_KEYS`
- `LEARNING_APPROVAL_CLAIM_TTL_SECONDS`

Wildcard tenancy requires API authentication with a distinct key per tenant.
All deployments deny cross-origin browser requests unless
`API_CORS_ALLOWED_ORIGINS` contains a comma-separated list of exact HTTP(S)
origins. Authenticated deployments reject wildcard CORS. Same-origin use of the
built-in UI does not need an allowlist entry. The UI stores a manually entered
API key only in browser `sessionStorage`, so it is scoped to that browser
session rather than Tacit's persistent application storage. An unauthenticated
local runtime may explicitly set `API_CORS_ALLOWED_ORIGINS=*`, but that insecure
opt-in must not be used on shared or network-accessible deployments.
HTTP request bodies are limited to 2 MiB by default before JSON parsing;
`API_MAX_REQUEST_BODY_BYTES` accepts values from 1 KiB through 64 MiB.
`HISTORY_DB_PATH`, `FEEDBACK_DB_PATH`, and `SIGNALS_DB_PATH` must each reference
a different SQLite file. Tacit rejects shared paths and cross-role database
identities before initializing the stores.

## More Documentation

The full repository README covers architecture, development workflows, Docker compose stacks, evaluation notes, and project roadmap:

https://github.com/aditki/tacit
