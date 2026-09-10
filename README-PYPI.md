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

AWS Bedrock support uses an optional, exactly pinned SDK extra:

```bash
pip install 'tacit-ai[bedrock]'
```

Tacit's protected local SQLite runtime currently supports Linux and macOS.
Windows is not a supported runtime platform yet, even though Python package
installers may accept the universal wheel.

Only the Linux x86_64 frozen binary is published in GitHub releases. macOS
users install this Python package or run from source until a dedicated Developer
ID signing and notarization gate is available. The generic Linux binary supports
glibc-based x86_64 distributions with glibc 2.35 or newer and is built and
smoke-tested on Ubuntu 22.04. Alpine and other musl-based systems should use this
wheel, source, or the container image instead.

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

`tacit demo` creates an ephemeral local API key and hands it to the loopback Web
UI without printing it or putting it in the browser URL. The UI keeps the key
only in browser `sessionStorage`. Direct `tacit serve` users enter their
configured `API_AUTH_KEY` in the same UI control.

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
- `SQLITE_SNAPSHOT_MAX_BYTES`
- `API_AUTH_ENABLED`
- `API_AUTH_KEY`
- `API_ALLOWED_HOSTS`
- `API_CORS_ALLOWED_ORIGINS`
- `API_MAX_REQUEST_BODY_BYTES`
- `API_REQUEST_BODY_MAX_CONCURRENT`
- `API_REQUEST_BODY_MAX_BUFFERED_BYTES`
- `API_REQUEST_BODY_TENANT_MAX_CONCURRENT`
- `API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES`
- `API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR`
- `API_REQUEST_BODY_MEMORY_FLOOR_BYTES`
- `API_REQUEST_BODY_READ_TIMEOUT_SECONDS`
- `KNOWLEDGE_TENANT_ID`
- `KNOWLEDGE_TENANT_API_KEYS`
- `LEARNING_APPROVAL_CLAIM_TTL_SECONDS`

Wildcard tenancy requires API authentication with a distinct key per tenant.
Local server commands bind to loopback by default. A non-loopback bind also
requires an explicitly configured compatible `API_ALLOWED_HOSTS` policy; the
Host allowlist is separate from CORS.

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
Authenticated aggregate admission defaults to 16 concurrent bodies and 512 MiB
of conservative request/decode memory, with a 15-second total body-read
deadline. Wildcard tenancy also enforces fixed per-tenant request and memory
subcaps; pinned tenancy can use the complete global capacity. Configure these
with the `API_REQUEST_BODY_*` settings listed above. Invalid authentication and
tenant headers are rejected before body receive or capacity reservation.
`HISTORY_DB_PATH`, `FEEDBACK_DB_PATH`, and `SIGNALS_DB_PATH` must each reference
a different SQLite file. Tacit rejects shared paths and cross-role database
identities before initializing the stores. Required store schemas and bootstrap
data are prepared before startup completes. `SQLITE_SNAPSHOT_MAX_BYTES` sets
the positive per-physical-database cap for protected read-only admission and
defaults to 1 GiB. Each database shares that cap across its main/WAL copies and
retries; separate history, feedback, and signals files can consume up to three
times the configured value during complete startup.

## More Documentation

The full repository README covers architecture, development workflows, Docker compose stacks, evaluation notes, and project roadmap:

https://github.com/aditki/tacit
