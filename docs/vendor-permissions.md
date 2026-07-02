# Vendor permissions and least-privilege setup

This document describes the vendor credentials Tacit needs for each supported
integration and the minimum practical permissions for each CLI command and API
endpoint.

Tacit's own API authentication is separate from vendor authentication. When
`API_AUTH_ENABLED=true`, callers need Tacit's `X-API-Key` header to call Tacit
endpoints. The vendor credentials below are used by Tacit after a request is
accepted.

## Supported vendor surfaces

| Vendor surface | Config | How Tacit authenticates | Used for |
|---|---|---|---|
| Grafana | `GRAFANA_URL`, `GRAFANA_API_KEY`, `GRAFANA_ORG_ID` | Grafana service account token in `Authorization: Bearer ...` | datasource discovery, query validation, dashboard publish, dashboard learning, alert learning |
| Splunk Observability Cloud / SignalFx | `SIGNALFX_ENABLED`, `SIGNALFX_API_TOKEN`, `SIGNALFX_REALM` | API access token in `X-SF-TOKEN` | metric discovery, query validation, dashboard/chart publish, dashboard learning, detector learning |
| Slack | `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `SLACK_SIGNING_SECRET` | Slack bot token and Socket Mode app token | Slack mention and `/tacit` command entry points |
| LLM providers | `LLM_PROVIDER`, `LLM_API_KEY`, provider-specific settings | Provider API key, AWS credentials, or local Ollama endpoint | intent classification, metric selection, query generation |
| Optional context providers | `CONTEXT_PROVIDER`, `CONTEXT_API_KEY`, provider URL settings | Bearer token to the configured internal service | runbook, service, and incident context enrichment during generation |

## Grafana

Tacit does not perform browser SSO, SAML, OAuth, Duo, or cookie-based login.
Grafana must accept a machine credential on its HTTP API. If an upstream SSO
gateway intercepts `/api/*` before Grafana sees the bearer token, configure an
API bypass or gateway service token path for Tacit.

Official references:

- Grafana service accounts: https://grafana.com/docs/grafana/latest/administration/service-accounts/
- Grafana HTTP API: https://grafana.com/docs/grafana/latest/developers/http_api/
- Grafana RBAC actions: https://grafana.com/docs/grafana/latest/administration/roles-and-permissions/access-control/

### Minimum practical roles

| Use case | Coarse Grafana role | Fine-grained permissions when Grafana RBAC is available |
|---|---|---|
| Connectivity checks | Viewer | read current org, read datasources |
| Datasource discovery and query validation | Viewer | read datasources, query datasource proxy/resources for the allowed datasources |
| Dashboard ingestion and dashboard crawl | Viewer | read/search dashboards, read folders |
| Alert ingestion and alert crawl | Viewer or custom alert-reader | read alert rules or legacy alerts |
| Dashboard publishing | Editor | create/read folders, create/write dashboards in the Tacit folder, read/query datasources |
| Full current Tacit feature set with one token | Editor | custom role combining the read/query permissions above plus folder/dashboard write in the Tacit folder |

Recommended setup:

- Prefer a dedicated Grafana service account, not a human user token.
- Prefer a custom RBAC role scoped to Tacit's folder and the datasources Tacit is allowed to query.
- If only basic roles are available, use `Viewer` for read-only learning jobs and `Editor` for any path that publishes dashboards. Because Tacit currently accepts one `GRAFANA_API_KEY`, a full-featured deployment normally uses an Editor-equivalent service account.
- Do not use Grafana Admin tokens unless you are deliberately testing admin-only behavior.

### Grafana API paths Tacit calls

| Capability | Grafana API paths |
|---|---|
| Connection check | `GET /api/org` |
| Datasource discovery | `GET /api/datasources` |
| Metric discovery and validation | `GET /api/datasources/proxy/uid/{uid}/...`, `POST /api/datasources/uid/{uid}/resources/...` |
| Dashboard publish | `GET /api/folders`, `POST /api/folders`, `POST /api/dashboards/db` |
| Dashboard ingest | `GET /api/dashboards/uid/{uid}` |
| Dashboard crawl | `GET /api/search?type=dash-db` |
| Alert ingest and crawl | `GET /api/v1/provisioning/alert-rules`, `GET /api/v1/provisioning/alert-rules/{uid}`, fallback `GET /api/alerts` and `GET /api/alerts/{uid}` |

### Grafana datasource vendors

Tacit accesses Grafana datasources through Grafana's datasource proxy or plugin
resource APIs. Tacit does not receive the downstream datasource credentials
directly. Configure the datasource credential inside Grafana with read-only
access to the metadata/query APIs below.

| Datasource type | Downstream permission needed by the Grafana datasource credential |
|---|---|
| Prometheus, Mimir, Cortex, Thanos | read Prometheus API metadata, label values, series, and instant query endpoints |
| Loki | read Loki labels and label values |
| CloudWatch | read CloudWatch metric namespaces, metrics, and dimension keys for the configured regions/namespaces |
| Elasticsearch / OpenSearch | read index mappings for the configured index pattern |
| Graphite | read metric-find results |
| InfluxDB | read measurements for the configured database/bucket |
| Splunk Observability Cloud / SignalFx Grafana plugin | read metric, metric time series, dimension, and metric metadata APIs |

## Splunk Observability Cloud / SignalFx

Tacit uses the Splunk Observability Cloud REST API directly. Use a dedicated API
access token or service principal equivalent for the realm configured in
`SIGNALFX_REALM`.

Official references:

- Splunk Observability Cloud API reference: https://dev.splunk.com/observability/reference

### Minimum practical permissions

| Use case | Required capabilities |
|---|---|
| Connectivity checks | read metric metadata |
| Metric discovery | read metrics, metric time series, and dimensions |
| Query validation | read metric metadata |
| Dashboard ingestion and dashboard crawl | read dashboard groups, dashboards, and charts |
| Detector learning | read detectors |
| Dashboard publishing | create dashboard groups when missing, create charts, create dashboards, delete charts for cleanup on failed publish |
| Full current Tacit feature set with one token | read metric metadata/MTS/dimensions, read detectors, read dashboards/charts/dashboard groups, write dashboards/charts/dashboard groups |

If your tenant exposes fine-grained token scopes, grant only the API families
listed above. If it uses role-based access through a user or service principal,
prefer a custom integration role over an administrator role.

### SignalFx API paths Tacit calls

| Capability | SignalFx API paths |
|---|---|
| Connection check and metric discovery | `GET /v2/metric`, `GET /v2/metric/{metric}`, `GET /v2/metrictimeseries`, `GET /v2/dimension` |
| Dashboard publish | `GET /v2/dashboardgroup`, `POST /v2/dashboardgroup`, `POST /v2/chart`, `POST /v2/dashboard`, `DELETE /v2/chart/{id}` on failed publish cleanup |
| Dashboard ingest and crawl | `GET /v2/dashboardgroup`, `GET /v2/dashboard/{id}`, `GET /v2/chart/{id}` |
| Detector ingest and crawl | `GET /v2/detector`, `GET /v2/detector/{id}` |

## Slack

Slack is an entry point into the same generation pipeline. A Slack-triggered
request still needs the Grafana, SignalFx, and LLM permissions for whatever
backends are enabled.

Official references:

- Slack app scopes: https://api.slack.com/scopes
- Slack Socket Mode: https://api.slack.com/apis/socket-mode

Minimum Slack app configuration:

| Token | Required scopes/features | Why |
|---|---|---|
| Bot token, `SLACK_BOT_TOKEN` | `app_mentions:read`, `chat:write` | receive mentions and reply in threads |
| Bot token, `SLACK_BOT_TOKEN` | `commands` | only required when enabling the `/tacit` slash command |
| App-level token, `SLACK_APP_TOKEN` | `connections:write` | connect over Socket Mode |
| Signing secret, `SLACK_SIGNING_SECRET` | app signing secret | request verification for Slack Bolt app setup |

## LLM providers

Tacit calls exactly one configured LLM provider. Give the credential access only
to the configured model or deployment.

| Provider | Config | Minimum permission |
|---|---|---|
| Anthropic | `LLM_PROVIDER=anthropic`, `LLM_API_KEY`, `LLM_MODEL` | API key allowed to call the configured model via Messages API |
| OpenAI | `LLM_PROVIDER=openai`, `LLM_API_KEY`, `LLM_MODEL`, optional `LLM_API_BASE` | project/API key allowed to call the configured chat model |
| Azure OpenAI | `LLM_PROVIDER=azure`, `LLM_API_KEY`, `LLM_API_BASE`, `LLM_AZURE_DEPLOYMENT`, `LLM_AZURE_API_VERSION` | Azure OpenAI resource key with inference access to the configured deployment |
| AWS Bedrock | `LLM_PROVIDER=bedrock`, region/model settings, optional role ARN | `bedrock:Converse` for the configured model or inference profile; `bedrock:ListFoundationModels` if relying on model-name auto-resolution; `sts:AssumeRole` when `LLM_BEDROCK_ROLE_ARN` is set |
| Ollama | `LLM_PROVIDER=ollama`, optional `LLM_API_BASE` | network access to the local or private Ollama `/api/chat` endpoint |

For `tacit doctor` with Bedrock, the current check also calls AWS STS
`GetCallerIdentity`. Grant `sts:GetCallerIdentity` to the checking principal if
you want the doctor check to pass.

## Optional context providers

Context providers enrich prompts with internal knowledge. They are disabled by
default.

| Provider | Config | Minimum permission |
|---|---|---|
| MCP | `CONTEXT_PROVIDER=mcp`, `CONTEXT_MCP_SERVER_URL`, `CONTEXT_MCP_TOOL_NAME`, optional `CONTEXT_API_KEY` | call the configured MCP `tools/call` search tool |
| A2A | `CONTEXT_PROVIDER=a2a`, `CONTEXT_A2A_AGENT_URL`, optional `CONTEXT_API_KEY` | send `tasks/send` requests to the configured agent |
| RAG API | `CONTEXT_PROVIDER=rag_api`, `CONTEXT_RAG_API_URL`, optional `CONTEXT_API_KEY` | `POST /search` with the configured retrieval filters |

## CLI command permission matrix

| CLI command | Vendor permissions required |
|---|---|
| `tacit init` | none during config writing; credentials entered here are used later |
| `tacit doctor` | Grafana connection check, Grafana datasource read, selected LLM provider check, SignalFx metric read if SignalFx is enabled |
| `tacit connect grafana` | Grafana org read and datasource read |
| `tacit connect signalfx` | SignalFx metric read |
| `tacit test` | selected LLM provider, enabled backend discovery/validation, and dashboard publish permissions |
| `tacit serve` | no vendor call at startup except Slack startup when configured; runtime endpoint calls need the permissions listed in the API matrix |
| `tacit serve --no-slack` | no Slack permissions; runtime endpoint calls still need backend and LLM permissions |
| `tacit learn dashboard <uid> --backend grafana` | Grafana dashboard read |
| `tacit learn dashboard <uid> --backend signalfx` | SignalFx dashboard read and chart read |
| `tacit learn grafana` | Grafana dashboard search and dashboard read |
| `tacit learn signalfx` | SignalFx dashboard group read, dashboard read, chart read |
| `tacit learn alerts --from grafana` | Grafana alert rule read or legacy alert read |
| `tacit learn alerts --from signalfx` | SignalFx detector read |
| `tacit learn runbooks`, `tacit learn incidents` | local file read only; no vendor credential |
| `tacit learn approve`, `reject`, `ignore`, `search`, `service` | local Tacit store only; no vendor credential |
| `tacit history list`, `show`, `stats` | local Tacit history store only; no vendor credential |

## API endpoint permission matrix

All endpoints except `/healthz` and `/` require Tacit's `X-API-Key` when
`API_AUTH_ENABLED=true`.

| Endpoint | Vendor permissions required |
|---|---|
| `GET /healthz`, `GET /` | none |
| `POST /api/v1/chart` | selected LLM provider, optional context provider, enabled backend discovery/validation, enabled backend dashboard publish |
| `POST /api/v1/learn/dashboard` with `backend=grafana` | Grafana dashboard read |
| `POST /api/v1/learn/dashboard` with `backend=signalfx` | SignalFx dashboard read and chart read |
| `POST /api/v1/learn/alerts` with `backend=grafana` | Grafana alert rule read or legacy alert read |
| `POST /api/v1/learn/alerts` with `backend=signalfx` | SignalFx detector read |
| `POST /api/v1/learn/dashboard/json` | no vendor credential; uploaded JSON is parsed locally |
| `POST /api/v1/learn/{backend_name}` | dashboard crawl/read permissions for the selected backend |
| `POST /api/v1/learn/backends/{backend_name}/alerts` | alert/detector crawl/read permissions for the selected backend |
| `GET /api/v1/learn/dashboards`, `GET /api/v1/learn/alerts` | local Tacit store only |
| `GET /api/v1/learn/runbooks`, `GET /api/v1/learn/incidents` | local Tacit store only |
| `GET /api/v1/learning/search`, `GET /api/v1/services/{service_name}` | local Tacit learning index only |
| `POST /api/v1/learn/dashboards/{dashboard_uid}/approve`, `reject`, `ignore` | local Tacit store only |
| `GET /api/v1/signals*`, `POST /api/v1/signals/teach` | local Tacit signal store only |
| `GET /api/v1/archetypes`, `POST /api/v1/archetypes/reload` | local archetype files only |
| `POST /api/v1/feedback`, `GET /api/v1/feedback*` | local Tacit feedback store only |
| `GET /api/v1/investigations*` | local Tacit history store only |

## Common deployment patterns

### Read-only learning job

Use this when Tacit should learn from existing operational artifacts but must
not publish anything.

- Grafana: Viewer/custom read role for dashboards, alerts, datasources, and datasource proxy queries.
- SignalFx: read metric metadata, dashboard/chart/dashboard group read, detector read.
- LLM: only required if you also run dashboard generation.

### Full dashboard-generation deployment

Use this for Slack, Web UI, CLI, or API generation that publishes dashboards.

- Grafana: Editor-equivalent for the Tacit folder plus datasource read/query access.
- SignalFx: read metrics and create dashboard groups/charts/dashboards.
- LLM: configured model invocation permission.
- Slack: only if Slack entry points are enabled.

### Enterprise SSO in front of vendors

Human SSO can protect the browser UI, but Tacit needs machine-to-machine API
access. If service-account/API tokens are disabled and no gateway service-token
path exists, Tacit can still parse uploaded dashboard JSON but cannot discover,
validate, crawl, or publish against that vendor directly.
