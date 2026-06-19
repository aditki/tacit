# DashForge Demo: Checkout Incident in a Box

This demo shows the part of DashForge that is easiest to understand in a short repo video or LinkedIn post:

> Upload a known-good incident dashboard, let DashForge learn the operational signals, then ask for a fresh investigation dashboard from a plain-English incident prompt.

It is built around a checkout-service incident because it tells a recognizable on-call story: latency, 5xxs, request pileups, downstream DB latency, CPU, and memory all need to appear together without turning into a noisy wall of charts.

## Run It

Start the local dev stack:

```bash
docker compose -f docker-compose.dev.yml up -d --build
```

Open:

- DashForge UI: <http://localhost:8000>
- Grafana: <http://localhost:3000>
- Prometheus: <http://localhost:9090>

Then drive the learning + generation flow:

```bash
demo/run_checkout_incident_demo.sh
```

## Real Telemetry Milestone

The original checkout demo uses a compact local metric fixture so it is fast and deterministic. The first dataset milestone adds a second path backed by the official ClickStack OpenTelemetry sample:

```bash
demo/run_clickstack_metrics_demo.sh
```

This command downloads the approximately 7 MB sample into the ignored `data/demo/clickstack` directory, replays `metrics.json` through the local OpenTelemetry Collector, and asks DashForge to investigate the sample's checkout/payment cache incident. Metrics are stored in VictoriaMetrics and discovered through Grafana's standard Prometheus datasource API.

This milestone intentionally does not ingest `logs.json` or `traces.json`. They remain in the downloaded archive for the later multimodal investigation-plan milestone described in [`docs/telemetry-dataset-testing-roadmap.md`](../docs/telemetry-dataset-testing-roadmap.md).

The first half of the script is deterministic and does not need an LLM key:

1. Health check
2. Upload `demo/checkout-service-incident.grafana.json`
3. Approve learned signal mappings
4. List ingested dashboards

The final dashboard generation step uses the normal DashForge LLM pipeline. If your local `.env` does not have an LLM provider/API key configured, use the first half as the learning demo, then run generation after configuring one of the supported providers.

## Demo Prompt

```text
checkout-service is in an incident: p95 latency is spiking after deploy, 5xx errors are rising on payment routes, and requests are piling up. Build the dashboard before creating anything noisy.
```

## Screen Recording Beats

1. Show the one-line prompt in the DashForge UI.
2. Upload or run the learning step and show the inferred signals: latency, errors, throughput, saturation, CPU, memory, and downstream DB latency.
3. Approve the learned dashboard so the mappings become active.
4. Generate the fresh dashboard from the prompt.
5. Cut to Grafana with the generated dashboard open.
6. End on history/feedback to show the loop: prompt, selected signals, generated panels, and review.

Keep the clip under 75 seconds. The story is stronger if the viewer sees the operator workflow, not only the final dashboard.

## Suggested Repo Blurb

~~~markdown
### Demo: checkout incident in a box

DashForge can learn from an existing incident dashboard, infer reusable observability signals, and use those signals when generating a fresh investigation dashboard from plain English.

Run:

```bash
docker compose -f docker-compose.dev.yml up -d --build
demo/run_checkout_incident_demo.sh
```

Prompt:

> checkout-service is in an incident: p95 latency is spiking after deploy, 5xx errors are rising on payment routes, and requests are piling up. Build the dashboard before creating anything noisy.
~~~

## LinkedIn Post Draft

```text
I've been building DashForge: an experimental observability tool that turns an incident description into a purpose-built investigation dashboard.

The demo flow is intentionally on-call shaped:

1. Upload a known-good checkout incident dashboard
2. Infer the operational signals it contains: latency, errors, throughput, saturation, CPU, memory, DB wait
3. Approve those learned mappings
4. Ask a fresh question in plain English:

"checkout-service is in an incident: p95 latency is spiking after deploy, 5xx errors are rising on payment routes, and requests are piling up. Build the dashboard before creating anything noisy."

DashForge then creates a Grafana dashboard focused on the investigation path instead of asking the engineer to hunt through static dashboards at 3AM.

The interesting part is not just natural language to charts. It is the feedback loop: existing dashboards teach reusable signal mappings, generated dashboards collect review feedback, and the system gets more operationally specific over time.

Still early, still experimental, but this is the shape I think AI infra tooling should take: reduce navigation burden during incidents, keep humans in review, and make the system learn from the dashboards teams already trust.
```

## Good Screenshot Targets

- `http://localhost:8000`: prompt and learning tabs
- `http://localhost:3000`: generated Grafana dashboard
- `http://localhost:8000/docs`: API surface if posting to the repo
- Terminal output from `demo/run_checkout_incident_demo.sh`: inferred signals and approval count
