# Operational Cognition

DashForge is built around a simple belief: the hardest part of incident response is often not querying metrics, but deciding what to investigate next.

Dashboards are necessary. They are not sufficient. A useful observability system also needs to encode investigation strategy, operational semantics, and local context.

## Why Dashboards Are Not Enough

Dashboards are static views over dynamic uncertainty. They show signals, but they rarely explain why those signals matter for the specific incident in front of an operator.

During an incident, engineers are not just asking:

> What is the p99 latency?

They are asking:

> Where should I look next, and what would confirm or falsify my hypothesis?

Traditional dashboards tend to assume the operator already knows:

- which service or dependency is suspicious
- which metrics are canonical in this environment
- which labels identify the right workload
- which saturation signals are causal versus incidental
- which drilldowns should come after the first graph

That knowledge often lives in senior engineers' heads, scattered runbooks, Slack threads, postmortems, dashboard naming conventions, and metric folklore. DashForge tries to turn more of that operational reasoning into executable investigation structure.

## Why Investigation Planning Matters

Good incident response is a sequence, not a single chart.

A useful investigation usually moves through stages:

1. establish whether the symptom is real
2. localize the blast radius
3. compare demand, errors, latency, and saturation
4. inspect dependencies and recent change indicators
5. choose the next drilldown based on evidence

DashForge models this explicitly. The intent agent classifies the problem type, the archetype engine selects or blends investigation plans, and the backend adapters turn that plan into native dashboards.

This matters because a dashboard generated from a prompt like "checkout is slow" should not be a bag of plausible graphs. It should reflect an investigation path:

- latency panels first
- request rate and errors nearby
- saturation and restarts as supporting context
- dependency/database signals when the prompt implies them
- query validation before publishing

The goal is not to replace the operator. The goal is to reduce the time spent rebuilding the same investigation scaffolding at 3AM.

## Why Operational Semantics Are Learned

Observability semantics are local.

The generic concept `request_latency` might map to:

- `http_request_duration_seconds_bucket`
- `service_latency_ms`
- `envoy_cluster_upstream_rq_time_bucket`
- `checkout.api.duration.p95`
- `data('service.latency').percentile(pct=99)`

There is no universal metric name for "the thing this team trusts when checkout is slow." Teams learn those mappings over time through dashboards, alerts, fixes, failures, and review.

DashForge treats operational semantics as learned infrastructure:

- the signal taxonomy defines canonical concepts such as latency, errors, throughput, saturation, and stability
- bootstrap patterns provide common defaults
- dashboard ingestion learns from existing Grafana and SignalFx dashboards
- context-aware mappings preserve service, datasource, environment, and backend scope
- feedback can raise or lower metric quality without retraining a model

This is why DashForge has a signal store rather than only prompt templates. The system needs a memory of what metrics mean in this environment.

## Why Agentic Systems Still Need Context

LLMs are useful for interpretation and synthesis, but they do not magically know a company's operational reality.

Without context, an agentic observability system will often:

- pick plausible but nonexistent metrics
- use the wrong datasource
- miss organization-specific naming conventions
- overfit to generic SRE examples
- ignore ownership, deployment, or dependency context
- generate queries that are syntactically valid but operationally useless

DashForge constrains the agentic parts with runtime context:

- live datasource catalogs
- per-metric label discovery
- learned signal mappings
- archetype templates
- query validation against the target backend
- optional context providers such as MCP, A2A, or RAG APIs

The long-term pattern is not "let an LLM invent dashboards." It is "let an LLM operate inside a typed, validated, context-rich investigation system."

## Where DashForge Is Heading

DashForge is moving toward an operational cognition layer for observability:

- investigation plans represented as reusable archetypes
- learned operational semantics through signals and feedback
- deterministic query compilation where possible
- vendor adapters that preserve native query languages and datasource identity
- retrieval over metric metadata and prior successful investigations
- context providers that consume customer-owned knowledge without custodying it
- evaluation loops that measure archetype accuracy, metric recall, critical recall, and dashboard usefulness

The next major architectural direction is a stronger intermediate representation:

```text
prompt
  -> operational intent
  -> investigation plan
  -> semantic observability IR
  -> datasource-native query compiler
  -> validated dashboard
```

That direction keeps the agent useful while making the output more inspectable, testable, and portable across backends.

DashForge is early. The core thesis is stable: incident response needs tools that understand not only telemetry, but the cognitive work of investigation.
