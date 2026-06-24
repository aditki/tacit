# ADR-011: Logs and traces should be introduced as evidence types

## Status

Deferred

## Context

The intent model can request `metrics`, `logs`, and `traces`, and Grafana datasource filtering knows about Loki,
Elasticsearch/OpenSearch, Tempo, Jaeger, and Zipkin. However, the strongest implemented path is still metrics and
dashboard panels. README roadmap lists Loki log panel support and Tempo trace support as current focus items.

## Decision

Future logs and traces should be introduced through evidence requirements and investigation needs, not as disconnected
retrieval features. Traces are likely the earlier evidence type for latency/dependency localization because they are more
structured.

## Consequences

- Logs/traces should share the same intent, context, validation, and evidence framing as metrics.
- Docs should not overclaim logs/traces as mature product surfaces.
- Datasource adapters may exist before full evidence semantics are ready.

## Implementation Notes

Implementation status: future/deferred.

Validated against:

- `tacit/models/schemas.py`: `SignalType` includes `LOGS` and `TRACES`.
- `tacit/grafana/datasource.py`: can filter log/trace datasource types.
- `tacit/grafana/adapters/loki.py` and `tacit/grafana/adapters/elasticsearch.py`: discovery adapters exist.
- `README.md`: marks Loki and Tempo as future/current-focus work.

TODO:

- Model logs/traces as evidence requirements before presenting them as independent products.
- Add validation/evaluation scenarios for trace and log usefulness once implemented.

