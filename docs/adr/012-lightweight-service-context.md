# ADR-012: Service context is a lightweight bridge toward operational memory

## Status

Proposed

## Context

Operational context such as ownership, dependencies, runbooks, criticality, and service names is important for useful
incident investigation. The current repo has pluggable context providers (`MCP`, `A2A`, `RAG API`) and service fields in
intent/mapping context, but there is no `dashforge/service_context.py` or first-class lightweight service-context YAML
layer on `main`.

## Decision

DashForge should eventually support a lightweight service-context layer before building heavier operational memory. That
layer should supplement learned dashboard semantics and external context providers.

## Consequences

- The first version should be simple YAML/config, not a full service graph database.
- Context should be scoped, inspectable, and optional.
- Enterprise knowledge should continue to come from customer-owned systems through context providers where appropriate.

## Implementation Notes

Implementation status: not implemented on this branch.

Validated against:

- `dashforge/context/`: contains MCP, A2A, and RAG API context provider integrations.
- `dashforge/models/schemas.py`: `Intent.services` and mapping context fields exist.
- `dashforge/signals.py`: mapping context can include services.
- No `dashforge/service_context.py` exists in the current repository state.

TODO:

- Add a lightweight service-context config file and loader if this becomes near-term scope.
- Document how service context interacts with external RAG/MCP/A2A providers.

