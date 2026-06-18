# ADR 0001: DashForge Is Not a System of Record

Date: 2026-06-16

Status: Accepted

## Context

DashForge connects to operational systems such as observability platforms,
dashboards, service metadata, runbooks, and incident workflows. Those systems
already own critical facts about services, telemetry, incidents, changes, and
operational history.

DashForge needs local persistence for derived state such as generated dashboard
history, feedback, learned signal mappings, cached context, and evaluation
artifacts. Without an explicit boundary, that persistence could be mistaken for
canonical ownership of operational data.

## Decision

DashForge is not a system of record.

DashForge consumes operational artifacts from existing systems of record and
produces operational intelligence.

Persistence, telemetry storage, change history, service catalogs, and incident
records remain owned by the source systems.

## Consequences

DashForge must preserve provenance for consumed artifacts and generated outputs
so operators can trace intelligence back to authoritative systems.

DashForge should store derived, reviewable, and reproducible information rather
than replacing canonical operational data. Examples include learned signal
mappings, dashboard generation history, user feedback, prompt metadata, and
links to generated dashboards.

DashForge integrations should treat source systems as authoritative for
telemetry, service ownership, incident records, change events, and dashboard
definitions. When source data changes, DashForge should refresh or re-derive its
local intelligence instead of becoming the place where operators correct the
canonical record.

DashForge APIs and documentation should describe local persistence as derived
state, cache, audit support, or feedback data. They should avoid implying that
DashForge owns production telemetry, service catalogs, incident timelines, or
change history.

DashForge may still enforce operational safety controls around generated
intelligence, such as approval workflows, retention policies, access controls,
and provenance display.
