# ADR-016: Culprit ranking has contextual and telemetry-evidenced tiers

## Status

Accepted

## Context

Tacit's strongest differentiation is learning from operational artifacts: service metadata, service graphs, ownership,
runbooks, alerts, existing dashboards, incident tickets, deployments, pull requests, documentation, and historical
investigations. Those artifacts help operators decide where to look before querying production telemetry.

At the same time, the roadmap includes stronger evidence models, logs, traces, metrics, and eventual culprit ranking.
Without an explicit boundary, future work could imply that telemetry-backed ranking is the real product and that
knowledge-driven ranking is only a weak placeholder.

That would blur two different deployment and governance models:

- Knowledge access: systems such as Jira, ServiceNow, GitHub, Confluence, runbooks, service catalogs, deployments,
  historical incidents, and reviewed dashboards.
- Production data access: systems such as metrics backends, traces, logs, PromQL/MetricsQL query paths, Datadog
  queries, and other runtime telemetry.

Production telemetry can contain customer identifiers, request paths, payload fragments, operational secrets, and
high-cardinality data. It usually requires a heavier security review than knowledge-system access.

## Decision

Tacit should model culprit ranking as two related product tiers:

1. **Contextual Culprit Ranking**: operational knowledge becomes suspect ranking. It does not require live production
   telemetry. Results must show contextual reasons and the knowledge sources used.
2. **Telemetry-Evidenced Culprit Ranking**: contextual ranking plus runtime evidence verification. Runtime telemetry can
   corroborate, contradict, demote, or reorder suspects. Results must label supporting runtime evidence separately from
   contextual reasons.

Telemetry-backed ranking should not be described as the real version of ranking. It is a stronger, optional evidence
verification layer on top of contextual ranking.

### Tier 1: Contextual Culprit Ranking

Contextual culprit ranking is Tacit's natural starting point.

Inputs:

- service catalog
- service graph
- ownership
- runbooks
- alerts
- existing dashboards
- incident tickets
- deployments
- pull requests
- documentation
- historical investigations

Output:

```text
Contextual Ranking

1. Checkout Service
   Reasons:
   - Similar to incident INC-482
   - Recent deployment
   - Runbook references latency symptoms

2. Checkout Database
   Reasons:
   - Historical correlation with checkout failures
   - Dependency relationship

3. Redis Cache
   Reasons:
   - Common downstream dependency
```

This tier is:

```text
Operational Knowledge
-> Suspect Ranking
```

No live telemetry is required. This aligns with the `Connect -> Learn -> Investigate` adoption path because the system
can learn from artifacts teams already use to investigate.

### Tier 2: Telemetry-Evidenced Culprit Ranking

Telemetry-evidenced ranking adds runtime evidence to contextual ranking.

Additional inputs:

- PromQL
- MetricsQL
- Datadog queries
- traces
- logs
- runtime metrics

Output:

```text
Telemetry-Evidenced Ranking

1. Checkout Database
   Supporting Evidence:
   - p95 latency +420%
   - Connection pool saturation
   - Error rate increase

2. Redis
   Supporting Evidence:
   - Elevated misses
   - No latency increase

3. Checkout Service Deployment
   Supporting Evidence:
   - Recent deploy
   - No corroborating runtime evidence
```

This tier is:

```text
Operational Knowledge
+
Runtime Evidence
-> Suspect Ranking
```

It is stronger than contextual ranking, but has more governance overhead. Production metrics, traces, and logs may
contain customer identifiers, request paths, payload fragments, operational secrets, and high-cardinality infrastructure
data. The product should therefore treat telemetry as optional verification, not as a prerequisite for useful ranking.

### Product Output

The distinction should be visible in the product.

Without telemetry:

```text
tacit investigate checkout

Contextual Ranking

1. Checkout Service
2. Checkout DB
3. Redis

Evidence Sources:
- Runbooks
- Historical Incidents
- Service Graph
- Recent Deployments

Telemetry:
Not enabled
```

With telemetry:

```text
tacit investigate checkout --telemetry

Telemetry-Evidenced Ranking

1. Checkout DB
2. Redis
3. Checkout Service

Evidence Sources:
- Runtime Metrics
- Historical Incidents
- Service Graph
- Recent Deployments
```

This makes the operator and security reviewer aware of which trust boundary was crossed.

## Consequences

- Product output should visibly distinguish contextual reasons from runtime evidence.
- When telemetry is disabled, Tacit should say so instead of implying evidence was checked.
- Contextual ranking can be adopted earlier because it asks for knowledge access rather than production data access.
- Telemetry integrations should remain optional amplifiers, not mandatory prerequisites for the product thesis.
- Evaluation should measure contextual suspect quality separately from telemetry-evidenced ranking accuracy.
- Governance, retention, RBAC, prompt construction, and provenance should treat knowledge artifacts and production data
  as separate trust boundaries.

## Implementation Notes

Implementation status: partially implemented.

The first implementation adds deterministic suspect ranking to pipeline responses and investigation history. It consumes
intent context, selected archetypes, validated dashboard panels, evidence requirements, evidence resolutions, and
evidence observations. Rankings are marked `contextual` unless at least one requirement has a supported runtime
observation, in which case the ranking is marked `telemetry_evidenced`. Missing evidence produces an explicit
abstention rather than a root-cause assertion.

Validated against:

- `README.md`: intentionally remains unchanged so the top-level project narrative stays focused on setup and the current
  investigation artifact flow.
- `SECURITY.md`: intentionally remains unchanged because it is the open-source security policy, not a product
  governance document.
- `docs/evaluation.md`: now notes that contextual suspect quality and telemetry-evidenced ranking accuracy should be
  measured separately.
- `tacit/culprit_ranking.py`: implements deterministic contextual and telemetry-evidenced suspect ranking.
- `tacit/models/schemas.py`: exposes `CulpritRanking` and `CulpritCandidate` in `DashResponse`.
- `tacit/pipeline/runner.py`: records a reason-coded `ranking` stage before publish or empty-panel failure.
- `docs/adr/012-lightweight-service-context.md`: already positions service context as the lightweight bridge toward
  operational memory.
- `docs/adr/015-evidence-lifecycle.md`: already requires culprit ranking to consume observed evidence rather than raw
  metric names alone.

TODO:

- Add richer contextual ranking from service catalogs, runbooks, ownership, deployments, PRs, incidents, and historical
  investigations once those artifacts are represented in the runtime context.
- Add governance metadata that labels each source as knowledge access or production data access.
- Add benchmark fixtures for contextual suspect quality and telemetry-evidenced ranking accuracy.
