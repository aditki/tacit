# ADR-010: Incident management integrations are downstream distribution

## Status

Accepted

## Context

Tacit is not an incident-management platform. The repo has Slack interaction and dashboard publishing, but no native
PagerDuty, Rootly, incident.io, remediation, scheduling, escalation, or incident lifecycle ownership.

## Decision

Incident-management integrations should be downstream distribution channels. Tacit's core differentiation is
operational investigation intelligence: learning telemetry language, generating evidence views, and preserving feedback.

## Consequences

- Do not position Tacit as a replacement for PagerDuty, Rootly, incident.io, or similar systems.
- Future integrations should export investigation context, dashboard links, evidence summaries, and provenance.
- Core development should prioritize learning quality and generated investigation usefulness first.

## Implementation Notes

Implementation status: implemented as a roadmap guardrail.

Validated against:

- `README.md`: does not position Tacit as an incident management replacement.
- `tacit/integrations/slack.py`: Slack is a user interaction channel, not an incident lifecycle system.
- No first-class PagerDuty, Rootly, or incident.io integrations exist.

TODO:

- If incident integrations are added, document them as export/distribution surfaces.

