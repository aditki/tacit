# ADR-025: Preserve discovery facts through existing investigation owners

## Status

Proposed; implemented on the acquisition branch, subject to repository review.
ADR-024 is reserved for the separate offline control-model pilot.

## Context

The existing catalog was capped for prompt size before deterministic compilation
and evidence selection. List results could not distinguish incomplete search
from absence, while compiler, declared-evidence and rescue consumers independently
selected owners. Separate SignalFlow translation at publication could also
restore an unsafe query rejected during compilation.

## Decision

Use an optional metadata discovery protocol and immutable per-call results.
Keep the legacy DashboardBackend protocol/list projection and explicitly label
external list-only results unknown. Successful datasource enumeration consumes
the investigation call/byte budget but is not a metric-search status. A source
summary carries no invented datasource identity or metric count.

Retain admitted acquisition facts separately from prompt projection. One shared
selector requires all exact operands at one owner and preserves conflicts and
ambiguity. Sampled labels and default flags cannot break an ownership tie.
All first-party transport uses one bounded-read path and the same investigation
work budget through the existing runtime-owned backend operation. No additional
controller, worker, permit owner or cleanup authority is created.

Repeated exact-owner lookups use one explicitly owned admitted catalog view per
operation, shared by nested compiler, evidence and rescue calls. Its private
bounded index retains full-catalog conflict semantics and exposes no mutable
facts. Public construction always derives normalization from raw admitted
facts; a supplied normalization cannot attest its own conflict completeness.
No process-wide, context-local or object-identity cache is introduced. Capture
construction and admission continue to reconstruct their own trust boundaries.

Preserve conflicting metadata facts until shared normalization. Identical
duplicates may share one retained identity; conflicting units/types cannot be
discarded by legacy adaptation, aggregation or enrichment. Native provider
envelopes count normalized metric rows rather than wrapper objects.
Completed admitted enrichment facts and bounded inventory candidates survive a
later search or phase reaching its own deadline, subject to final identity
admission. Unfinished work is incomplete rather than grounds to erase completed
facts or conflicts. Caller cancellation still propagates; the legacy
coordinator's outer timeout behavior is unchanged.

Use the existing guarded compiler at publication as well as compilation.
Unsupported selected SignalFlow translation fails the investigation before any
backend publishes, with no partial dashboard or query guessing. Template/native
support expansion remains a separate semantic decision.

Before publication, prepare private copied assembly inputs and a base-reference
index using the existing public assembler. Admit bounded acquisition records
and extensions against that index. Initial completion and exact replay append
the same records and stamp fingerprints; contract/snapshot persistence retains
its existing transaction and parent check. Historical absent-field snapshots
and fingerprints retain their meaning.

## Alternatives

Changing the legacy backend return type would break list-only integrations.
The optional extension costs one explicit compatibility path without duplicate
discovery implementations. A new source-selection/compiler framework or runtime
manager would create additional maintained authority; the existing boundaries
are sufficient. Discarding conflicts or publishing only supported panels would
conceal missing evidence. Explicit failed runs preserve the safety contract.

## Consequences and evidence

Typed discovery initially bypasses legacy metric caches and can make more calls.
Existing per-provider search caps remain incomplete. Acquisition configuration
must pass the measured static envelope before typed I/O; generic Settings and
unrelated runtime functions retain their existing concurrency range.

Grounding, ranking arithmetic, tenant authorization and historical replay remain
authoritative. New metadata does not establish a numerical condition, absence
of telemetry or root cause. The foundation matrix and acquisition tests define
the mandatory failure, ownership, limit and replay evidence before activation.
No classifier, LangGraph migration, database migration or inference is included.
