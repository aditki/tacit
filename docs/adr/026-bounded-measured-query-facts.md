# ADR-026: Retain bounded measured query facts in investigation revisions

## Status

Proposed; implemented on the measured-evidence branch subject to review.

## Context

The frozen ClickStack/GAMMA exercise retained query validity and availability,
but all eight GAMMA fault/healthy projections were identical. The validator had
already received numerical results and discarded them. A classifier cannot
recover those facts from availability alone.

## Decision

Extend ADR-015's evidence lifecycle through the existing query records. Retain
bounded finite instant samples, exact query/datasource provenance and a small
declared scope-label allowlist in the immutable contract/replay capture.
Other returned label values are excluded from plaintext retention. Query-output
units remain unknown unless established by a future explicit compiler contract.
Actual sample facts are semantic inputs; collection-clock metadata is not.

Numeric capture is additive and optional. Historical absent-field fingerprints
retain their meaning. Existing tenant authorization, optional telemetry access,
bounded retention, revision transaction and assessment-export permissions govern
these facts. They are sensitive runtime telemetry, not ordinary diagnostics or
automatically approved model input. Do not emit values/labels into logs or
implicitly transmit them to a model.

Capture the already-issued validation query through existing bounded transport
and run budget. Use a typed sidecar rather than accepting a measurement on an
LLM-generated dashboard. Keep runtime admission, operation lifetime and cleanup
with their existing owners. Numeric availability never establishes abnormality,
priority, comparison or causality and does not change ranking/grounding policy.

## Consequences

Contracts become useful for inspecting actual observed values and later offline
evaluation, at a strictly bounded retention cost. Unsupported/invalid/partial or
over-limit results remain explicitly unavailable. The existing remote budget
also bounds measured validation; exhausted discovery may leave no query capacity.
There is no unbounded fallback, hidden retry or extra telemetry query.
Future range/baseline comparisons, broader scope retention, classifier packets
and semantic acquisition improvements need their own reviewed changes.

See [implementation spec](../specs/measured-evidence-capture.md) and the measured
evidence gate in the foundation invariant matrix.
