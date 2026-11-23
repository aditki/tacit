# Measured evidence capture

Status: implementation spec; review on `issue/measured-evidence-capture`.
Predecessor: acquisition PR #105. The frozen scenario experiment remains on
the separate offline evaluation branch/PR #106.

## Outcome and scope

The current Prometheus validator receives numeric results but retains only
query validity/nonemptiness. Preserve bounded instant measurements from that
same response in the Investigation Contract and replay snapshot. Operators can
inspect actual values and source scope. Recording a value does not establish
abnormality, a comparative baseline, a preferred suspect or causality.

This increment performs no new query, range scan, metadata enrichment, model
call or retry. It changes neither ranking arithmetic nor evidence support and
grounding decisions. Semantic acquisition coverage, failed-run revision policy,
baseline comparisons, classifiers and LangGraph remain separate work.

## Shared boundary and interfaces

`tacit/measurements.py` owns closed immutable numeric records, bounded
Prometheus result normalization, query identity and cross-record admission.
It has no client, runtime, worker, cache or persistence authority.

Use an optional `MeasuredQueryValidation` backend capability:
`validate_queries_with_measurements(spec, catalog, work_budget)` returns
`MeasuredValidationResult(spec, warnings, measurements)`.
The existing `DashboardBackend.validate_queries` tuple API remains compatible.
One helper selects the capability or labels legacy validation `not_captured`.
The service-owned backend proxy forwards this capability through its existing
lease/operation boundary; legacy fallback executes inside that same operation.

Grafana's measured path reads its existing Prometheus instant query through
the existing bounded acquisition transport and the exact investigation-owned
`SignalResolutionWorkBudget`. Discovery plus validation share its existing
32-call/8-MiB ceiling; no unbounded fallback is permitted after denial.
Measured validation is serial, so it cannot race the shared budget or build a
query fan-out. Main validation, symptom rescue and gap rescue use this helper
and the same supplied budget. Standalone legacy validation without the budget
does not promise numeric capture or acquire a hidden runtime/budget.

Direct SignalFx still validates metadata existence only and records
`unsupported` numeric capture. Non-Prometheus Grafana types likewise remain
explicitly unsupported; no guessed execution adapter is added.

## Measurement contract

Each versioned record binds exact expression, datasource UID/type and query
language to a SHA-256 query identity. It records a closed disposition/reason,
result type, instant evaluation time, returned series count, bounded numeric
samples and a deterministic hash of normalized facts. Dispositions distinguish
captured, empty, unsupported, invalid, limited and not captured. Partial provider
responses/warnings never become a complete captured result.

Support Prometheus instant `vector` and `scalar` results only. Each sample
contains its actual timestamp and finite numeric value, a stable series hash
and exact admitted scope dimensions. Zero and negative values remain values;
bools, NaN/infinity, malformed timestamps, duplicate series and malformed
envelopes cannot become numeric evidence. Sort by stable series identity so
provider ordering does not alter the fact hash. Reject a whole measurement on
limits or invalid samples; do not retain an apparently complete prefix.

Bounds: 64 series/samples per query; 16 labels per series; 64-character label
keys and 256-character/1-KiB UTF-8 label values. Retain only `service`,
`service_name`, `environment`, `namespace`, `cluster`, `region` and `node`
scope labels, without inferring canonical ownership. Other labels contribute
only to a bounded stable series hash; their plaintext never persists. Unit is
unknown in this increment: raw metric metadata does not prove the output unit
of a rate, ratio or arbitrary expression. Comparisons remain unevaluated.

The existing run budget also admits aggregate retained measurement facts
before append: at most 512 numeric samples and 256 KiB canonical measurement
content per investigation. A separate static pre-copy shape guard admits at
most 1,024 records and 2 MiB total sidecar metadata; it is not another mutable
work budget. Exact retained datasource UID/type/language are limited to
256/128/64 characters and 1,024/512/256 UTF-8 bytes. An identity that cannot be
represented fails closed rather than being truncated. The existing
dashboard/evidence composition limits also bound record count, and unavailable
records contain no numeric source values.
Admission counts physical sample/byte copies before query-record expansion and
contract loading, including repeated query identities. Sidecar deduplication
cannot bypass the output allocation bound.
Budget contention fails closed without blocking the service owner. Exhaustion
does not reset between rescue stages. No measurement value or raw label enters
logs, diagnostics, LLM/query-generation inputs or the offline model projection.

## Data flow, provenance and replay

Keep numeric records in a typed sidecar, never a generated/public `PanelQuery`
input field. Validation creates records only from its admitted backend response;
model/caller-supplied dashboard fields cannot supply verified telemetry.
`ValidationEvidenceResult` carries records across all three validation paths.
Duplicate exact query identities share one admitted result or reject conflicting
facts; no rank/order-based association is permitted.

Before any publication, completion's existing bounded private-copy admission
pins the sidecar with the ordinary assembly inputs. The assembler attaches a
record to `QueryRecord.execution.measurement` only after matching exact query,
UID, type and language. Observation query references remain the path to those
facts; a foreign-owner observation must not acquire a measurement reference.
No new numeric fact changes observation outcome or ranking.

Persist an optional typed `query_measurements` list in the existing replay
snapshot, transactionally with its immutable revision. Rebuild passes the same
records to the assembler without external access. Actual measured values,
sample/evaluation timestamps and source identities affect fingerprints;
collection-clock diagnostics do not. An absent historical sidecar emits no
new contract keys or nulls, preserving existing input/output fingerprints.
All API/CLI/export/refresh surfaces consume the same contract. Existing failed
attempts keep their run audits; this feature does not manufacture revisions.
Counterfactual evidence removal removes a query's measurement when no other
surviving observation still supports that query; duplicate surviving support
keeps the actual fact. Historical measure-free counterfactual behavior remains
unchanged.

## Lifecycle gate

Single runtime owner: the existing RuntimeStores root/execution graph.
Admission: its existing PipelineAdmissionController plus the already shared
SignalResolutionWorkBudget for query I/O and numeric retention. Capacity release:
the existing provider/backend operation owner after real settlement. Cleanup:
the existing adopted Grafana client/provider owner; bounded response streams
close on all exits. Parsing and sidecar assembly are bounded synchronous work
inside the operation. Cancellation or requester-loop loss retain existing
operation/capacity ownership through settlement. There is no new loop, thread,
subprocess, permit class or cleanup fallback.

## Test-first gates and delivery

Select matrix rows for entry points, runtime ownership, tenant/side-effect
ordering, investigation lifecycle, persistence, exact provenance/replay,
aggregate work/scaling, packaging and evidence quality.

1. Before implementation, reproduce value loss and test the pure record/parser:
   distinct numeric facts; finite zero/negative; invalid/partial/unsupported;
   ordering; identity; per-query limits and run sample/byte limit-plus-one.
2. Test bounded transport before decoding, zero calls after denial, cancellation
   stream closure, no extra requests, main/rescue shared-budget propagation,
   owned-proxy legacy fallback and spoofed generated fields.
3. Test sidecar admission before publication, exact query/observation links,
   historical fingerprint goldens, changed-value fingerprints, immutable private
   copies and repeated offline contract/bundle replay. Numeric capture alone
   must leave grounding/ranking unchanged.
4. After each implementation boundary, focused verification plus independent
   architecture and security/reliability review must be clean before dependent
   integration. Final whole-diff review, full unit/integration/public pipeline
   tests, Ruff, Black, mypy, Grounding, Operational Learning, schema/package and
   read-only container smoke precede the PR. No live 100-prompt/model gate is
   claimed: this increment changes no intent, retrieval, resolution, archetype
   selection or ranking policy.

New live scenario comparisons require a separately versioned corpus, new
baseline capture and labels frozen before any new classifier calls. The current
13-recording corpus remains immutable.
