# Measured evidence capture implementation

The implementation retains bounded numeric facts from the Prometheus instant
query Tacit already validates. Contracts and snapshots now preserve the value,
sample timestamp, exact datasource/query identity and declared service scope.
This addresses the availability-only representation exposed by the frozen
ClickStack/GAMMA exercise. It does not establish an anomaly, a comparison or
better investigation ranking.

Implementation branch: `issue/measured-evidence-capture`, stacked on acquisition
PR [#105](https://github.com/aditki/tacit/pull/105). Feature commit `6d2fcd1`;
acquisition review fixes integrated in merge `addb314`. Offline evaluation stays
in [#106](https://github.com/aditki/tacit/pull/106).

## Result and limits

- One pure shared boundary creates immutable source-bound measurement records.
  It accepts finite instant vectors/scalars, preserves zero/negative values and
  canonicalizes series order. Invalid, partial, unsupported and limited results
  retain explicit unavailable dispositions without a numeric prefix.
- The measured backend capability preserves the existing validation tuple API.
  Primary, symptom-rescue and gap-rescue validation share the same root work
  budget. Grafana uses the existing bounded HTTP/decode path and issued instant
  probe; duplicate queries within one validation call are probed once. Direct
  SignalFx remains metadata-only and reports unsupported numeric capture.
- Capture allows 64 samples per query, 16 bounded source labels and seven scope
  label names. Other label values are excluded from plaintext retention. The
  run ceiling is 512 samples/256 KiB numeric content. Static sidecar admission
  additionally bounds identity metadata, record count and the complete graph.
  Every physical embedded sample/byte copy counts before contract allocation.
- The already-owned completion preflight pins the sidecar before publication.
  Exact replay uses captured facts without a live backend. Values and actual
  sample timestamps affect fingerprints; absent historical measurements create
  no new contract keys. Counterfactual removal clears facts lacking remaining
  support. Ordinary lifecycle events carry only disposition/count/hash metadata.
- Query-output units remain unknown, and baseline/trend comparisons remain
  unevaluated. Measurements never supply a stronger grounding/ranking outcome,
  enter generated dashboard inputs or become automatically approved model input.

The existing RuntimeStores root, admission controller, backend operation owner
and provider/client cleanup authority remain in control. No new worker, loop,
retry, refetch, cache or controller was added. No package dependency changed.
There is no classifier or LangGraph integration.

## Test-first and review evidence

Before implementation, numeric parser/admission tests failed because the module
did not exist, validation integration gates failed on the missing optional
capability, and replay tests failed on missing captured facts. Subsequent
adversarial regressions ran red before repairs for forged model fields,
raw numeric copies, envelope cardinality, contract-wide sample admission,
physical duplicate-query fanout and sensitive lifecycle event payloads.

The focused integration set passes **80 tests**: 46 parser/admission gates,
15 measured validation/ownership gates, 17 capture/replay gates and the two
legacy/measured public-pipeline variants. The existing plus new contract suite
also passed 159 tests before the final aggregate/fanout additions. Independent
architecture and security/reliability reviews cleared the complete feature
after the reproduced findings were fixed. Acquisition review-fix interaction
was independently checked on the combined branch.

All nine actual historical captures from the frozen scenario baseline at
`1af49fd1ef552446ff9856164e0d9811f5d76e85` replayed with identical input/output
fingerprints, decisions and provenance under this implementation, with socket,
SQLite and subprocess capabilities denied. Local replay audit SHA-256:
`e43dfcec0fbc8b831a872cc24bd92883fc69412182fd63651d677735a435379e`.
The source captures and their frozen labels were not changed.

## Final verification

The integrated branch passed on the local macOS ARM64 environment:

- Full default unit suite: **4,803 passed, 12 skipped, 82 integration/e2e
  cases deselected**, 259.32 seconds. The separate vendor and end-to-end runs
  below cover those 82 cases.
- Hermetic vendor integration: **25 passed**. Public end-to-end: **57 passed**,
  including the legacy and measured 588-metric pipeline variants.
- Ruff passed; Black accepted all **361** Python files; mypy accepted all
  **171** production source files.
- Grounding: **40/40**, trustworthy-answer rate 1.0, unsafe-assertion rate 0.0.
  Operational Learning: **18/18**, every stage failure count zero.
- Linux ARM64 Docker build passed as `tacit:measured-evidence`. The installed
  CLI reports `0.1.1rc5`. A synthetic measured contract replayed exactly twice
  as UID 999 with networking disabled, a read-only filesystem, dropped
  capabilities and socket/SQLite/subprocess creation trapped.
- The built wheel independently supplied the measurement module and packaged
  contract schema under the same isolated replay checks. No publication or
  hosted inference was performed.

These checks verify capture, bounds, packaging and compatibility. They do not
measure improvement on production incidents or provide new classifier quality
evidence.

## Implementation boundaries

The applicable matrix rows cover runtime ownership, entry points, tenant and
side-effect ordering, immutable revisions, provenance/fingerprints, historical
and counterfactual replay, aggregate response/retention/fanout work, packaging
and evidence quality. Tests exercise owned-proxy legacy fallback, cancellation
stream closure, zero I/O after denial, pre-publication admission, both contract
and snapshot parsing, and sensitive-field exclusion.

SQL schema/query plans are unchanged: facts use the existing immutable revision
and snapshot transaction. New 100-prompt clean/long-lived quality evidence is
not claimed: this increment changes no intent, retrieval, signal resolution,
archetype selection or ranking policy. Existing deterministic Grounding and
Operational Learning gates remain required. Protected release evidence and
production deployment remain separate.

## Follow-up

Diagnose semantic acquisition coverage for Visa-cache/network evidence and the
application-only failures separately. Measured instant values alone do not
provide a baseline or a useful set of competing suspects. Any new investigation
comparison requires a newly versioned corpus and labels frozen before further
model calls; the current scenario corpus remains immutable.

See the [implementation spec](../specs/measured-evidence-capture.md),
[ADR-026](../adr/026-bounded-measured-query-facts.md), and the mandatory measured
evidence gate in the [foundation matrix](../foundation-invariant-matrix.md).
