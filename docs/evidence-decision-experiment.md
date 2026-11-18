# Offline evidence-decision experiment

This file-only experiment compares the captured historical baseline with recorded
Jev and Tev priority Choices. It asks which already-admissible suspect merits
attention next. It neither establishes numeric conditions or root cause nor
changes Tacit's production ranking, grounding, providers, stores or CLI.
The checked-in corpus is fictional conformance data. No real model result or
140-incident labeled corpus is supplied, and no inference was performed.

## Freeze a corpus before collection

Use separate, independently adjudicated manifests for 40 development incidents
and 100 held-out incidents. Each invocation accepts at most 100 cases, one case
per incident, in incident-ID order. Incident families must be disjoint across
splits; compare both manifests before freezing because the driver cannot inspect
an unseen split. Freeze a single pre-acquisition or post-acquisition stratum per
manifest. State the supported population, exclusions, adjudicators, label
resolution, and approval reference before looking at responses.

Inputs use the closed `evidence-decisions-manifest/1` protocol implemented in
[`tests/eval/evidence_decisions.py`](../tests/eval/evidence_decisions.py).
[`tests/eval/fixtures/evidence_decisions_v1.json`](../tests/eval/fixtures/evidence_decisions_v1.json)
contains synthetic identity, wire, interval and reporting goldens; the unit tests
show how to construct source-format assessment bundles and freeze a manifest.
This fixture is not evidence that any current vendor endpoint conforms.

Select local assessment bundle 1.0 files explicitly and bind their exact bytes
with SHA-256. Contract and capture investigation ID, revision and concrete tenant
must agree with the manifest. Captured baseline ranking takes precedence over
captured current ranking. Missing or invalid captures produce failed coverage;
the driver never opens a database to reconstruct them. Labels come from
independent adjudication, never the bundle's expected outcomes. Every label ID
must resolve to a protected captured candidate identity.

Corpus construction first derives IDs from original captured suspect/type and
tagged requirement and resolution/observation payload references. The protected
candidate preimages identify labels. Build the identity-keyed projection and
explicitly approve each projected captured text leaf, with its original SHA-256
and replacement. Unchanged text also needs approval. Approval pointers use
candidate/requirement/digest identities, and canonically ordered subordinate
reason/scope array positions. IDs, links, status/outcome enums and support-tier
membership cannot be edited through redactions. All approval leaves must resolve
exactly once; stale, missing or unused entries fail rather than silently passing
raw text or trimming facts.

Freeze exact hashes of the six eligible model packets and all model identities,
labels, approved text, attempts and protocol versions into the corpus digest.
The digest omits its own field and local bundle paths. Moving the same selected
bytes does not change scientific identity. Changed labels, identity, packet,
model or attempts require a new frozen experiment identity. Preparation validates
this manifest; it does not mutate or auto-freeze it. A small disposable local
construction script may call the driver's pure `captured_projection`,
`approval_leaves`, `wire_bytes` and `corpus_digest` helpers to construct inputs;
the frozen manifest remains the public input boundary.

## Prepare protected files

Use an existing repository environment; no additional package is needed:

```bash
uv run python tests/eval/evidence_decisions.py prepare --manifest M --out NEWDIR
```

`NEWDIR` must not exist. It is created with mode 0700; files use mode 0600.
Preparation emits one request per eligible case, arm and variant, plus
`candidate_map.json`, `projection.json`, `index.json` and a last-only
`complete.json` with artifact digests. The candidate map contains original
identity preimages and must stay protected. The approval map is never copied
into output. Packets contain only approved projected facts and the fixed
question. They exclude labels, rank, score, confidence, baseline answer,
expected outcomes and future facts. Never send the candidate map, manifest,
projection summary or bundle itself to a model.

All arms carry forward captured abstention. Zero/one admissible candidates yield
common deterministic outcomes. More than 12 is resource-ineligible with zero
quality. Only 2–12 candidates with captured nonempty runtime evidence produce
requests. This reuses a historical policy tier; it does not prove causal or
numeric eligibility. Candidate ordering is canonical by ID. The rotation moves
the first candidate and corresponding option to the end, keeping `insufficient`
last; wording changes only fixed instructions. The hidden baseline retains its
captured order.

The file reader rejects symlinks in any selected path component and nonregular
files. Archives are never extracted and permit only the existing writer's
allowlisted top-level regular members. Compressed/expanded bundle caps are
10/32 MiB; manifests are 10 MiB, requests 64 KiB and receipts 256 KiB. JSON is
lexically bounded before decoding. A run permits 600 requests, 1,200 receipts,
300 MiB aggregate receipt input and 64 MiB output. Output is exclusive and
counted; a partial directory is not a completed run. The operator owns its
retention or deletion. The driver never prunes or reuses it.

## Collect only with separate authorization

Collection is an explicit external operator step. This driver has no network,
SDK, process launcher, automatic model runner, retry or download capability.
Preparation and scoring do not authorize inference or data egress.

Before real Jev collection, obtain approval for hosted transmission of the exact
sanitized request packets. Pin and verify the actual hosted provider version;
a model name is not a weights digest. Before local Tev collection, check the
checkpoint license, exact artifact digest, requested tag, returned model and
Ollama server version. Validate saved request/response wire fixtures against each
exact official endpoint/version outside Tacit. The checked-in fictional
`jev-fixture-v1`/`tev-fixture-v1` identities cannot establish real conformance.
A different response envelope requires a versioned protocol and re-freeze.

The operator and external account/process controls own credentials, spend,
absolute deadline, cancellation, process cleanup and any egress approval. Use
one request per file, no automatic retries. Attempt 1 is fixed as primary. A
second attempt is permitted only when predeclared as a separate immutable
receipt. Never replace an unsuccessful recording with a favorable answer.
The scorer cannot attest that unrecorded external calls did not occur or that
an external process was cleaned up.

Write only the expected receipt filenames in the selected response directory:
`CASE.ARM.VARIANT.aATTEMPT.json`. An optional 4-KiB `collection.json` records
`{"schema_version":"collection/1","approval_ref":"..."}`. Each closed receipt
binds experiment/case/arm/variant/attempt, exact request SHA-256, returned
model/artifact/server identity, status, raw typed response, nullable known usage,
nullable elapsed milliseconds and a bounded error code. Preserve the exact file
bytes. Error receipts use `raw:null`; missing evidence of usage or timing stays
null. Neither missing identity nor unknown cost is reported as verified or zero.

The pinned wire mapping expects `raw.model`, `raw.answers.priority` with
`type:"choice"`, explicit `choice`, complete option-ID `probabilities`, nullable
`confidence`, and nullable token `usage`. Its exact closed schema is executable
in the driver and illustrated by the synthetic receipt golden. Unknown or
missing options, duplicate JSON keys, ineligible choices, malformed probabilities,
identity/hash disagreement and executable extension fields are rejected.
Probabilities are not renormalized. Confidence describes concentration, not the
chance an answer is correct. Each arm fails independently.

## Score and replay recorded files

```bash
uv run python tests/eval/evidence_decisions.py score --manifest M --responses DIR --out NEWDIR
```

Scoring re-derives every approved packet from frozen bundles and validates exact
request hashes; it does not depend on a previous preparation directory. It emits
`rows.jsonl`, `report.json`, then `complete.json`. Reports contain bounded reason
codes and never raw response text. A repeat into a new directory with identical
inputs must reproduce the semantic digest. Every declared receipt, including
attempt 2, binds this digest even when excluded from quality.

Only primary-variant attempt-1 rows supply arm quality and paired estimates.
Failures and resource exclusions earn zero quality; policy abstention earns no
model execution. Coverage, acceptable top-1, trustworthy answers, abstention
precision/recall and unsafe assertions use explicit denominators. Multiple
acceptable labels are supported but excluded from singleton multiclass Brier
calibration. Full-ranking MRR/top-3 is undefined for one Choice. Separate rotate
and wording metrics and per-incident sensitivity describe robustness without
increasing independent sample size. Known usage, receipt completeness and unsafe
attempts include all declared variants and attempts; unknown cost stays visible.

Each model is paired separately with the same captured baseline over all frozen
incidents. Deterministic 10,000-sample SHA-256 counter-mode bootstrap intervals use
seed 20261117, rational type-7 quantiles and six-place half-even decimals.
Development data, repeated attempts, variants and acquisition strata never pool
into the 100-held-out-incident recommendation gate. Considering a separate
production design requires at least 0.05 improvement, a positive paired lower
95% bound, zero unsafe assertions/attempts, and available abstention recall with
no loss. A passing gate does not authorize deployment or prove code savings.
An inconclusive or negative experiment can end classifier work.

Exit zero means the offline computation completed, including a negative quality
result or recorded external error. Invalid protocol, missing requested receipts,
unreadable inputs or exceeded budgets return nonzero. Bounded failed rows and
reports are retained when possible. Over-limit or interrupted runs never publish
a completion marker. Inspect `comparisons.*.gate_passed` independently of exit
status. No live quality results have been measured for this implementation.

## Implementation validation, 2026-11-18

Implementation was isolated on `issue/offline-jev-tev-evaluation` from base
`49816476f5d11422922b002ec3762fa1620bf616`. Four maintained experiment files were
added (driver, synthetic fixture, unit tests and these instructions), with three
existing guidance documents updated. The only new command interface is the
standalone file driver's `prepare`/`score` pair. No production code was removed
or changed, no package was installed, and no real inference was run. A resulting
implementation commit is assigned after review; this evidence does not invent a
commit SHA before that point.

Test-first evidence was preserved before implementation: 32 public-boundary
and golden tests failed because the driver did not exist
(`/private/tmp/tacit-offline-b0-red.log`). B1 review identified two additional
shared-boundary repairs. Two independently frozen malicious-enum preparation
fixtures first reproduced unapproved text reaching requests
(`/private/tmp/tacit-offline-b1-enum-red.log`). An adversarial 200-candidate by
200-observation fixture first reproduced unchecked derived-reference expansion
(`/private/tmp/tacit-offline-b1-fanout-red.log`). Both regressions are green.
Derived case material now has a conservative 4-MiB encoded-work allowance charged
before reference fanout or whole-case serialization; oversize facts are rejected
with failed coverage, never truncated. Additional B2 receipt/report tests were
also run red before scoring implementation
(`/private/tmp/tacit-offline-b2-extra-red.log`). Temporary log paths are local
implementation audit evidence and are not required inputs to the experiment.

Independent architecture and security reviews cleared B1 before B2, then
reviewed the complete B2 driver and diff with 65 focused tests passing. Checks:

```bash
.venv/bin/python -m pytest -q tests/unit/test_offline_evidence_decisions.py
.venv/bin/python -m ruff check .
.venv/bin/python -m black --check .
.venv/bin/python -m mypy tacit
.venv/bin/python -m tacit.cli benchmark-grounding
.venv/bin/python -m tacit.cli operational-learning-benchmark
```

Focused tests: **65 passed**. Full-project Ruff passed; Black accepted all 350
Python files; mypy accepted all 169 production source files. The unchanged
Grounding gate passed with trustworthy-answer rate 1.0 and unsafe-assertion rate
0.0. The unchanged Operational Learning gate passed all 18 cases with zero
reported unsafe, rejected/unresolved contribution, causal leakage or injection
policy-override failures. These are existing deterministic gates, not model
experiment results.

A full `tests/unit` run completed with **4,472 passed, 12 skipped, 45 failed** in
180.87 seconds. The failures were restricted-environment failures in existing
local socket/process-fixture and UV cache tests: 42 direct permission errors,
one handoff timeout downstream of prohibited local socket setup, and two denied
UV cache accesses. All exact 45 failed node IDs reproduced on the pristine base
in a separate temporary checkout (4.58 seconds), without this experiment's files.
The complete unit log and comparison are locally preserved at
`/private/tmp/tacit-offline-unit-suite.log` and
`/private/tmp/tacit-offline-baseline-failures.log`. No full-suite success is
claimed from that restricted run.

The reproducible synthetic report semantic SHA-256 is
`aafa280515a496c285dd8d47451afebdc524c69ddbc6fa1490b1a68e84c02b03`.
The fixture additionally pins hand-calculated identity/request/receipt,
bootstrap, primary/variant denominator, calibration and sensitivity goldens.
A 100-case preparation test emits exactly 600 requests; case 101 is rejected
before output. These checks validate file/protocol/scoring behavior only.
Real independently adjudicated development/held-out corpora, external model
identity/license/egress authorization, saved official-version conformance and
response collection remain separate operator work.

### Local permission retry

The full unit run passed 4,472 tests with 12 skips; 45 failures reproduced on
unchanged HEAD under restricted socket/cache permissions. Re-running exactly
those 45 failed nodes with local hermetic socket/cache access passed all 45
in 26.19 seconds. Across the full run and targeted retry, 4,517 unit tests passed
and 12 were skipped. No production provider or model was contacted. Full-project
Ruff, Black and mypy passed; Grounding passed 40 cases with zero unsafe assertions
and Operational Learning passed 18/18. Focused experiment tests passed 65/65.

### Local container verification

The Docker build of implementation commit `00b4ec6` passed on Linux ARM64 as
`tacit:offline-eval-local`, with the configured `tacit` user. Disposable
network-disabled containers passed `tacit --version` and
`python -m tests.eval.evidence_decisions --help`, exposing the `prepare` and
`score` commands. No inference, model collection or production startup occurred.
