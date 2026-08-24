# Operational Knowledge Stabilization Plan

Status: active execution plan

Last reviewed: 2026-08-18

This plan applies the mandatory
[Foundation Invariant Matrix](foundation-invariant-matrix.md) to the current
Operational Knowledge branch. It replaces comment-by-comment patching with
owned architectural targets, matrix-first tests, and bounded parallel waves.

## Exit criteria

The stabilization is complete only when:

- every target below is complete or explicitly deferred with a documented risk
- every applicable matrix row has test evidence or a justified exclusion
- two independent whole-diff reviews against `main` report no P0 or P1 issue
- every P2 issue is fixed or explicitly accepted
- unit, integration, hermetic E2E, Ruff, mypy, Grounding, Operational Learning,
  clean-state, and representative long-lived-state gates pass
- required source, schema, corpus, and documentation files are tracked and
  present in the built wheel where applicable

## Coordination rules

- One target owns each production file during its wave.
- Tests fail before implementation begins.
- Workers do not expand their write set without coordinator approval.
- Shared integration files are modified by one target at a time.
- A target is integrated and its focused tests pass before the wave review starts.
- Agents review the complete current branch state, not only their own patch.
- A later wave does not start until the current wave completes the acceptance
  loop below.

## Per-wave acceptance loop

Every wave is an independent stabilization loop, not one step in a single
implementation pass:

1. Write the matrix-first tests and implement the bounded target changes.
2. Run the target's focused verification, static analysis, and applicable
   fault, concurrency, permission, tenant, lifecycle, and scaling tests.
3. Run independent principal architecture and security/reliability reviews of
   the complete branch diff against `main`, with explicit scaling assessment.
4. Fix every P0/P1 finding in the current or completed targets and either fix
   or explicitly disposition each corresponding P2. A finding wholly owned by
   a locked dependent target is recorded as a mandatory acceptance criterion
   for that target; it does not force the dependency graph to be violated.
5. Repeat verification and both principal reviews until the wave is clean.
6. Stage the accepted wave and only then unlock dependent targets.

The final whole-branch review remains required after all waves. It validates
cross-wave interactions; it does not replace any wave's own review loop.
No finding may be deferred beyond its owning target, and all findings must be
closed or explicitly accepted before the final review can pass.

## Target graph

### S0: Worktree completeness and guardrails

Status: complete

Purpose: make the matrix, contributor guidance, PR evidence template, and
runtime-ownership module impossible to omit from the change.

Write set:

- `AGENTS.md`
- `CONTRIBUTING.md`
- `.github/pull_request_template.md`
- `docs/foundation-invariant-matrix.md`
- `docs/engineering-design-notes.md`
- `docs/operational-knowledge-stabilization-plan.md`
- `tacit/runtime_ownership.py`

Gate: required files are tracked; documentation links and `git diff --check`
pass. Required matrix tests must be present in the Git index before the wave can
close; a dirty-worktree wheel is not sufficient evidence. S0 does not require
the retired SQLite connection wrapper to remain in the package.

### S1: Runtime ownership and SQLite architecture reset

Status: round-7-remediation-in-progress

Purpose: establish one public composition identity for settings, tenant policy,
permissions, database paths, remote identity, cache namespace, and explicit
unavailability. Remove private-field probing from ownership decisions. Replace
the custom `sqlite3.Connection` lifetime/VFS emulation with ordinary SQLite
connections operating under the protected-path contract below.

Primary write set:

- `tacit/runtime_ownership.py`
- `tacit/runtime_stores.py`
- `tacit/config.py`
- SQLite path preflight, connection, migration, and role-identity helpers
- public ownership properties in history, feedback, signal, and knowledge stores
- public ownership properties in remote clients and backends
- dedicated runtime-ownership and SQLite acceptance tests

Principal review reopened S1 for two independent architecture failures:

1. Retaining and policing main, WAL, and SHM generations in Python breaks
   ordinary multi-process WAL behavior. A cooperating SQLite process may create,
   checkpoint, unlink, or recreate sidecars as connections open and the last
   owner closes; treating those lifecycle events as a hostile rebind rejects a
   healthy database.
2. A `sqlite3.Connection`/`sqlite3.Cursor` subclass cannot establish a complete
   lifetime boundary. Inherited native APIs can bypass Python overrides, and
   base/native close or finalization paths can bypass wrapper cleanup. Adding
   more method overrides does not turn the wrapper into a SQLite VFS.

The supported SQLite threat model is a protected path, not hostile same-UID
mutation. The complete effective database-role map is validated before any
store opens. Every configured SQLite role uses a distinct canonical file;
same-path and detectable existing-file aliases are rejected across configured
and fallback paths. On POSIX, configuration-time path walking canonicalizes
only trusted root-level operating-system aliases such as macOS `/tmp`, then does
not follow application symlinks and rejects symlink components, special files,
and non-regular existing main, WAL, or SHM entries. Application directories,
database files, and pre-existing sidecars are service-owned and not writable by
group or other. A root-owned sticky temporary ancestor is accepted only above a
private service-owned application directory and never as the final parent.
Other platforms fail closed before path creation until equivalent ACL admission
exists. Validation reports stable, path-free reason codes and never repairs an
unsafe path with `chmod`.

After that admission check, ordinary SQLite owns the native handle and the full
WAL/SHM lifecycle. Tacit does not retain guard descriptors, inspect CPython
object layout, intercept every statement, pin inode generations, or poison a
store after pathname replacement. Every store still requires an exact `wal`
result before schema or data mutation. First-open role identity and structural
schema setup commit in one explicit writer transaction. Potentially large
legacy tenant-owner backfills then run as restartable keyset batches; each batch
and its cursor commit together, prior completed batches survive later failure,
and the final current marker is withheld until reconciliation completes.
Post-lock fast paths validate the same role before returning. Composition and
migration adapters consume public runtime, database, and projection-store
capabilities rather than private fields.

Same-UID pathname replacement, swap-and-restore between preflight and SQLite
I/O, and hostile hot replacement are explicitly out of scope. Planned database
replacement requires all Tacit processes to stop. Any claim of descriptor-bound
opening, immutable live pathname identity, sidecar generation pinning, or other
stronger VFS behavior requires a real SQLite VFS/native integration or migration
to a server database.

Required acceptance tests:

- Ownership coverage includes equivalent owners, every mismatch dimension,
  ownerless injection, explicit unavailable state, and denial before database
  creation, file access, or a remote call. Provider-specific account values such
  as SignalFx realms are canonicalized before URL construction, and malformed
  or empty ports fail before client creation.
- The protected-path matrix covers first creation, an existing regular file,
  every component and final symlink, FIFO/socket/device/directory entries,
  untrusted ownership, group/world-writable ancestors and files, path aliases,
  and every cross-role collision. Rejections happen before
  `sqlite3.connect()` or any schema, role, or data write.
- Unsupported platforms fail before filesystem creation. POSIX tests cover the
  narrow trusted system-alias and sticky-temporary-ancestor exception as well
  as rejection of a writable final parent.
- Connections and cursors used by stores are ordinary `sqlite3` objects. Tests
  exercise the standard execute, cursor, transaction, context-manager, and close
  paths without depending on interception, retained descriptors, poison state,
  or CPython layout inspection.
- Exact WAL failure leaves role identity, schema, migration markers, and user
  data unchanged. Fault injection after each structural first-open statement
  rolls role identity and schema back together. Fault injection in a bounded
  owner-backfill batch rolls back that batch, preserves the prior committed
  cursor, omits the final completion marker, and resumes without duplication.
  Concurrent first-open processes converge on one role and schema.
- Pre-tenant feedback and signal tables that require new tenant-qualified keys
  retain the legacy table as authority while an empty shadow is prepared.
  Copies run in bounded keyset transactions with durable cursors; the final
  rename/drop/index transition is atomic, and the current-schema marker is
  withheld until the swap succeeds. Tests exceed one batch and inject failures
  after a committed page and inside the final swap.
- Tenant-owner admission for existing history, feedback, and signal databases
  runs through one protected-path read-only connection boundary before WAL,
  role identity, or structural migration. The writer transaction repeats the
  decision. Denial tests compare journal mode, schema, markers, and user rows
  before and after the attempted open.
- Every keyset migration represents not-started separately from legal source
  keys and is tested with negative, zero, sparse, and boundary identifiers.
  Optional FTS support records an explicit available or unavailable capability
  result and converges on repeated startup.
- A knowledge repository that shares the signal database proves the same
  durable role and tenant owner before use and repeats both checks as the first
  actions after every acquired write lock, including caller-bound transactions.
  Conflicting first-openers leave no losing-side schema, marker, or data write.
- Runtime cursors and summaries preserve the complete legal domain admitted by
  persistence, including finite nonpositive timestamps, boundary integer IDs,
  and empty artifact identifiers. Invalid non-finite cursors fail as input
  errors rather than narrowing the persisted key space.
- Projection-audit validation uses bounded key rows and an indexed join against
  the exact tenant, governance reference, and revision identity. The production
  plan at 50,000 rows performs neither a full mapping-table scan nor a temporary
  sort.
- Startup failure diagnostics contain stable reason codes, bounded exception
  classes and fingerprints only; canary payloads, raw exception text, tenant
  values, authority identifiers, and filesystem details are not logged.
- A real-filesystem subprocess test, with no monkeypatching of SQLite or file
  operations, synchronizes two processes against a missing database. Both pass
  first-open and observe exact WAL; while one connection remains open the other
  writes, commits, reopens, and verifies data; a process performs a real
  `wal_checkpoint(TRUNCATE)`; the final owner closes; then a fresh process
  reopens, verifies all committed rows, writes again, checkpoints, and closes.
  The test uses bounded handshakes and child timeouts, varies which process is
  last to close, and makes no assertion that WAL/SHM pathnames or inodes persist
  across last close.
- A checked-in benchmark command compares the Tacit store-opening path with an
  ordinary stdlib SQLite control using the same temporary filesystem, schema,
  pragmas, operation counts, warmups, and samples. It covers protected-path
  validation plus connect/WAL/close, single-row commits, batched statements,
  checkpoint/reopen, and the subprocess WAL scenario. Machine-readable output
  records the revision, Python and SQLite versions, platform, filesystem root,
  journal/synchronous settings, workload parameters, failures, descriptor delta,
  and latency/throughput percentiles. It exits nonzero for errors or empty
  samples, and the exact command and output are retained as review evidence.

Current Wave 1 gate evidence (2026-08-20):

- `uv run pytest -q`: 2,556 passed, 54 deselected.
- `uv run pytest -m integration -q`: 25 passed.
- `uv run pytest -m e2e -q`: 29 passed.
- The complete signal, signal-migration, signal-startup, SQLite-admission, and
  artifact-learning slice is 586 passed. The complete knowledge, contract, and
  tenant-scoped history slice is 603 passed. These include real subprocess
  first-open races, interrupted migrations and projection repair, every legal
  integer boundary, empty text and composite keys, source growth during live-WAL
  copy, and shared-deadline expiry.
- `uv run mypy .`: 166 source files clean.
- `uv run ruff check .`, `uv run black --check .`, and `git diff --check`:
  clean.
- `tacit benchmark-grounding`: 40/40, trustworthy-answer rate 1.0, unsafe
  assertion rate 0.0.
- `tacit operational-learning-benchmark`: 18/18 with zero causal leakage,
  rejected/unresolved contribution, unsafe fuzzy resolution, or prompt-policy
  override.
- A fresh `0.1.1rc5` sdist and wheel were built under
  `/private/tmp/tacit-dist-round7-s1-20260820`. The wheel contains both benchmark
  corpora and all runtime schemas/data; an isolated install reported the correct
  version and passed both benchmark commands from outside the repository.
- Reproducible SQLite command:
  `uv run python -m tests.eval.sqlite_storage_benchmark --samples 10 --warmups 2 --batch-size 100 --subprocess-workers 4 --subprocess-writes 8 --output /private/tmp/tacit-sqlite-storage-benchmark-round7-s1.json`.
  It completed with zero failures and descriptor delta `+1`. Tacit versus
  stdlib-control p50 was 0.550/0.331 ms for connect/WAL/close, 1.274/0.985 ms
  for a single commit, 1.391/1.094 ms for a 100-row batch, 1.856/1.280 ms for
  checkpoint/reopen, and 119.105/144.857 ms for the 32-write subprocess WAL
  workload (272.9/227.5 operations per second). The coordinated first-open,
  overlapping-owner, checkpoint, both-last-close-order, and fresh-owner
  lifecycle measured 417.933/410.338 ms p50 (14.391/14.587 operations per
  second).

The 2026-08-13 custom-wrapper microprobe is historical only. The first
post-reset principal review round reproduced every gate and found bounded fixes
for unsupported-platform admission, export page sizing, benchmark provenance,
and missing-service retrieval status. S1 remains open until those regressions
pass and both independent principal whole-diff reviews are clean on the updated
state. The second round confirmed those fixes and added configured-path
benchmark isolation, coordinated lifecycle measurement, bounded startup
diagnostics, and a precise structural-versus-batched migration contract; those
regressions and the complete gate suite now pass pending round three.
Round three found incomplete partial-schema role signatures, raw identifiers in
history and later projection diagnostics, and monolithic pre-tenant feedback
and signal copies. The role registry now covers canonical, knowledge, FTS, and
interrupted-migration tables; diagnostics expose bounded fingerprints only; and
feedback plus signal migrations use the prepare/copy/final-swap contract.
Round four found remaining high-cardinality signal transforms, legal key values
that collided with migration sentinels, mutation before owner admission,
malformed shared-role ambiguity, non-convergent optional FTS startup, and an
unbounded history lease backfill. Those paths now use bounded restartable
batches, shared read-only admission, fail-closed role-shape validation, durable
capability state, and post-lock revalidation. The integrated S1/S4 storage matrix
was green at 1,007 tests before principal review.
Round five found two remaining S1 boundary mismatches. Runtime signal scans
still reserved zero and rejected empty names even though migration preserved
those legal keys, and ordinary SQLite read-only opens could create or update
authoritative WAL/SHM sidecars before owner admission. Runtime keysets now use
explicit absence across audit, quarantine, forward/reverse resolution, and API
pagination. Admission now uses immutable quiescent reads, bounded isolated
live-WAL snapshots with source-stability retry and telemetry, and fail-closed
rollback-journal handling. The refreshed integrated S1/S4 matrix is green at
1,022 tests. The complete default, integration, E2E, static, quality, package,
and storage-performance gates are green on the same frozen state, pending two
new whole-diff principal reviews.

Round six found four remaining S1 contract gaps: several runtime callers still
reserved legal empty or nonpositive keys after migrations preserved them;
all-empty composite projection cursors could be mistaken for “not started”;
live-WAL admission bounded the pre-copy size rather than aggregate bytes
actually copied and did not share one absolute deadline across every admission
phase; and a losing first-opener could reach structural work without repeating
tenant-owner admission under its acquired writer lock. The fixes use explicit
absence or a durable started bit end to end, chunk-reserved aggregate copy
budgets with cooperative SQLite interruption and late-callback rejection, and
post-lock owner validation before every structural migration write. Startup
diagnostics now retain only bounded classes, counters, and fingerprints. The
focused integrated signal/artifact slice is green at 586 tests and the complete
knowledge/history slice is green at 603 tests. The complete default, integration,
E2E, static, deterministic quality, installed-wheel, and storage-performance
gates are green on the same frozen tree; two new whole-diff principal reviews
remain required before S1 closes.

Round seven completed both independent whole-diff principal reviews. They
confirmed the live-WAL, deadline, legal-key migration, projection-repair, and
first-opener fixes, then found five bounded S1 integration gaps: the knowledge
repository did not independently enforce the signal database's durable tenant
owner; learning cursors rejected legal nonpositive timestamps; empty artifact
identifiers were omitted from summaries; projection-audit validation used an
unindexed disjunction; and a few knowledge, history, and signal diagnostics
still exposed raw paths or tenants. Those regressions are now the only S1 code
targets in progress. The reviews also identified later-wave acceptance criteria
already assigned to S2, S3, S5, S6, S7, and S8; they are mandatory follow-up
work, not reasons to widen this storage remediation.

### S2: Pipeline, API, remote, and cache composition

Status: pending

Purpose: validate factory descriptors and realized objects before starting
history or consuming a dependency. Bind app lifespan, providers, backends,
caches, semaphores, replay, and administrative mutations to the runtime owner.

Primary write set:

- `tacit/dependencies.py`
- `tacit/pipeline/runner.py`
- `tacit/api/dependencies.py`
- `tacit/api/app.py`
- `tacit/api/lifespan.py`
- backend, provider, context, and remote-client composition adapters
- `tacit/cache.py`
- administrative route and composition tests

Required tests: construction and realization mismatch, zero history rows before
preflight failure, zero remote calls, app/lifespan mismatch, replay owner
mismatch, cache isolation, runtime-scoped admission, and authorized global
archetype reload. Realized compatibility fallbacks must be checked against the
container descriptor before consumption. Every dependency and application
factory must close over the container's immutable settings snapshot rather than
the caller-owned `Settings` object. Admission must be runtime-owned and safe
across multiple applications, event loops, and cancellation.
Preflight the complete realized owner graph before initializing knowledge,
starting history, or calling dashboard/alert backends. Typed tenant,
permission, and ownership failures during knowledge pin or reconciliation must
fail publication instead of degrading as optional outages. Runtime admission
must expose bounded queue-wait and active-count instrumentation.
Streaming fallback and retries require a request-scoped idempotency identity so
one user action cannot start duplicate investigations. Provider caches must be
owned by every remote-account dimension, including region/account as well as
model. Direct assessment and bundle helpers enforce the same semantic read and
export permissions as API and CLI wrappers before store or output creation.
UI request identity includes the active API credential generation; changing a
credential invalidates in-flight tenant-scoped responses and clears rendered
tenant data.
At 390px, the primary tab row must keep Generate reachable after Knowledge is
added. Browser acceptance covers start-aligned horizontal scrolling or wrapping,
no page-width overflow, stale tenant-response suppression, and keyboard access.

Depends on: S1.

### S3: Complete selection provenance and replay semantics

Status: pending

Purpose: persist every scope dimension that controls knowledge selection and
make replay independent of future prompt-parser behavior. Reject
counterfactuals whose affected stages cannot be recomputed.

Primary write set:

- `tacit/investigation_contract.py`
- `tacit/schemas/investigation/v1.0.schema.json`
- `tacit/investigation_replay.py`
- replay-specific portions of `tacit/history.py`
- history API/CLI replay validation
- contract and replay tests

Required tests: every scope dimension across missing, exact,
normalized-equivalent, disjoint, multi-value, refresh, exact replay, and current
replay; legacy fingerprint preservation; unsupported context counterfactuals.
Generated shadow selection must consume the captured request scope and return
`not_applicable` before filesystem access when tenant or exact-scope identity is
missing; it must never reconstruct request identity from deployment settings.
Replay must treat the history store's snapshotted owner as authoritative,
reject disagreeing caller settings or knowledge services before lookup or run
creation, and never allow a permissive caller to override stored authorization.
Missing or ambiguous environment scope is not equivalent to an exact empty
environment for generated shadow evaluation.
The immutable request scope captures every selection dimension, including
region, cluster, namespace, archetype, and version constraints, and current
replay consumes that captured scope instead of re-parsing historical prose.
Context-removal counterfactuals must either deterministically recompute every
affected output stage or fail before persistence; filtering provenance while
reusing its dashboard or ranking is forbidden.

Can run in parallel with: S2, with exclusive ownership of `history.py` assigned
to S3 during the wave.

### S4: Explicit retrieval configuration and evidence obligations

Status: complete

Purpose: remove process-global archetype settings and preserve declared
evidence requirements when resolution fails.

Primary write set:

- `tacit/archetypes/engine.py`
- `tacit/pipeline/stages/archetypes.py`
- `tacit/evidence.py`
- `tacit/pipeline/stages/evidence.py`
- focused archetype and evidence tests

Required tests: two app-scoped configurations in one process; resolver failure
after declaration retains unresolved requirements, missing observations, and
abstention. Typed tenant and authority failures must propagate through initial
and rescue resolution, while ordinary secondary rescue-validation failures
retain the already validated dashboard. Degraded logs expose only stable,
bounded diagnostics. Experimental retrieval must bound aggregate artifacts,
panels, queries, returned results, and open descriptors across files, and its
persisted lifecycle status must distinguish degraded retrieval from a clean
no-match. Missing service or environment identity is a skipped, reason-coded
lookup before quarantine access, never a clean exact-scope no-match.

Can run in parallel with: S1.

### S5: Authority delivery and recovery

Status: pending

Purpose: make authority transitions strict and make cross-database usage/audit
delivery durable, observable, idempotent, and recoverable.

Primary write set:

- authority-delivery portions of `tacit/history.py`
- `tacit/pipeline/completion.py`
- `tacit/pipeline/recording.py`
- `tacit/knowledge/service.py`
- transactional learning-index portions of `tacit/signals/store.py`
- concurrency, crash-injection, and recovery tests

Required tests: fault after each transaction phase, pending delivery after
contract commit, retry without duplicate usage/events, bounded lease recovery,
and approve/reject/ignore rollback when the retrieval projection fails. Stored
and public failure records must use bounded reason codes, exception classes,
and fingerprints rather than raw exception text; canary secrets must not appear
in history, replay, contracts, or API responses.
Rejecting a pending candidate and retiring active authority are distinct
capabilities: active revision/projection withdrawal also requires apply
permission. History and feedback connections must enable and verify foreign-key
enforcement before use, with invalid child writes failing atomically.

Depends on: S3. `history.py` and `signals/store.py` are exclusive to S5 during
this wave.

### S6: Learning admission and source boundaries

Status: pending

Purpose: enforce realized remote ownership, bounded work, source lifecycle, and
one runtime-wide admission policy across dashboard, alert, artifact, directory,
PagerDuty, API, CLI, and direct Python paths.

Primary write set:

- new runtime-owned learning-admission primitive
- `tacit/dashboard_ingest/service.py`
- `tacit/alert_ingest.py`
- `tacit/artifact_learning.py`
- `tacit/integrations/pagerduty.py`
- learning routes and CLI adapters
- learning, connector, and bounded-work tests

Required tests: remote mismatch before I/O, runtime-wide concurrency, bounded
task creation, directory-entry and file-byte limits, PagerDuty item/page limits,
complete versus partial crawl, stale return, terminal-state preservation,
copied lineage, tenant/datasource isolation, and retry idempotence.
Mutable signal mappings that combine support from multiple source families must
persist `source_type` beside each individual source reference. A row-level last
writer type is not sufficient: refresh or retirement of one dashboard, alert,
runbook, or incident may remove only that source's support, including when two
families use the same external reference text. This requires a versioned,
restartable source-reference projection migration and collision regressions;
filtering the current row-level type is not an acceptable substitute.
Directory and artifact crawls must use the shared bounded no-follow crawler,
including clutter, symlink, byte, and file-swap cases. Dashboard, alert, and
connector crawls must use fixed worker pools so memory and queued work remain
O(configured workers), not O(discovered items).
Generated-quarantine writes must also use descriptor-relative no-follow
creation and atomic rename so a symlinked scope directory cannot redirect an
artifact outside the configured root.
Direct file learning must reject symlinks, FIFOs and other special files before
reading, enforce per-file and aggregate byte budgets, and validate runtime
ownership before traversal. Bulk dashboard and alert failures must return
bounded diagnostics without raw exception text.

Depends on: S1 and S2. Can run in parallel with S5 because its write set does
not include `history.py`, `knowledge/service.py`, or `signals/store.py`.

### S7: Executable quality and packaging gates

Status: pending

Purpose: turn correctness and long-lived-state expectations into reproducible
release evidence.

Primary write set:

- `.github/workflows/ci.yml`
- `tests/eval/`
- frozen clean and representative long-lived-state manifests
- package-data and installed-wheel smoke tests
- benchmark documentation

Required gates: Grounding, Operational Learning, clean 100-prompt, long-lived
100-prompt, hermetic E2E, installed-wheel resources, and stable nonzero failure
behavior. Expensive or credentialed gates may run as scheduled/release jobs, but
must remain reproducible and required before release.
Installed-artifact store-opening smoke tests must cover every advertised Python
implementation/version, SQLite runtime, and release platform using ordinary
`sqlite3` connections. Packaging must not depend on CPython object layout or
advertise a native SQLite VFS that the distribution does not ship.

Depends on: behavioral stabilization through S6.

### S8: Long-lived resolver scaling

Status: pending

Purpose: replace linear active-mapping scans with an indexed literal/trigram
candidate projection while retaining bounded glob fallback.

Primary write set:

- `tacit/signals/migrations.py`
- reverse-resolution portions of `tacit/signals/store.py`
- exact production query-plan and scaling tests

Required gates: no full scan or temporary sort on indexed paths; reverse
resolution at 10,000 and 50,000 mappings stays within recorded p95 budgets;
limit-plus-one fails closed without partial authority changes.
Candidate pruning must occur before reserving mapping-by-catalog work; unrelated
active mappings may not exhaust the Cartesian budget before indexed matching.

Depends on: S5 and a frozen performance baseline from S7.

## Parallel execution waves

| Wave | Parallel targets | Integration barrier |
|---|---|---|
| 0 | S0 | Required artifacts are tracked |
| 1 | S1 and S4 | Ordinary SQLite protected-path and retrieval gates pass, then both principal reviews repeat to clean |
| 2 | S2 and S3 | Composition and replay gates pass, then both principal reviews repeat to clean |
| 3 | S5 and S6 | Fault/concurrency gates pass, then both principal reviews repeat to clean |
| 4 | S7 and S8 | Quality/scaling gates pass, then both principal reviews repeat to clean |
| 5 | Final independent whole-diff architecture and security reviews | Cross-wave review has no unresolved P0/P1; P2 disposition recorded |

## Observability evidence

Each target must add or confirm structured measurements for its expensive or
degraded boundary. The final evidence includes dependency preflight failures,
runtime-owner identity without secrets, queue wait and active workers, source
and byte counts, transaction rollback, lease recovery, pending usage age and
retries, scope candidates, pinned revisions, evidence declaration/resolution,
rows scanned, pattern checks, benchmark state fingerprints, and cardinalities.
