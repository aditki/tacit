# Foundation Invariant Matrix

Status: mandatory implementation and review guidance

Last reviewed: 2026-08-18

This document turns Tacit's recurring cross-cutting failures into a test-first
engineering contract. It applies to humans and coding agents. The living
engineering notes explain the invariants; this matrix defines the surfaces and
failure modes that must be considered before implementation.

## Required workflow

For every change:

1. Classify the foundations touched by the change.
2. Select the applicable rows from this document.
3. Write failing contract, matrix, and no-side-effect tests before implementation.
4. Implement through a shared service or ownership boundary.
5. Run the focused tests, the relevant quality gates, and the full diff review
   against the target branch.
6. Record covered rows and justified exclusions in the PR description.

For decomposed work, repeat steps 3-5 as an acceptance loop after every wave:
implementation, focused verification, independent principal architecture and
security/reliability reviews of the whole diff, finding fixes, and re-review.
Dependent waves remain locked until both reviews are clean. A final whole-diff
review still checks cross-wave interactions and does not replace these gates.
Whole-diff findings that are wholly owned by a deliberately locked dependent
target are recorded as mandatory acceptance criteria for that target rather
than forcing the dependency graph to be violated. Findings in the current or
completed targets block immediately, and nothing may be deferred beyond its
owning target or the final whole-diff review.

For a behavior-preserving refactor, capture the existing behavior first. For a
bug fix, at least one test must fail before the fix. Tests written only around
the function named by a review comment are not sufficient for a foundation
invariant.

## Pattern escalation rule

Stop local patching and return to design when either condition is true:

- The same invariant is missing from two entry points, owners, stores, or
  lifecycle transitions.
- A fix requires another special case, fallback, private-field probe, repair
  path, or duplicated permission/tenant check.

At that point:

- Enumerate the complete owner and entry-point set.
- Add or amend the matrix before changing implementation.
- Introduce one shared abstraction or state transition.
- Test the abstraction at its public boundary and test every adapter once.

Do not continue with one-comment, one-function patches after this trigger.

## Change classification

| Foundation | Change triggers | Required matrix sections |
|---|---|---|
| Runtime composition | Settings, factories, stores, services, clients, backends, caches | Runtime ownership; side-effect ordering; scaling |
| Tenant and authorization | Tenant fields, API/CLI flags, permissions, credentials, reads or mutations | Tenant and permission; entry points; side-effect ordering |
| Governed authority | Candidates, revisions, mappings, projections, corrections, usage | Lifecycle and authority; concurrency; persistence |
| Investigation lifecycle | Runs, events, revisions, refresh, replay, contracts | Lifecycle and authority; concurrency; replay and fingerprints |
| Learning and ingestion | Dashboards, alerts, artifacts, connectors, crawls, FTS | Runtime ownership; side-effect ordering; scaling; source lifecycle |
| Retrieval and ranking | Intent, scope, archetypes, signal resolution, evidence, candidates | Scope and provenance; quality gates; long-lived state |
| Persistence and migrations | Schema, SQL, indexes, table rebuilds, legacy data | SQLite protected path; persistence and migration; concurrency; query plans |
| API and browser UX | Routes, headers, pagination, response models, static UI | Entry points; tenant and permission; UX and packaging |
| Packaging and release | Resources, commands, schemas, versions, wheels | UX and packaging; quality gates |

## Entry-point matrix

Every shared capability must account for each applicable entry point.

| Entry point | Boundary that must be tested | Typical hidden failure |
|---|---|---|
| HTTP API | Request tenant, permissions, app-scoped dependencies, status mapping | Route checks differ from service checks |
| Browser UI | Tenant header/body, stale view state, pagination, error rendering | Selected tenant is not sent by one tab or fallback request |
| CLI | Fresh settings after env loading, permissions, nonzero failures | Module-global settings differ from store settings |
| Direct Python | Public service authorization and ownership checks | API protections are bypassed by embeddings |
| Background crawl | Runtime ownership, bounded work, source checkpointing | Per-request bounds do not limit aggregate work |
| Refresh | Recorded tenant, captured parent revision, request-scoped dependencies | Old prompt is parented to a newer revision |
| Replay | Recorded tenant, exact inputs, current runtime, parent CAS | Stale or cross-tenant knowledge is applied |
| Benchmark and assessment | Isolated settings, tenant, permissions, deterministic corpus | Production config changes the gate result |
| Migration/startup | Legacy owner policy, atomic schema transition, restartability | Ownerless data is exposed or stranded |

If an entry point is intentionally unsupported, reject it explicitly and test
the rejection. Silence or global fallback is not an exclusion.

## Runtime ownership matrix

One operation has one composition owner. Before file, network, database, cache,
or history side effects, identify and compare all supplied owners:

- explicit runtime settings
- request or application settings
- CLI runtime store container
- dependency factory descriptor
- realized history, feedback, and signal stores
- Operational Knowledge service and repository
- remote backend and its client
- effective endpoint, organization/account, and credentials after overrides
- cache or index identity where learned state affects output

| Case | Expected result | Required no-side-effect assertion |
|---|---|---|
| No owner supplied on a legacy default path | Resolve one documented default owner | Only the documented default is used |
| One explicit owner | Use that owner end to end | No process-global fallback |
| Multiple equivalent owners | Proceed | Same tenant, permissions, stores, and remote identity observed |
| Settings disagreement | Fail closed | No store initialization, file read, or network call |
| Tenant or permission disagreement | Fail closed | No lookup, schema creation, or mutation |
| Store and repository path disagreement | Fail closed | No migration, projection, or candidate write |
| Backend/client endpoint disagreement | Fail closed | Remote call count remains zero |
| Effective credential override differs from settings | Fail closed | Remote call count remains zero |
| Factory returns an owner different from its descriptor | Fail closed | Returned dependency is never consumed |
| Ownerless injected factory/store/service/backend | Fail closed | No fallback to a global owner |
| Injected dependency is unavailable | Preserve explicit unavailable state | No global retry or fallback |

Required tests must include construction-time and realization-time disagreement.
A factory can be valid while the object it returns is not.

## SQLite protected-path matrix

Wave 1 supports ordinary SQLite on POSIX under a protected-path threat model.
Other platforms fail before path creation or SQLite access until an equivalent
owner/ACL admission implementation exists. Path preflight is a
configuration-time admission check, not a Python emulation of a SQLite VFS and
not a connection-lifetime inode lease. After admission, SQLite owns its native
connection, WAL/SHM generations, checkpoints, and close lifecycle.

| Configuration or operation | Required result | Required evidence |
|---|---|---|
| Effective role map contains the same canonical path twice | Reject the complete configuration before any store opens | No database, role, schema, or migration side effect |
| Existing paths for two roles identify the same file | Reject the cross-store alias before any store opens | Existing files and canary rows remain unchanged |
| Platform is not POSIX | Reject before path inspection or creation | Stable `sqlite_unsupported_platform` reason and no filesystem side effect |
| Any configured path component is a symlink after trusted system-root alias canonicalization | Reject without following it | No target access and no database creation |
| Existing main, WAL, or SHM entry is a symlink or non-regular file | Reject at configuration time without opening it as SQLite | FIFO/socket/device probes do not block; canaries remain unchanged |
| Ancestor is neither a service-owned application component nor a root-owned platform component, or is writable by another identity outside the narrow sticky-platform-temp exception | Reject before `sqlite3.connect()` | No mode repair and no SQLite side effect |
| Existing database or sidecar is not service-owned or is group/world-writable | Reject before `sqlite3.connect()` | Ownership and mode remain unchanged |
| Main file is missing beneath an admitted protected parent | Permit ordinary SQLite creation | Created file has the service owner and is not group/world-writable before role/schema mutation |
| SQLite reports a journal mode other than exact `wal` | Fail before role identity, schema, migration marker, or user-data mutation | Reopen shows the pre-attempt state |
| Existing file carries a different store role | Fail the first writer transaction | No schema or data from the requested role is committed |
| A known shared role table has a malformed or ambiguous column shape | Fail closed before claiming any role | No identity table, schema repair, journal-mode change, or data mutation |
| Tenant-owner metadata is absent, mismatched, or ambiguous for the configured boundary | Resolve or reject through a genuinely nonmutating owner snapshot before enabling WAL or entering structural setup | DELETE, closed-WAL, live-WAL, and hard-link denials preserve directory entries, main/WAL/SHM bytes, journal mode, schema, indexes, markers, and rows exactly |
| Owner admission observes a live WAL that changes during inspection | Retry the complete admission callback under one bounded deadline; never weaken source-stability validation | Concurrent first-open converges, retries are observable without paths or tenant values, and timeout fails closed |
| A live-WAL snapshot grows while main and WAL files are copied | Enforce the aggregate byte cap against bytes actually read, reserving each chunk before writing it | Source growth cannot exceed the cap on disk; the isolated copy is removed and authority files remain unchanged |
| Snapshot copy, SQLite inspection, or the trusted callback consumes the admission deadline | Share one absolute deadline across copy, query, callback, source verification, and retries | SQLite work is interrupted cooperatively; a trusted Python callback that returns late is rejected and never authorizes mutation |
| Owner admission observes a live rollback journal | Fail closed until rollback recovery completes under a trusted writer | Journal and database bytes remain exact; no pre-trust recovery is attempted |
| First-open role identity or structural schema setup fails | Roll back the structural writer transaction together | Fault injection after every structural statement reopens without a role/schema split |
| A pre-tenant schema needs a potentially large table rebuild | Keep the legacy table authoritative, prepare an empty shadow atomically, copy with durable keyset batches, then swap only after the complete copy | More than one batch, interruption after a committed batch, atomic final-swap failure, exact row preservation, and no current-schema marker before the swap |
| A bounded legacy tenant-owner backfill batch fails | Roll back only that batch while preserving prior completed batches and their durable cursor | Restart resumes at the last committed cursor and no final completion marker exists early |
| A keyset migration accepts zero, negative, sparse, or empty-string keys | Represent “not started” outside the legal key domain and copy every legal key exactly once, including an all-empty composite key | Boundary-key fixtures survive interruption and finalization without omission or retry loops |
| A migration preserves a source key that runtime scans or cursors also consume | Use the same legal domain end to end; runtime pagination, audit, quarantine, forward lookup, and reverse lookup may not reintroduce a narrower sentinel | Negative, zero, sparse, boundary, and empty-string fixtures remain reachable after migration and use indexed bounded plans |
| An optional SQLite capability such as FTS is unavailable | Persist a stable degraded capability decision or fail closed | Reopening twice converges without replaying schema and audit work indefinitely |
| Two processes race first-open | Revalidate role and tenant ownership as the first action after each structural `BEGIN IMMEDIATE`; equivalent owners converge and conflicting owners produce one winner | No loser schema, marker, tenant row, partial migration, or split identity |
| Knowledge and signal stores share one database | Require the knowledge repository to prove the signal role and durable tenant owner at construction and after every acquired write lock | Pinned mismatch, bound transaction, and real concurrent first-open tests leave the losing owner with zero schema, marker, or data writes |
| Store uses execute, cursor, transaction, context-manager, or close APIs | Preserve ordinary stdlib `sqlite3` behavior | No custom connection/cursor subtype, guard descriptor, poison state, CPython layout access, or override-dependent assertion |
| Cooperating processes open, write, reopen, checkpoint, and last-close a WAL database | Succeed under SQLite's normal sidecar lifecycle | Real subprocess test completes with exact committed rows and bounded timeouts |

The subprocess WAL acceptance test uses a real filesystem and public store-open
boundaries without monkeypatching SQLite or file operations. Two synchronized
processes race an absent database through first-open and exact WAL. While one
connection stays live, the other writes, commits, reopens, and verifies data. A
process runs `wal_checkpoint(TRUNCATE)`, the last owner closes, and a fresh
process reopens, verifies every committed row, writes, checkpoints, and closes.
Vary which process closes last. Do not assert that WAL/SHM pathnames, file
descriptors, or inodes survive last close.

Structural first-open atomicity and high-cardinality ownership backfills are
different contracts. Role identity, table/index/trigger structure, and the
metadata needed to begin reconciliation commit atomically. Potentially large
tenant-owner data moves then run in bounded keyset batches. When the tenant
dimension changes a primary or unique key, the legacy table remains the public
authority while an empty target-schema shadow is prepared; copy batches and
their cursors commit together; and one final writer transaction swaps the
complete shadow into the public name. Prior batches remain durable after a
later failure, a failed final swap restores the legacy public table, and the
database is not certified current until the terminal marker is written.
Mixed-version writers are not supported during an ownership migration; all
processes using the database must run the same Tacit release.

Same-UID pathname replacement after admission, swap-and-restore between
preflight and SQLite I/O, and hostile hot replacement are explicit exclusions.
No matrix test may claim that Python detects or safely continues through those
events. Planned replacement requires all Tacit processes to stop; stronger
descriptor-bound or VFS guarantees require a real SQLite VFS/native integration
or a server database.

Trusted operating-system aliases at the filesystem root are canonicalized
before component admission, for example macOS `/tmp` to `/private/tmp`. This is
not a general symlink-following rule. A root-owned sticky temporary ancestor may
be writable by other users only when it is not the final database parent and
the remaining application directory is service-owned and not group/world
writable. The final parent never receives this exception.

SQLite performance evidence comes from one checked-in benchmark command, not an
ad hoc timing note. The harness runs a stdlib control and the Tacit path with the
same temporary filesystem, schema, pragmas, warmups, operation counts, and
samples. It covers protected-path validation plus connect/WAL/close, single-row
commits, batched statements, checkpoint/reopen, and the subprocess lifecycle.
Machine-readable output includes the revision, Python and SQLite versions,
platform, filesystem root, journal/synchronous settings, parameters, failures,
descriptor delta, and latency/throughput percentiles. Empty samples or execution
errors exit nonzero; the exact command and output are review evidence.

## Tenant and permission matrix

| Tenant configuration | Request | Expected result |
|---|---|---|
| Pinned | Missing tenant | Resolve to the configured tenant where documented |
| Pinned | Matching tenant | Proceed |
| Pinned | Different tenant | Reject before lookup or mutation |
| Wildcard | Missing tenant | Reject |
| Wildcard | Concrete valid tenant | Proceed only with tenant-bound authentication |
| Wildcard | Reserved/bootstrap tenant | Reject |
| Wildcard | Duplicate tenant credential | Reject configuration or authentication |
| Wildcard with auth disabled | Any request | Reject configuration at startup |
| Legacy ownerless data under pinned migration | No recorded tenant | Assign the explicit pinned migration owner |
| Legacy ownerless data under wildcard migration | No recorded tenant | Fail before schema mutation |

For each semantic action, test the complete permission tuple at API, CLI, and
public service boundaries:

| Action | Permissions to consider |
|---|---|
| Read/explain/export | read, plus export where data leaves the system |
| Review/approve | review |
| Trust/teach | review, trust, read, and apply where activation occurs |
| Reject/ignore | reject and apply where active authority changes |
| Correct | correct to propose; review and apply to approve and activate |
| Policy override | override in addition to the underlying action |
| Refresh/non-exact replay | read and apply |

Permission denial must preserve the correct 4xx or CLI failure and occur before
resource initialization or external access.

## Side-effect ordering matrix

Test denial, mismatch, malformed input, and cancellation at every applicable
boundary. The required ordering is:

1. Resolve one runtime owner.
2. Resolve and authorize the tenant.
3. Authorize the semantic action.
4. Validate input identity, scope, parent revision, and limits.
5. Initialize persistence or remote clients.
6. Read files or call remote systems.
7. Compute and validate derived state.
8. Commit authoritative state and audit records atomically.
9. Publish best-effort projections or optional telemetry only where documented.

No-side-effect probes should assert the strongest observable boundary:

- database file does not exist
- remote client call count is zero
- file traversal or `read_text` was not invoked
- current revision and lifecycle state are unchanged
- no partial mapping, projection, usage, or audit row exists
- cache contains no cross-tenant or failed-result entry
- CLI exits nonzero and API returns the intended status

## Lifecycle and authority matrix

Authoritative state and runtime projections must be tested together.

| Transition | Concurrent or failure case | Required invariant |
|---|---|---|
| Candidate to approved/trusted | Opposing review | One CAS winner; provenance matches state |
| Approved to promoted revision | Evaluation failure | No active revision or projection on failure |
| Active to stale | Source disappears | Surviving support is recomputed before retirement |
| Stale to active | Source returns | Explicit reactivation; terminal withdrawal is not revived |
| Active to superseded/withdrawn | Correction applies | Pinned target verified and changed atomically |
| Correction pending to approved/applied | Target advances | Conflict; correction does not become falsely applied |
| Source pending/rejected/ignored | Projection retirement fails | Source and authority do not disagree visibly |
| Revision persisted | Usage or audit persistence fails | Documented atomicity or explicit degraded state |
| Publication commit starts | Authority mismatch or caller cancellation | All owners preflight before remote I/O; cancellation is deferred until publication and authoritative audit finish |
| Knowledge selected | Consuming stage does not use it | Remain considered, not applied |
| Applied usage | Counterfactual removes its output | Usage is downgraded with the output |

Cover candidate, approved, trusted, active, stale, reactivated, superseded,
withdrawn, expired, and rejected states. Terminal states remain terminal unless
an explicit authorized transition says otherwise.

## Concurrency and failure-injection matrix

For every read-check-write sequence, test:

| Race or fault | Expected result |
|---|---|
| Two reviews of one candidate | One winner; loser receives conflict |
| Evaluation versus rejection | Rejection or winning CAS cannot be overwritten |
| Refresh/replay versus newer revision | Stale parent is rejected |
| Correction versus target advancement | Pinned target remains unchanged on conflict |
| Source retirement versus review | Terminal review state is preserved |
| Lease expiry while waiting for SQLite lock | Expired worker cannot publish |
| Two SQLite processes race first-open and migration | Both converge on one transactional role and schema |
| One SQLite process checkpoints or last-closes while another cooperates | Ordinary WAL lifecycle completes without application-level generation rejection |
| Process failure after each transaction boundary | No split authoritative state |
| Retry after partial optional work | Idempotent result without duplicate revision or usage |
| Parallel crawls | Runtime-wide admission bound is preserved |

Inject faults after each durable statement or transaction phase, not merely at
function entry. Validate database state after reopening a new connection.

## Persistence and migration matrix

| Database state | Required coverage |
|---|---|
| Clean database | Schema, indexes, bootstrap data, and configured owner |
| Previous supported schema | Forward migration preserves behavior and tenant |
| Interrupted migration | Restart resumes or rolls back without stranded tables |
| Concurrent startup recheck | Every post-lock fast path validates the transactional role identity before returning |
| Pinned legacy owner | Tenantless data moves to the explicit owner |
| Wildcard legacy owner unknown | Startup fails before mutation |
| Shared or conflicting paths | Configuration fails before either store initializes |
| Existing file has another role identity | Startup fails without committing the requested role's schema or data |
| Corrupt, locked, unwritable, or full database | Required store fails; optional store degrades explicitly |
| Large table | Keyset progress, bounded transactions, and real production query plan |

SQLite role identity, tenant migration, schema migration, table rebuilds, and
their completion markers remain transactional. Markers are written only after
final locked validation. Test the exact production query, not a simplified
query that happens to use the intended index.

## Scope, provenance, replay, and fingerprint matrix

Test every scope dimension used by selection or conflict analysis:

- tenant, service, environment, datasource, region, cluster, namespace
- archetype, version constraint, valid-from, and valid-until

For each dimension test missing, exact, normalized-equivalent, disjoint, and
multi-value cases. Version selectors also need ranges, wildcards, exclusions,
arbitrary equality, and local-version syntax.

Exact-scope experimental retrieval requires every mandatory dimension to be
resolved and nonempty before filesystem access. Missing scope is a stable
skipped result, never an exact match on two empty sets.

Provenance tests must preserve exact source, lineage, query, observation,
candidate, context, knowledge, and revision references through refresh, replay,
removal, and reordering. Volatile timestamps must not change semantic output
fingerprints.

Replay tests cover exact, current-engine, counterfactual, unavailable inputs,
stale parents, changed engine policy, tenant mismatch, and fingerprint mismatch.

## Scaling and long-lived-state matrix

Every collection or background operation needs a documented bound.

| Dimension | Required test |
|---|---|
| Input size | At limit succeeds; limit plus one fails before partial writes |
| Directory traversal | Entry, file, and byte budgets; no symlink escape or path reopen race |
| Structured document | Node, depth, alias, scalar, and result-cardinality limits; one file validates atomically |
| Database rows | Query is keyset-paged and uses the exact intended index |
| Cursor and summary keys | Preserve every legal persisted value, including finite nonpositive timestamps, boundary integers, and empty text keys; reject only invalid non-finite cursors |
| Projection audit | Join a bounded key page through the tenant/governance/revision index; the exact 50,000-row production plan has no full mapping scan or temporary sort |
| Fan-out | Candidate/projection expansion is bounded before writer lock |
| Pattern matching | Aggregate scan and comparison budgets fail closed |
| Multi-stage resolution | One investigation-owned budget spans discovery, selection, compilation, evidence, and rescue |
| Object composition | Aggregate panels, queries, nested nodes, scalar characters, and bytes are admitted before allocation |
| Concurrent requests | Runtime-wide bound, not only per-request semaphore |
| Task creation | Worker or batch count is bounded; no gather over unbounded input |
| Source crawl | Completeness is true only when every source was retained |
| Long-lived learned state | Quality and latency compared with clean state |
| SQLite connection path | Reproducible stdlib-control comparison for open/WAL/close, writes, batching, checkpoint/reopen, and subprocess lifecycle |
| Cache | Tenant and every output-relevant input are in the key; size and TTL bounded |

Record timings, rows scanned, candidates considered, pattern checks, queue wait,
and configured budgets at expensive boundaries. A response limit alone is not a
work limit.

## UX, packaging, and observability matrix

- Browser requests carry the selected tenant on every tenant-aware tab and
  fallback request. Actions are bound to the tenant that rendered the row.
- Paginated UIs expose continuation and discard stale responses after tenant
  changes.
- Expected validation and concurrency errors map to stable API and CLI outcomes.
- New schemas, corpora, and data files are present in built wheels.
- Documented commands exist and are exercised in CI.
- Runtime manifests report the package version actually shipped.
- Success, degraded success, cancellation, timeout, stale conflict, and failure
  are distinguishable in structured events and metrics.
- Expected degraded events expose stable reason codes and bounded counters, not
  tracebacks, raw payloads, query text, tenant data, credentials, or local paths.
- Benchmarks used as gates exit nonzero on execution errors, empty corpora, or
  threshold failure.

## Quality gates

| Change area | Minimum additional gate |
|---|---|
| Knowledge, evidence, ranking, replay, contracts | Grounding and Operational Learning benchmarks |
| Intent, retrieval, signal resolution, archetypes, ranking | 100-prompt clean and representative long-lived state |
| Browser or API workflow | Hermetic E2E and browser security tests |
| Migration, package data, CLI command | Built-wheel smoke test |
| Query/index or crawl scaling | Production query plan and limit-plus-one test |
| Concurrency or transactions | Parallel race and crash/fault-injection test |
| SQLite path, connection, or migration | Protected-path matrix, real subprocess WAL lifecycle, transactional fault injection, and benchmark artifact |

Green tests do not replace whole-diff review. After implementation, request an
architecture and security review of the complete diff against the target branch,
including scaling. Review of only the latest patch is insufficient.

## PR evidence template

Every cross-cutting PR description should include:

```text
Foundations touched:
Matrix rows covered:
Tests written before implementation:
No-side-effect assertions:
Concurrency/fault cases:
Scaling bounds and query plans:
Quality gates run:
Whole-diff reviews:
Explicit exclusions with rationale:
Remaining risks and observability gaps:
```

An empty field must be written as `Not applicable` with a reason. It must not be
silently omitted.
