# Sync/Async Boundary Roadmap

## Status

Active follow-up roadmap. The runtime-shared provider manager remains the
foundation, while later changesets may deliver bounded slices of the boundaries
below. A row is still deferred unless the delivery ledger names the exact
implemented slice; partial delivery never certifies the rest of that row.

Current delivery ledger:

- Change 7 has an authenticated, fail-fast baseline: global request/accounted-
  memory caps, wildcard-tenant subcaps, incremental ASGI accounting, one total
  read deadline, bounded health/rejection telemetry, and exact release on every
  terminal path. It does not admit response memory or add a waiting queue.
- Change 8A has cold readiness only: API startup prepares history, feedback,
  signals/bootstrap, and Operational Knowledge schema/migrations under one
  configurable snapshot-copy cap before runtime or optional work starts. Store
  leasing and deterministic close remain deferred.
- Change 10A has the API-owned Slack containment slice: one bounded daemon loop
  owner per runtime identity, deadline-bounded shutdown, late-callback
  revocation, and process-lifetime fencing after non-cooperative close. Direct
  low-level Slack callers and reusable HTTP/SDK client pools remain deferred.

## Authority

[ADR-023](adr/023-contain-blocking-bedrock-before-native-async.md) is the
authoritative decision for containing blocking Bedrock work and for the order
of the native-async migration. The
[foundation invariant matrix](foundation-invariant-matrix.md) is the
authoritative implementation and review contract. If this roadmap conflicts
with either source, the ADR and matrix win. Durable changes to ownership or
product behavior require an ADR amendment before implementation.

The [engineering design notes](engineering-design-notes.md) remain useful
context, but they do not turn a deferred item into a delivered guarantee.

## Current Merge Blockers

The following work belongs to the current runtime-shared provider-manager
changeset. It must not be deferred into this roadmap:

1. Dependency bundles for one runtime share one provider manager, generation,
   admission authority, and lifecycle owner under overlapping API, Slack, CLI,
   and direct-Python use.
2. Caller cancellation cannot let generation cleanup race an operation that is
   still settling.
3. Close failure, child cancellation, and owner-loop loss fence executable
   resources and converge all threads and permits to zero. A permanent cleanup
   failure latches a process-lifetime fatal circuit for that runtime; recovery
   requires process restart, not a later generation in the same runtime. The
   runtime admission layer is the sole owner of that bounded, immutable
   process-lifetime circuit; providers and generic cleanup paths must not keep
   parallel registries. A permanent capacity hold is not an acceptable
   terminal state.
4. The implementation, ADR, matrix, and tests agree on accepted-resource loop
   ownership. A test cannot silently expand a guarantee that the ADR defers.

The current changeset is complete only when the lifecycle matrix passes, two
same-runtime dependency bundles overlap without duplicate ownership, all
terminal-failure counters reach zero, and independent principal, security,
SDET, and scaling reviews of the whole diff are clean. This roadmap assumes
that foundation but does not certify that it exists.

## Audited Boundary Inventory

Audit snapshot: current working tree against `origin/main` on 2026-08-27. This
inventory identifies concrete owners for follow-up planning; it does not certify
that a boundary is safe merely because its implementation uses an `async` API.

| Current boundary | Concrete surfaces in this branch | Owning follow-up |
|---|---|---|
| Blocking Bedrock transport | Operation-scoped Boto3/Botocore credential discovery, STS, client construction, `converse`, parsing, and close | 1-4 |
| Accepted provider and context clients | OpenAI, Azure OpenAI, Anthropic, Ollama, A2A, MCP, and RAG clients created behind the provider graph, with library-owned pools and close behavior | 6 and 10A |
| Backend and learning HTTP clients | Grafana, SignalFx, PagerDuty, datasource adapters, discovery fan-out, crawlers, retries, and synchronous `Response.json()` decoding | 10A and 10B |
| Long-lived socket clients | API-owned Slack Socket Mode has a bounded daemon-loop containment owner; direct low-level Slack callers, connection pools, reconnect/backoff, callback/event admission, and general socket clients remain | 10A and 12 |
| Synchronous SQLite stores | API cold readiness covers history, feedback, signal/bootstrap, and knowledge migrations; leasing, close, protected-path inspection, live-WAL snapshots, transactions, and steady-state query execution called by async API/pipeline paths remain | 8A and 8B |
| Structured configuration and registries | YAML/JSON settings, signal and archetype registries, package resources, reload, and validation | 9A |
| General filesystem and archive work | Runbook/incident reads, directory traversal, generated-archetype quarantine, assessment/export bundles, evaluation artifacts, release image/binary archives, compression, temporary files, and atomic publication | 9B |
| Threads and default executors | Provider service owner, admission maintenance, lifecycle blocking/cleanup workers, `asyncio.to_thread` waits/joins, and CLI doctor's `ThreadPoolExecutor` | 12 |
| Subprocesses and external tools | Demo-flow and evaluation command execution, CI release/scan helpers, and any future isolation of a noncooperative SDK or external helper | 13 |
| CPU work on event loops | JSON/YAML decode, validation, intent/scope parsing, archetype selection, compilation, evidence resolution, ranking, hashing, serialization, archive compression, and regex/fuzzy scans | 9A, 9B, 10B, and 11 |
| Inbound buffering | Authenticated ASGI request-body and decode-memory admission is implemented; response memory and any future waiting queue remain outside that baseline | 7 |

Any new call site in one of these families joins the listed owner and gate. It
does not create a local semaphore, executor, client pool, retry budget, or
cleanup task as a substitute. The first implementation commit for each row must
refresh this inventory and the foundation matrix from a repository-wide search.

## Delivery Contract

Each numbered item below is an independently mergeable changeset unless it
explicitly contains ordered subchangesets. Every changeset must:

1. select and, when needed, amend the applicable foundation-matrix rows;
2. add failing matrix and no-side-effect tests before production code;
3. name one runtime owner, admission owner, capacity-release owner, and cleanup
   owner before crossing a loop, thread, process, or runtime boundary;
4. commit a reproducible baseline or load artifact when a gate depends on
   measurements;
5. run focused tests, the full hermetic suite, and independent principal,
   security, SDET, and scaling review against the target branch; and
6. ship an independent disable or rollback path that does not silently fall
   back to a different owner, credential source, database, or executor.

Numbers below are initial release gates, not implementation claims. Changing a
number requires the owning changeset to record evidence and update the matrix
or ADR before production code changes. Metrics must use bounded labels and must
not expose prompts, response bodies, credentials, tenant identifiers, database
paths, or document content.

Rollback is required when an enabled boundary violates an ownership invariant,
fails to converge its counters to zero, loses required audit attribution, or
breaches its accepted error, latency, memory, or cardinality gate in two
consecutive observation windows. Each section names the rollback mechanism;
rollback never authorizes an unbounded or globally owned fallback.

## Changeset Definitions

The identifiers below are stable roadmap references, not execution order.
The dependency graph later in this document is authoritative: changeset 5
precedes changeset 1.

### 1. Native `aiobotocore` Lifecycle And Service-Model Pinning

**Owner and entry points:** The runtime execution graph is the sole owner of the
async session, Bedrock client, credential-refresh work, SSL context, loader
state, and `AsyncExitStack`. API lifespan, Slack, CLI, and direct-Python
composition roots own only generation-fenced root handles into that graph.

**Prerequisites:** All current merge blockers and changeset 5 are closed. The
runtime identity, credential declaration, region, endpoint, transport mode,
SDK/service-model identity, admission controller, and shutdown owner are
immutable inputs to the generation. A compatibility investigation selects and
locks an exact `aiobotocore`/`botocore` pair whose Bedrock service model exposes
the required operations. Before adoption, inventory DNS, TLS, model loading,
credential refresh, and default-executor work; any library-created worker makes
changeset 12 a prerequisite.

**Scope:** Pin the compatible SDK pair and service-model identity; initialize
the session, loader, SSL state, client, and `AsyncExitStack` on the declared
owner loop during readiness; close them through the same owner at shutdown.
Freeze retries, proxy behavior, TLS/CA inputs, endpoints, and a conservative
connection cap derived from runtime admission as correctness configuration.

**Non-goals:** No user-facing `converse` switch, streaming, numerical
connection-pool optimization, generic async factory, or automatic fallback
from partially initialized native state to Boto3.

**Acceptance gates:**

- The lock, wheel metadata, and release constraints resolve one tested
  `aiobotocore`/`botocore` pair; a service-model fingerprint test fails if the
  required operation or modeled response shape changes.
- One hundred cold readiness cycles create and close exactly one session/client
  generation each, with zero retained clients, threads, permits, or response
  bodies after every cycle.
- A committed cold-start artifact records loader, certificate, SSL, and client
  initialization timing. Reference CI p95 readiness is at most 5 seconds and no
  synchronous SDK file, loader, or SSL initialization occurs on the request
  event loop after readiness.
- Limit and limit-plus-one lifecycle tests prove one owner per runtime,
  reference-counted leases, one final close, and zero state after shutdown.
- Credential rotation, readiness cancellation, partial construction, close
  failure, and application shutdown pass the cross-runtime lifecycle matrix.

**Observability and scaling:** Record reason-coded readiness duration, SDK/model
identity, active leases, active operations, client generations, close duration,
and terminal failure count. Labels identify a runtime by a non-secret bounded
class, not by account, endpoint, profile, or tenant. One runtime may own at most
one active Bedrock client generation.

**Rollback:** Keep the operation-scoped ADR-023 bridge selectable until native
readiness and lifecycle parity pass. A failed native start leaves no published
client and may select the bridge only before a request starts, never midway
through a request or after native side effects.

### 2. Non-Streaming `converse`

**Owner and entry points:** The native Bedrock client from changeset 1 serves
the existing provider interface used by API, Slack, CLI, benchmarks, and direct
Python. The provider manager owns calls and response disposal.

**Prerequisites:** Changeset 1 is merged and its readiness gate is green. A
frozen fixture corpus covers supported request fields, model families, normal
responses, modeled service errors, malformed responses, and credential
rotation.

**Scope:** Parse and validate supported `converse` requests and responses,
propagate one absolute deadline, map modeled failures to stable provider errors,
and dispose every response through the native owner.

**Non-goals:** No streaming, speculative retries, pool tuning, new model
features, or implicit fallback to the blocking bridge after native admission.

**Acceptance gates:**

- The frozen corpus has 100% request/response parity for supported fields and
  100% deterministic rejection for unsupported or malformed shapes.
- One thousand hermetic calls with cancellation before admission, during I/O,
  after response arrival, and during disposal leave zero active operations,
  response bodies, pool leases, and admission permits.
- At configured concurrency `C`, exactly `C` calls may be active; `C + 1`
  queues or rejects through the existing runtime controller without creating a
  second controller or connection.
- Deadline tests reject every late result. Provider-error fixtures disclose no
  response body or credential material and retain stable reason codes.
- The native and bridge implementations produce equivalent normalized provider
  results on the frozen compatibility corpus before native becomes the default.

**Observability and scaling:** Record queue wait, service latency, total
latency, cancellation phase, modeled error class, active operations, and
response-disposal duration. Do not record prompts or generated text.

**Rollback:** A release flag may restore the bridge as the next-request
implementation while it remains supported. In-flight native calls finish or
fail under their original generation; no call switches transport in place.

### 3. Bounded `converse_stream`

**Owner and entry points:** The same provider manager and native client own the
event stream. API streaming routes, Slack streaming adapters, CLI output, and
direct async consumers receive bounded application events rather than the raw
SDK stream.

**Prerequisites:** Non-streaming `converse` is the default and has a clean soak
artifact. The stream protocol, terminal events, disconnect semantics, and
partial-result policy are frozen in tests before implementation. API SSE,
Slack, CLI, and direct-consumer queues each have an explicit bounded capacity,
slow-consumer policy, disconnect path, and terminal cleanup owner; the current
dashboard SSE queue is not reused as an unbounded token transport.

**Scope:** Parse modeled event frames; enforce frame, wire-byte, output-byte,
idle, queue, and total-deadline budgets; apply backpressure; close the response
on disconnect, cancellation, error event, or normal completion.

**Non-goals:** No pool tuning, resumable streams, automatic replay, silent
partial success, or unbounded fan-out to multiple consumers.

**Acceptance gates:**

- Initial defaults are at most 1 MiB per event frame, 16 MiB total wire bytes,
  8 MiB emitted text, 30 seconds idle time, and 32 queued parsed events. Limit
  succeeds and limit plus one fails before the excess event is exposed.
- A blocked consumer stops additional reads once the 32-event queue is full;
  measured resident stream buffering remains within the declared byte budgets
  plus one SDK frame.
- Disconnect and cancellation close the SDK response and return its pool slot
  within 5 seconds in hermetic tests.
- Modeled error events fail the stream. Partial output is discarded by default;
  an explicitly typed partial result may be enabled only by a separate product
  decision and is never reported as a complete answer.
- Ten thousand generated frames across normal, malformed, truncated, error,
  idle, cancellation, and backpressure cases leave zero bodies, tasks, leases,
  and pool slots.
- Limit and limit-plus-one tests cover API SSE, Slack, CLI, and direct
  consumers independently; no downstream adapter can buffer beyond the stream
  owner's declared queue and byte budgets.

**Observability and scaling:** Record active streams, queue occupancy, wire and
output byte buckets, time to first event, idle timeout, backpressure duration,
close duration, and terminal reason. Never record frame payloads.

**Rollback:** Disable the streaming capability and direct callers to
non-streaming `converse`. Do not buffer the full stream to emulate streaming.

### 4. Measured Connection-Pool Tuning

**Owner and entry points:** The runtime provider manager owns pool configuration
for all Bedrock entry points. The existing runtime admission controller remains
the only request-capacity authority.

**Prerequisites:** Changesets 1-3 are stable. A reproducible benchmark measures
concurrency 1, 4, 16, and the configured runtime limit for both streaming and
non-streaming calls, including credential refresh and cancellation.

**Scope:** Tune connection count, keepalive, and timeouts only from benchmark
evidence. Derive the maximum connection count from the runtime limit.

**Non-goals:** No second semaphore, provider-local executor, unbounded keepalive,
or default change justified only by a microbenchmark.

**Acceptance gates:**

- A tuning change must reduce p95 queue wait or end-to-end latency by at least
  15% at one declared supported load without regressing either metric by more
  than 5% at another supported load.
- Error and timeout rates may not worsen by more than 0.1 percentage points;
  p99 event-loop delay may not worsen by more than 10%.
- Open plus connecting sockets never exceed the configured runtime limit, and a
  30-minute saturation soak ends with zero leaked sockets or pool waiters.
- Limit-plus-one tests prove that pool capacity cannot bypass runtime admission.

**Observability and scaling:** Record active, idle, connecting, and waiting
connection counts; queue wait; reuse rate; timeout class; and saturation. The
benchmark artifact records hardware, SDK versions, configuration, and raw
percentiles.

**Rollback:** Pool settings remain runtime-configurable. Reverting to the last
validated values requires no schema or provider-interface change.

### 5. Runtime-Scoped Credential-Plan Composition

**Owner and entry points:** One composition-root credential-plan owner serves
API lifespan, Slack runtime, each CLI invocation, and direct-Python runtime
construction. Per-operation credential generation remains with the admitted
provider operation.

**Prerequisites:** Stable runtime identity and settings-generation identity.
The allowlisted sources and rejection behavior in ADR-023 remain unchanged
unless amended separately.

**Scope:** Centralize plan capture, compatibility comparison, settings-change
invalidation, and secret-safe diagnostics. Every entry point receives the same
immutable plan handle for one runtime generation.

**Non-goals:** No new ambient credential source, SSO, credential process,
metadata service, credential caching beyond the modeled generation, or secret
material in runtime identity.

**Acceptance gates:**

- API, Slack, CLI, and direct Python pass one shared parity suite and produce
  the same declaration or stable rejection for every credential fixture.
- One hundred concurrent dependency bundles for one runtime perform one plan
  capture. A settings-generation change causes exactly one invalidation and one
  new capture.
- Explicit/store/service settings disagreement fails before file traversal,
  token read, subprocess execution, SDK construction, or network access.
- Rejected traffic performs zero credential-source reads. Rotated modeled
  temporary credentials remain valid under the unchanged declaration, while a
  pinned source-principal change fails before Bedrock client construction.

**Observability and scaling:** Record capture count, invalidation count,
declaration class, source class, rejection reason, and capture duration without
paths, account IDs, profile names, access keys, or tokens. At most one plan
capture may be active per runtime generation.

**Rollback:** Disable composition-root caching and return to one admitted,
operation-scoped capture only if ADR-023 still permits it. Never fall back to
ambient Boto3 discovery.

### 6. Generic Async-Factory Removal And Accepted-Resource Ownership

**Owner and entry points:** The runtime execution graph owns accepted providers,
context clients, backends, and future adopted async resources. API, Slack, CLI,
direct Python, refresh, replay, and benchmarks receive typed leases, not a
generic submit-any-factory helper.

**Prerequisites:** The provider-specific manager is merged and its ownership
model is explicit. An inventory identifies every generic factory submission and
every resource that can outlive the realizing call.

**Scope:** Remove or make private generic async factory APIs; require typed
resource specifications, stable runtime identity, owner-loop declaration,
atomic adoption, operation tracking, revocation, and owner-loop cleanup. Add
bounded lifecycle observability for adopted resources. Add a public,
generation-validated controller query for inherited request admission before
moving another adapter off the provider manager's localized compatibility
read; the query must distinguish active, released, stale-inherited, and foreign
controller contexts without exposing lease tokens.

**Non-goals:** No universal executor abstraction, arbitrary cross-loop object
transport, forced thread termination, or support for a resource without a
cooperative bounded close contract.

**Acceptance gates:**

- Repository search and an invariant test find zero public production call
  sites capable of submitting an undeclared arbitrary factory.
- Two independently constructed bundles for one runtime overlap through one
  generation and one owner; two distinct runtimes remain isolated.
- Incompatible resource specifications fail before factory invocation. Stale
  generation handles fail without mutating the current generation.
- One thousand acquire/use/release cycles, including cancellation and loop
  loss, end with zero operations, leases, owner threads, cleanup tasks, and
  executable quarantine entries.
- Cooperative close completes within the declared 30-second shutdown budget;
  adapters that cannot meet it are rejected or routed to changeset 13.

**Observability and scaling:** Record owner generations, active leases and
operations, adoption and revocation counts, cleanup phase/duration, stale-handle
rejections, owner-loop health, and bounded quarantine count. One runtime has at
most one lifecycle owner per declared resource class.

**Rollback:** Keep typed legacy adapters behind the same graph while call sites
migrate. Do not restore a public generic factory or global executor fallback.

### 7. Aggregate Inbound Request Admission Before Buffering

**Owner and entry points:** Application lifespan owns one aggregate body-byte
and active-request admission service for HTTP API and browser/Slack webhook
traffic. Route handlers retain a lease through buffering, decoding, model
validation, and disposal.

**Prerequisites:** Preserve the existing per-request limit. Define aggregate
byte budget `B`, active-request budget `R`, queue budget `Q`, and wildcard-tenant
fairness before changing middleware.

**Scope:** Reserve capacity before accepting body bytes, account incrementally
for ASGI chunks, stop reads at budget exhaustion, release on success, malformed
input, disconnect, cancellation, or handler failure, and expose bounded
utilization metrics.

**Non-goals:** No payload logging, disk spooling, unbounded queue, or claim that
response-memory admission is solved.

**Acceptance gates:**

- Aggregate byte usage never exceeds `B` plus one declared maximum ASGI receive
  chunk. At `B` the request succeeds; `B + 1` is rejected or queued before the
  excess bytes are retained.
- Active requests never exceed `R`; queued requests never exceed `Q`; limit plus
  one has a stable 4xx/503 outcome and performs no model validation or route
  side effect.
- A disconnect or cancellation returns all body and request permits within 1
  second in hermetic tests.
- Under wildcard tenancy, one tenant cannot consume every active slot when
  `R > 1`; a two-tenant saturation test preserves at least one eligible slot
  and bounded queue progress for the second tenant.
- Malformed, deeply nested, and slow-chunk bodies pass the same accounting and
  end at zero retained bytes and leases.

**Observability and scaling:** Record active and queued requests, active and
queued bytes, queue wait, rejection reason, disconnect count, and per-tenant
fairness counters using opaque bounded tenant buckets.

**Rollback:** Disable aggregate admission while retaining the per-request limit
only through an explicit emergency setting and release note. Rollback may reduce
protection but must not bypass authentication or change route behavior.

### 8. SQLite And Store Lifecycle

This boundary is intentionally split into two independently mergeable
changesets.

#### 8A. Startup, Readiness, Leasing, And Close

**Owner and entry points:** Application lifespan and CLI/direct composition
roots own history, feedback, signal, knowledge, and other required stores. API,
Slack, CLI, direct Python, migration, benchmark, refresh, and replay receive
store leases from that owner.

**Prerequisites:** A complete store inventory and required-versus-optional
classification. Existing protected-path, role-identity, migration, and tenant
ownership invariants remain authoritative.

**Scope:** Perform path admission, role checks, migrations, schema validation,
bootstrap, and required-store readiness before traffic; reference-count leases;
reject new leases during shutdown; close stores deterministically.

**Non-goals:** No ORM migration, query rewrite, async SQLite claim, shared-path
relaxation, or hidden global singleton fallback.

**Acceptance gates:**

- Clean, upgraded, interrupted, corrupt, locked, unwritable, ownerless wildcard,
  and conflicting-role databases pass the persistence matrix before the server
  reports ready.
- One hundred concurrent lease cycles initialize each store once and close it
  once. Shutdown rejects new leases and reaches zero leases and connections
  within 30 seconds for cooperative stores.
- Required-store failure prevents readiness; optional-store degradation has one
  stable reason and does not initialize a global substitute.
- Read-only ownership and migration preflight materializes a stable disposable
  main/WAL snapshot before any SQLite open. The authority file is never opened
  by SQLite merely to create its inspection copy, and source movement retries
  remain under one absolute byte/time budget.
- A committed cold-readiness artifact records migration, bootstrap, path
  admission, and open duration per store without revealing paths or tenants.

**Observability and scaling:** Record readiness phase/duration, open stores,
active leases, migration version, lock wait, close duration, and degraded reason.

**Rollback:** A store may return to lazy construction only through an explicit
compatibility setting that preserves the same owner and never falls back to a
process-global path.

#### 8B. Steady-State Execution And Process-Wide Limits

**Owner and entry points:** A process-level store execution budget partitions a
declared connection and worker limit among runtime-owned stores and all entry
points.

**Prerequisites:** 8A is merged. A baseline artifact measures event-loop delay,
query latency, lock wait, busy retries, connection count, and throughput for the
actual production queries. Changeset 12 is merged before this work creates or
uses a worker; an async adapter that proves it creates no worker still registers
its connections and managed tasks with the process owner.

**Scope:** Choose bounded offload or an async database adapter from evidence;
enforce process-wide connection limit `D`, worker limit `W`, and queue limit
`Q`; preserve transaction, cancellation-before-start, busy-deadline, migration,
and shutdown-drain semantics.

**Non-goals:** No per-store private executor, unbounded connection pool, ORM as
a performance fix, or cancellation that abandons an active writer transaction.

**Acceptance gates:**

- Open SQLite connections never exceed `D`, active workers never exceed `W`,
  and queued operations never exceed `Q` across all runtimes and stores.
- `D + 1`, `W + 1`, and `Q + 1` tests have stable queue/rejection behavior and
  eventually return all counters to zero.
- At the baseline's declared supported load, p99 event-loop delay is no worse
  than the lower of 25 ms or 110% of the recorded pre-change value, while
  throughput is at least 95% of baseline.
- Writer race, lease expiry while waiting for a lock, cancellation before start,
  process failure after each transaction phase, and shutdown drain pass with no
  split authoritative state.

**Observability and scaling:** Record connection/worker/queue utilization,
queue and lock wait, busy retries, transaction duration, cancellation phase,
and shutdown drain. Production query-plan tests exercise exact SQL.

**Rollback:** Keep the prior execution adapter behind the same process budget
and store lease contract until parity passes. Rollback cannot reintroduce an
unbounded per-store executor.

### 9A. Bounded Structured-Document I/O

**Owner and entry points:** One application-owned loader serves curated and
bootstrap YAML/JSON registries used by startup, reload, CLI validation,
benchmarks, and direct Python.

**Prerequisites:** Inventory every structured-document call site and freeze its
last-known-good replacement semantics. Changeset 12 is merged before parser
offload creates a thread or process worker.

**Scope:** No-follow regular-file admission, bounded reads, safe YAML parsing,
JSON/YAML structural budgets, parse deadline, validation, and atomic registry
swap only after the complete document succeeds.

**Non-goals:** No remote fetching, arbitrary YAML tags, partial registry merge,
or use of parser limits as a substitute for semantic validation.

**Acceptance gates:**

- Initial per-document limits are 5 MiB input, depth 64, 100,000 nodes, 64
  aliases, and 1 MiB per scalar; limit succeeds and limit plus one fails before
  registry mutation.
- A 500 ms parse budget is enforced in the bounded owner. A late result cannot
  replace the registry.
- Symlink, non-regular file, alias expansion, deep nesting, oversized scalar,
  malformed input, path replacement, and cancellation retain the previous
  complete registry and leave no temporary artifact.
- One hundred concurrent reload attempts publish at most one validated revision
  per content identity and never expose a partial registry.

**Observability and scaling:** Record document class, admitted bytes, node/depth
buckets, parse/validation duration, revision identity, and rejection reason.
Never record paths or content.

**Rollback:** Retain the previous immutable registry revision. Disabling reload
does not revert to unbounded parsing; startup remains bounded.

### 9B. Bounded Filesystem, Source-Ingestion, And Archive I/O

**Owner and entry points:** Application lifespan owns filesystem admission used
by API and background ingestion. A CLI or CI invocation owns its bounded local
file operation. Runbook and incident learning, directory crawls, generated
archetype quarantine, assessment/export bundles, evaluation artifacts, release
archives, configuration publication, and direct Python use share the same
descriptor-based boundary.

**Prerequisites:** Inventory every read, traversal, temporary file, archive,
compression, and publication path. Preserve the stronger protected SQLite and
credential-file contracts; this changeset may reuse them but may not weaken or
silently bypass them. Changeset 12 is merged before async file or compression
work creates a thread or process worker.

**Scope:** Admit descriptors before reads or traversal; reject links and
non-regular inputs where the product contract requires regular files; bound
file count, depth, bytes, archive entries, expanded bytes, compression CPU, and
aggregate in-flight memory. Publish complete outputs atomically, report
post-publication durability uncertainty, and clean temporary files through the
operation owner after cancellation or failure.

**Non-goals:** No arbitrary archive extraction, recursive traversal without a
root capability, content or path logging, unbounded in-memory bundle creation,
or automatic retry after an output may have been published.

**Acceptance gates:**

- Every source family has declared per-file, per-operation, and aggregate byte
  and file-count limits. Limit succeeds and limit plus one fails before source
  extraction, authority mutation, archive publication, or output replacement.
- Symlink and path-replacement races, sparse files, special files, directory
  cycles, source mutation during read, disk-full, cancellation, and temporary
  file cleanup preserve the old complete result or no result.
- Archive creation bounds entry count, input bytes, compressed output, metadata,
  and CPU time. A cancelled or late compressor cannot publish an output after
  its operation has failed.
- Async API/crawler paths perform no admitted filesystem or compression work on
  the request event loop. CLI-only local work still uses the same safety and
  publication contract even when it executes synchronously.

**Observability and scaling:** Record operation class, admitted file/byte
buckets, traversal depth, archive entry/output buckets, queue wait, I/O and CPU
duration, publication phase, and stable rejection reason. Never record path,
filename, source content, archive member name, tenant, or credential material.

**Rollback:** Disable the affected ingestion/export capability while retaining
the bounded reader and atomic publisher. Do not return to direct unbounded
`Path.read_text()`, recursive traversal, or in-memory archive construction on an
async path.

### 10A. Async HTTP, SDK, And Long-Lived Socket Lifecycle

**Owner and entry points:** The runtime execution graph owns each accepted
HTTP/SDK client and any reusable pool for providers, context sources, backends,
and learning connectors. Operation-scoped clients remain admitted through that
same graph. The Slack composition root owns long-lived Socket Mode state. API,
Slack, CLI, crawlers, refresh, replay, benchmarks, and direct Python receive
typed leases rather than constructing request-local or module-global clients.

**Prerequisites:** Inventory HTTPX, OpenAI, Azure OpenAI, Anthropic, Ollama,
Grafana, SignalFx, PagerDuty, A2A, MCP, RAG, Slack, and library-created DNS/TLS,
retry, keepalive, task, and thread state. Freeze endpoint, credential, proxy,
TLS, retry, and idempotency declarations before a client or socket opens. A
client with library-created workers depends on changeset 12 unless it can prove
that those workers are already registered and bounded by the process owner.

**Scope:** Create and close each client on its declared owner loop; derive
connection and operation limits from aggregate runtime/process admission;
propagate one absolute deadline through queueing, retries, backoff, I/O, body
disposal, and close; close response bodies on every terminal path; and bound
Slack reconnect, callback, and event queues. Keep response decoding in 10B.

**Non-goals:** No process-global client, per-adapter capacity authority, ambient
proxy/credential drift after admission, unbounded retry, retry after ambiguous
non-idempotent side effects, raw SDK stream escape, or automatic fallback to a
different client owner.

**Acceptance gates:**

- Two same-runtime dependency bundles share one compatible client generation
  per reusable resource declaration; operation-scoped clients remain
  individually admitted, and distinct runtimes remain isolated. Incompatible
  endpoint, credential, proxy, TLS, or retry declarations fail before network
  or DNS I/O.
- Connection, active-operation, retry, callback, reconnect, and queue counts
  have aggregate limits. Limit plus one queues or rejects without opening an
  extra connection, task, SDK pool, or adapter-local worker.
- Cancellation during DNS/connect/TLS, request write, response read, retry
  sleep, callback execution, reconnect, and close leaves zero operation leases,
  response bodies, pool waiters, and abandoned socket tasks.
- Required-client readiness failure prevents traffic; optional integrations have
  one explicit disabled/degraded state. Partial construction, readiness
  cancellation, credential or endpoint generation change, and close failure
  publish no half-initialized client and cannot mutate a later generation.
- One absolute deadline includes queue and retry time. Only explicitly declared
  idempotent operations retry, and shutdown returns only after cooperative
  client/socket close and pool drain reach zero.
- A load artifact covers API, Slack, CLI-in-process, and crawler overlap and
  records connection reuse, event-loop delay, memory, throughput, and fairness
  across at least two runtimes or tenants.

**Observability and scaling:** Record bounded client class, generation, active
and idle connections, pool/operation queue wait, retry reason/count, reconnect
state, callback queue occupancy, deadline phase, response-disposal duration,
close duration, and terminal reason. Never record endpoints containing secrets,
headers, bodies, prompts, tokens, tenant IDs, or exception strings.

**Rollback:** Disable the adapter or long-lived integration at startup while
leaving its shared client owner and limits intact. Do not fall back to a local
`AsyncClient`, a synchronous SDK on the event loop, or a hidden library pool.

### 10B. Bounded Remote Response Decoding

**Owner and entry points:** One shared decoder contract serves Grafana,
Prometheus-compatible backends, SignalFx, CloudWatch adapters, learning crawlers,
webhooks, and future HTTP integrations.

**Prerequisites:** Inventory adapters, compression modes, content types, current
item limits, and whether each client exposes compressed bytes before decoding.
Adapters that cannot enforce pre-decompression limits remain unsupported until
they can.

**Scope:** Enforce compressed, decompressed, expansion-ratio, JSON
depth/node/scalar, decode-CPU, and aggregate in-flight memory budgets before
adapter result-cardinality limits.

**Non-goals:** No automatic retry with a larger limit, payload logging,
best-effort truncated JSON, or assumption that item count bounds encoded size.

**Acceptance gates:**

- Initial per-response limits are 8 MiB compressed, 32 MiB decompressed, 100:1
  expansion ratio, depth 64, 250,000 nodes, and 1 MiB per scalar. Each limit and
  limit-plus-one case fails before adapter-domain objects are published.
- One application-owned aggregate decode budget `M` covers compressed buffers,
  decoded buffers, parser structures, and queued results; measured usage stays
  within `M` plus one network chunk.
- Decompression bomb, deep JSON, oversized scalar, malformed encoding,
  cancellation, timeout, and connection loss leave zero decoder leases and no
  partial adapter result.
- Each adapter migrates in a separate commit or PR and passes the shared corpus
  plus one adapter-specific live-shape fixture.

**Observability and scaling:** Record compressed/decoded byte buckets,
expansion-ratio bucket, structure buckets, decode duration, active memory
leases, and stable rejection reason without remote bodies or URLs containing
secrets.

**Rollback:** Roll back an adapter to its prior implementation only while that
prior path is explicitly disabled in production. There is no unbounded decoder
fallback after the shared boundary becomes required.

### 11. Pipeline CPU Isolation

This boundary is delivered as two independently mergeable changesets: 11A adds
instrumentation and freezes the baseline; 11B isolates only measured hotspots
after changeset 12 supplies the process-wide worker owner.

**Owner and entry points:** The runtime execution graph owns any bounded CPU
worker or process pool used by intent selection, compilation, evidence,
ranking, validation, hashing, or serialization across API, Slack, CLI,
benchmarks, refresh, replay, and direct Python.

**Prerequisites:** Land instrumentation first. Commit a baseline artifact that
records scenario corpus, hardware, runtime settings, stage CPU, event-loop
delay, throughput, latency, output fingerprints, and clean versus representative
long-lived state. The artifact establishes exact numeric thresholds before any
offload implementation. Changeset 12 is then required before 11B can create or
use workers.

**Scope:** Move only stages proven to violate the baseline event-loop budget to
one bounded owner. Preserve deterministic inputs, context propagation,
cancellation, deadline accounting, and output fingerprints.

**Non-goals:** No blanket offload, per-stage executor, performance claim from a
single prompt, output change, or mixing of this work with ranking-quality
changes.

**Acceptance gates:**

- The baseline declares supported load `L`, throughput `T`, p95/p99 loop delay,
  and per-stage CPU. A stage qualifies for offload only if it contributes at
  least 20% of p99 loop-delay violations or exceeds 10 ms CPU in at least 5% of
  runs at `L`.
- After isolation, p99 event-loop delay is at most `max(20 ms, 110% of the
  uncontended baseline)` at `L`; throughput is at least `0.95T`; p95 end-to-end
  latency is no worse than 110% of baseline.
- Normalized output fingerprints match for 100% of the frozen corpus. The
  100-prompt clean and representative long-lived-state gates show no quality
  regression.
- Worker count and queue length stay within the process-wide limits from
  changeset 12; cancellation and deadline tests end at zero retained work.
- The baseline distinguishes GIL-bound Python work from native code that
  releases the GIL. The selected thread or process owner is justified by that
  evidence, and process serialization/input-output bytes are admitted before a
  process task starts.

**Observability and scaling:** Record per-stage CPU/wall time, event-loop lag,
queue wait, active workers, cancellation phase, output-equivalence failures,
and throughput artifact identity.

**Rollback:** Select the prior inline stage implementation at process startup
while retaining instrumentation and admission. Never dynamically switch an
in-flight stage between owners.

### 12. Process-Wide Worker Cardinality And Shutdown

**Owner and entry points:** Application lifespan owns one process worker
registry shared by every runtime and by API, Slack, CLI-in-process, crawlers,
benchmarks, stores, pipeline CPU work, and cleanup.

**Prerequisites:** Inventory every thread, executor, maintenance worker, async
task population, and process, including library-created DNS/resolver and SDK
workers. The initial inventory explicitly includes the provider service owner,
admission maintenance thread, lifecycle blocking/cleanup workers,
`asyncio.to_thread` waits and joins, CLI doctor's `ThreadPoolExecutor`, pipeline
publish/cleanup tasks, adapter fan-out tasks, HTTP/SDK pools, Slack tasks, and
any event loop default executor. Classify fixed service owners, normal work,
cleanup, CPU work, and subprocess work without allowing one class to borrow
another's authority implicitly.

**Scope:** Enforce process-wide worker limit `W`, active managed-task limit `T`,
queue limit `Q`, per-runtime partitions, startup rollback, broken-worker
recovery, shutdown rejection, drain, join, and zero-state reporting. Replace
implicit default-executor submission with typed registry operations or prove
that the library's bounded internal executor is included in `W`.

**Non-goals:** No one global queue that defeats tenant/runtime fairness, daemon
thread as a shutdown policy, force-killing arbitrary Python threads, or hidden
provider-local pool.

**Acceptance gates:**

- The sum of fixed owners and active normal, cleanup, CPU, and subprocess
  workers never exceeds `W`; queued work never exceeds `Q`. Limit plus one has a
  stable rejection or bounded wait and starts no extra worker.
- Managed background, reconnect, fan-out, publish, and cleanup tasks never
  exceed `T`; `T + 1` has a stable bounded outcome and creates no hidden child
  population.
- Thread-start definite and ambiguous failures roll back exactly once and invoke
  no work inline. Broken-worker replacement cannot temporarily exceed `W`.
- Cooperative shutdown rejects new work, drains admitted work, joins every
  worker, and reaches zero active/queued/retained state within 30 seconds.
- Two runtimes at saturation preserve their configured partitions and cannot
  create `2W` workers. A process-level load artifact proves the bound under all
  production entry points.
- Cancellation of a coroutine waiting in `asyncio.to_thread`, an SDK helper, or
  a default executor does not release registry capacity before the underlying
  work exits; stopped requester loops cannot strand an unowned worker.

**Observability and scaling:** Record workers, managed tasks, and queues by
bounded class, startup failure, retirement, broken-worker replacement, queue
wait, saturation, shutdown phase, and retained work. Export process totals that
can be alerted against `W`, `T`, and `Q`.

**Rollback:** Disable new worker-backed features in reverse dependency order.
The registry and hard process limit remain; rollback must not restore unmanaged
thread creation.

### 13. Managed Subprocesses And Isolation For Noncooperative Boundaries

**Owner and entry points:** A process-level command/isolation manager, when
runtime subprocess support is approved, owns subprocess admission, executable
identity, argument and environment projection, IPC, output capture, deadlines,
termination, descendant cleanup, and shutdown for every runtime entry point
using an external command or uncooperative blocking adapter. Build- and CI-only
commands retain their reproducible release-workflow owner.

**Prerequisites:** Inventory every `subprocess` and external-tool call, including
CLI/demo-only commands, and classify unsupported, synchronous-local, or runtime
managed use. Create a decision ADR before production isolation. Each blocking
adapter must first pass a cooperative-close conformance test under cancellation,
transport timeout, owner-loop loss, and shutdown. A failure means either
subprocess isolation or explicit unsupported status; it does not justify a
permanent thread/permit hold.

**Scope:** Give all runtime-launched commands one shell-free, typed, bounded
execution boundary. Decide whether to reject noncooperative SDKs or isolate them
in a bounded subprocess. If isolation is selected, use a narrow typed protocol,
allowlisted executable identity, minimal environment, bounded input/output,
process-group termination, and a fixed process/queue budget integrated with
changeset 12.

**Non-goals:** No general remote-code worker, arbitrary object serialization,
secret inheritance, automatic retry after unknown side effects, or claim that a
subprocess makes a non-idempotent operation safe.

**Acceptance gates:**

- The inventory classifies every command as build/CI-only, synchronous local
  CLI, runtime managed, isolated adapter, or unsupported; no async path invokes
  an unclassified command.
- The ADR lists every blocking adapter and records a supported, isolated, or
  rejected decision with a reproducible conformance result.
- An injected noncooperative call that ignores cancellation and close is
  terminated with its descendants within 5 seconds of the isolation deadline;
  all IPC descriptors, process slots, and temporary secret material are gone.
- Active subprocesses never exceed configured limit `P`; queued jobs never
  exceed `Q`; `P + 1` and `Q + 1` start no unaccounted process.
- Crash, malformed IPC, oversized output, parent cancellation, parent shutdown,
  and kill escalation produce stable errors and no partial success. One hundred
  fault cycles end with zero child and descendant processes.
- Spawn failure, executable replacement, argument injection, inherited file
  descriptors, environment leakage, blocked stdout/stderr, and a stopped caller
  loop fail without an orphan, leaked secret, or unbounded output buffer.

**Observability and scaling:** Record process/queue counts, startup and kill
duration, exit class, deadline phase, output-byte bucket, and orphan detection.
Do not record command-line secrets, environments, payloads, or child output.

**Rollback:** Disable the isolated adapter or return it to unsupported status.
Do not fall back to an unbounded in-process thread for a provider that failed
the cooperative lifecycle contract.

### 14. Non-POSIX Runtime And Release Support

**Owner and entry points:** One platform-admission owner covers application and
CLI startup, protected database paths, secret-file publication, packaging,
release smoke tests, and every published executable target.

**Prerequisites:** Record an explicit Windows threat model for owner and ACL
validation, junction/reparse-point handling, SQLite sidecars, atomic file
replacement, directory durability, and process shutdown. A version-only binary
smoke test is not platform support.

**Scope:** Implement native Windows equivalents for the POSIX protected-path
and secret-publication guarantees, then restore the Windows release artifact
only after the packaged executable completes setup, offline readiness, SQLite
open/write/checkpoint/reopen, and terminal runtime drain on a Windows runner.

**Non-goals:** No silent weakening of ownership checks, no POSIX API emulation,
no partial configuration publication, and no artifact whose only exercised
command is `--version`.

**Acceptance gates:**

- Path admission rejects symlinks, junctions, reparse-point redirection,
  non-regular entries, and unauthorized ACLs before file or SQLite mutation.
- Secret publication sets the intended owner-only ACL before the first byte,
  replaces atomically, reports post-publication durability uncertainty without
  retrying, and never leaves a readable temporary file.
- Real-process SQLite tests cover create, WAL sidecars, concurrent open/write,
  checkpoint, reopen, and last close without weakening role identity.
- The exact packaged executable runs noninteractive setup and an offline
  settings/runtime readiness workflow on the release runner, then exits with
  zero runtime owners, workers, handles, and temporary artifacts.

**Observability and scaling:** Record only bounded platform class, admission
reason, setup phase, runtime-drain phase, and stable failure category. Never
record usernames, paths, ACL principals, credentials, or configuration values.

**Rollback:** Remove the unsupported platform from the publication matrix. Do
not publish a known-inoperable binary or bypass protected-path validation.

## Dependency Order

```text
current runtime-shared provider manager
  -> 5 credential-plan composition
  -> 1 native lifecycle and SDK pinning
  -> 2 non-streaming converse
  -> 3 converse_stream
  -> 4 measured pool tuning

current runtime-shared provider manager
  -> 6 generic factory removal and accepted-resource ownership

7 aggregate ingress admission
8A store readiness
11A pipeline CPU baseline
12 process-wide worker/task lifecycle
  -> 8B steady-state store execution when it uses workers
  -> 9A structured-document parser offload
  -> 9B async filesystem and archive execution
  -> 11B measured pipeline CPU isolation
  -> 13 runtime-managed subprocess execution
10A async HTTP/SDK/socket lifecycle -> 10B remote response decoding
  (10A also depends on 6, and on 12 for library-created workers)
9A/9B inventory, limits, and synchronous startup/CLI safety may land before 12
13 subprocess inventory/design may start before 12
14 non-POSIX support is independent but gates restoring those release targets
15 observability delivery ownership may inventory independently; any queued or
   worker-backed exporter depends on 12
```

Changesets 7, 8A, the inventory/limit portions of 9A and 9B, 11A, and the
inventory/design portion of 10A may proceed independently after the current
merge blockers close. Runtime-managed HTTP/SDK clients in 10A depend on the
typed-resource work in 6; 10B may build its bounded decoder independently but
an adapter migrates only after its 10A owner exists. Any 8B, 9A, 9B, 10A, 11B,
or 13 implementation that creates threads, processes, library workers, or
default-executor work waits for the process-wide ownership implementation in
changeset 12.

### 15. Observability Delivery Ownership

**Owner and entry points:** One process composition owner governs asynchronous
metric export, log delivery, buffering, flush, and shutdown across API, Slack,
CLI, workers, and direct-Python runtimes. Producers emit only bounded,
secret-safe records and never own exporter workers.

**Scope and prerequisites:** Inventory synchronous logging and every metrics or
trace exporter. Define bounded queues, cardinality and byte budgets,
backpressure or drop policy by severity, flush deadlines, and terminal drain.
Any queued or worker-backed implementation waits for changeset 12.

**Acceptance gates:** Limit and limit-plus-one tests prove bounded memory and
label cardinality. Exporter failure cannot block lifecycle cleanup, expose
payloads or credentials, or silently discard required audit events. Shutdown
finishes within its declared deadline and reports bounded dropped-event counts.

**Rollback:** Disable the exporter and retain bounded local diagnostics; never
fall back to an unbounded queue or synchronous network delivery on request
loops.

## Final Program Exit

This roadmap is complete only when every boundary is either implemented behind
its named owner and quantitative gate or explicitly rejected as unsupported by
an ADR. Completion requires:

- no blocking SDK resource escaping an admitted owner;
- no generic factory, store, HTTP/SDK client, long-lived socket, decoder, parser,
  filesystem/archive operation, request body, CPU task, worker, or subprocess
  population without aggregate admission and deterministic shutdown;
- zero executable work retained after terminal cleanup failure;
- load artifacts proving limit and limit-plus-one behavior at process and
  runtime scope; and
- whole-program principal, security, SDET, and scaling review against the
  target branch.

Documentation alone does not satisfy any exit gate.

## Immediate Next Changeset

After the current containment changeset merges, the next PR is changeset 5,
runtime-scoped Bedrock credential-plan composition. It moves the stable,
secret-safe credential declaration to the shared runtime composition boundary
and proves API, Slack, CLI, and direct-Python parity before credential I/O. It
does not introduce `aiobotocore`, reusable clients, `converse_stream`, pool
tuning, a generic worker pool, or changes to unrelated persistence and HTTP
boundaries.

Changesets 1 through 4 follow only after that composition boundary is green.
Changesets 6 through 15 remain independently reviewable debt with the owners,
prerequisites, acceptance gates, observability, and rollback paths above. A
finding in one of those deferred families opens or updates its owning changeset;
it must not silently expand the current containment PR.
