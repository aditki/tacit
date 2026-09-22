# Foundation Invariant Matrix

Status: mandatory implementation and review guidance

Last reviewed: 2026-08-27

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

## Cross-runtime lifecycle design gate

Complete this gate before production implementation whenever work can outlive a
request coroutine, cross an event loop, use a worker thread or subprocess, or
construct a runtime-owned resource. The design is not implementation-ready
until it names exactly one authority for each row:

| Concern | Required declaration | Rejection condition |
|---|---|---|
| Runtime identity | Stable composition-root identity shared by every adapter. An explicit process identity is claimed only through the canonical controller factory; direct constructors begin isolated and may bind only when no live canonical authority exists | Two dependency graphs or direct constructors for one runtime can construct separate authorities, multiply capacity, or replace an active owner |
| Runtime service generation | One runtime execution graph/provider manager per admission namespace, shared by every dependency bundle for that runtime. A stable semantic provider specification decides compatibility before any factory executes | API, Slack, CLI, or direct bundles sharing a controller can create competing service-loop owners, duplicate provider factories, or select compatibility from closure identity |
| Runtime root lifetime | Every independently started composition root, including isolated/direct dependency builders, registers one generation-fenced owner token with the shared execution graph before public pipeline work. One root exit releases only that token; final root exit rejects new work, drains the graph, and permits a later root generation | One app can terminally shut down a sibling app, an isolated pipeline can run unmanaged, or a terminally shut-down graph can remain cached and poison a later lifespan |
| Direct provider injection identity | Settings-derived default LLM/context factories are semantically shareable. An explicitly injected LLM factory, context factory, or chained cleanup callback is compatible only when the exact retained declaration/callback object is reused; compare a process-local opaque identity without exposing it in logs or errors | Two different executable factories or cleanup authorities with identical ownership descriptors can silently share the first manager, execute the wrong implementation, or run the wrong cleanup |
| Injected SQLite product identity | Resolve and cache the selected history, feedback, Signals, and Operational Knowledge products before registering the runtime root. Readiness admits those exact products and generations; Signals and Knowledge share one exact admission. Every later accessor returns the pinned product, rejects capability or generation rebinding, and never reruns a changing compatibility or injected factory | A root can admit an internal store and later execute against a different injected product, a changing factory can create multiple generations, or late access can perform unadmitted SQLite snapshot and migration work |
| Admission | One controller for the aggregate runtime limit; reserve capacity and unambiguously commit worker startup before factory realization | A request, provider, cleanup path, or direct caller can create a private controller, or a factory can run before its worker owns capacity |
| Capacity lifetime | Permit is reserved before submission and released by the worker that consumes it | Release requires a coroutine, future callback, garbage collection, or live caller loop |
| Lifecycle owner exit | A state-owned thread-safe completion is published from the owner thread's outermost `finally`. Async shutdown shields and awaits that completion under a wall-clock deadline, then performs only `join(0)` | Provider shutdown borrows the caller loop's default executor, polls for thread exit, lets waiter cancellation cancel owner completion, or blocks indefinitely behind unrelated executor work |
| Resource adoption | Worker retains an unclaimed resource until one consumer atomically adopts it. Constructor-owned children are rolled back if any later constructor phase fails, before a product can be returned. Service-loop adoption uses only nonblocking ownership transitions | Cancellation or loop loss can leak or publish an unowned result, constructor failure can strand a child transport, or a foreign mutex can stall terminal monitoring and cleanup |
| Lease identity | Every provider lease carries the execution-graph nonce, generation epoch, and unique lease ID; release and callbacks compare all three before mutation | Duplicate, stale, or foreign releases and callbacks can decrement or retire a newer generation |
| Requester owner lifecycle | Work that must survive requester-loop loss is bound to controller ownership before submission. A task already started on a requester loop cannot be transferred afterward: its retained lease remains charged until that task really terminates, and final root drain waits rather than falsifying capacity. Provider leases abandoned by completed requesters are revoked by the generation-fenced final root transition; a merely paused live root remains active | Loop `is_closed()` heuristics either strand abandoned authority, revoke live paused work, or release capacity while resumable work still exists |
| Operation lifetime | Provider operations have generation-scoped active-operation ownership independent of run leases. Last lease release revokes new calls, drains active operations, then closes | Cancelling a cross-loop transport future can let cleanup overlap the still-settling provider call |
| Provider owner-loop transport failure | Before readiness, each provider generation installs exactly one owner-local terminal monitor. Cross-thread submission, ambiguity-probe, cancellation, and owner-stop callbacks are transports only: if callback delivery fails before or after enqueue, the generation records one primary terminal cause and the owner monitor makes the final submission decision. It settles unstarted committed submissions, preserves already-started operation results, retires handoffs and aggregate permits exactly once, completes provider cleanup, and releases the service owner from the owner thread. Recovery uses a fixed polling interval, at most two callback attempts per transition, and no caller-loop task, fallback controller, or additional thread | A live but unreachable owner loop can remain `revoked` with committed submissions, active operations, handoffs, normal permits, or its service-owner permit permanently charged; a retry can execute work twice; cleanup-stop transport failure can turn successful cleanup into an unrecoverable generation; or a secondary probe error can replace the primary operation/cancellation error |
| Cleanup | Cleanup inherits the resource's runtime and admission owner | Cleanup can run through a global fallback, another runtime, or an uncharged thread |
| Permit class | A normal worker may finish cleanup for resources it already owns, but a supplemental cleanup permit remains cleanup-only through synchronous and asynchronous re-entry | Cleanup capacity can be reused to realize new normal work |
| Failure rollback | Every pre-submit and post-submit phase, including owner-loop readiness, has one idempotent rollback owner; a rejected or abandoned product remains with the realizing owner through bounded cleanup | Cancellation while waiting for admission can strand transitional state, readiness can time out into an unbounded terminal wait, or cleanup can be handed back to a caller loop |
| Optional integration readiness | `starting`, `ready`, `reconnecting`, `failed`, and `stopped` describe confirmed external lifecycle state. `ready` follows a successful connection/handshake, not task scheduling. The API composition owns Slack on one bounded daemon event-loop thread per runtime identity rather than the application loop. Overlapping application roots subscribe to that one transport generation: non-final release removes only its status subscription, and the final subscriber owns transport cancellation. Subscriber admission, final-release commitment, primary pre-start abandonment, and natural task completion share one execution-state transition; lifecycle replay, cancellation, callbacks, and waits run only after that state lock is released. Each subscriber loop retains at most one pending wake-up; intermediate publications coalesce to the latest snapshot and terminal state wins. A stopped-but-open loop therefore cannot accumulate callbacks and receives the latest or terminal snapshot if it resumes. If the primary subscriber exits before a worker starts, it terminally settles the complete unstarted generation even when secondary subscribers remain, revokes every subscription's callback authority, publishes completion exactly once, and releases the registry entry immediately. A later root acquires a fresh generation rather than attaching to an ownerless `starting` generation. API-hosted Slack borrows the API root instead of acquiring a hidden second runtime root; standalone Slack still owns an explicit root. Every borrowed callback is pinned to that exact root generation and can never create a later generation after shutdown. Post-connect health probes have one in-flight owner and a fixed deadline; a timed-out non-cooperative probe is retained exactly once and stops further probes. Optional close has an inner deadline shorter than the application-root shutdown deadline, terminal shutdown revokes callback authority, and optional work cannot block required runtime cleanup or process exit. Terminal result publication and resource retirement are separate transitions: if inner cleanup times out with a live task, publish the bounded failure, process-fence the identity, retain its one daemon loop until the task really retires, and only then release registry capacity. A non-cooperative owner fences that runtime/integration identity until process restart and cannot accumulate replacements | A blocked DNS, credential, socket handshake, health probe, or close is reported healthy; reconnecting transport stays `ready`; an overlapping app starts a duplicate transport or stops a sibling's transport; a stopped subscriber loop accumulates wake-ups; a surviving subscriber remains attached to an unstarted generation after its primary exits; a subscriber attaches after final shutdown or terminal callback collection has committed; acquisition without startup consumes bounded registry capacity; API-hosted Slack leaks a nested runtime root; a late callback creates another runtime root; a terminal result frees an owner with an unretired task; late callbacks overwrite terminal state; probes or owner threads accumulate; or an optional integration blocks the required application root or process exit indefinitely |
| Terminal cleanup failure | Consumer-visible managed proxies cannot close their shared product. The generation owner retries a transient close failure within a fixed bound while its service-owner capacity remains charged. Concurrent close callers serialize through one guard: only a successful close becomes permanently settled, while a failed attempt leaves the guard retryable for the generation owner's bounded second attempt. If cleanup still fails, fence every proxy, revoke and drop all runtime strong references, and have pipeline admission latch the sole process-lifetime fatal circuit for that runtime before joining the owner and releasing permits. The admission-owned registry uses a domain-separated digest of stable runtime identity, retains only bounded immutable non-secret metadata, never evicts failed identities, and fails closed globally if its fixed cardinality is exhausted. The final root may drain to zero, but no controller recreation, provider path, queue admission, or root reuse may perform normal work for that identity until process restart | A consumer closes a sibling lease's provider, one failed close poisons the retry without invoking the SDK again, concurrent callers execute a successful close twice, failed cleanup is treated as success, executable authority is reported as zero, a permit is retained forever, a failed runtime creates repeated resources, another module owns a competing fatal registry, raw identity or authority survives in fatal metadata, or controller recreation bypasses the fence |
| Final runtime shutdown | The final root token synchronously rejects new and queued normal work, then transfers the drain to one generation-scoped runtime lifecycle owner. If request cancellation returns while retained work remains, keep that generation active and bounded by its remaining admission capacity; register one idle transition that transfers the final root only after every requester path settles. Final-drain thread creation/readiness runs through one coalesced transition worker so delayed readiness cannot synchronously block an async caller loop; synchronous entry points retain the same bounded result. That transition may run on a worker thread and therefore cannot require a caller event loop. Caller-loop futures transport the result only: cancellation or closure of the releasing loop cannot stop cleanup, and transport-loop wakeups retain at most one outstanding callback. Forced collection after requester-loop loss preserves `GeneratorExit`, emits no synthetic cleanup-failure telemetry, and does not require `asyncio.current_task()` to have a running loop; any coroutine explicitly created by a test or adapter remains owned until it is closed or settled. The lifecycle owner first waits for every pre-fence requester path, including release-pending retained requester work, to really finish, then revokes and shuts down the exact final provider generation, then drains retained provider work, shutdown-created cleanup permits, and the service owner to zero before completing exactly once. Failure to construct the lifecycle owner's event loop restores the same root and admission generation before reporting the error; authority is never restored after drain execution begins. Every API, Slack, CLI, direct-pipeline, and evaluation composition root invokes one shared startup/drain helper: a pre-drain lifecycle-owner startup failure is retried exactly once, while unrelated failures are never retried. If the retry is exhausted, recovery authority transfers to a durable owner or the runtime is terminally fenced; a caller token that is about to become unreachable is never restored. Any transport-thread startup used by a synchronous boundary is itself a bounded, rollback-safe lifecycle phase, and ambient state or disposable storage remains owned until terminal zero. Opt-out flags remove every case variant of their environment aliases before settings resolution and executable composition; a reload subprocess may re-import the app only from that canonicalized environment | Lifespan, CLI, or evaluation completion can report shutdown while executable work and capacity remain live, provider revocation can race an already-admitted requester path, delayed lifecycle-owner readiness can freeze an async caller loop, caller-loop loss can strand the graph in `draining`, forced collection can misreport `GeneratorExit` as cleanup failure or raise from a dead-loop task lookup, stopped transport loops can accumulate unbounded wakeups, lifecycle-loop construction failure can destroy retry authority, retry exhaustion can strand an unreachable use token, a transport start failure can skip drain, an entry point can omit the bounded retry, an opt-out alias can survive settings capture, or concurrent releases can create duplicate drain owners |
| Request-path idle transition | Ordinary leases, queued waiters, blocking permits, retained tasks, and maintenance transitions all use one controller-owned transition. It collects idle callbacks while holding the controller lock and invokes them exactly once after releasing the lock, including cancellation, expiry, rejection, and maintenance exit | One request-path type can strand the final root, or callback re-entry occurs under the controller lock |
| Scaling | Aggregate normal, queued, handoff, and cleanup work have explicit bounds | A per-request bound permits unbounded runtime-wide workers or retained results |

Write the lifecycle matrix tests before implementation. At minimum, cover:

- cancellation before admission, after reservation, while the worker runs, and
  after the result is produced but before adoption;
- synchronous provider construction runs on a thread distinct from both the
  requester and provider service-loop owner; while that factory is blocked,
  owner-local callbacks and terminal monitoring remain responsive. Requester
  cancellation and loop destruction retain worker ownership and capacity until
  construction and cleanup settle, then converge to exactly one adoption or
  retirement and terminal zero;
- both provider owner-loop submission attempts failing before enqueue, followed
  by requester cancellation and detached final-root shutdown; an ambiguously
  enqueued first submission whose FIFO probe also fails; and repeated
  cleanup-completed owner-stop dispatch failure. Every case reaches terminal
  zero through the single owner-local monitor while preserving the primary
  cancellation or operation error;
- a selected waiter's event loop stopping before it claims capacity;
- cleanup cancellation while waiting for capacity and cleanup after caller-loop
  shutdown;
- pre-reserved cleanup-group permit ownership, kind, activity, uniqueness,
  iterable, and cardinality validation failures before submission; invalid
  permit tuples invoke no callback and preserve their owners' ability to release
  legitimate capacity, while failures after a valid reservation return every
  permit exactly once;
- one child cleanup self-cancelling while a sibling remains blocked, with the
  generation gate and capacity retained until every sibling settles or the
  bounded cooperative grace revokes and retires the whole epoch;
- attempted normal-work re-entry from a cleanup worker plus same-owner inline
  cleanup from a normal worker, proving cleanup capacity cannot be promoted;
- two independently constructed API/Slack-shaped dependency bundles for the
  same runtime/controller, with overlapping acquisition, one factory generation,
  shared use, reference-counted release, and final zero state; plus two genuinely
  different runtimes;
- two overlapping application roots for one runtime, one root exiting while its
  sibling remains usable, one shared optional-integration transport and no
  hidden nested runtime root, final-subscriber transport stop, final-root drain,
  and a later sequential lifespan starting a clean generation;
- final release after optional-integration acquisition without worker startup,
  exception and cancellation between acquisition and startup, and more than one
  registry-capacity worth of sequential unstarted generations, proving terminal
  completion, subscriber revocation, no fence, no thread, and reusable capacity;
- barrier-controlled final-subscriber release versus new acquisition in both
  lock orderings, plus natural task completion paused after terminal callback
  collection, proving a late subscriber never attaches to a dying generation;
- partial construction, partial close, credential/resource generation change,
  and an abandoned constructed resource;
- worker construction failure, definite start failure, ambiguous
  `Thread.start()` failure, and readiness timeout/error across provider,
  final-drain, recovery, admission-maintenance, and synchronous cleanup-
  transport owners. Each site must use the shared bounded startup transition,
  preserve or terminally fence its pre-transition authority, and leave no
  unreachable root, selected claim, service permit, or cleanup owner;
- provider-generation and final-root transition-thread constructor failure,
  proving construction itself is inside rollback, the first final-root failure
  remains retryable, and failed recovery transport terminally fences authority;
- optional-integration children discovered from the owner loop even when an SDK
  never registered them, including cancellation resistance and deferred task
  creation, proving the identity fence remains until no live task can be closed;
- consumer close attempts, transient child-close failure, bounded permanent
  close failure, and stopped owner-loop failure, proving manager-only close,
  retry while charged, stable failure reporting, weak-reference release, a
  process-lifetime per-runtime fatal circuit after permanent failure, bounded
  quarantine metadata, and eventual zero service-owner/blocking/admission state;
- cancellation while `STARTING`; cancellation of a cross-loop provider call
  concurrent with final lease release; last-release/new-acquire overlap; stale,
  duplicate, foreign-graph, and prior-epoch lease handles and callbacks; runtime
  shutdown; and bounded owner cardinality across same-runtime and isolated graphs;
- completed requester owners on stopped-but-open loops versus pending owners on
  deliberately paused loops; retained requester-loop tasks prove final drain
  remains charged until real termination, while loop-loss-tolerant workers prove
  graph ownership was established before submission and release from their own
  `finally`;
- final API and CLI root shutdown while admitted Bedrock work is blocked, proving
  new-work rejection and return only after worker, retained-task, permit, and
  service-owner counters reach zero; install the normal-work fence before
  scheduling the asynchronous drain, and realize a provider manager from
  already-admitted work after that fence to prove the drain closes the exact
  late-installed manager; close or stop the releasing transport loop and prove
  cleanup still reaches terminal zero with at most one pending transport wakeup;
  inject lifecycle event-loop construction failure and prove the same root can
  retry without decrementing live counters or creating a second cleanup owner;
- saturation at every normal and cleanup bound, followed by eventual zero
  retained permits, workers, handoffs, and resources.

Tests must assert aggregate controller state and side effects, not only the
request's returned exception. A whole-diff review cannot substitute for this
pre-implementation proof. If a review later finds two failures in this gate,
stop the wave and redesign the shared lifecycle boundary before applying either
local fix.

### Operation-scoped blocking compatibility bridge

ADR-023 temporarily permits a blocking SDK only when no constructed resource
leaves its admitted worker. For this bridge the lifecycle declaration is:

| Concern | Bedrock bridge owner and required evidence |
|---|---|
| Runtime identity | The composition root supplies one immutable credential plan and one runtime admission controller |
| Admission | The runtime controller reserves capacity and worker startup unambiguously commits before credential realization, client construction, or SDK I/O |
| Capacity release | The consuming worker releases its permit in `finally`, including after caller cancellation or event-loop loss |
| Resource adoption | Not applicable: sessions, clients, and realized credentials never leave the worker; tests reject retained SDK resources |
| Cleanup | The same worker closes every partially or fully constructed resource before releasing capacity |
| Failure rollback | Construction, validation, request, parsing, and close failures converge to no retained resource and no retained permit |
| Scaling | Botocore connect/read timeouts are best-effort transport bounds; the aggregate admission and queue limits, including the Bedrock cap of 32, bound the population of blocking workers rather than each worker's lifetime. A permit remains charged until the worker-owned call and cleanup return; no separate cleanup pool or provider-local controller exists |
| Operator diagnostics | Setup guidance and `doctor` resolve the same allowlisted credential declaration as production and perform any remote probe through the runtime-owned admitted path; ambient Boto3 discovery is not a diagnostic fallback |
| Geographic inference | Retry a bare model only for the specific AWS error that requires an inference profile, and derive a geography-preserving profile (`us`, `eu`, `apac`, or an explicitly configured wider scope). Never silently widen to `global` | An unrelated validation error causes a second request, or regional data residency changes after fallback |
| Secret persistence | Credential files are written through one no-follow atomic replacement boundary whose temporary file is `0600` before the first byte. Every pre-publication failure leaves the old complete file or no file. After `os.replace()` publishes the new complete file, parent-directory open or sync failure returns an explicit `published_durability_uncertain` outcome; callers warn and do not retry automatically because rollback is no longer possible |

Matrix tests cover cancellation before admission, during credential/client
construction, during the request, and after result production; stopped caller
loops; partial construction and close failures; sequential rotated credential
generations; saturation; rejection of a result that returns after the operation
deadline; and eventual zero active permits and SDK resources after the worker's
call and cleanup return. Credential-file tests use nonblocking, no-follow opens,
reject non-regular files, inject pre-replacement rollback failures, and inject
parent-directory open and sync failures after publication. Those post-publication
tests assert one complete visible replacement, an explicit durability-uncertain
result, and no automatic retry. Consequently, symlink-backed Kubernetes or IRSA
projected-token files may be rejected by this compatibility bridge; that is a
documented compatibility limit, not permission to weaken containment.
If a session or client must survive the worker, this bridge no longer applies
and the full adoption and cleanup gate above is mandatory.

### Next-changeset sync/async boundary gate

The current changeset's containment contract is deliberately narrow. A
runtime-owned factory covered by the compatibility boundary may run only after
the runtime controller has reserved capacity and worker startup has
unambiguously committed. The realizing worker retains an unadopted product. If
validation rejects it, or cancellation or loop loss abandons it before atomic
adoption, that same worker and runtime owner perform cleanup before releasing
capacity. There is no caller-loop cleanup fallback, manually invoked thread
body, supplemental controller, or uncharged cleanup thread. The operation-
scoped Bedrock bridge is stricter: no SDK product is adopted out of its worker.

This contract does not complete the following work. Each row is a separate
next-changeset target and must repeat this design gate before production edits:

| Deferred boundary | Required next-changeset decision and evidence |
|---|---|
| Native Bedrock async transport | Land runtime-scoped credential-plan composition first. Make the established runtime execution graph the sole owner of the `aiobotocore` session/client, credential refresh, and `AsyncExitStack`; composition roots own only generation-fenced handles. Inventory DNS/TLS/model-loading/default-executor work before adoption, establish lifecycle before or with non-streaming `converse`, then add `converse_stream` only after lifecycle and downstream-consumer bounds pass. Derive connection-pool capacity from the runtime controller rather than creating a second limit |
| Accepted non-provider resource affinity | Extend the explicit runtime owner/loop/lease contract to adopted backends and stores; provider affinity, operation routing, draining, and runtime shutdown are implemented, but that proof does not transfer automatically to other resource types |
| Closable store factories | Define composition-root ownership, leasing, readiness, and deterministic close for history, feedback, signal, and knowledge stores without private global fallbacks |
| SQLite steady-state execution | Choose bounded offload or an async database adapter from measurements while preserving protected-path admission, writer transactions, migrations, busy deadlines, cancellation-before-start, and shutdown drain |
| Structured-document I/O | Give curated/bootstrap YAML and JSON reads one bounded loader with byte, node, depth, alias, parse-time, and atomic last-known-good registry-swap gates |
| Remote response admission | Bound compressed bytes, decoded bytes, JSON depth/nodes, decode CPU, and aggregate in-flight response memory before adapter item limits; migrate remote adapters through one decoder contract |
| Pipeline CPU work | Instrument selection, compilation, evidence, ranking, validation, hashing, and serialization first; introduce a bounded worker or process owner only when loop-delay measurements justify it and output equivalence is proven |
| Fixed/shared worker-pool lifecycle | Decide pool creation, process-wide and per-runtime cardinality, aggregate sizing, queue bounds, shutdown, broken-worker recovery, and runtime isolation; do not add a provider-local executor |
| Cross-loop cleanup and admission | Extend the provider execution-graph contract to remaining adopted resource types without loop-bound futures becoming capacity or cleanup owners |
| Observability and scaling gates | Land event-loop lag, queue depth/wait, active and retained work, cleanup duration/failure, SQLite queue/lock wait, pool saturation, and limit-plus-one/load gates with or before each boundary |
| General filesystem and archive I/O | Establish one descriptor-based owner for artifact reads, traversal, quarantine, exports, archives, compression, temporary files, and publication. Bound descriptors, temporary-disk bytes, input/output bytes, depth, CPU, and aggregate memory; cancellation cannot publish a late result |
| Async HTTP, vendor SDK, and socket lifecycle | Extend the typed runtime-owner contract from the API-owned Slack Socket Mode boundary to reusable HTTPX/provider/backend/context clients and direct low-level Slack callers; add aggregate connection/retry/callback limits, response disposal, and deterministic close. Inventory DNS/TLS and library workers before adoption |
| Managed subprocess lifecycle | Require allowlisted executable identity, shell-free typed arguments, minimal environment, bounded IPC/output, process admission, process-group termination, descendant cleanup, and no retry after ambiguous side effects |
| Runtime-scoped credential-plan ownership | Resolve one immutable, secret-safe Bedrock plan per runtime composition before native client readiness. Reject settings disagreement before credential I/O and define generation invalidation and refresh ownership |
| Generic async factory API | Remove or redesign arbitrary factory submission. Any replacement requires typed runtime identity, aggregate admission, atomic adoption, loop policy, cancellation-safe retirement, and deterministic cleanup |
| Observability delivery lifecycle | Give log/metric/trace queues and exporters one process owner with bounded bytes/cardinality, explicit backpressure or drop policy, flush deadline, secret-safe failures, and shutdown drain. Exporter failure cannot own or block runtime cleanup |

Deferred rows are not implemented merely because their invariant is recorded
here. Until a row lands, unsupported compositions must fail closed rather than
falling back to global state, an uncharged thread, or a caller-owned event loop.

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
| Runtime-root or provider lifecycle hooks are omitted, split across owners, or borrowed from another runtime | Reject the complete dependency bundle. Acquire/release and lease/cleanup remain methods of one explicit owner capability, and provider factories must resolve through that same lifecycle owner | No store readiness, root registration, credential read, SDK/client construction, provider realization, pipeline stage, permit, service owner, or cleanup callback occurs |
| Production pipeline dependencies omit their runtime store owner | Fail construction; use the explicitly named isolated builder only for isolated graphs | No store, cache, provider, or admission controller is constructed |
| History or feedback factory realizes an owner with missing, wrong-role, or mismatched identity | Fail at realization before the store is returned to a pipeline stage | No history start, provenance write, or other store method is called |
| Direct or isolated pipeline omits an LLM/context capability | Install settings-bound owned factories through the isolated builder, with disabled context represented by a factory returning `None` | No process-global provider lookup |
| Provider factory realizes an owner with missing or mismatched settings/configuration | Fail before the provider is returned to an agent stage | No prompt, context query, cache write, or remote call |
| Injected store or provider factory lacks a declared owner, or its declaration conflicts with the runtime | Reject the declaration before invoking the factory | Factory call count remains zero; no database, SDK session, client, model discovery, or remote call occurs |
| A declared factory realizes an owner different from its declaration | Reject the realized object before returning it | No store method, prompt, context query, cache write, or downstream stage consumes the object |
| A provider product may be rejected or abandoned | Reserve runtime capacity and unambiguously commit worker startup before invoking the factory. Keep the product with that worker until atomic adoption; rejection or pre-adoption abandonment is cleaned by the same worker before it releases capacity | Admission denial and definite or ambiguous worker-start failure invoke no factory; repeated ownership mismatches, cancellation, loop loss, and cleanup failure do not leak clients, file descriptors, tasks, permits, or credential-bearing objects |
| A backend factory declares multiple remotes | Realize exactly one backend for every declared remote identity, with no omissions or duplicates | A mismatched set performs no publication and every constructed backend is cleaned up |
| One dependency bundle serves concurrent pipeline runs | Provider lifetime is leased per run or reference counted across active runs | One run cannot close a provider still used by another; initialization and final close occur once without leaks |
| SDKs support ambient endpoint or account overrides | Pass the settings-derived canonical endpoint/account explicitly and include the effective remote in ownership | Ambient endpoint/account variables cannot redirect a client or alter its owner identity |
| Provider SDK dependency floors | Every declared minimum SDK version accepts the constructor arguments required by the runtime's endpoint, credential, header, proxy, and webhook-neutralization policy. CI installs the exact direct dependency floors and constructs every affected provider without network access | A valid installation at the published lower bound fails before its first request, or compatibility is restored by dropping a security-neutralization argument |
| A credential-bearing async LLM SDK owns an HTTP transport | Create exactly one HTTPX client inside the accepted provider generation, pin it to the settings-derived endpoint, disable redirects and ambient proxy discovery, and explicitly neutralize every SDK-captured credential or custom-header environment value without process-global mutation. OpenAI admin/webhook authority, Azure admin/webhook and AD token/provider authority, and Anthropic webhook authority must be absent from the adopted client; the configured API key remains the sole request credential. Close the transport through the provider lifecycle owner. Until SDK construction and credential/header isolation all succeed, construction owns rollback of the SDK and transport. Bound connect, read, write, pool wait, total connections, and idle connections from validated runtime settings; connection pooling is a transport resource bound and never a second admission controller or queue | Cross-origin `307` and `308` responses reach no redirect target and forward no prompt or credential header for Anthropic, OpenAI, or Azure OpenAI; ambient custom `Host`, credential, admin, webhook, Azure AD token/provider, and arbitrary headers never alter adopted client state or reach the wire, including simultaneous app-scoped providers; the configured API key wins even when every supported ambient credential variable is populated; SDK-construction and credential/header-isolation failures close the unadopted transport exactly once; the configured endpoint still succeeds; all three clients expose the same redirect, proxy, timeout, header-isolation, and connection policy; provider close closes the SDK and owned HTTP client without changing admission counters |
| A credential-bearing remote client has no declared proxy identity | Disable ambient proxy discovery and redirects before the client can issue a request, including one-shot CLI diagnostics and setup probes. Canonicalize the complete settings-derived origin before attaching credentials, close one-shot transports deterministically, and clear proxy routes copied eagerly by SDKs. An explicit proxy remains unsupported until its endpoint and trust roots are represented in runtime ownership | Uppercase and lowercase proxy variables and cross-origin redirects cannot receive Grafana, SignalFx, context-provider, Slack, or Bedrock credentials; malformed CLI destinations fail before transport construction; direct constructors expose the proxy-disabled policy without mutating process environment |
| An SDK supports an ambient credential chain | Resolve one credential/account snapshot before ownership admission and bind client construction to that snapshot | Profile, environment, metadata, or process changes after admission cannot change the effective principal |
| An admitted credential plan produces rotating temporary credentials | Keep the planned declaration as the cross-generation expectation and validate each realized output only within its current provider generation; pin raw environment credentials and static source credentials used to perform assume-role | Sequential STS or web-identity outputs with different non-secret fingerprints are accepted when the stable plan is unchanged; source access-key, secret, or session-token mutation fails before STS or runtime client construction |
| A credential chain contains a provider whose local execution or remote authority is not represented by the admitted plan | Reject the complete plan before constructing the SDK session, executing a process, reading an SSO cache, or contacting container/instance metadata; provider-selector keys are presence-sensitive, including blank values | Credential-process, MFA, SSO, login, container, and instance-metadata probes have zero side effects unless their exact capability is explicitly modeled and admitted |
| A credential selector implies secondary remotes such as STS role assumption | Model the SDK's complete provider order, environment-name precedence and presence semantics; freeze the selected environment and local credential/config source identities; preserve separate static-credential providers; then synthesize one explicit private profile for the winning source | A valid role-assuming profile is admitted exactly once; profile/environment collisions, split static fields, conflicting credential/config fields, token paths, missing HOME with explicit files, source mutation, or environment mutation cannot make the SDK select a different principal, source profile, token, or remote set |
| Signal or knowledge authority is injected into a pipeline | Preflight and realize its ownership before any LLM or context stage | A foreign authority owner causes zero provider constructions, prompts, context queries, or remote calls |

Required tests must include construction-time and realization-time disagreement.
A factory can be valid while the object it returns is not.
Factory ownership failures emit only a stable phase, capability, reason code,
and mismatch-dimension set. Paths, tenant identifiers, prompts, endpoints,
accounts, and credential material are never observability fields.

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
| A required runtime starts with history, feedback, signals, or Operational Knowledge unavailable or above its configured snapshot-copy capacity | Make the shared runtime-root activation boundary validate and prepare all four roles, including schema migration and signal bootstrap, before registering an owner. Capture one positive per-runtime, per-physical-database byte cap and pass it to every store owner; each database shares its cap across main/WAL copies and retries, while the fixed history, feedback, and signals role set bounds complete-startup copying to three times that value. Every later root generation revalidates the exact admitted capabilities and, after acquiring the authority writer lock, its current capacity before activation; API, CLI, direct Python, standalone integrations, and evaluation do not own bypasses | Startup fails closed with a stable path-free role/reason and zero active root owners; health cannot pass and the first request cannot become the cold migration or capacity probe. Database replacement, lock-wait growth, and exact-limit/limit-plus-one growth between sequential root generations fail before remote or persistence work |
| Multiple runtime capabilities share one physical SQLite authority, such as Signals and Operational Knowledge | One storage owner performs one protected-path readiness admission and issues a public, role-bound capability. Dependent stores consume that exact capability and revalidate its database generation before schema use; they do not independently copy or inspect the authority file | Startup telemetry reports the exact copy count, copied bytes, and admitted roles. Owner, path, capacity, role, or generation disagreement fails before a second snapshot copy, migration, or user-data mutation |
| SQLite reports a journal mode other than exact `wal` | Fail before role identity, schema, migration marker, or user-data mutation | Reopen shows the pre-attempt state |
| Existing file carries a different store role | Fail the first writer transaction | No schema or data from the requested role is committed |
| A known shared role table has a malformed or ambiguous column shape | Fail closed before claiming any role | No identity table, schema repair, journal-mode change, or data mutation |
| Tenant-owner metadata is absent, mismatched, or ambiguous for the configured boundary | Resolve or reject through a genuinely nonmutating owner snapshot before enabling WAL or entering structural setup | DELETE, closed-WAL, live-WAL, and hard-link denials preserve directory entries, main/WAL/SHM bytes, journal mode, schema, indexes, markers, and rows exactly |
| Owner admission or raw readiness-capacity sampling observes a live WAL that changes during inspection | Retry the complete admission callback or raw capacity sample under one bounded deadline; never weaken source-stability validation. Readiness still revalidates the exact role generation and final capacity after acquiring the writer lock | Concurrent same-owner first-open converges, retries are observable without paths or tenant values, replacement/oversize still fails closed, and timeout cannot publish readiness |
| A live-WAL snapshot grows while main and WAL files are copied | Enforce the aggregate byte cap against bytes actually read, reserving each chunk before writing it | Source growth cannot exceed the cap on disk; the isolated copy is removed and authority files remain unchanged |
| Snapshot copy, SQLite inspection, or the trusted callback consumes the admission deadline | Share one absolute deadline and one cumulative raw-copy budget across copy, query, callback, source verification, and every retry | SQLite work is interrupted cooperatively; retrying cannot reset byte capacity, and a trusted Python callback that returns late is rejected and never authorizes mutation |
| A benchmark snapshots a quiescent or live SQLite database | Copy admitted main/WAL state without opening the authority database, verify source stability, then open only the disposable snapshot | Closed-WAL, clean, and live-WAL sources are never passed to SQLite; source directory entries, bytes, modes, mtime, and ctime remain unchanged, including when the source directory is read-only; access-time suppression is best-effort because POSIX platforms do not expose one portable read-without-atime primitive |
| A benchmark snapshots multiple SQLite roles | Verify the complete source-set identity before and after all role copies, enforce operation-scoped aggregate raw-copy and backup-output budgets across every retry, preflight destination space for both bounded copies, and retry or fail if any role generation moves. Publish only through a pinned owner-only generation directory; reject or safely isolate group/world-writable caller destinations and revalidate directory/file identity before consumer open | One report never combines history, feedback, and signals from different source generations; every generated directory is owner-only `0700`, every disposable or published main/sidecar file is owner-only `0600` independent of umask or destination mode, pathname replacement cannot substitute a database or completion manifest, caller-owned directories are unchanged, and oversized or capacity-starved snapshots fail with stable limit/storage reasons before atomic publication |
| Owner admission observes a live rollback journal | Fail closed until rollback recovery completes under a trusted writer; the high-level admission operation may retry within its existing absolute deadline without performing recovery itself | Journal and database bytes remain exact; transient first-open journals converge, persistent journals time out closed, and no pre-trust recovery is attempted |
| First-open role identity or structural schema setup fails | Roll back the structural writer transaction together | Fault injection after every structural statement reopens without a role/schema split |
| A pre-tenant schema needs a potentially large table rebuild | Keep the legacy table authoritative, prepare an empty shadow atomically, copy with durable keyset batches, then swap only after the complete copy | More than one batch, interruption after a committed batch, atomic final-swap failure, exact row preservation, and no current-schema marker before the swap |
| A bounded legacy tenant-owner backfill batch fails | Roll back only that batch while preserving prior completed batches and their durable cursor | Restart resumes at the last committed cursor and no final completion marker exists early |
| A keyset migration accepts zero, negative, sparse, or empty-string keys | Represent “not started” outside the legal key domain and copy every legal key exactly once, including an all-empty composite key | Boundary-key fixtures survive interruption and finalization without omission or retry loops |
| A migration preserves a source key that runtime scans or cursors also consume | Use the same legal domain end to end; runtime pagination, audit, quarantine, forward lookup, and reverse lookup may not reintroduce a narrower sentinel | Negative, zero, sparse, boundary, and empty-string fixtures remain reachable after migration and use indexed bounded plans |
| An optional SQLite capability such as FTS is unavailable | Persist a stable degraded capability decision or fail closed | Reopening twice converges without replaying schema and audit work indefinitely |
| Two processes race first-open | Revalidate role and tenant ownership as the first action after each structural `BEGIN IMMEDIATE`, and durably claim the tenant-owner migration in the first structural transaction before copying any tenant-specific row; equivalent owners converge and conflicting owners produce one winner | No loser schema, marker, tenant row, partial migration, or split identity, including across the gap between schema-copy completion and bounded owner backfill |
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

Public pipeline entry points resolve the effective settings, semantic action,
and tenant boundary before constructing default dependencies. Every denial case
must prove zero store/schema initialization, credential-source access, provider
construction, and remote calls.

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
| Curated archetype registry | read to list; read plus override to hot-reload the process-global registry |

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
| Multiple dependency bundles share one runtime | One explicit runtime-owned admission controller enforces the aggregate limit |
| One wildcard tenant saturates active work | Per-tenant active capacity preserves at least one slot for another tenant |
| Large cross-loop waiter handoff | Selected-waiter maintenance is amortized, bounded, and does not poll idle queues |
| Timeout, cancellation, or a stopped event loop leaves blocking provider work alive | Bind the exact runtime controller before submission, reserve a controller-owned permit and commit worker startup before factory realization, keep unadopted results in a bounded worker-owned handoff, and release from the worker's `finally`; asyncio futures transport results only | Loop shutdown, task cancellation, and garbage collection cannot start an unowned factory, release capacity while effective work is active, leak an unadopted product, or strand capacity after the worker exits |
| A realized provider product is rejected or abandoned before adoption | The realizing worker performs exactly-once cleanup under its existing runtime permit; caller-loop, inline thread-body, supplemental-controller, and uncharged-thread fallbacks are prohibited | Active-loop cancellation, cleanup failure, cleanup saturation, and ambiguous worker start all converge to zero retained products and permits without manually invoking `Thread.run()` |
| A worker result completes while its requester is cancelled before the runtime cache records it | Treat worker-to-cache installation as the final adoption transaction; either the cache owns the exact product or the realizing owner retires it, never neither or both | Deterministic cancellation on the realization task's completion callback leaves exactly one cached-or-closed product and eventually zero retained capacity |
| An accepted provider generation reaches its last lease | Move `ACTIVE -> DRAINING`, revoke new operations, drain the generation's active-operation count, then close on the runtime service loop. Caller cancellation cannot reopen the epoch. Retry transient close failure within a fixed bound while charged. Permanent failure revokes all proxy authority, drops runtime strong references, latches the runtime-fatal provider circuit, and only then releases capacity; no later epoch may be realized in that runtime before process restart | A cancelled release can strand `closing`, detach cleanup from its owner, retain a service permit forever, let epoch N callbacks mutate epoch N+1, or repeatedly realize resources after permanent cleanup failure |
| An accepted generation has multiple close operations | Treat child `CancelledError` as a child outcome while the cleanup owner remains live; settle every sibling before successful release, or revoke and retire the complete epoch after bounded cooperative grace | One self-cancelling close plus one blocked sibling cannot expose the singleton factory, close a re-adopted generation, or reduce retained capacity before settlement/revocation |
| A cleanup group fails before worker submission | Before creating a call or thread, validate that every supplied permit is unique, active, cleanup-only, and owned by the exact controller/runtime. Consume at most permit-count plus one functions. Invalid permit tuples invoke no callback and remain releasable by their legitimate owner; after valid permit acceptance, iterable failure or under/overflow releases every permit exactly once | Duplicate, foreign, wrong-runtime, inactive, and normal-work permits cannot start cleanup; raising and oversized iterables are bounded and converge to zero retained cleanup permits |
| A task cancellation handoff declares retained ownership | Supply lifecycle and lease together or neither, and reject a partial pair before adding callbacks or cancelling the task | A half-specified owner cannot mutate the task or let effective work outlive uncharged capacity |
| Admitted work re-enters from a worker thread | A cleanup worker cannot realize normal work; a normal worker may perform teardown for its own product without acquiring supplemental cleanup capacity | Nested async and sync probes preserve normal/cleanup accounting at limit one and converge to zero state |
| Admitted worker validation or retirement re-enters the same lifecycle asynchronously | Reuse the current worker's exact permit and thread owner; do not enqueue behind the work that is awaiting the nested result | Limit-one nested async realization completes with one active permit, no queued waiter, same worker identity, and eventual zero state |
| An accepted provider may outlive its requester or be called from another event loop | Resolve it through the admission namespace's single execution graph. Run synchronous factory construction and pure product validation in a real admitted worker that owns the unadopted product and rejection cleanup. Keep worker escrow and capacity until exactly one service-owner-thread adoption CAS publishes cache/proxy authority; do not invoke adoption off-owner or hold a blocking lock on the owner loop. Provider-generation thread creation/readiness uses one coalesced transition worker so an async caller loop remains responsive while startup rollback and ambiguity stay with the generation owner. Once provider shutdown is requested, fence every public and already-in-flight internal generation-creation boundary before joining the current owner; shutdown cannot return after allowing a replacement epoch. Use, drain, and close the accepted async generation on that service loop with explicit graph/epoch/lease handles, task-plus-loop requester tokens, and a separate active-operation count. A direct operation acquires a standalone lease, while an operation inheriting request admission reserves a controller-owned retained-work permit before publishing the cross-loop handoff and releases it only at owner-side settlement | Request-loop futures transport results only; factory construction, adoption, and delayed owner readiness never block the lifecycle or caller loop; cancellation, duplicate/stale release, completed owners on stopped-open loops, pending owners on paused loops, closed abandoned owners, direct limit-plus-one calls, and runtime shutdown cannot own cleanup, bypass aggregate admission, publish cache authority off-owner, create generation N+1 during shutdown of N, or mutate another epoch. Cancelling an inherited call and exiting its request slot leaves aggregate capacity charged until the non-cooperative owner operation settles. This proof does not cover stores or backends |
| A credential-bearing async SDK constructor fails before provider adoption | Establish the synchronous realizing owner and reserve one of the process owner's fixed 32 rollback-quarantine slots before allocating transport or SDK state. Construction on a running event loop is rejected before reservation or allocation. Rollback runs on the single bounded daemon event-loop owner; the admitted factory worker observes a monotonic wall-clock deadline outside the cleanup coroutine and settles the constructor result exactly once. On cleanup failure or timeout, that worker reports the original construction error as cause and emits the terminal retained-capacity signal; the lifecycle blocking worker is the only authority that converts the signal into the runtime-fatal fence before releasing its permit. A cancellation-resistant cleanup remains owned by its quarantine slot, but pipeline capacity reaches zero and the final root may drain to closed under the persistent process-lifetime fence. No constructor-failure path may create a per-failure or after-allocation cleanup thread | SDK-construction and validation failures close SDK and transport exactly once; running-loop construction, quarantine saturation, owner-start failure, and admitted-worker-start failure allocate nothing; cooperative cleanup releases its slot; an isolated cancellation-suppressing close proves the constructor worker returns by the wall deadline, preserves the primary cause, fences the runtime, rejects later work, reaches zero blocking/retained capacity and a closed root, and retains exactly one slot on exactly one bounded owner thread |
| A response-first ASGI application listens for disconnect before reading a lazy body | Keep disconnect observation separate from body delivery. Authenticate before observing transport messages; return only `http.disconnect` to the listener. Any `http.request` frame remains inside the same admission, byte, amplification, and absolute read-deadline envelope and is consumed rather than exposed. Before forwarding response start with body ownership incomplete, replace any HTTP/1.x `Connection` value with `close`. For a same-task sequential responder, start the disconnect-observation deadline before the first post-response receive | Authentication denial, aggregate saturation, limit-plus-one bytes, timeout, admitted body completion, normal terminal body without disconnect, and disconnect-only streaming preserve bounded ingress without truncating a normal streaming response; HTTP/1.0 and HTTP/1.1 response-start headers contain exactly one `Connection: close` while preserving unrelated headers |
| An authenticated deployment serves browser bootstrap or health without credentials | Reserve anonymous access to exact `GET`/`HEAD` requests for `/` and `/healthz`. Register explicit bodyless HEAD handlers whose status matches GET, including fatal health status. A valid zero content length remains lease-free, while positive or ambiguous body framing is rejected before request-body admission or receive; method and path near-misses authenticate before body receive or admission. Authenticated-mode documentation paths return a fixed true 404 | Real app GET and HEAD bootstrap and health succeed anonymously with an empty HEAD body and without consuming shared admission, including at global saturation; body-bearing public requests cannot starve protected traffic; path and method near-misses return 401; `/docs`, `/redoc`, and `/openapi.json` return 404 |
| Middleware returns a terminal HTTP response before request-body ownership | Force the HTTP/1.x connection closed instead of leaving unread bytes on a keep-alive socket outside ingress deadlines. Authentication, Host, CORS preflight, disabled documentation, public-body rejection, declared oversize, saturation, and framework failure paths share this terminal policy | An ASGI response-header matrix covers every pre-body owner, and a raw keep-alive socket that withholds its declared body observes the complete response followed by EOF within a fixed deadline |
| An unframed `GET`/`HEAD` body is arbitrarily fragmented, or body failure follows response start | Use one lease, one bounded buffer, one timeout scope, and one canonical downstream request message. After response start, discard request frames while observing actual disconnect; only a same-task sequential responder gets a bounded synthetic disconnect fallback whose deadline begins before its first receive, while a concurrent response sender retains cancellation authority | A 100,000-frame probe retains constant timeout state and delivers one message; queued disconnect and no-disconnect fallback complete sequential apps after normal body termination, saturation, timeout, oversize, or invalid input, while Starlette streams remain complete |
| An operation-scoped blocking bridge constructs SDK resources | Keep construction, request, parsing, and cleanup in one admitted worker; propagate the pipeline's one absolute deadline into transport construction and retries; reject responses returned after that deadline; keep admission charged until worker-owned call and cleanup return; cap the existing shared admission limit for the blocking bridge instead of adding another controller; no resource adoption or supplemental cleanup path exists | Cancellation and stopped-loop tests eventually end with zero retained permits and resources after worker exit; a deliberately late SDK response is rejected rather than accepted; transport timeouts are treated as best effort, and the cap bounds worker population rather than lifetime; limit-plus-one configuration fails before workers start |

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
| Input size | At limit succeeds; limit plus one fails before partial writes; HTTP bodies are bounded in the ASGI receive path before framework buffering or decoding; one application-owned request-count and byte controller rejects aggregate saturation before buffering |
| Directory traversal | Entry, file, and byte budgets; no symlink escape or path reopen race |
| Structured document | Node, depth, alias, scalar, and result-cardinality limits; one file validates atomically |
| Database rows | Query is keyset-paged and uses the exact intended index |
| Cursor and summary keys | Preserve every legal persisted value, including finite nonpositive timestamps, boundary integers, and empty text keys; reject only invalid non-finite cursors |
| Projection audit | Join a bounded key page through the tenant/governance/revision index; the exact 50,000-row production plan has no full mapping scan or temporary sort |
| Fan-out | Candidate/projection expansion is bounded before writer lock |
| Pattern matching | Aggregate scan and comparison budgets fail closed |
| Multi-stage resolution | One investigation-owned budget spans discovery, selection, compilation, evidence, and rescue |
| Object composition | Aggregate panels, queries, nested nodes, scalar characters, and bytes are admitted before allocation |
| Concurrent requests | Runtime-wide bound shared by API, Slack, CLI, benchmark, and direct defaults; different runtime owners remain isolated |
| Concurrent request bodies | Authenticate header credentials and resolve one concrete tenant before any protected body admission or receive. Exact public bootstrap and health requests never acquire shared admission: zero-length requests remain lease-free and positive or ambiguous body framing is rejected before receive. One application-owned aggregate byte and request budget then precedes buffering across protected connections and remains charged through downstream consumption. Once eager buffering owns the complete transport body, response start is the disposal boundary: clear any pending replay and release ingress before a long-lived response continues. Wildcard runtimes add fixed per-tenant request and accounted-memory subcaps strictly beneath both global ceilings; pinned runtimes retain the full global capacity. App construction revalidates those cross-field relationships after copied or mutated settings and uses the greater of the configurable and mandatory JSON decode factors when admitting the maximum envelope. Direct Python pipeline boundaries revalidate the complete request before tenant resolution, runtime-root acquisition, or queue admission; copied and constructed model instances are untrusted input. Coalesce arbitrary ASGI fragmentation into one bounded byte buffer and replay one canonical request message instead of retaining per-frame objects. Charge a conservative body-to-decode memory envelope rather than raw wire bytes; JSON and `+json` use a non-weakenable measured structural-allocation factor. Eager and lazy/unframed reads share the same controller and one absolute total read deadline; cancellation, disconnect, timeout, dishonest length, malformed/deep payloads, and downstream failure release exactly once. Framework validation failures return a bounded, sanitized client error without recursively encoding attacker-controlled input. Any response start or terminal response sent before complete body ownership replaces the HTTP/1.x connection policy with `close`. Rejection and read-only health telemetry contain bounded reason codes and aggregate counters, never payload, key, tenant, runtime, or model values, and a health probe cannot construct runtime authority |
| Admission queue | Global and wildcard-tenant partition lengths are bounded; one tenant cannot fill the global queue; partitions are scheduled fairly; capacity-eligible partitions are indexed separately from capped partitions; blocked full queues cannot strand spare capacity from a newly eligible tenant; queue wait consumes the overall pipeline deadline; stopped or closed event loops cannot retain permits; at-limit and limit-plus-one tests exercise the supported maximum with deterministic operation-count assertions and without full-queue rescans |
| Active admission | Wildcard tenants cannot consume every slot when multiple slots exist; a lease's controller, token, and partition are validated before active state is removed; cross-controller and other forged releases leave the legitimate lease releasable; direct dependency graphs choose an explicit runtime owner or isolated owner |
| Cross-loop admission handoff | A selected waiter must claim its reservation within a controller-owned lease; one bounded condition-driven maintenance worker recovers reservations whose target loop accepted a wake but stopped before executing it. Treat maintenance-thread startup as a fault-injected phase: clear failed registration, reclaim claims, retry once autonomously, then reject and wake every queued caller if startup still fails | Stopped loops and thread-start failures cannot reserve idle capacity indefinitely; fairness and queue order survive a transient failure; persistent failure has bounded attempts and leaves zero selected, queued, or in-flight state; idle or merely queued controllers start no maintenance worker |
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
- Authenticated APIs default to same-origin browser access. Any CORS allowlist
  contains exact HTTP(S) origins and never `*`, and admits the API-key and tenant
  headers explicitly. App construction revalidates copied or mutated settings;
  model construction is not the final security boundary. For state-changing
  methods, an explicit browser `Origin` that is not admitted is rejected before
  route invocation, request-body admission, storage, pipeline work, reload, or
  remote calls; omitting `Origin` remains the non-browser client contract. CORS
  response-header filtering alone is not mutation authorization.
- Every API-server entry point uses one settings-owned bind validator. Local
  commands and direct module execution bind loopback by default. A non-loopback
  bind is admitted only when API authentication is enabled and an explicitly
  configured Host allowlist can address that bind. Container commands route
  through the same validator, require a runtime-supplied credential, and expose
  host ports on loopback by default; release preparation may not replace or
  bypass that command. A development reload worker must re-prove the same
  bind/authentication declaration before serving; when that declaration cannot
  be transferred immutably, non-loopback reload is rejected before worker
  launch.
- HTTP request-size enforcement wraps the ASGI receive channel. It rejects a
  declared oversize before reading the body and counts streamed chunks when a
  length is absent, invalid, or dishonest; model validation is not a memory
  admission boundary. A disconnect, premature end of stream, or declared-length
  mismatch is terminal: the downstream handler is never invoked with partial
  bytes, and middleware emits no response after `http.disconnect`. Exact public
  bootstrap and health requests never consume request-body admission; positive
  or ambiguous body framing is rejected before receive. A response produced
  before complete body ownership, including Host, CORS, authentication,
  documentation, oversize, and saturation rejection, closes the HTTP/1.x
  connection so unread request bytes cannot outlive ingress deadlines. Once a
  complete body becomes a pipeline request, every retained string has a strict
  schema cap and the conservative per-request heap bound multiplied by maximum
  active plus queued runs must fit within the configured decoded-memory
  envelope. Response start may release ingress bytes only after this bounded
  representation owns the remaining lifetime. A separate response sender keeps
  disconnect observation open only while that task is live; completion,
  cancellation, or failure starts the bounded fallback deadline. Public Python
  entry points revalidate copied or constructed request models before runtime
  ownership or admission. Validation-error serialization is bounded and omits
  attacker-controlled input and context so excessive nesting cannot turn an
  ordinary 422 into a recursive 500.
- Utility and demo HTTP clients declare their own credential, redirect, origin,
  and proxy policy. Health probes remain anonymous. Every credential-bearing
  local workflow disables ambient proxy discovery and redirects so an API key
  cannot cross its declared origin; the exact expected origin is checked before
  every request. Anonymous local dependency probes, including Ollama, use the
  same proxy-disabled, redirect-disabled origin boundary.
- The container readiness probe connects only to loopback and derives its Host
  header from the canonical configured allowlist. An exact external host is used
  directly; a wildcard pattern produces one matching synthetic subdomain. The
  probe never weakens public Host policy by requiring a loopback exception.
  Canonical runtime-admission fatal fencing returns HTTP 503, while saturation
  and optional-integration degradation remain HTTP 200 with bounded metadata.
- Local demo teardown is credential-independent and idempotent: Compose-only
  interpolation receives a fixed non-secret placeholder when no runtime key is
  available, a nonzero teardown result is surfaced, and success is never
  reported before the command succeeds. A zero-config startup hands its
  ephemeral key to the loopback Web UI through a one-shot, no-store loopback
  bootstrap and transient browser state. The key never appears in a URL,
  command argument, terminal output, or persistent browser storage; the UI
  accepts the handoff only on a loopback HTTP(S) origin, moves it into its
  existing session-scoped key control, and immediately clears the transient
  value.
- The demo browser handoff is an exact-origin, one-time protocol rather than a
  credential-bearing navigation-state transfer. Its bootstrap document contains
  no API key, exposes only its origin as referrer, and uses same-tab navigation
  to the exact Tacit origin. A hidden frame on the bootstrap origin redeems the key
  once only after exact source/origin checks and a fresh nonce. Delivery first
  enters a non-authoritative session-scoped pending record that API requests do
  not read. A server preparation response may promote it provisionally, but the
  pending rollback marker keeps the new key unusable through the server's
  activation response. After the frame reports promotion, the parent rechecks
  the absolute deadline, removes the marker to activate browser authority, and
  sends a nonce-bound final browser-commit acknowledgment through the frame.
  Only that final server POST completes the CLI; the activation response alone
  cannot report success. CLI success therefore waits for the post-promotion
  acknowledgment. Reload and same-origin navigation before browser commit
  restore the previous key and purge the pending record; wrong source or origin,
  redirects, malformed messages, replay, frame failure, and timeout do the same
  before the new key can become usable. Once browser authority is committed,
  loss of the final POST is reported as indeterminate rather than as success or
  definite failure; valid server receipt is success even if its empty response
  is lost. All bootstrap responses are no-store and unlogged; the protocol never
  calls `window.open`.
- Authenticated browser deployments expose no same-origin documentation page
  that loads third-party script, deny framing, and mark sensitive API responses
  `private, no-store`, including framework-generated error responses. Tenant and
  credential headers never rely on a shared cache's default keying for isolation.
- Direct `file://` UI use is rejected locally; browser clients are served from
  Tacit's HTTP(S) origin and never synthesize cross-origin localhost fallbacks.
- API tests use a supported ASGI transport boundary rather than a deprecated
  framework compatibility adapter; repository invariants prevent its return.
  A synchronous helper preserves ASGI lifespan state and cookie deletion as
  well as one-shot request behavior. Startup and shutdown failure messages and
  original task exceptions retain their identity while failed lifespan tasks
  are observed and cleaned up without waiting for an extra protocol message.
- Paginated UIs expose continuation and discard stale responses after tenant
  changes.
- Expected validation and concurrency errors map to stable API and CLI outcomes.
- New schemas, corpora, and data files are present in built wheels.
- Documented commands exist and are exercised in CI. Security-sensitive startup
  examples run with a scrubbed environment, supply every required Compose
  variable, and advertise only paths that exist under the documented
  authentication mode.
- Runtime manifests report the package version actually shipped.
- The PyPI/GHCR release tag exactly matches a supported package version before
  builds start, resolves to the exact checked-out SHA, equals the current tip of
  a freshly fetched `origin/main`, and has a successful completed main CI run
  for that same SHA before publication. Installed metadata, runtime version, and
  CLI output agree semantically. The `v*` namespace and the `ghcr` and `pypi`
  deployment environments are protected repository controls. Publication
  actions use immutable commit identities; downloaded tools and privileged
  images use explicit versions and immutable digests where available. Release
  checkouts do not persist credentials, and emulation is enabled only where
  required.
- Container architectures are built twice without cache on independent pinned
  BuildKit builders, and their same-SHA child digests must match before the
  first artifact upload. Only the first portable artifact is authoritative; it
  is scanned as a verified disposable copy and promoted without rebuilding. The
  authoritative archive and checksum are uploaded before scanner execution;
  publication downloads that pre-scan artifact, never scanner workspace bytes.
  Dependency caches are BuildKit cache mounts, never runtime layer contents,
  and installed environments copy rather than link through those disposable
  mounts.
  Current-run architecture archives are published to checksum-bound staging
  references and pinned by digest before an existing full-version tag can be
  reused. Retry reuse requires its exact child digests to equal those current
  build digests; labels alone are not authority. Stable channel aliases move
  only to newer semantic versions, and cross-tag publication uses a
  non-cancelling queue. Every `v*` publisher shares this graph. Distribution and
  platform-binary smoke tests and read-only registry preflight finish before the
  first registry mutation. The final multi-architecture GHCR version precedes
  PyPI publication, and only the protected GitHub-release job receives
  repository write permission. Immediately before GitHub publication that job
  re-fetches the protected tag, requires it still resolve to the authorized
  SHA, and binds the release target to that SHA. Stable releases alone may
  update the latest channel; prereleases are explicitly marked and cannot.
- GHCR publication completes archive hashing, image loading, local tagging, and
  local inspection before fresh authorization. Every architecture push,
  immutable-index creation, and conditional stable-alias move invokes the
  exact-SHA authorization helper on the immediately preceding shell command;
  the helper token is command-scoped and absent from Docker child environments.
- Release retries pin the scanned architecture and index digests through alias
  publication. The immutable runtime base is refreshed to a version and
  multi-architecture index digest that passes the release scanner's current
  fixed HIGH/CRITICAL policy for every published architecture; the scanner
  remains a mandatory pre-publication gate because a previously clean digest
  can acquire a later advisory. Frozen executables are built twice from
  independent exact-SHA checkout, dependency, cache, and virtual-environment
  paths before upload. The release-only packager uses an exact pin at or above
  the current maintainer security-advisory floor; reproducibility does not
  authorize a binary produced by a vulnerable packager. Each archive is audited for required runtime providers,
  forbidden development modules, and one complete Tacit `METADATA` plus
  `entry_points.txt` directory. Installation-specific `.dist-info/RECORD`
  manifests and timestamped build-tool cache sidecars are excluded, while the
  frozen executable must resolve its version and console entry point through
  `importlib.metadata`. The resulting binary archives are byte-identical.
  Existing release assets are name- and size-preflighted before bounded
  streaming digest verification and are never overwritten. Build jobs emit the
  exact publication filenames and descriptor-bound SHA-256 digests as immutable
  job outputs; read-only preflight jobs verify and carry those descriptors into
  the final publishers. Each final publisher copies only that exact file set
  through no-follow descriptors into a root-created backing directory beneath a
  root-controlled parent. It then exposes that exact directory inode at one
  direct workspace child through a verified read-only `nodev`, `noexec`, and
  `nosuid` bind mount before its adjacent authorization check, and bind-pins every
  writable ancestor between the workspace and its root-controlled boundary. The
  action's actual package or wildcard input names only that mounted path. A
  same-UID process cannot rename and recreate the publisher path or any writable
  ancestor after authorization, while replacing downloaded workspace paths
  cannot change the mounted bytes. The mounts remain through publisher
  postflight; privileged always-run cleanup finds them by recorded mount identity
  even if a privileged actor moved a leaf mount, verifies backing/mount identity
  and the exact file set, and leaves no recorded mount or backing path active.
  The final publisher rejects any unexpected local file before its wildcard
  upload. Publisher features that create adjacent sidecars, including action-
  generated PyPI attestations, are disabled unless those files are generated,
  validated, and added to the authorized snapshot before the read-only mount.
  Immediately
  before publication it accepts an absent release or a matching subset so a
  partial prior upload can converge, while rejecting duplicate, unexpected, or
  mismatched remote assets; after the action returns it requires the exact
  remote set and digests. Generated release notes are enabled only when that
  preflight proves the release is absent; a partial-upload retry preserves the
  existing body instead of appending generated notes again. Binary packaging uses no-follow open plus descriptor
  identity checks and rejects symlinks, path swaps, non-regular, empty, or
  oversized inputs before archive creation. It streams both archive creation
  and hashing under explicit input/output bounds. PyPI preflight and postflight
  share one checked-in client with ambient proxy discovery disabled, redirects
  rejected, one fixed HTTPS origin, a per-request timeout, and content-length
  plus byte bounds before JSON decoding. Both compare remote files with the same
  build-carried descriptors consumed by the PyPI publisher snapshot.
  Downloader-managed scanners run only in jobs without registry write
  permission and receive only a verified disposable artifact copy. Downloaded
  privileged tooling is checksum-verified outside the Docker build context and
  release image smoke inspection rejects installer or release-tool payloads
  beneath `/app`. Docker inspection treats only a canonical no-such-object
  response as absence; daemon, permission, socket, transport, and malformed
  failures remain errors and cannot suppress cleanup. Every action
  in the CI workflow that authorizes release is immutable, and every release job
  has a bounded timeout.
- Only the Linux x86_64 frozen binary is published in this release. Although the
  Python runtime supports macOS, a macOS frozen binary remains excluded until a
  dedicated Developer ID signing and notarization gate signs the authoritative
  post-reproducibility artifact and verifies the final distributed bytes.
  Windows binaries are not published while runtime storage and initialization
  require POSIX filesystem controls. An unsupported or unsigned platform must
  be absent from build, preflight, and publication matrices rather than passing
  a version-only smoke test.
- Cross-registry publication ordering and residual failure states are documented.
  Tests must not claim transactional rollback across independent registries.
- Success, degraded success, admission overload, queued cancellation, timeout,
  stale conflict, and failure are distinguishable in structured events and
  metrics.
- Expected degraded events expose stable reason codes and bounded counters, not
  tracebacks, raw payloads, query text, tenant data, credentials, or local paths.
- Benchmarks used as gates exit nonzero on execution errors, empty corpora, or
  threshold failure. Every configured rate threshold must be finite and inside
  its declared bounds before evaluation begins. Provider exceptions can never
  be normalized into a passing prediction, and required positive/negative populations must both be nonempty.
  Live API modes carry only the evaluation state's configured credentials and tenant,
  and clean versus representative long-lived state is explicit, isolated, and
  reported rather than inferred from the caller's mutable files.
- Public release quality evidence is bound to the exact current `origin/main`
  tip and contains complete finite per-prompt measurements for both clean and
  representative long-lived state. Publication independently recomputes every
  score and zero-error condition from those measurements rather than trusting a
  reported aggregate or `gate.passed`. The protected job selects one concrete
  tenant. It materializes the representative SQLite databases once, fingerprints
  that exact logical snapshot, and carries the tenant, fingerprint, and one
  shared 2 GiB-per-database/4 GiB-aggregate size contract through evaluation,
  archived evidence, and publication authorization. Copy and backup enforce
  both limits while bytes are consumed, including source growth after preflight;
  post-copy validation is not the capacity boundary. Closed and live-WAL inputs,
  exact limits, and limit-plus-one rejection use the same path. The external evaluation has one shared
  serial request budget: the expected two 100-prompt modes require at least 400
  provider calls, every retry and repair counts, and request 501 is rejected
  before upstream contact. A protected, isolated runner and environment own the
  local model, Grafana fixture, sanitized long-lived state, and explicit spend
  approval; the runner label is routing metadata, not a security boundary.
- Offline evaluation gates create no network capability. Live destructive
  harnesses accept only explicit local endpoints, require a destructive-action
  acknowledgement, and fail before filesystem or network access. Isolated
  model runs use only their dependency-owned provider and scrub ambient SDK
  credentials and uppercase/lowercase proxy variables while process-global
  state is held under one serialized owner. Evaluation-owned local HTTP clients
  disable environment proxy discovery; the isolation boundary restores the
  caller's environment exactly on exit.
- Committed-history secret scanning has one explicit full reachable-history
  baseline before incremental event ranges are accepted. Current-tree scanning
  uses a Git-object-free export; neither mode silently substitutes for the other.
- Optional-integration terminal settlement and resource retirement are separate
  facts. A plain-cancelled top-level Slack task still retains its owner loop,
  registry identity, and generation fence while lifecycle-reported detached
  work remains; replacement admission begins only after that work retires. The
  owner transitively drains ready callbacks through a fixed turn budget and
  atomically seals callback scheduling before closing its loop. A callback chain
  that cannot quiesce keeps one process-fenced daemon owner parked without
  spinning, reporting completion, discarding accepted callbacks, or admitting a
  replacement generation.
- Browser handoff has one server-authored absolute deadline shared by bootstrap,
  redemption, delivery, and acknowledgment. Accepted sockets bound every read
  by the remaining deadline. Bootstrap schedules only against that absolute
  value, redemption must return the identical value, and the target starts no
  independent TTL before or after a delayed `READY` message.
- Credential-bearing GitHub release reads use one checked-in request boundary:
  ambient proxies are disabled, the API base and every token-bearing request
  must match the expected HTTPS origin, and workflow jobs execute that exact
  helper before and after publication.

## Quality gates

| Change area | Minimum additional gate |
|---|---|
| Knowledge, evidence, ranking, replay, contracts | Grounding and Operational Learning benchmarks |
| Intent, retrieval, signal resolution, archetypes, ranking | 100-prompt clean and representative long-lived state |
| Direct LLM command or evaluation | Construct and adopt the provider through the runtime lifecycle owner before entering the requester event loop; pass the exact provider explicitly, close it through that owner, and return nonzero when requested enrichment fails |
| Browser or API workflow | Hermetic E2E and browser security tests |
| Migration, package data, CLI command | Built-wheel smoke test; frozen-binary builds include and load every runtime schema/resource through the actual executable |
| Release workflow | Exact tag/version and current-main-tip tests; independently recomputed finite clean and representative long-lived 100-prompt evidence with zero execution errors and a pre-contact external-request ceiling; exact-SHA CI authorization repeated after each protected-environment delay and immediately before its first external write; lock-required dependency sync; a hash-pinned build-backend closure; a current-database scan of every immutable runtime-base architecture; pre-scan authoritative artifact upload; disposable read-only scanning; cache-free runtime layers; bounded image/binary and remote-metadata I/O; reproducible same-SHA OCI child digests carried as immutable artifacts and compared to each complete pushed manifest digest; current-build child-digest equality on retry; descriptor-bound root-owned backing directories exposed to the actual pinned publishers only through verified hardened read-only bind mounts; an exact same-UID directory rename/recreate race; execution of every checksum-verified authoritative OCI archive before publication (including arm64 under pinned emulation); deterministic Docker resource names with unconditional absence-verified cleanup after ambiguous CLI outcomes; non-root/read-only-root/writable-data/tmpfs/resource/version/startup/readiness smoke; an explicit signed-platform publication set; a maintained immutable Node 24 publisher action; a tested minimum glibc baseline for a generic Linux frozen binary; frozen `importlib.metadata` version/entry-point lookup; partial-asset retry convergence followed by exact postflight; committed-history secret scanning; protected publication boundaries; redirect-safe credential handling; and publication dependency graph |
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
