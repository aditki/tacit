# ADR-023: Contain blocking Bedrock before native async migration

## Status

Accepted

## Context

Tacit's Bedrock integration currently uses blocking Boto3/Botocore APIs from an
async runtime. Reusing SDK sessions and clients across requests, event loops, or
credential generations makes runtime ownership, cancellation, cleanup, and
capacity accounting difficult to prove. Continuing to extend that shared
resource lifecycle would increase implementation risk before the native-async
direction is established.

This decision contains the blocking adapter so the current changeset can be
correct and mergeable. It is not the desired high-throughput Bedrock design.

## Decision

The temporary Bedrock bridge is operation-scoped:

- The composition root is the runtime owner and its admission controller is the
  only admission authority.
- The planned credential declaration is validated before submission. After one
  runtime permit is reserved and worker startup unambiguously commits, that
  worker realizes the current credential generation, validates it against the
  declaration, creates the SDK client, performs one `converse` call, parses the
  response, and closes every SDK and credential resource in `finally`. A
  definite or ambiguous worker-start failure aborts before realization.
- The long-lived declaration stores selectors and non-secret fingerprints, not
  credential-file bytes, web-identity tokens, or raw environment credentials.
  Temporary credentials returned by modeled role or web-identity operations may
  rotate. Raw environment credentials and the static source credentials used to
  assume a role stay pinned because arbitrary source-principal rotation cannot
  be validated from the target role selector.
- No Boto3 session, client, credential client, or realized credential generation
  leaves the worker or survives for reuse. Resource adoption is therefore not a
  bridge state.
- The worker that consumes the permit is the capacity-release and cleanup owner.
  Cancellation or loss of the requesting event loop may discard the result, but
  it does not release capacity or prevent cleanup; the permit remains charged
  until the worker exits.
- Botocore connect and read timeouts are best-effort transport bounds, not hard
  worker-lifetime deadlines. A response that arrives after the operation
  deadline is rejected, but the runtime permit remains charged until the
  worker-owned SDK call and cleanup return. Bedrock configurations cap the
  shared pipeline admission limit at 32 while this bridge exists; the cap bounds
  the population of blocking workers, not the lifetime of an uninterruptible
  SDK call, and it is one limit rather than a second Bedrock controller.
  Provider `close()` is idempotent and does not initiate SDK cleanup because the
  provider owns no long-lived SDK resource.

The compatibility bridge intentionally supports only credential sources whose
authority can be frozen and validated within one admitted operation: explicit
settings or environment credentials, static credential/config profiles,
one-level assume-role profiles backed by static source profiles, web identity
read and frozen per operation, and one configured role assumption over an
accepted base source. Credential-process, SSO/login, ECS/container metadata, and
instance metadata fail closed until their authority and lifecycle are explicitly
modeled. An unrecognized `llm_model` also requires an explicit
`LLM_BEDROCK_MODEL_ID`; it is not redirected to a default Claude model.

Credential-source files are opened with nonblocking, `O_NOFOLLOW` semantics and
must be regular files. Kubernetes and IRSA commonly expose projected service
account tokens through symlink-backed paths, so those token files may be rejected
by this compatibility bridge. This is a current compatibility limitation. The
bridge does not follow such symlinks or weaken its containment boundary to make
them work.

This intentionally gives up connection pooling and may repeat credential and
client construction, including STS construction when role assumption is needed.
Lower throughput and additional authentication calls are accepted as the cost
of a smaller, auditable compatibility boundary. This decision does not claim
that the bridge is the production-scale Bedrock transport. Because the bridge
uses Botocore credential-provider internals, its optional dependency pins both
Boto3 1.43.16 and Botocore 1.43.16 exactly instead of claiming an unverified SDK
range. Release validation installs the built wheel with that extra in isolation
and exercises the credential-provider and transport configuration contracts.

Accepted async provider objects use a separate, explicit lifecycle contract.
Each runtime admission namespace owns exactly one execution graph and one
semantically compatible provider manager, shared by API, Slack, CLI, and direct
dependency bundles. The manager creates, uses, drains, and closes a provider
generation on one persistent runtime service loop. Run leases carry a graph
nonce, generation epoch, and lease ID; provider operations have an independent
active-operation count. The final lease revokes new calls, drains active calls,
then closes the generation. Cleanup failure fences every proxy, releases the
service owner after cooperative shutdown, drops executable references, and
keeps only bounded immutable failure metadata while latching a process-lifetime
fatal circuit for that runtime. Recovery requires process restart; the failed
runtime cannot realize a later provider epoch.

"Creates" does not permit synchronous factory work to block that service loop.
The runtime reserves aggregate capacity and commits a real worker before calling
an LLM or context factory. That worker owns the unadopted product, validation
rejection, and pre-adoption cleanup. It retains escrow and admission capacity
until pure validation completes and exactly one service-owner-thread adoption
CAS publishes cache/proxy authority. Adoption never runs on the worker or
requester and never waits on a blocking lock in the service loop. Provider
generation creation/readiness is itself a coalesced transition outside the
requester loop, while rollback and startup ambiguity remain with the generation
owner. Only the accepted async generation's use, drain, and close execute on
the service loop. Request cancellation or loop loss cannot transfer the
worker's capacity or cleanup authority back to the requester.
Once provider shutdown is requested, the locked generation-creation boundary
rejects both new and already-in-flight acquisitions before they can publish a
replacement epoch; shutdown cannot join one generation and return with another
generation already active.

Each independently started API, CLI, or evaluation composition registers one
generation-fenced root handle. Releasing a non-final handle cannot revoke its
siblings. Releasing the final handle synchronously fences normal admission,
then drains admitted work and re-reads any manager realized by that pre-fence
work before terminal provider and service-owner shutdown. Application lifespan
drains this manager before its runtime owner disappears.
The Bedrock provider participates in this provider lifecycle, but its Boto3
session and clients remain operation-scoped inside admitted workers.

## Native-Async Follow-up

The next Bedrock sequence first moves the stable credential plan to the runtime
composition root, then replaces this bridge with `aiobotocore` rather than
adding pooling or more cross-loop state to Boto3. It must attach the native
session/client to the existing runtime execution graph before, or atomically
with, non-streaming `converse`; `converse_stream` follows only after that
lifecycle is stable. It will:

1. make the runtime execution graph the sole owner of async sessions, clients,
   credential refresh, and `AsyncExitStack`; application lifespan owns only a
   generation-fenced root handle;
2. implement and validate non-streaming `converse` request and response parsing;
3. size the HTTP connection pool from the same runtime admission limit, with no
   second independent capacity controller;
4. prove cancellation, timeout, credential rotation, shutdown, saturation, and
   load behavior; and
5. add `converse_stream` only after the non-streaming lifecycle is stable.

The temporary bridge is removed, rather than retained as an automatic fallback,
when those acceptance tests pass. Pool tuning follows measured load evidence and
does not precede lifecycle correctness.

## Changeset Boundary

This changeset guarantees containment, not a complete async resource model.
Factory realization covered by this compatibility boundary is behind committed
runtime admission. Until a product is atomically adopted, it remains owned by
the realizing worker. Validation rejection, cancellation, or requester-loop loss
before adoption leaves rejected or abandoned product cleanup on that same owner,
and capacity remains charged until cleanup returns. Caller-loop cleanup,
manually invoking a thread body after an ambiguous start, a second admission
controller, and uncharged cleanup threads are outside the design.

Bedrock SDK resources do not exercise adoption because no realized session or
client leaves its worker. Accepted provider objects do use the runtime service
loop described above. This changeset does not claim the same affinity or
cross-loop shutdown guarantees for stores or backends.

## Separate Sync/Async Debt

The following boundaries are real technical debt and are not implemented by
this changeset:

| Boundary | Deferred decision |
|---|---|
| Native Bedrock async transport | First land runtime-scoped credential-plan ownership, then use `aiobotocore`; attach its session/client, credential refresh, and `AsyncExitStack` lifecycle solely to the established runtime execution graph before or with non-streaming `converse`, then land `converse_stream`. Composition roots own generation-fenced handles; the existing runtime admission authority sizes connection pooling |
| Accepted non-provider resource affinity | Extend the proven provider loop/lease/drain contract to stores and backends; do not infer that the provider proof covers them |
| Closable store factories | Add explicit composition-root readiness, leasing, and deterministic shutdown for stores rather than assuming process lifetime or global ownership |
| SQLite startup and migrations | Complete required schema, migration, bootstrap, and readiness work before serving requests; retain restartable protected-path semantics and report cold-start cost |
| SQLite steady-state execution | Choose bounded offload or an async adapter from measurements without weakening protected-path, transaction, deadline, cancellation-before-start, or shutdown-drain invariants |
| Structured-document I/O | Give curated/bootstrap YAML and JSON one bounded loader and atomic last-known-good registry replacement; cap bytes, nodes, depth, aliases, and parse time |
| Remote response decoding | Bound compressed/decoded bytes, JSON structure, decode CPU, and aggregate memory before adapter item limits, then migrate each HTTP adapter through the shared contract |
| Pipeline CPU work | Instrument loop delay and stage CPU first, then assign selection, compilation, evidence, ranking, validation, hashing, and serialization to bounded owners only where measurements require it |
| Fixed/shared worker-pool lifecycle | Define lifespan creation, process-wide and per-runtime cardinality, aggregate sizing, queue limits, failure recovery, shutdown, and runtime isolation; do not introduce provider-local pools |
| Cross-loop cleanup and admission | Extend the runtime execution-graph contract beyond providers without making request-loop futures capacity or cleanup owners |
| Observability and scaling gates | Add event-loop lag, admission/pool queue depth and wait, retained work, cleanup duration/failure, SQLite queue/lock wait, phase timing, saturation, and limit-plus-one/load gates |
| Runtime-scoped credential-plan ownership | Move stable Bedrock plan capture for API, Slack, CLI, and direct-provider owners to their composition roots so request rejection happens before credential-source reads; retain per-operation generation capture inside the admitted worker and define explicit settings-change invalidation |
| Aggregate HTTP request-body admission | Preserve the per-request ASGI limit but add an application-owned aggregate byte/concurrency permit before buffering and retain it through decode, validation, and disposal; test limit, limit-plus-one, malformed/deep JSON, disconnect, and tenant saturation behavior |
| Generic async factory submission | Remove or replace any public helper that can submit arbitrary construction to an unowned executor. A supported replacement must require runtime declaration, aggregate admission, atomic adoption, loop policy, and cancellation-safe retirement |

These items must be delivered as bounded changesets, with the observability needed
by each item landing with or before it. Recording them here does not claim they
exist, and they do not justify expanding the temporary Bedrock bridge. The
Bedrock sequence is fixed: runtime-scoped credential-plan ownership, native
lifecycle under the runtime execution graph, non-streaming `converse`, then
`converse_stream`; connection-pool tuning follows measured scaling evidence.

### Sequenced follow-up changesets

The debt is intentionally ordered so later work cannot create a second runtime
authority while trying to optimize an earlier boundary:

| Order | Changeset | Required exit gate |
|---|---|---|
| 1 | Runtime-scoped Bedrock credential plan | One immutable, secret-safe plan is resolved before native readiness; settings disagreement fails before credential I/O; refresh and invalidation ownership are explicit |
| 2 | Native Bedrock lifecycle plus `converse` | `aiobotocore` session/client, refresh work, and `AsyncExitStack` are attached solely to the established runtime graph before request use; composition roots hold generation-fenced handles; exact credential declaration parity, cancellation, shutdown, rotation, saturation, and limit-plus-one tests pass; the blocking bridge remains available only until parity passes |
| 3 | Bedrock `converse_stream` | Bounded event stream parsing, downstream-consumer limits, disconnect/cancellation cleanup, partial-response policy, and no leaked response bodies or pool slots |
| 4 | Bedrock pool tuning | Pool size derives from the existing runtime admission authority; measure queue wait, active connections, saturation, latency, and credential refresh before changing defaults |
| 5 | Remaining runtime composition boundaries | Extend accepted-resource ownership to stores/backends and add store readiness/close contracts |
| 6 | Aggregate ingress admission | Hold one bounded request permit from first byte through decode, model validation, and disposal; partition fairly by tenant and expose aggregate utilization without payload data |
| 7 | Store and document boundaries | Separate SQLite cold readiness from steady-state execution; add bounded structured-document loading and remote-response decoding with loop-delay and memory gates |
| 8 | Pipeline CPU boundaries | Instrument stage CPU and event-loop delay, then offload only measured hotspots with cancellation, concurrency, and output-equivalence tests |
| 9 | Shared worker infrastructure | Consider a fixed shared pool only after the preceding owners and measurements exist; preserve process-wide and per-runtime limits, bounded queues, broken-worker recovery, and deterministic shutdown |

Every changeset begins with its matrix tests and receives independent principal,
security, SDET, and scaling review against `origin/main`. Work from a later row
does not enter an earlier changeset merely because both use threads or async I/O.

## Consequences

- Cancellation and event-loop loss cannot orphan a reusable SDK resource or
  release runtime capacity while blocking work is still active.
- Independently constructed bundles for one runtime share a single provider
  generation and service-loop owner; stale handles and callbacks cannot mutate
  a later generation.
- Accepted provider cleanup failure is visible to the completing run, revokes
  the generation, converges to zero executable authority, and latches a
  process-lifetime fatal circuit for that runtime. Recovery requires process
  restart; the failed runtime cannot realize a later provider generation.
  Python cannot force-kill noncooperative third-party code; providers whose
  calls or close handlers do not cooperate with cancellation require native
  async support or subprocess isolation before adoption.
- The operation deadline prevents acceptance of a late result; it cannot
  interrupt arbitrary blocking Botocore or cleanup work. Admission remains
  charged until that worker exits, and the cap of 32 bounds population only.
- Temporary credentials returned by modeled assume-role and web-identity
  operations are validated independently against one stable role selector and
  are not pinned to the first temporary access key. Raw environment credentials
  and static source-profile credentials used to perform assume-role remain
  pinned because arbitrary source-principal rotation cannot be verified against
  the target role authority.
- Bedrock throughput may be lower until the native-async migration lands.
- Symlink-backed Kubernetes or IRSA projected-token paths may fail closed under
  the bridge's no-follow credential-file policy.
- The compatibility implementation stays intentionally disposable; connection
  reuse, streaming, and transport tuning belong only to the native-async path.
- Other synchronous boundaries remain visible, sequenced debt rather than
  implicit scope in this changeset.
