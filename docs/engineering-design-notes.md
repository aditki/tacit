# Engineering Design Notes

Status: living implementation guidance

Last reviewed: 2026-08-25

This document records recurring engineering lessons and refactor signals found
while building Tacit's Investigation Contract, Operational Knowledge, learning,
and generated-archetype workflows. It is a memory aid for future changes, not a
substitute for an Architecture Decision Record. Accepted ADRs are authoritative;
tests remain the executable specification for fixed regressions.

## How to use this document

- Start with the mandatory
  [foundation invariant matrix](foundation-invariant-matrix.md) and write the
  applicable matrix tests before implementation.
- Read the relevant sections before changing a cross-cutting workflow.
- Apply the invariants to API, CLI, direct Python, replay, refresh, background,
  and web UI entry points.
- Add a regression test when an invariant fixes a concrete defect.
- Update this document when work reveals reusable design pressure.
- Promote a note into an ADR when it requires a durable product or architecture
  choice.

## Core invariants

### One runtime composition root

Application, CLI, learning, replay, refresh, benchmark, and direct pipeline
paths must use the same resolved runtime settings and store graph.

- App-scoped paths must never fall back silently to process-global stores.
- Distinguish "not injected" from "injected but unavailable" so an unavailable
  scoped store cannot trigger a global fallback.
- Optional stores should fail best-effort only where the feature is genuinely
  optional. Their failure must not select data from a different database.
- Store caches must be scoped to the store identity, tenant, and relevant
  configuration.

The app and CLI must not develop separate heads. A capability available through
both surfaces should share the same service method, policy checks, tenant
resolution, transaction semantics, and projections.

Runtime-wide admission control belongs to that composition root, not to a
request dependency bundle or a module-global singleton. Every dependency bundle
from one `RuntimeStores` owner shares one controller; different runtime owners
remain isolated even when their configured limits differ. The controller must
remain safe when synchronous ASGI tests or embeddings drive one app through
multiple event loops. API, Slack, public defaults, and evaluation harnesses must
reuse their composition owner's controller rather than constructing one per
request. Bound both in-flight and queued work, count admission wait against the
overall pipeline deadline, and reclaim waiters whose loop closes or stops while
holding a selected permit. Wildcard runtimes partition the bounded queue by
tenant, cap each partition below the global queue bound, and schedule ready
partitions round-robin so one tenant cannot starve another. Queue maintenance
uses event notification and bounded selected-permit checks rather than rescanning
every waiter on each release. The scheduler indexes only partitions whose queue
head is ready and whose partition has capacity; capped partitions re-enter that
index in constant time when their own active lease or selected reservation is
released. Maximum-scale tests count scheduler examinations instead of relying
on wall-clock thresholds. After reserving older eligible queue heads, blocked
partition queues do not strand spare global capacity from a newly eligible
partition; an arrival never bypasses an older waiter in its own partition.
Wildcard runtimes also cap active work per tenant below the global limit when at
least two slots exist; a single-slot runtime can only provide round-robin
progress. Admission leases carry an opaque controller identity and their
partition, and must be validated as a controller-token-partition tuple before
any active state is removed, so a cross-controller or otherwise forged release
cannot consume the legitimate lease. Record queue
wait, global and partition depth, active counts, configured limits, overload
rejection, and queued cancellation with stable reason codes.

Evaluation isolation is a capability boundary, not just a temporary database.
Offline gates receive no network capability. Live harnesses opt into explicit
loopback endpoints and use the isolated dependency graph rather than global
providers or credentials. Any destructive fixture replacement requires a
separate acknowledgement after endpoint validation, before files, clients, or
network calls are created. Contexts that temporarily mutate process-wide
environment or archetype state serialize across threads and fail closed on
overlapping asynchronous tasks. Evaluation-owned local HTTP clients set
`trust_env=False`; cold isolation also removes and restores the standard
uppercase and lowercase HTTP, HTTPS, and all-proxy variables. Production HTTP
clients retain their normal environment-proxy behavior unless their owner
explicitly selects the evaluation-safe construction path.

Pipeline dependency bundles are capability manifests, not bags of optional
callables. The production builder requires one explicit `RuntimeStores` owner;
only the deliberately named isolated builder may synthesize an isolated owner.
History and feedback factories are checked again when they realize a store,
including settings, tenant policy, semantic permissions, role, and exact
database identity, before the store reaches a pipeline stage. LLM and context
factories are always present and settings-bound; disabled context is a factory
that explicitly returns `None`. A missing factory never means "consult the
process singleton." Provider realizations carry a public ownership descriptor
and are rejected before agent or network use when their settings identity does
not match the dependency graph.

Backend factories follow the same declaration-before-invocation contract as
stores and providers. An SDK with an ambient credential chain resolves a stable
credential/account snapshot before ownership admission and constructs every
lazy client from that snapshot. Ambient endpoint, profile, organization, and
project variables cannot add undeclared remotes or identities after admission.

### Tenant isolation is end to end

Tenant selection is a data boundary, not request decoration.

- Resolve and validate the tenant at every public boundary.
- A pinned deployment rejects another tenant. A wildcard deployment requires a
  concrete tenant. Internal bootstrap tenants are never valid user tenants.
- Propagate the resolved tenant through contracts, runs, revisions, feedback,
  corrections, raw learned artifacts, extractions, signal definitions, signal
  mappings, FTS indexes, provenance, exports, benchmarks, and caches.
- Filter reads, aggregates, list endpoints, and mutations. Authorization after a
  lookup is not enough if the lookup itself leaks another tenant's data.
- Browser requests must send the selected tenant consistently across Generate,
  Learning, Knowledge, Signals, and History views.
- Authenticated browser deployments deny cross-origin requests unless exact
  HTTP(S) origins are configured. Wildcard CORS is never compatible with an API
  key held by the browser; same-origin remains the default. Revalidate this at
  app construction because unvalidated model copies and failed assignment
  validation can leave a settings object in an invalid state.
- A UI opened from `file://` is not a supported API origin. It shows the local
  serve instruction and makes no `Origin: null` request or hard-coded localhost
  fallback.
- Request-body admission wraps the ASGI receive channel before framework
  buffering and decoding. Declared oversize bodies are rejected without a body
  read; missing, duplicate, invalid, or dishonest lengths remain subject to the
  same streamed byte count.
- HTTP test helpers are compatibility boundaries. A supported ASGI transport
  must preserve lifespan startup/shutdown, lifespan state, cookie persistence
  and deletion, exception behavior, and ordinary one-shot requests. A
  `lifespan.*.failed` response is already terminal evidence: preserve its exact
  message or the app's original task exception, observe and cancel unfinished
  lifespan work, and never wait for an additional protocol message that may not
  arrive.
- Legacy migrations inherit the configured pinned tenant. A wildcard runtime
  must fail before schema mutation when pre-tenant user data has no explicit
  owner; migrate once under a pinned owner before enabling wildcard tenancy.

Every tenant-aware feature should be tested across this matrix:

| Configuration | Entry point | Expected behavior |
|---|---|---|
| Pinned tenant | API, CLI, direct call | Missing tenant may resolve to the pin; a mismatch fails |
| Wildcard | API, CLI, direct call | Concrete tenant is required |
| Any | Read and aggregate | Only the selected tenant is visible |
| Any | Mutation | Permission and tenant are checked before state changes |

### Governed knowledge is authoritative

Operational Knowledge is the authority for learned organizational behavior.
Mutable signal tables, indexes, and caches are projections for efficient runtime
selection.

- Do not activate a resolver projection before its governed revision is
  eligible.
- Projection writes must preserve exact runtime values such as metric patterns;
  normalized proposition identities are not resolver patterns.
- Projection identity must retain tenant, scope, and governed support. One row
  must not make independent scoped revisions retire each other.
- Signal-to-metric semantics are many-to-many unless a predicate is explicitly
  exclusive.
- Datasource provenance belongs to each query target and metric, not to the
  containing panel or alert. Query language may narrow a compatibility family,
  but exact governed scope is checked against the selected catalog entry before
  authority is learned, applied, fingerprinted, or reconciled.
- Bootstrap mappings remain globally identifiable and cannot be mutated or
  retired by a tenant named `default`.
- Lifecycle changes and source disappearance must reconcile both governed
  revisions and their runtime projections.
- Reappearing sources may preserve review history, but they need an explicit
  transition out of stale or ineligible state.
- Authority revisions and resolver projections that share a database commit in
  one unit of work. A retry may repair or verify an idempotent projection, but
  it must not report an active revision whose projection failed to commit. A
  service receiving explicit authority and resolver stores verifies their
  canonical database paths at construction; two individually valid stores do
  not form a valid dependency graph when they point at different files.
- External dashboard or alert IDs establish provenance, not independence.
  Corroboration groups copied sources by stable operational-content lineage.
- Human feedback is assessment input until it becomes a reviewed Operational
  Knowledge candidate. Raw feedback scores must not directly boost or penalize
  runtime metric ranking because that would bypass revisions, snapshots, and
  usage attribution.

### Usage is confirmed by the consuming stage

Selection does not mean application. Knowledge starts as considered and becomes
applied only after a stage confirms that it changed or selected an output.

- Record the exact knowledge ID and revision, target, stage, effect, reason, and
  contribution.
- A zero score delta can still be a real effect, such as query compilation or
  candidate exclusion, but it must name that effect.
- Do not assign `used_for` from knowledge kind alone.
- Rebuild the final knowledge snapshot after live-evidence and stage-use
  reconciliation.
- Once a governed mapping changed compilation or evidence resolution, later
  negative telemetry may annotate that effect but cannot erase it from usage.
  Marking it unapplied requires rebuilding and validating the output without
  the mapping.
- Counterfactuals must downgrade usage when they remove the output that knowledge
  generated or changed.
- Impact analysis includes only applied usage. Considered and rejected items
  remain audit records, not affected investigations.

Current design pressure: compilation returns the governed mapping references it
actually selected, and the pipeline reconciles those references into usage. If
ownership, investigation patterns, evidence requirements, or other knowledge
types begin affecting more stages, introduce a typed stage-effect ledger instead
of adding more per-stage reference sets. A likely record contains:

- stage and effect type
- knowledge ID and revision
- target artifact or candidate
- before/after value or score delta
- reason and provenance references

### Investigation revisions are immutable and replayable

- Schema versions are explicit and unsupported versions fail closed.
- Exact replay either reproduces the stored output fingerprint or fails/returns
  the stored contract according to the documented mode. It must not return a
  divergent result as a successful exact replay.
- Legacy fingerprint compatibility omits fields that were absent when the old
  fingerprint was created.
- Normalize volatile timestamps in every provenance source before output
  fingerprinting.
- Current-engine and counterfactual replay require captured inputs; they do not
  silently fall back to an exact load.
- Offline current-engine replay must fail when the recorded engine, contract
  policy, ranking, or vocabulary version changed and the capture cannot rerun
  discovery, compilation, and validation. A refresh is the authoritative path
  for those changes; replay must not relabel captured stage output as current.
- Refresh, replay, and correction writes are pinned to the revision that supplied
  their inputs and use an expected-parent check.
- Assessment bundles for an old revision contain no future revisions.

### Evidence controls conclusions

- Observation references point to the matching query and requirement.
- Missing-observation fields contain observation IDs, not requirement IDs.
- A requirement is missing only when none of its observations are supported.
- Missing and contradicted observations participate in grounding even when the
  requirement itself resolved successfully.
- Explicit negative telemetry appears as contradiction and cannot be hidden by
  contextual support.
- Abstention suppresses leading-suspect language and causal conclusions.
- Knowledge may add contextual candidates, but it does not turn context into
  telemetry evidence.
- Safety gates count any unsafe suspect conclusion in expected-abstention cases,
  not only claims labeled proven.

### Review and lifecycle transitions are state machines

- Authorization is semantic: read, review, reject, trust, correct, apply, and
  policy override are separate capabilities.
- Enforce those capabilities at API, CLI, and public domain-service boundaries.
  Repositories remain tenant-scoped persistence primitives; callers must not
  mistake direct repository access for an authorized product operation.
- Workflows that consume stored authority require read permission before their
  first lookup. Workflows that publish or persist a new authoritative result,
  including refresh and non-exact replay, also require apply permission before
  starting a run or external side effect.
- Dashboard and alert learning, including dry-run inference, reverse-resolve
  existing tenant mappings. They require read permission before constructing a
  store or contacting a backend; pending persistence additionally requires
  apply, and teaching requires the full read/review/trust/apply capability set.
- Persisted artifact learning also consumes governed candidate history and
  returns extracted rows plus governed identifiers. It requires read permission
  before storage or extraction at API, CLI, and shared-service boundaries. A
  dry run still returns structured Operational Knowledge records, so it requires
  read before file traversal, extraction, or remote source access. Persistence
  uses one semantic `learn_artifacts` action requiring read, review, and apply.
- Remote artifact connectors retain explicitly supplied runtime settings.
  Connector, store, and learning settings must agree before a remote request;
  falling back to process-global authorization after constructing a scoped
  client is a split-head security failure.
- Composed workflows use one shared runtime-owner guard across explicit
  settings, runtime store containers, signal stores, the Operational Knowledge
  service, and remote connectors. All supplied settings must agree, and
  persistence owners must resolve to the same canonical database before
  extraction, file traversal, backend access, or dependency construction.
  Core stores and services expose public settings and database identities;
  compatibility introspection is isolated in the descriptor adapter.
- Directory artifact learning preflights a configurable hard file limit before
  reading or persisting any source. Each accepted artifact still owns a bounded
  transaction; directory-wide write locks are prohibited.
- Policy override inputs require their own privileged permission in API and CLI.
- Human-readable actor labels in request bodies are untrusted metadata. Audit
  ownership is derived from the authenticated credential slot (or explicitly
  recorded as local unauthenticated operation), so callers cannot impersonate
  another reviewer in durable events. This applies to investigation creation and
  refresh, feedback review, and manually taught signal mappings as well as
  explicit knowledge-review endpoints.
- Review transitions use compare-and-swap predicates on tenant, identity, and
  expected state.
- Candidate state and provenance review state change together.
- Terminal states are idempotent and are not overwritten by expiry handling.
- Corrections bind to the exact investigation revision and knowledge revision
  that the reviewer saw.
- Stale and incorrect corrections require and validate a target before approval.
- Rejection or source retirement recomputes surviving support before retiring an
  active revision.
- Rejected, pending, stale, unresolved, copied, or entity-unresolved candidates
  cannot manufacture corroboration or active conflicts.

### Concurrency-sensitive writes are atomic

- Acquire a SQLite write lock before reading a parent/current revision used by a
  compare-and-swap decision.
- Keep parent verification, immutable revision insert, projection update, and
  terminal candidate transition in one transaction when they form one operation.
- Candidate evaluation must not overwrite a concurrent rejection or lifecycle
  transition.
- A source's terminal review state, mutable resolver retirement, governed
  lifecycle transition, and audit writes commit together. Atomic source fan-out
  is bounded; exceeding the bound leaves the source unchanged instead of holding
  SQLite's single writer indefinitely. Larger fan-out requires a durable
  revocation/outbox state machine whose pending state fails runtime selection
  closed.
- Correction application must verify and supersede the pinned target revision in
  the same atomic operation.
- Table-rebuild migrations must not allow an implicit commit between rename,
  create, copy, and validation.

Raw SQL remains acceptable while repositories own these invariants clearly. See
"Data access refactor triggers" below for the point where a stronger persistence
layer is warranted.

### Persistence semantics match the user operation

- A successfully published dashboard remains a chart success if later optional
  history, provenance, or contract persistence fails. Record the degraded audit
  state visibly.
- Refresh, replay, and correction endpoints explicitly request an authoritative
  new revision. Persistence failure or a stale-parent race must be surfaced to
  those callers rather than returned as HTTP 200 success.
- A failed refresh must not overwrite the last successful investigation summary
  or dashboard state.
- Terminal run events occur after all successful reconstruction and revision
  events.
- A run row and its corresponding start or terminal lifecycle event are one
  transaction. Neither side may become visible without the other.
- Client cancellation is cancellation, not timeout.
- Timeout and caller-cancellation cleanup has a separate bounded grace period.
  Admission release is unconditional; a dependency close that never completes
  is observed and abandoned without extending the request deadline forever.

### Release publication is a staged external commit

PyPI and GHCR do not provide one cross-registry transaction or a reliable
rollback primitive. The release workflow must describe and test the ordering it
can guarantee instead of claiming strict atomic publication.

- Every `v*` publisher belongs to one validated workflow. Python distributions,
  container images, and platform binaries may not have independent tag paths
  that bypass the shared gates. A release tag exactly equals `v` plus the package
  version before any build or publication starts. Supported versions are
  three-part stable or prerelease versions; post releases and local versions are
  rejected until their image-channel semantics are explicitly designed. The
  tagged SHA is checked out explicitly by every source-consuming release job,
  must remain reachable from a freshly fetched `origin/main`, and must have a
  successful completed main CI run for that exact commit. This authorization is
  downstream of all read-only release gates and upstream of the first registry
  mutation.
- The wheel smoke test compares the validated version with installed package
  metadata, the runtime module version, and CLI output using PEP 440 semantic
  equality from an isolated environment rather than the source checkout.
  Publication actions are pinned to reviewed commit identities. Downloaded tools
  and privileged helper images use explicit versions and immutable checksums or
  digests where available, and release checkouts do not persist credentials.
- Build each architecture image once into a portable archive. Bound and
  checksum it, then upload that authoritative archive before invoking any
  downloader-managed scanner. A separate read-only job verifies the checksum,
  scans a disposable copy, and cannot replace the authoritative archive or its
  checksum. Publication downloads and verifies the original pre-scan artifact
  and loads it without a second image build.
- OCI layers use the tagged commit time as `SOURCE_DATE_EPOCH` and BuildKit
  timestamp rewriting so clean builds of one authorized SHA produce the same
  child digests. Portable image archives are regular, nonempty, and bounded
  before upload and after every download.
- Complete package and binary build/smoke tests, every architecture scan, and a
  read-only PyPI and GitHub-release digest preflight before the first registry
  write. Release-asset preflight rejects duplicate or unexpected names and
  invalid declared sizes before downloading any asset body, then stream-hashes
  only expected assets under an enforced byte limit. Binary archives use fixed
  metadata and timestamps; retries refuse to overwrite an existing asset with
  different bytes.
- Architecture staging references include the transferred archive checksum.
  Every run publishes its current local architecture images to those staging
  references and captures their exact digests. A full-version
  multi-architecture tag is create-once: retries verify its source revision,
  package-version annotation, platforms, child labels, and exact child digest
  set, then require those children to equal the current build digests before
  reusing the pinned index digest. Self-asserted image labels are never enough
  to authorize reuse. Stable aliases are created from that pinned digest, never
  by re-resolving a mutable tag.
- Stable releases update major/minor and `latest` aliases only when the candidate
  version is newer than the current stable target. Publication runs share one
  repository-wide concurrency group with a non-cancelling queue so a third tag
  cannot evict a pending release or race that read/compare/write transition.
  Prereleases retain only their full-version tag.
- Publish the immutable multi-architecture GHCR version and any eligible channel
  aliases before starting PyPI publication.
- Repository administration is part of the release boundary: protect the `v*`
  tag namespace, restrict tag creation to the release role, and configure
  `ghcr`, `pypi`, and `github-release` as protected environments that permit
  deployment only from protected release tags. Only the final GitHub release
  job receives `contents: write`. Workflow checks are defense in depth and
  cannot make a tag trustworthy when an arbitrary writer may create or redefine
  it.
- A GHCR success followed by a PyPI failure remains possible. The workflow keeps
  PyPI from publishing first, leaves the successful container artifact available
  for a verified retry, compares any pre-existing PyPI files by digest, and does
  not pretend it can roll back an external registry atomically. Registry tags
  have no cross-client compare-and-swap, so protected environments and tightly
  scoped publication credentials remain required controls.
- Every release job has a bounded timeout. Privileged jobs install downloaded
  tooling only after verifying a fixed checksum, before registry login. A
  downloader-managed vulnerability scanner may run only in a build job without
  registry credentials or a pre-publication job with read-only package access;
  it never executes in a registry-write-capable job and never owns publication
  bytes. Platform-binary packaging stats and rejects invalid or oversized input
  before opening it, streams archive construction and hashing, and bounds the
  resulting package before its checksum is accepted. Remote PyPI and GitHub
  metadata is byte-bounded before decoding, and local distribution artifacts
  are stream-hashed in bounded chunks in both PyPI preflight and postflight.
  Because successful main CI is a
  release authorization input, CI actions, setup tools, and scanner containers
  use immutable identities and fixed checksums just like the release workflow.
  Secret scanning has a separate committed-history pass over the event's
  reachable range; a clean current worktree cannot hide a credential retained
  in an earlier commit.

### Entity and scope normalization is symmetric

- Canonicalize entity IDs, aliases, service references, and scope lists at write
  boundaries with the same normalizer used for lookup.
- Parse prompt-derived scope through one shared implementation. Ambiguous prose
  aliases such as `test` and `stage` require an explicit environment label, and
  version values use a version-specific canonicalizer that preserves selector
  and local-version syntax.
- IDs are kind-safe and entity kind is immutable.
- Exact-ID resolution still checks active status, expected kind, tenant, and
  scope.
- Alias scope is enforced before accepting an exact alias match.
- Scope lists are canonicalized before proposition fingerprinting.
- Applicability and conflict overlap use the same dimensions: tenant, service,
  environment, region, cluster, namespace, archetype, and version constraints.
- Normalize timezone-naive validity values before comparing them with UTC times.
- Tri-state scope inputs preserve omitted fields, interpret explicit empty
  lists as an unscoped variant, and use populated lists as explicit scope. If several
  scoped variants share a source identity, omission is ambiguous and fails
  closed; a future update API should target a stable mapping revision directly.

## Generated archetype containment and growth

Generated archetypes remain quarantined and shadow-only under ADR-019 through
ADR-021. An experimental retrieval mode must not blend generated output into the
authoritative dashboard. File mutation is not a promotion mechanism, and signal
mapping approval is independent from generated-archetype persistence.

The long-lived-state benchmark showed that accumulated learned archetypes can
reduce retrieval recall and signal-to-noise. Before any generated archetype can
be promoted or used authoritatively, the lifecycle needs controls for:

- content and semantic deduplication before creation
- deterministic scope-aware identity
- per-tenant and per-scope quotas
- source lifecycle reconciliation and tombstones
- retention windows and expiry
- clustering or consolidation of near-duplicate programs
- novelty and marginal-utility scoring against curated behavior
- shadow comparisons against clean and long-lived state
- explicit review, revision, rollback, and retirement

Do not solve growth only by increasing retrieval limits. The system should stop
redundant state from being created, consolidate existing state, and prove that a
candidate adds durable value before investing in an authoritative lifecycle.

## Data access refactor triggers

Do not adopt an ORM merely to hide SQL. Preserve SQLite-specific behavior such as
`BEGIN IMMEDIATE`, explicit uniqueness constraints, and compare-and-swap checks.
Introduce a stronger repository/unit-of-work layer, query builder, or ORM when
several of these conditions are present:

- one aggregate mutation repeatedly spans three or more tables
- transaction boundaries are duplicated across services
- tenant predicates or lifecycle predicates are repeatedly omitted and repaired
- schema rebuilds and compatibility migrations dominate feature work
- parent revision and candidate version checks are hand-coded in many places
- pagination, filtering, and aggregate queries are duplicated across API, CLI,
  export, and benchmark paths
- tests cannot inject one store graph without patching globals
- projection and authority updates need coordinated rollback
- reverse resolution approaches its active-row scan budget or latency grows
  with total tenant history rather than candidate patterns
- projection repair is bounded by authority count but not by projection rows or
  trigger writes inside one SQLite writer transaction

The likely first step is a shared unit-of-work and typed repository API, not a
wholesale ORM rewrite. Keep domain state transitions in services; keep storage
mechanics, tenant predicates, and transaction ownership in repositories.

## Recovery, replay, and selection invariants

- A source-refresh checkpoint is committed only after candidate lifecycle and
  authority transitions succeed against state re-read under the write lock.
  Compare-and-swap loss is a failed reconciliation, not a successful skip.
  The checkpoint is bound to the source's `missing_since` generation; a worker
  from an earlier stale interval cannot certify a source that disappeared
  again after being restored.
- Resolver projections are disposable views of immutable knowledge revisions.
  Startup repair is bidirectional: quarantine unauthorized rows and recreate
  missing or deactivated rows from current eligible authority. Version the audit
  marker whenever the repair contract changes. If active immutable authority
  lacks enough exact resolver data to rebuild safely, leave the audit dirty and
  fail that knowledge store closed instead of certifying a partial graph. The
  pipeline may continue only through its explicit knowledge-unavailable path.
  Legacy duplicate resolver entries are canonicalized with the same variant and
  maximum-confidence rule during both projection repair and validation so the
  audit always converges.
- Current-engine replay fails closed when historical knowledge changed a stage
  that captured inputs cannot rebuild. Applied non-ranking usage must be pinned
  to an exact historical snapshot before replay can compare current authority.
- Snapshot result caps are applied after every scope dimension, version range,
  validity window, and dependency-subject predicate. Page authority rows with a
  stable keyset so an early non-applicable row cannot hide a later match. Read
  every page plus conflict and active-support eligibility in one SQLite snapshot,
  then persist the deduplicated snapshot after releasing that read view. Initial
  archetype scope contains only concrete templates resolved from classifier
  matches plus their legacy labels. Catalog-driven widening requires an explicit
  staged pin; never approximate it by selecting the entire curated universe.
  A staged pin preserves exact revisions already exposed to earlier stages and
  preserves their recorded usage dispositions; only revisions newly eligible
  because of the wider scope are evaluated against current state. Resolver
  projections are replaced from that complete staged snapshot before any
  wider-scope stage executes.
- Schema alterations and their required backfills share an explicit rollback
  boundary. Derived-index migrations may commit bounded, idempotent batches and
  set their completion marker only after the final page so interruption is
  resumable without one long write lock. Tenant-owner migration treats the
  remaining legacy rows as durable progress: governed rows are archived and
  removed in the same batch transaction, retargetable rows move in bounded
  batches, the intended owner is pinned in a progress marker across restarts,
  and the owner marker is terminal rather than aspirational. The progress
  marker is claimed in the first structural writer transaction before any
  tenant-specific schema row is copied; it cannot be deferred until the later
  owner-backfill loop, because a competing opener could otherwise claim the
  terminal owner after schema copy completed under a different tenant.
- In-memory caches have capacity bounds as well as TTLs; ordinary writes prune
  expired keys. Metric caches also carry a total item-weight budget and refuse
  to retain one oversized catalog; a key-count limit alone does not bound
  memory. Datasource cache identities include endpoint, organization, and a
  non-secret credential fingerprint so app-scoped runtimes cannot share catalog
  values accidentally. LLM caches belong to one runtime dependency graph and
  include provider, model, endpoint, engine version, and the complete rendered
  prompt identity. Cache identity must change when any field rendered to the
  model changes; maintaining a second hand-picked field list will drift. Exact
  aliases and non-authoritative fuzzy entity suggestions both use bounded
  indexed candidate buckets; truncation is ambiguous and fails closed.
- Immutable knowledge usage is an authority projection, not a free-form audit
  API. Persisting it requires apply permission and an exact semantic match to
  the usage stored in the tenant-scoped investigation contract. The immutable
  contract also owns usage identity: retry callers may omit revision-assigned
  IDs, but persistence must map them back to the contract IDs rather than minting
  new audit records.
- Runtime use is authorized before knowledge can affect resolver selection,
  evidence, query compilation, or ranking. Read permission permits inspection;
  it does not permit a pipeline to publish knowledge-modified output and defer
  the apply check until best-effort usage persistence.
- Dirty resolver projection repair commits bounded batches while the audit stays
  dirty. A read-only validation captures a generation token, and only an
  unchanged token may be marked clean. This avoids a database-wide write lock
  without certifying a concurrent or partial repair. Another initializer may
  complete the same repair concurrently; observing a clean marker is success,
  while a missing marker remains an invalid state. Validation pages immutable
  authorities and resolver rows, joins mappings to authority in each page, and
  restarts if the generation changes; never validate a large graph with one
  long read snapshot or one authority query per mapping.
- History, signal, and feedback ownership backfills use the same restartability
  rule: pin the intended owner before the first data batch, release the SQLite
  writer lock between batches, persist a per-table keyset cursor, and publish
  the terminal owner marker only after an under-lock sweep finds no legacy rows.
  A batch limit without a cursor is still quadratic on unindexed FTS state.
- Schema and ownership migration preflight is part of the migration transition.
  Read table shape, ownership markers, and migration necessity only after the
  writer lock is acquired; metadata captured before lock contention can replay
  a completed migration and reassign already-owned rows.
- Bulk learning creates one tenant-scoped service/repository graph per crawl and
  reuses it across records and stale-source reconciliation. If more ingestion
  paths need this behavior, replace the private store-to-service helper with a
  public dependency factory rather than adding new process-global fallbacks.
- Candidate review priority is persisted as a base priority plus an indexed
  unresolved-conflict bit. Conflict writes adjust that bit set-wise instead of
  deserializing and rewriting every candidate while holding the writer lock.
- Ranking post-processing preserves runtime-evidence-first ordering even when
  contextual knowledge changes scores. After exclusions, mode, telemetry status,
  confidence, and abstention are derived again from surviving runtime evidence;
  inherited non-abstention must never elevate a contextual-only survivor. The
  same ordering and state derivation apply to counterfactual replay.
- Entity bindings are leases on current entity authority, not permanent facts.
  Promotion revalidates referenced entities while holding the authority write
  lock, and snapshot selection batch-loads every referenced entity before use.
  Missing, withdrawn, kind-mismatched, or out-of-scope entities fail closed.
  Entity status and scope updates use an indexed candidate-to-entity projection
  for bounded keyset repair; the runtime checks remain the safety boundary if a
  repair is interrupted. Do not replace that projection with a full JSON scan or
  one entity query per candidate. Entity registration uses compare-and-swap;
  withdrawn and superseded entities cannot return to active through the generic
  registration path and require an explicit future reactivation transition.
- Source approval claims an exact dashboard or alert generation before governed
  promotion begins. Generation identity includes the inferred mappings and
  generated artifact, not only the raw upstream payload: rerunning inference can
  invalidate an approval even when the dashboard or alert fingerprint is stable.
  While that generation is `approving`, re-ingestion, terminal review, and
  complete-crawl staleness must not overwrite it. The claim is retryable after
  interruption, so recovery completes the same generation rather than exposing
  authority that lost its source-state compare-and-swap. Claims are leases, not
  permanent locks: a complete crawl may recover an `approving` source only after
  the configured claim TTL expires.
- Candidate source generation and workflow state use separate persistence clocks.
  `source_updated_at` changes only when extracted source material changes; review,
  policy, and lifecycle transitions leave it untouched. Source reconciliation
  captures a generation cutoff, scans provenance with bounded keyset pages, and
  commits bounded checkpoints. A newer source generation is skipped, while a
  concurrent review is preserved and still receives the source lifecycle result.
  Complete-crawl retirement similarly compares the source's `last_seen_at` value
  from before the crawl and uses a compare-and-swap update, so an unchanged source
  refreshed during the crawl cannot be marked stale. The exact `missing_since`
  generation is revalidated on the same write-locked connection before every
  bounded authority checkpoint; a restored source therefore cannot be retired by
  an older worker. Reviewed `source_changed`
  candidates may reactivate explicitly; pending source changes and human terminal
  decisions may not inherit old authority.
- A learned artifact's source fingerprint and extracted IR rows are one atomic
  generation. Retry validation compares deterministic extraction identities, not
  row counts, because equal-sized old and new extractions are not equivalent.
- Mapping provenance is projected into an indexed relational table maintained by
  SQLite triggers. Source refresh and retirement must query this relation rather
  than deserialize every tenant mapping. The JSON `source_refs` field remains a
  compatibility payload, not a license to reintroduce history-wide scans. Its
  public write boundary and database triggers enforce the same canonical,
  non-blank string-array shape so the authoritative JSON and indexed projection
  cannot diverge.
- Corroboration filters review, lifecycle, and entity eligibility in SQL and has
  an explicit candidate scan budget. Exceeding that budget fails evaluation
  closed instead of holding the SQLite writer lock for an unbounded history scan.
  Conflict transitions have a separate per-checkpoint work budget; exhausting it
  rolls back that lifecycle page and asks the caller to retry or consolidate.
- Candidate admission, proposition membership, and initial audit events commit in
  one transaction. Duplicate submissions do not emit duplicate creation events;
  source merges and stale-source reactivation use distinct audit event types.
  Corroboration snapshots and promotion decisions use deterministic semantic
  identities that exclude evaluation output and timestamps. Retrying an unchanged
  review/evaluation reuses the revision and emits no false audit history.
- Explicit runtime settings own every store resolved below that boundary. Public
  ingestion helpers may use legacy process-global factories only when neither a
  store nor runtime settings were supplied. A scoped runtime must never fall back
  to global persistence after initialization or lookup failure.
- One user command that changes several authoritative records is one transaction.
  Signal teaching therefore commits the tenant signal definition, every governed
  candidate/revision, and every resolver projection together. Adding another
  multi-record mutation requires a shared unit-of-work API, not compensating
  writes after independently committed calls. Atomic batches must also be
  schema-bounded so request size cannot hold SQLite's single writer indefinitely.
- Source approval may persist an `approving` recovery claim separately, but no
  authority is visible at that point. Candidate promotion, immutable revisions,
  resolver projections, source reconciliation, and the final approved status
  commit together; a failed finalization rolls all authority back while leaving
  only the retryable claim.
- Pending refresh, rejection, ignore, and complete-crawl disappearance are also
  authority transitions. Source workflow state, retrieval-index state, mutable
  resolver support, governed lifecycle, projection repair, and the reconciliation
  checkpoint commit on one write-locked connection. A failure surfaces to the
  caller and rolls back the source state; it must not leave a terminal source row
  beside authority that is still selectable.
- Reviewing a candidate and publishing it are distinct capabilities. Review or
  trust permission can change queue state; any evaluation that can create or
  repair active authority additionally requires apply permission at the API,
  CLI, and shared service boundary before the review transition is attempted.
- Expiring authority is revalidated after acquiring the writer lock and again
  in the final compare-and-swap that publishes its effect. A preflight TTL check
  cannot authorize work that waited past expiry behind another SQLite writer.
- Resource lookup and lifecycle transition are separate API outcomes. Missing
  tenant-scoped resources return `404`; a present resource whose state no longer
  permits the requested transition returns `409`. Broad `ValueError -> 404`
  mappings hide races and are prohibited at mutation boundaries.
- Browser tenant state is a request generation. Every tenant-scoped request and
  rendered mutation action captures that generation and tenant, rejects stale
  responses, and displays authorization or transport failures rather than
  converting them into empty data. Tenant changes invalidate all scoped views.
- Stored prompts, identifiers, provenance, and URLs are untrusted browser input.
  Build dynamic rows with DOM text and attribute APIs instead of HTML string
  interpolation, and allow only HTTP(S) schemes for external links. Security
  regressions execute the page in a browser; source-string assertions are not an
  XSS test, especially while the UI holds an operational API key.
- Long-lived audit collections use stable keyset pagination backed by the same
  sort index. A fixed limit without a cursor makes old governance history
  unreachable; an unbounded list trades that bug for memory and latency risk.
  Parent list responses expose bounded child summaries loaded in set queries;
  child details use a separate cursor rather than an N+1 query loop or an
  unbounded nested response. Primary operator views consume those cursors with
  tenant-generation-bound continuation actions; a paginated API with a
  first-page-only browser still strands old review and audit work.
- Derived authority migrations fail closed on malformed rows and publish their
  completion marker only after every row is projected. Skipping invalid JSON and
  marking the migration complete certifies a graph that cannot be audited or
  repaired on a later startup. Potentially large derived-index backfills use
  keyset batches and a durable cursor so interruption resumes from the last
  committed page instead of deleting and rebuilding the whole index under one
  startup write lock. Any ownership preflight performed outside a writer
  transaction is repeated after `BEGIN IMMEDIATE` and before schema or progress
  mutation; the unlocked result is advisory only.
- Import and migration modes are authorized as a complete batch before the
  first storage read or write. A caller must not leave admitted candidates
  behind when a later requested trust/reject transition is unauthorized, and a
  failed row rolls back the batch's candidates, authority, projections, and
  audit events together.
- Request authorization and tenant resolution precede persistence dependency
  construction. A denied request must not create or migrate a database, load
  bootstrap data, or acquire a SQLite write lock; route dependencies that open
  stores therefore run only after the complete semantic action is authorized.
- A learned source, its extracted rows, retrieval index, governed candidates,
  lifecycle reconciliation, and resolver projections form one authority
  transaction when they share a database. Candidate fan-out is checked before
  opening that write transaction so one source cannot monopolize SQLite's
  writer indefinitely.
- Source lifecycle reconciliation remains one bounded authority transaction
  even when candidate reads are paged. Paging bounds memory, not commit scope;
  a later-page failure rolls back every earlier candidate, revision,
  projection, conflict, and event mutation.
- Tenant-owner migration includes every tenant-bearing authority child table.
  Provenance, entity bindings, current-scope projections, and contributor
  projections are archived before their parent authority rows, with foreign-key
  enforcement enabled as a second integrity check.
- Direct ingestion validates the configured tenant boundary before constructing
  or bootstrapping a persistence store. A denied request must have no durable
  side effects, including an empty SQLite file or bootstrap schema.
- Replace-in-place child collections carry a source-generation identity in
  their pagination cursor. If a refresh replaces the generation between pages,
  continuation fails with a conflict and the caller restarts; silently mixing
  two generations produces an audit view that never existed.
- Automation success means the requested work ran and completed. CLI operational
  failures return nonzero, and CI invokes suites excluded by default pytest
  markers explicitly; a documented-but-deselected E2E suite is not coverage.

## SQLite protected-path architecture reset

Wave 1 is resetting Tacit's SQLite integration to ordinary stdlib
`sqlite3.Connection` and `sqlite3.Cursor` behavior. The custom Python
connection/cursor subclasses, CPython object-layout inspection, native
file-moved probe, retained main/WAL/SHM descriptors, generation lease,
per-statement identity interception, and poison-on-rebind lifecycle are not a
supported architecture and must be removed. SQLite, not a Python wrapper, owns
the native handle, statement dispatch, checkpoint behavior, sidecar generations,
and final close.

The two principal-review findings that forced the reset are independent and
must remain visible:

1. The wrapper breaks normal multi-process WAL behavior. Cooperating SQLite
   processes legitimately create, checkpoint, unlink, and recreate WAL/SHM
   generations across first open, reopen, and last close. A Python layer that
   freezes those pathname/inode generations cannot reliably distinguish normal
   SQLite lifecycle from hostile replacement and can reject a healthy store.
2. Inherited SQLite APIs and the native close lifecycle bypass the proposed
   enforcement boundary. Base `sqlite3.Connection`/`sqlite3.Cursor` entry points
   can be invoked without the Python overrides, and base/native close or object
   finalization can run without wrapper cleanup. Retained guards can therefore
   be skipped or outlive the connection they claim to protect. Expanding the
   override list does not provide a complete VFS or lifetime guarantee.

The replacement contract is a POSIX protected path. Other platforms fail before
filesystem creation or SQLite access until equivalent owner/ACL admission is
implemented:

- Every configured SQLite role uses a distinct file. Resolve configured and
  fallback paths into one effective role map and reject same-path or detectable
  existing-file aliases before opening any store.
- Canonicalize only trusted root-level operating-system aliases, such as macOS
  `/tmp` to `/private/tmp`, before admission. Configuration-time traversal then
  does not follow application symlinks. Reject symlink components and symlink
  or non-regular main, WAL, and SHM entries before `sqlite3.connect()`.
  Application directories, database files, and pre-existing sidecars are
  service-owned and not group/world-writable. Root-owned platform ancestors are
  trusted only when they are not group/world-writable, except for a sticky
  temporary ancestor above a private service-owned directory; the final parent
  never receives that exception. Never repair an unsafe configured path with
  `chmod`.
- Once admitted, the deployment protects the directory from untrusted mutation
  and ordinary SQLite controls sidecar creation, replacement, checkpoint, and
  deletion. Configuration preflight is not an inode pin and is not repeated as
  a claim of connection-lifetime identity.
- Every store requires an exact `wal` journal-mode result before role, schema,
  migration, or user-data writes. A different result fails with the pre-attempt
  database state intact.
- The immutable role marker is file content, not pathname authority. First-open
  role identity, legacy tenant ownership migration, schema migration, and their
  completion markers commit in one explicit writer transaction. Every
  concurrent post-lock fast path validates the role before returning.
- Public runtime descriptors and the complete cross-store role map remain the
  composition boundary. Private store fields and path coincidence do not grant
  ownership.

This threat model trusts the service identity and its protected deployment path.
It does not defend same-UID pathname replacement, a swap-and-restore between
preflight and SQLite I/O, or hostile hot replacement. Planned replacement
requires all Tacit processes to stop. Detecting or safely continuing through
those cases, descriptor-bound opening, and stronger live VFS guarantees require
a real SQLite VFS/native integration or a server database.

Architecture acceptance requires the full protected-path matrix and a real
subprocess WAL scenario, not only unit tests with monkeypatched file calls. Two
processes race first-open on a missing database, confirm exact WAL and one
transactional role/schema, keep overlapping live connections while one writes
and reopens, run a real `wal_checkpoint(TRUNCATE)`, vary which process closes
last, and then use a fresh process to reopen, verify, write, checkpoint, and
close. Assert committed state and bounded completion, not WAL/SHM pathname or
inode persistence.

Performance evidence must come from a checked-in reproducible harness. Run an
ordinary stdlib control and the Tacit path against the same temporary filesystem,
schema, pragmas, warmups, operation counts, and samples. Cover protected-path
validation plus connect/WAL/close, single-row commits, batched statements,
checkpoint/reopen, and the subprocess lifecycle. Emit machine-readable revision,
Python/SQLite/platform, filesystem root, settings, parameters, error count,
descriptor delta, and latency/throughput percentiles; fail on errors or empty
samples. The local 2026-08-13 wrapper timings are historical diagnostics, not an
acceptance baseline, because they measured the architecture being removed and
were not produced by this harness.

## Observability expectations

Best-effort behavior must be visible. Silent fallback is not resilience.

- Emit structured diagnostics for store initialization failure, blocked global
  fallback, persistence degradation, stale-parent races, replay mismatch,
  projection mismatch, and resolver/snapshot usage mismatch.
- Expected degraded paths log stable reason codes, bounded counters, and error
  classes only. They do not attach tracebacks, raw payloads, query text, tenant
  data, credentials, or filesystem paths to routine warning events.
- Time intent classification, discovery, semantic resolution, archetype
  selection, compilation, validation, publication, and persistence separately.
  Persist enough precision to distinguish sub-10ms stages; rounding seconds to
  two decimal places erases the cost of snapshot pinning and repinning.
- Record knowledge counts by disposition and reason, selected knowledge IDs and
  revisions, stage effects, lifecycle backlog, stale sources, conflicts, and
  projection health.
- Record learned-source authority transaction duration, candidate fan-out,
  configured fan-out limit, governed candidate count, index row count, and
  rollback reason. These fields distinguish extraction growth from lock
  contention and lifecycle/projection failures.
- Track the count and age of `approving` source claims plus authority-transaction
  rollback reasons. A growing claim backlog is an operator-visible recovery
  problem even when active authority remains transactionally safe.
- Separate timeout, cancellation, validation failure, backend failure, and audit
  persistence failure in run status and metrics.
- Benchmark reports identify clean versus long-lived state and include latency
  percentiles, recall, signal-to-noise, and zero-match cases.
- A benchmark called a gate owns explicit quality floors and error ceilings,
  exits nonzero when either fails, and carries API authentication and concrete
  tenant identity. Clean state is disposable; long-lived state is evaluated
  from SQLite backup snapshots and never mutated in place.
- Failed chart runs should expose a run identifier and terminal status even when
  validation drops every panel and no investigation revision is produced. The
  same identity travels with unexpected API and streaming failures; if the run
  audit itself cannot be started, the pipeline fails closed and reports that
  audit degradation instead of continuing without a lifecycle record. The
  audit record should retain per-datasource catalog counts and truncation,
  intended versus compiled archetypes, signal-binding disposition, and rejected
  query reasons so a safe abstention remains externally explainable.
- Lifecycle runs carry a bounded lease. If a terminal audit write cannot be
  persisted, the response reports that degradation and a later run/event read
  atomically converts the expired lease into a failed terminal row and event.
- Run leases are authority fences, not only cleanup hints. Revision writes,
  public lifecycle-event appends, and terminal completion prove that the run is
  still active inside the same write transaction; a targeted run read repairs
  that exact expired lease even when a larger abandonment backlog exists.
  Lease time is sampled after the SQLite writer lock is acquired and immediately
  before the compare-and-swap. A timestamp captured before lock contention or
  expensive fingerprint work is not a valid authority fence.
- A new run's lease begins only after its `BEGIN IMMEDIATE` writer lock is
  acquired. Queueing for SQLite cannot consume a run's active lease before the
  run row and start event exist.
- Initial-run completion updates the legacy investigation row, lifecycle run,
  and terminal event in one transaction. A terminal run cannot subsequently
  publish an authoritative revision; abandoned initial runs repair a still-
  running legacy row when their lease is reconciled.
- Operator-facing knowledge lists use bounded keyset pagination and indexes for
  every supported filter combination. Compatibility offsets are capped and are
  not the default path for long-lived audit stores.
- Signal taxonomy definitions and per-signal mapping expansion are bounded too.
  List APIs keyset-page effective tenant definitions, counts cover only the
  visible page, and legacy approval refuses an oversized compatibility fan-out
  before it can monopolize the authority writer transaction.
- Display-oriented compatibility pages must never become an authority boundary.
  Runtime semantic resolution uses a bounded, indexed reverse mapping scan and
  fails closed when its candidate or scan budget is exceeded; it does not treat
  a truncated taxonomy page as the complete organizational vocabulary.
- Reverse semantic resolution is bounded across aggregate work, not merely SQL
  pages. It bulk-loads mappings and definitions in one read snapshot, narrows
  metric candidates with literal-fragment indexes, and reports mapping rows,
  applicable mappings, exact pattern checks, and the configured check budget.
  Broad patterns that cannot be indexed consume that budget and fail closed.
- Pipeline resolution has one investigation-owned work budget. Discovery,
  archetype selection and compilation, initial evidence, symptom rescue, and
  evidence-gap rescue all consume that same budget; a leaf helper may create a
  budget only for a genuinely standalone call.
- Dashboard rescue composition admits aggregate panels, queries, nested nodes,
  scalar characters, and encoded bytes before allocating combined lists. Each
  component being valid independently does not make their composition valid.
- Experimental generated-archetype retrieval requires a concrete environment
  as well as tenant and service scope. Missing mandatory scope produces a
  stable skipped result before the quarantine filesystem is opened; empty scope
  is never interpreted as an exact match.
- Governed resolver projections preserve applicability per metric pattern. Scope
  from one backend-specific pattern must not be unioned into another pattern,
  and projection reconciliation independently checks that invariant.
- Reverse resolution retains same-signal, same-pattern variants for every scope
  dimension that is resolved against an individual catalog entry. In particular,
  datasource-scoped variants may be deduplicated only after the matching metric's
  datasource is known; collapsing them during the mapping scan loses authority
  and can suppress the correct fallback.
- The resolver projection identity therefore includes the normalized datasource
  scope in addition to tenant, signal, metric pattern, and knowledge ID. Schema
  migration, idempotent upsert, and startup audit all compare that exact variant;
  counting only distinct metric patterns can certify an incomplete projection.
- Large derived-index migrations persist their keyset cursor in the same
  transaction as each completed page. Idempotence protects duplicate writes;
  durable progress prevents every process restart from replaying the entire
  completed prefix.
- Store admission that depends on tenant-owner metadata uses the shared
  protected-path read-only connection boundary before enabling WAL, claiming a
  role, or entering structural setup. The same owner decision is revalidated
  inside the first writer transaction. A denied open must preserve the main
  database bytes, journal mode, schema, indexes, markers, and rows exactly.
- Ordinary SQLite `mode=ro` is not a nonmutating filesystem boundary in WAL
  mode: it may create WAL/SHM files or update the shared WAL index. Quiescent
  admission therefore uses SQLite immutable access. When committed live-WAL
  frames exist, admission reads an isolated main-plus-WAL snapshot, verifies
  source stability before and after the complete callback, and retries source
  movement under one absolute busy deadline shared by file copy, SQLite query,
  trusted callback, source verification, and retries. Snapshot work is capped
  at 256 MiB against aggregate bytes actually read from main and WAL; each
  chunk reserves budget before it is written, so source growth cannot overrun
  the cap. SQLite work is interrupted cooperatively at expiry. Arbitrary
  trusted Python callbacks cannot be force-killed safely, but a callback that
  returns after expiry is rejected and cannot authorize mutation. Telemetry
  contains only byte count, duration, attempt, and stable reason codes.
  Larger live-WAL databases fail closed until checkpoint/last-close makes the
  constant-space immutable path available. A live rollback journal also fails
  closed; recovery is a trusted-writer operation, not an admission side effect.
  The enclosing bounded admission operation may retry a transient journal. If
  immutable SQLite inspection fails while the protected main/WAL/journal state
  demonstrably changed, the attempt is classified as source movement and the
  complete callback is retried. An unchanged malformed database is never
  reclassified or admitted.
- Migration cursors encode the not-started state outside the source key's legal
  domain. SQLite row IDs and application IDs may be negative, zero, positive,
  sparse, empty text, or at their integer bounds; none of those values, nor an
  all-empty composite key, doubles as an initialization sentinel. Durable
  progress stores an explicit started bit; runtime scans use `None`.
- Preserving a legal migration key commits every runtime consumer to the same
  domain. Projection audit, quarantine, forward and reverse resolution, and API
  cursors all use explicit absence for their initial state; zero, negative IDs,
  and empty string keys remain data rather than control values. Tightening that
  domain later requires a versioned schema and quarantine migration.
- Optional database capabilities are durable migration outcomes. In particular,
  FTS availability is recorded as available or unavailable after a bounded
  capability probe, so a degraded runtime converges across restarts instead of
  replaying schema and audit work indefinitely.
- Public runtime descriptors and capabilities are the only composition API.
  Migration and learning adapters may request `runtime_settings`,
  `runtime_ownership`, `database_path`, or the public projection store, but may
  not infer ownership from `_db_path`, `_runtime_settings`, `_settings`, or
  `_signal_store`. A descriptor-only adapter is a required regression fixture.
- Lazy dependency factories have two ownership gates. An immutable declared
  descriptor is checked before invocation, and the realized store or provider
  is checked again before any method, bootstrap, prompt, context query, cache
  write, or remote call. Injected factories without declarations fail closed;
  runtime-owned defaults declare themselves at composition time.
- Provider lifetime belongs to active pipeline runs, not callers that happen to
  share a dependency object. A shared provider bundle uses synchronized leases
  and closes only after the final active run releases it. SDK endpoint and
  account defaults are explicit settings-derived values, never ambient process
  variables that can silently change the effective remote owner.
- Credential-chain names are not credential identities. Profile, process,
  metadata, SSO, and web-identity sources are resolved to one frozen,
  non-secretly fingerprinted snapshot before ownership admission, and the SDK
  client is constructed only from that snapshot. Potentially blocking chain
  resolution runs outside the event loop; a later refresh is a new generation
  that must be admitted again.
- Request completion and effective-work completion are separate lifecycle
  events. Cancellation may return a response after a bounded grace period, but
  non-cancellable SDK work keeps consuming the runtime-owned work budget until
  it actually ends. Resistant cleanup is re-cancelled and moved into a bounded
  quarantine; it cannot wedge the active generation or grow retained tasks and
  clients without bound. Rejected factory products enter the same cleanup owner
  instead of being discarded.
- Declared backend ownership is a set contract as well as a per-object
  contract. Realization produces exactly one backend for each declared remote,
  with neither omission nor duplication, before any publisher receives work.
- Factory preflight and realization failures emit only the phase, capability,
  stable reason code, and mismatch dimensions. Component names, paths, tenant
  identifiers, prompts, endpoints, account identifiers, and credentials are
  excluded from these events.
- Sharing a physical SQLite database does not imply sharing authority. The
  knowledge repository must prove the signal-store role and durable tenant
  owner through that public capability at construction and again immediately
  after every writer lock, including caller-bound transactions. A conflicting
  first-opener may leave no repository-specific schema, marker, or row behind.
- Projection audits validate bounded key pages with an indexed relational join,
  never a generated `OR` predicate over the authority table. Query-plan tests
  exercise the exact production statement at representative long-lived-state
  cardinality and reject full scans or temporary sorts.
- Store first-open structure is one transaction: role identity, migration
  metadata, and empty target-schema shadows either commit together or leave the
  original database unchanged. Potentially large legacy rows are not copied in
  that transaction. The legacy table remains authoritative while bounded
  keyset batches populate the shadow and durably advance a cursor; only a final
  writer transaction swaps names, rebuilds indexes, records completion, and
  removes the legacy table. Do not use `sqlite3.executescript()` inside these
  boundaries because it commits implicitly. Mixed-version writers are excluded
  during migration. Feedback, history, knowledge, and signals still duplicate
  parts of transaction-safe migration execution; consolidate that mechanism in
  the persistence wave once the prepare/copy/swap contract is frozen.
- Concurrent migration rechecks are ownership boundaries too. After acquiring
  the writer lock, owner and role validation is the first structural action.
  Every branch that discovers a now-current schema still claims and validates
  the transactional database role identity before returning. Two first-open
  processes with conflicting tenant owners must leave only the winner's schema,
  markers, and tenant data.
- Remote identity and executable settings are separate parts of one owner.
  Backends require exactly one provider identity and a public settings snapshot,
  then pass that snapshot through discovery and publication helpers. Authority
  failures propagate; ordinary availability failures use stable reason codes
  and bounded fingerprints rather than raw exception text or tracebacks.
- Pipeline startup, backend construction, knowledge degradation, and terminal
  failure persistence follow the same diagnostic rule: durable state and logs
  contain a stable reason, exception class, and bounded fingerprint, while raw
  exception messages and traceback state remain out of audit records.
- External publication is an explicit commit phase. Every realized backend is
  ownership-preflighted before remote I/O, the run records a required commit
  marker before the first write, and cancellation is deferred through backend
  fan-out, contract persistence, and terminal audit completion.
- Representative evaluation state is copied without opening the authority
  SQLite files. The benchmark snapshots admitted main/WAL components into
  disposable storage, verifies the complete history/feedback/signals source
  set before and after the copy, and retries or rejects movement. This keeps
  read-only benchmark sources byte-for-byte and directory-entry immutable and
  prevents one report from mixing role generations.
- Evaluation credentials belong to the selected evaluation state, never the
  process-global runtime. Local benchmark HTTP clients disable ambient proxy
  discovery, and classifier/provider failures cannot be normalized into a
  passing label. Gate corpora must contain the required cases and both required
  polarity populations before rates are defined.
- Release input identity is descriptor-bound. Binary packaging rejects
  symlinks and non-regular files with `lstat`, opens with no-follow semantics,
  and revalidates identity and size with `fstat` before streaming. Incremental
  secret ranges are accepted only after an explicit full reachable-history
  baseline; the Git-object-free current-tree scan remains a separate control.
- Bedrock credential discovery is an allowlisted ownership boundary. The
  current runtime admits explicit keys, static credential/config profiles,
  frozen web identity, and one-level assume-role profiles backed by a static
  source profile. Credential-process, SSO/login, container metadata, instance
  metadata, and other unmodeled providers fail before Botocore construction or
  external side effects. Classification follows Botocore's complete provider
  order, environment-name precedence, file precedence, and presence-sensitive
  provider keys. Blank or edge-padded credential controls fail closed instead
  of falling through to a different profile, file, token, or principal. After
  selecting the winner, web identity is represented by one synthesized private
  profile and token copy.
  Static credentials remain separated by provider file, and all admitted file
  discovery is rebound to an explicit private profile so ambient providers
  cannot re-enter after admission. Supporting another provider later requires
  an explicit local/remote identity, mutation tests, and lifecycle ownership.
- Three independent runtime/release improvements remain deliberately separate:
  rotate admitted Bedrock generations before temporary credentials expire,
  replace provider-initialization polling with a notification primitive,
  reserve cleanup capacity before off-lease product construction, and run a
  mandatory second clean same-SHA OCI build in CI. These require focused
  lifecycle or release changes and must not be hidden inside credential-source
  or cleanup fixes. Until cleanup reservation exists, runtime-owned product
  construction must remain inside an admission lease; saturated off-lease
  cleanup fails closed rather than creating unbounded maintenance work.
- When debugging exposes missing telemetry, call it out and add focused
  instrumentation when it is in scope.

The current compilation reconciliation emits a structured
`signal_mapping_usage_disposition_mismatch` warning when the resolver reports a
governed mapping that the selected snapshot cannot safely mark as applied.

## Validation expectations

The [foundation invariant matrix](foundation-invariant-matrix.md) is the
authoritative implementation checklist. Cross-cutting work should use focused
regression tests plus every applicable matrix row, including:

- tenant: pinned, wildcard, missing, mismatch, reserved, and legacy migration
- permissions: read, review, reject, trust, correct, apply, and override
- entry point: API, browser UI, CLI, direct Python, refresh, and replay
- lifecycle: candidate, approved, trusted, active, stale, reactivated,
  superseded, withdrawn, expired, and rejected
- concurrency: duplicate review, evaluation versus rejection, refresh/replay
  races, and stale correction application
- persistence: clean database, upgraded database, unavailable optional store,
  and interrupted multi-table write
- packaging: resources included in wheels and installed-package smoke tests

Run the deterministic Operational Learning and Grounding gates for knowledge,
ranking, evidence, replay, and contract changes. Run the 100-prompt harness when
intent extraction, retrieval, signal resolution, archetype compilation, ranking,
or learned-state selection changes. Compare both clean and representative
long-lived state; a clean-state pass alone does not establish operational
usefulness.

## Related decisions

- [ADR-015: Evidence lifecycle](adr/015-evidence-lifecycle.md)
- [ADR-016: Contextual versus telemetry-evidenced ranking](adr/016-contextual-vs-telemetry-evidenced-ranking.md)
- [ADR-018: Investigation Contract](adr/018-investigation-contract.md)
- [ADR-019: Governed knowledge authority](adr/019-governed-knowledge-authority.md)
- [ADR-020: Generated archetypes stay in shadow](adr/020-generated-archetypes-shadow-before-lifecycle.md)
- [ADR-021: Generated archetype scope](adr/021-generated-archetype-scope-context.md)
- [ADR-022: Operational Knowledge lifecycle](adr/022-operational-knowledge-lifecycle.md)
- [Generated archetype evaluation roadmap](generated-archetype-evaluation-roadmap.md)
