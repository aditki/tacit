# OpenSRE Integration Review

**Branch:** `codex/opensre-integration-review`
**Source:** https://github.com/Tracer-Cloud/opensre @ `4207c718` (2026-07-02, shallow clone)
**Date:** 2026-07-02

## Purpose

Evaluate whether OpenSRE contains reusable *integration plumbing* (API clients, auth,
pagination, retries, connector ergonomics) that can accelerate Tacit's Phase 2 adoption
work. Explicitly out of scope: OpenSRE's agent planner, memory systems, autonomous RCA,
hypothesis graph, and tool-execution loop. All adopted code must terminate at Tacit's
artifact-learning interfaces or normalized backend clients, and must respect Tacit's
safety rules (no RCA/culprit emission, artifacts are untrusted input, provenance
preserved, telemetry evidence separate from contextual ranking).

## 1. License assessment

- **OpenSRE:** Apache License 2.0 (root `LICENSE`, 201 lines, standard text). No separate
  NOTICE file was found in the repo root.
- **Tacit:** MIT.
- **Compatibility:** Apache-2.0 code *may* be included in an MIT-licensed project, but the
  copied portions remain governed by Apache-2.0. Requirements: retain the Apache-2.0
  license text for copied portions, mark modified files as changed, and preserve any
  attribution notices. Verbatim copying therefore carries per-file bookkeeping overhead.
- **Decision taken:** no verbatim copying. The one integration adopted (PagerDuty, see §5)
  is a clean adaptation: Tacit-native async structure, original pagination/retry code, with
  OpenSRE's *field-normalization choices* (which incident fields to keep) credited in the
  module docstring. This keeps attribution simple and unambiguous. If future work copies
  OpenSRE code verbatim, add the Apache-2.0 header to that file and note it here.

## 2. OpenSRE integration inventory

OpenSRE ships ~96 integration modules under `integrations/`. Only those relevant to the
task's target list are inventoried in detail; the rest (databases, queues, cloud infra,
messaging, LLM CLIs) are low relevance to Tacit's artifact-learning mission.

General characteristics of OpenSRE integrations:

- Sync `httpx` clients (a few have async variants, e.g. Datadog `fetch_all`).
- Auth/config via shared `integrations/config_models.py`; connectivity checked via a
  `probe_access()` / `verifier.py` pattern.
- Error handling returns `{"success": False, "error": ...}` dicts routed through
  `platform.observability.capture_service_error` — tightly coupled to OpenSRE internals.
- Purpose-built to feed OpenSRE's RCA/investigation loop ("used for incident
  investigation … during RCA" per docstrings), i.e. outputs are shaped for its agent, not
  for artifact learning.

| Integration | Path | Connects to | Auth | R/W | Fetches | Tests | Maturity | Tacit relevance | Recommendation |
|---|---|---|---|---|---|---|---|---|---|
| PagerDuty | `integrations/pagerduty/` | REST API v2: incidents, log entries, on-calls, services | `Token token=` API key | Read-only | Incident/ownership metadata (artifacts) | Yes | Medium | **High** | **Adapt** (done, §5) |
| GitHub | `integrations/github/` | REST + MCP: repos, issues, comments | PAT / OAuth (`login.py`) | Read + write (issue comments) | Metadata + source code | Yes | Medium-high | Medium | Borrow ideas only (Link-header `paginate()`, rate-limit surfacing in errors). Defer; metadata-only clean-room if Phase 2 needs it |
| Grafana | `integrations/grafana/` | Grafana API + Loki/Mimir/Tempo datasource proxy | Bearer token | Read-only | Dashboards/alert rules/annotations (artifacts) + telemetry | Yes (incl. wire-format fixtures) | Medium-high | Medium (overlap) | Keep Tacit version; borrow ideas (§3) |
| Datadog | `integrations/datadog/` | Logs, metrics, monitors, events APIs | API key + app key headers | Read-only | Monitors (artifacts) + telemetry | Yes | Medium | Medium | Defer. Monitor/dashboard metadata would be a new Tacit backend — clean-room when a design partner needs Datadog |
| Splunk | `integrations/splunk/` | Splunk Core search REST API (not SignalFx) | Bearer token | Read-only | Raw log search (telemetry) | Yes | Medium | Low | Ignore (raw log scraping — excluded by task) |
| Slack | `integrations/slack/` | Report delivery to channels | Bot token | **Write** | Chat delivery | Partial | Medium | Low | Ignore (write path; chat ingestion excluded by default) |
| Alertmanager | `integrations/alertmanager/` | Alertmanager API | Bearer token (or proxy auth) | Read-only | Active alerts (telemetry-ish state) | Yes | Medium | Low-medium | No action; Tacit ingests alert *rules* from backends, not live alert state |
| incident.io | `integrations/incident_io/` | incident.io REST | Bearer token | Read + **write-back** | Incident metadata | Yes | Medium | Medium | Ignore write path. Its retry helper (Retry-After parsing, jittered backoff) informed the retry design in §5 |
| Kubernetes | `integrations/eks/` (+ `helm/`, `argocd/`) | EKS/K8s API | AWS SigV4 / kubeconfig | Read-only | Workload metadata | Yes | Medium | Low-medium | Defer; EKS-specific, not generic K8s metadata. Clean-room later if needed |
| Docs | `integrations/notion/`, `integrations/google_docs/` | Notion / Google Docs APIs | Token / OAuth | Read-only | Doc text (artifacts) | Partial | Low-medium | Medium | Defer; Confluence (the priority) is absent. Tacit's file-based runbook ingestion + these as future connectors |
| OpenTelemetry/Jaeger | `integrations/grafana/tempo.py`, `integrations/tempo/` | Tempo traces | Bearer | Read-only | Telemetry | Partial | Low-medium | Low | Ignore (telemetry evidence path, kept separate by design) |

**Absent from OpenSRE** (despite the target list): Prometheus client (only Mimir via
Grafana proxy), SignalFx (its `splunk` module is Splunk Core), Confluence, FireHydrant,
Rootly, ServiceNow, Backstage/service-catalog (its `port.py` is a hexagonal-architecture
"ports" module, not the Port catalog product).

## 3. Comparison with existing Tacit integrations

| Dimension | Tacit | OpenSRE | Verdict |
|---|---|---|---|
| Grafana client | Async `httpx`, thin, datasource-proxy + dashboard/folder ops, backend abstraction (`tacit/backends/grafana.py`) feeding `DashboardFeatures`/`AlertFeatures` normalization | Sync, richer discovery heuristics (`discover_datasource_uids` with deprioritization), annotations query, explore-URL builders | **Keep Tacit.** Borrow *ideas* later: datasource-UID discovery heuristics; annotation queries are a possible future incident-context artifact |
| Prometheus / VictoriaMetrics | Query/discovery via backend + `dashboard_ingest/promql.py` | No direct client | **Keep Tacit** (no counterpart) |
| SignalFx | Native client + detector/alert ingestion (`tacit/signalfx/`) | Absent | **Keep Tacit** |
| Runbook/incident file ingestion | `artifact_learning.py`: fingerprinting, stale marking, causal-claim suppression, review-state preservation, dry-run | No counterpart (docs fetched as RCA context only) | **Keep Tacit** — this is the differentiated layer |
| Auth handling | Settings-driven (pydantic-settings), env/.env | Central `config_models.py` + `probe_access()` verifier pattern | Tacit fine. The *probe/verify-connection* UX is a nice adoption idea for `tacit connect` (follow-up) |
| Pagination | None (limit params only) | GitHub client: proper Link-header pagination; most others: none | **Gap in both.** New PagerDuty connector implements offset/`more` pagination natively |
| Retry / rate limits | None in HTTP clients (only LLM layer) | Only in a few clients (incident.io: Retry-After + jitter) | **Gap in both.** New PagerDuty connector implements bounded retry honoring `Retry-After` — pattern available to lift into Grafana/SignalFx clients as follow-up |
| Error handling | Exceptions + structlog | `{"success": False}` dicts + vendored observability hooks | Keep Tacit style |
| Data normalization | Strong: `DashboardFeatures`/`AlertFeatures`, Operational IR rows | Per-integration ad-hoc dicts shaped for RCA agent | Keep Tacit |
| Safety boundaries | Causal-claim suppression, artifacts untrusted, telemetry separate | None — integrations feed an autonomous RCA loop | Keep Tacit; do not import OpenSRE data flow |
| Provenance | `source_vendor/source_instance/external_id/provenance_url` on every artifact | `html_url` retained on some payloads; no systematic provenance model | Keep Tacit |
| Test coverage | Extensive unit suite (~560 unit tests) | Present for major integrations (fixtures, wire-format tests) | Comparable practice |

Overlap decisions: **keep Tacit** on every overlapping surface; **borrow** the pagination/
retry/probe ideas; **no replacement** of any Tacit component by OpenSRE code.

## 4. Candidate selection

Ranked against Phase 2 adoption value and governance friction:

1. **PagerDuty incident metadata — selected and implemented** (highest task priority,
   read-only, no Tacit counterpart, feeds incident artifact learning directly).
2. Confluence ingestion — not available in OpenSRE; needs clean-room work. Follow-up.
3. GitHub metadata-only — OpenSRE client exists but mixes in source-code access and MCP
   write paths; a metadata-only clean-room slice is a follow-up.
4. Kubernetes read-only metadata — OpenSRE only has EKS-flavored clients; follow-up.
5. Prometheus client improvements — nothing to take (no OpenSRE Prometheus client).
6. Grafana client improvements — Tacit's is better structured; ideas noted in §3.
7. Datadog read-only monitors/dashboards — viable future backend; clean-room, demand-driven.
8. Service-catalog integrations — absent in OpenSRE.

Explicitly not copied (per task constraints): chat transcript ingestion, source-code
indexing, raw log scraping (Splunk/Loki search), remediation/write operations, autonomous
command execution, and all agent/RCA/memory/hypothesis machinery.

## 5. What was implemented

**`tacit/integrations/pagerduty.py`** — read-only PagerDuty incident-metadata connector.

- **Auth:** `Token token=<key>` (REST API v2), configured via new settings
  `pagerduty_api_token` / `pagerduty_base_url` (secrets via env/.env, consistent with
  existing conventions).
- **Pagination:** classic PagerDuty offset/`more` loop, page size 100, `max_items` cap
  (validated ≥ 1) — an improvement over the OpenSRE client, which fetches a single page.
  Offset advances by the server's raw batch length (pre-filter) so malformed entries
  cannot skew subsequent offsets. Non-object JSON responses yield no items; invalid JSON
  raises.
- **Retries:** bounded (3), exponential backoff capped at 8s, honors `Retry-After` on
  429/5xx clamped to 60s (a hostile/broken header cannot hang the CLI); transport errors
  raise immediately on the final attempt (no wasted terminal sleep). Absent in the
  OpenSRE client (pattern informed by OpenSRE's incident.io client).
- **Normalization:** `normalize_incident()` keeps metadata only (id, number, title, status,
  urgency, service, escalation policy, teams, assignees, timestamps, `html_url`), with
  stable PagerDuty ids (`service_id`, `escalation_policy_id`, `team_ids`, `assignee_ids`)
  preserved alongside display names so learned context survives renames. Free-text fields
  (descriptions, notes, log-entry messages) are deliberately excluded so the connector
  cannot smuggle causal narratives into the store. Field selection adapted from OpenSRE
  `integrations/pagerduty/client.py` (Apache-2.0) — attribution in the module docstring;
  no code copied verbatim.
- **Termination:** `learn_pagerduty_incidents()` converts each incident to a
  `LearnedArtifact` (`artifact_type="incident"`, `source_vendor="pagerduty"`,
  `source_instance=<base_url>`, `external_id=<incident id>`,
  `provenance_url=<html_url>`; stable ids also appear in an inert `pagerduty ids:` body
  line) and feeds `learn_artifact(..., PagerDutyIncidentExtractor())` — a thin wrapper
  over `IncidentExtractor` that re-points ownership hints at the stable `service:` entity
  instead of the per-incident title. Same untrusted-input path as file-based incidents,
  so causal-claim suppression and body-text sanitization apply unchanged.
- **Dry run:** `--dry-run` extracts and summarizes without touching the signal store.
- **CLI:** `tacit learn pagerduty --since <ISO8601> [--until --status --limit --dry-run]`.
  `--since` is required (the PagerDuty list API otherwise silently serves only its default
  recent window); `--limit` must be ≥ 1; failures exit non-zero for CI/cron use.
- **Write operations:** none. The client exposes GET-only methods.

## 6. Tests

`tests/unit/test_pagerduty_integration.py` (25 tests):

- auth/config parsing (missing-token error; settings-driven token/base URL; `Token token=` header)
- pagination follows the `more` flag across offsets and advances by raw batch length past
  malformed entries; multi-value filters encode as repeated `statuses[]`/`service_ids[]`
  params; non-positive `max_items` rejected; non-dict JSON yields no incidents; invalid
  JSON raises
- retry honors `Retry-After` on 429 and clamps hostile values to the 60s cap; transport
  errors exhaust retries without a wasted final sleep; non-retryable 401 raises
- normalization excludes free-text fields (description / trigger log entries) and
  preserves stable ids; ownership hints attach to the service entity, not the title
- CLI contract: `--since` required, `--limit` ≥ 1 enforced, failures exit non-zero
- **safety:** causal claim leaking through an incident title is suppressed by
  `IncidentExtractor` (warning emitted, no extraction carries the claim); prompt-injection
  text is treated as inert data (no extractions produced)
- provenance fields preserved end-to-end (vendor, instance, external id, `html_url`)
- dry run never opens the signal store; non-dry-run persists artifacts with provenance

### Verification run (Linux sandbox)

The sandbox has Python 3.10 only (repo targets ≥3.12; the pinned interpreter could not be
downloaded there), so tests ran under 3.10 with a two-line stdlib compat shim
(`datetime.UTC`, `enum.StrEnum`). Results:

- `tests/unit/test_pagerduty_integration.py`: **25 passed**
- `tests/unit/test_artifact_learning.py`: **46 passed** (no regressions)
- broader unit suite: **559 passed**; remaining failures/errors are pre-existing sandbox
  environment gaps only (missing optional deps `openai`/`boto3`, and modules using PEP 695
  generics or `tomllib`, which cannot load on 3.10) — none touch this change
- `ruff check`, `black --check`, `mypy` (3.12 target) clean on all touched files

**Follow-up:** re-run `uv run pytest tests/unit`, `mypy`, and the benchmark suite on a
Python 3.12 environment before merge.

## 7. Acceptance criteria check

- No agent/RCA/memory/hypothesis logic imported — nothing from `core/`, `surfaces/`,
  `tools/` was taken; only field-selection knowledge from one client module.
- No write actions introduced — connector is GET-only.
- No telemetry evidence mixed into contextual ranking — connector produces artifacts only.
- No artifact text treated as instructions — ingestion goes through `IncidentExtractor`
  (causal-claim suppression + sanitized indexing); covered by tests.
- License compatibility documented — §1; no verbatim copying, attribution in module docstring.
- This report exists at `docs/research/opensre-integration-review.md`.

## 8. Risks and follow-ups

- **3.12 verification:** run full suite + benchmarks on Python 3.12 before merge (sandbox limitation).
- **Retry/pagination reuse:** lift the PagerDuty retry/backoff helper into a shared
  `tacit/integrations/http.py` if a second HTTP connector lands (Grafana/SignalFx clients
  currently have no retry).
- **Probe/verify UX:** OpenSRE's `probe_access()` connection-check pattern would improve
  `tacit connect pagerduty` onboarding; small follow-up.
- **Stale marking:** `learn_pagerduty_incidents` does not yet call
  `mark_missing_artifacts_stale` (file-based dir ingestion does); add once windowed
  re-syncs are a real workflow, since a time-windowed fetch must not mark unseen older
  incidents stale.
- **Confluence/GitHub-metadata/Datadog:** clean-room implementation plans per §4 —
  OpenSRE offers little to copy for these (absent, or entangled with write/source-code paths).
