"""Read-only PagerDuty incident-metadata connector.

Fetches incident *metadata* (title, service, urgency, status, assignments,
timestamps) from the PagerDuty REST API v2 and feeds it into Tacit's
artifact-learning pipeline as ``incident`` artifacts.

Design rules (Tacit safety boundaries):

- Read-only. This module performs GET requests only; no write operations.
- Metadata only. Incident notes/log-entry free text is excluded by default so
  the connector cannot smuggle causal narratives into the store. What little
  text is ingested is still treated as untrusted input by ``IncidentExtractor``
  (causal claims are ignored, artifact text is never executed as instructions).
- No RCA output. The connector emits ``LearnedArtifact`` objects and learn
  summaries only — never culprits or causal claims.
- Provenance preserved. Every artifact carries ``source_vendor="pagerduty"``,
  the API base URL as ``source_instance``, the PagerDuty incident id as
  ``external_id``, and the incident's ``html_url`` as ``provenance_url``.

Field normalization is adapted from the Apache-2.0-licensed OpenSRE project
(https://github.com/Tracer-Cloud/opensre, ``integrations/pagerduty/client.py``);
pagination and retry handling are original to Tacit. See
docs/research/opensre-integration-review.md for license/attribution notes.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx
import structlog

from tacit.artifact_learning import (
    ExtractionResult,
    IncidentExtractor,
    LearnedArtifact,
    artifact_from_text,
    learn_artifact,
)
from tacit.config import Settings, settings

logger = structlog.get_logger()

_DEFAULT_TIMEOUT = 30.0
_PAGE_LIMIT = 100  # PagerDuty API maximum per page
_MAX_RETRIES = 3
_MAX_RETRY_AFTER = 60.0  # cap header-provided delays so a bad proxy can't hang the CLI
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class PagerDutyConfigError(RuntimeError):
    """Raised when the PagerDuty connector is not configured."""


class PagerDutyClient:
    """Async, read-only client for the PagerDuty REST API v2.

    Only exposes GET endpoints needed for incident-metadata ingestion.
    """

    def __init__(
        self,
        api_token: str | None = None,
        base_url: str | None = None,
        runtime_settings: Settings | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        config = runtime_settings or settings
        self.api_token = api_token if api_token is not None else config.pagerduty_api_token
        self.base_url = (base_url or config.pagerduty_base_url).rstrip("/")
        if not self.api_token:
            raise PagerDutyConfigError("PagerDuty API token is required (pagerduty_api_token via env or .env).")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Token token={self.api_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=_DEFAULT_TIMEOUT,
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> PagerDutyClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ── Low-level helpers ────────────────────────────────────────────────

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET with bounded retry on 429/5xx, honoring (capped) Retry-After.

        Returns ``{}`` for valid-JSON responses that are not objects; invalid
        JSON raises ``json.JSONDecodeError``.
        """
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = await self._client.get(path, params=params)
            except httpx.TransportError:
                if attempt >= _MAX_RETRIES:
                    raise
                await asyncio.sleep(min(2**attempt, 8))
                continue
            if resp.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                header_delay = _retry_after_seconds(resp)
                delay = header_delay if header_delay is not None else min(2**attempt, 8)
                if delay > _MAX_RETRY_AFTER:
                    logger.warning("pagerduty_retry_after_clamped", path=path, requested=delay)
                    delay = _MAX_RETRY_AFTER
                logger.warning(
                    "pagerduty_retry",
                    path=path,
                    status=resp.status_code,
                    attempt=attempt + 1,
                    delay=delay,
                )
                await asyncio.sleep(delay)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, dict) else {}
        raise httpx.TransportError(f"PagerDuty request failed after retries: {path}")  # pragma: no cover

    async def _paginate(
        self,
        path: str,
        collection_key: str,
        params: dict[str, Any] | None = None,
        max_items: int = 1000,
    ) -> list[dict[str, Any]]:
        """Iterate PagerDuty classic (offset/more) pagination."""
        if max_items < 1:
            raise ValueError(f"max_items must be >= 1, got {max_items}")
        items: list[dict[str, Any]] = []
        offset = 0
        while len(items) < max_items:
            page_params = dict(params or {})
            page_params.update({"limit": _PAGE_LIMIT, "offset": offset})
            data = await self._get(path, params=page_params)
            raw_batch = data.get(collection_key, [])
            if not isinstance(raw_batch, list):
                raw_batch = []
            items.extend(item for item in raw_batch if isinstance(item, dict))
            if not data.get("more") or not raw_batch:
                break
            # Advance by the server's returned collection length (pre-filter),
            # so the next offset matches the API's view of what was served.
            offset += len(raw_batch)
        return items[:max_items]

    # ── Incidents (read-only) ────────────────────────────────────────────

    async def list_incidents(
        self,
        *,
        statuses: list[str] | None = None,
        service_ids: list[str] | None = None,
        since: str | None = None,
        until: str | None = None,
        max_items: int = 1000,
    ) -> list[dict[str, Any]]:
        """List incidents (normalized metadata), filtered by status/service/time."""
        params: dict[str, Any] = {}
        if statuses:
            params["statuses[]"] = statuses
        if service_ids:
            params["service_ids[]"] = service_ids
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        raw = await self._paginate("/incidents", "incidents", params=params, max_items=max_items)
        return [normalize_incident(item) for item in raw]


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _ref_name(obj: dict[str, Any] | None) -> str:
    if not isinstance(obj, dict):
        return ""
    return str(obj.get("summary") or obj.get("name") or "")


def _ref_id(obj: dict[str, Any] | None) -> str:
    if not isinstance(obj, dict):
        return ""
    return str(obj.get("id") or "")


def normalize_incident(inc: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw PagerDuty incident to metadata-only fields.

    Display names are kept for readable artifact bodies; stable PagerDuty ids
    (``*_id`` / ``*_ids``) are kept alongside them so learned context survives
    renames and name collisions. Field selection adapted from OpenSRE
    (Apache-2.0). Free-text fields such as notes and log-entry messages are
    intentionally excluded.
    """
    assignments = [a for a in inc.get("assignments", []) if isinstance(a, dict)]
    teams = [t for t in inc.get("teams", []) if isinstance(t, dict)]
    return {
        "id": str(inc.get("id", "")),
        "incident_number": inc.get("incident_number"),
        "title": str(inc.get("title", "")),
        "status": str(inc.get("status", "")),
        "urgency": str(inc.get("urgency", "")),
        "service": _ref_name(inc.get("service")),
        "service_id": _ref_id(inc.get("service")),
        "escalation_policy": _ref_name(inc.get("escalation_policy")),
        "escalation_policy_id": _ref_id(inc.get("escalation_policy")),
        "teams": [_ref_name(t) for t in teams if _ref_name(t)],
        "team_ids": [_ref_id(t) for t in teams if _ref_id(t)],
        "assigned_to": [_ref_name(a.get("assignee")) for a in assignments if _ref_name(a.get("assignee"))],
        "assignee_ids": [_ref_id(a.get("assignee")) for a in assignments if _ref_id(a.get("assignee"))],
        "created_at": str(inc.get("created_at", "")),
        "resolved_at": str(inc.get("resolved_at") or ""),
        "html_url": str(inc.get("html_url", "")),
    }


class PagerDutyIncidentExtractor(IncidentExtractor):
    """``IncidentExtractor`` with PagerDuty-aware ownership attribution.

    The base extractor attaches ownership hints to the artifact *title*, which
    varies per incident. PagerDuty incidents carry a stable ``service:`` line
    in the connector-generated body, so re-point ownership hints at the
    service entity instead. No other extraction behavior changes.
    """

    _SERVICE_LINE_RE = re.compile(r"^service:\s*(?P<service>.+)$", re.M)

    def extract(self, artifact: LearnedArtifact) -> ExtractionResult:
        result = super().extract(artifact)
        match = self._SERVICE_LINE_RE.search(artifact.body_text)
        if match:
            service = match.group("service").strip()
            if service:
                for hint in result.ownership_hints:
                    hint.entity = service
        return result


def incident_artifact(incident: dict[str, Any], *, source_instance: str) -> LearnedArtifact:
    """Convert normalized incident metadata into a Tacit ``incident`` artifact.

    The body is a factual, metadata-only rendering. Ownership context
    (team/escalation policy) is expressed with the ``owner:`` pattern that
    ``IncidentExtractor`` already understands; no causal language is emitted.
    """
    lines = [
        f"Incident #{incident.get('incident_number') or incident.get('id')}: {incident.get('title', '')}",
        f"status: {incident.get('status', '')}",
        f"urgency: {incident.get('urgency', '')}",
    ]
    service = incident.get("service") or ""
    if service:
        lines.append(f"service: {service}")
        owner = ", ".join(incident.get("teams") or []) or incident.get("escalation_policy") or ""
        if owner:
            lines.append(f"owner: {owner}")
    if incident.get("created_at"):
        lines.append(f"created at {incident['created_at']}")
    if incident.get("resolved_at"):
        lines.append(f"resolved at {incident['resolved_at']}")
    # Stable PagerDuty ids as an inert reference line (survives renames).
    id_parts = [f"incident={incident.get('id', '')}"]
    if incident.get("service_id"):
        id_parts.append(f"service={incident['service_id']}")
    if incident.get("team_ids"):
        id_parts.append(f"teams={','.join(incident['team_ids'])}")
    lines.append(f"pagerduty ids: {' '.join(id_parts)}")
    return artifact_from_text(
        artifact_type="incident",
        title=incident.get("title", "") or f"PagerDuty incident {incident.get('id', '')}",
        body_text="\n".join(lines),
        external_id=str(incident.get("id", "")),
        source_vendor="pagerduty",
        source_instance=source_instance,
        provenance_url=incident.get("html_url") or None,
    )


async def learn_pagerduty_incidents(
    client: PagerDutyClient,
    *,
    statuses: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    max_items: int = 1000,
    dry_run: bool = False,
) -> dict[str, object]:
    """Fetch PagerDuty incident metadata and learn it as incident artifacts.

    ``dry_run=True`` extracts and reports without persisting anything.
    """
    incidents = await client.list_incidents(
        statuses=statuses or ["resolved"],
        since=since,
        until=until,
        max_items=max_items,
    )
    extractor = PagerDutyIncidentExtractor()
    learned = [
        learn_artifact(
            incident_artifact(inc, source_instance=client.base_url),
            extractor,
            dry_run=dry_run,
        )
        for inc in incidents
        if inc.get("id")
    ]

    def _count(key: str) -> int:
        total = 0
        for item in learned:
            value = item.get(key, [])
            if isinstance(value, list):
                total += len(value)
        return total

    return {
        "artifact_type": "incident",
        "source_vendor": "pagerduty",
        "dry_run": dry_run,
        "artifacts_discovered": len(incidents),
        "artifacts_learned": 0 if dry_run else len(learned),
        "learned": learned,
        "summary": {
            "artifact_type": "incident",
            "source_vendor": "pagerduty",
            "learned": 0 if dry_run else len(learned),
            "evidence_requirements": _count("evidence_requirements"),
            "ownership_hints": _count("ownership_hints"),
            "dependency_hints": _count("dependency_hints"),
            "signal_mapping_candidates": _count("signal_mapping_candidates"),
        },
    }
