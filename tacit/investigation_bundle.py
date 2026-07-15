"""Portable, self-contained assessment bundles for one investigation revision."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tacit.history import InvestigationStore


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def build_investigation_bundle(
    store: InvestigationStore,
    investigation_id: str,
    *,
    revision: int | None = None,
) -> bytes:
    contract = store.get_contract(investigation_id, revision)
    if contract is None:
        raise ValueError("Investigation contract not found")
    snapshot = store.get_snapshot(investigation_id, contract.investigation.revision)
    revisions = store.list_revisions(investigation_id)
    files: dict[str, bytes] = {
        "contract.json": _json_bytes(contract.model_dump(mode="json", by_alias=True)),
        "expected_outcomes.json": _json_bytes(
            {
                "grounding_status": contract.grounding.status.value,
                "abstained": contract.grounding.abstained,
                "input_fingerprint": contract.runtime.input_fingerprint,
                "output_fingerprint": contract.runtime.output_fingerprint,
                "unsafe_to_conclude": contract.grounding.unsafe_to_conclude,
            }
        ),
        "revisions.json": _json_bytes(revisions),
    }
    if snapshot is not None:
        files["captured_inputs.json"] = _json_bytes(snapshot.model_dump(mode="json"))
    previous = [item for item in revisions if item["revision"] < contract.investigation.revision]
    if previous:
        comparison = store.compare_revisions(
            investigation_id,
            previous[-1]["revision"],
            contract.investigation.revision,
        )
        files["comparison.json"] = _json_bytes(comparison)
    manifest = {
        "bundle_schema": "tacit.investigation-assessment/1.0",
        "investigation_id": investigation_id,
        "revision": contract.investigation.revision,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "captured_inputs_included": snapshot is not None,
        "files": {
            name: {"sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content)}
            for name, content in files.items()
        },
    }
    files["manifest.json"] = _json_bytes(manifest)
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, content in sorted(files.items()):
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            info.mtime = 0
            info.mode = 0o600
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def export_investigation_bundle(
    store: InvestigationStore,
    investigation_id: str,
    output: Path,
    *,
    revision: int | None = None,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(build_investigation_bundle(store, investigation_id, revision=revision))
    return output
