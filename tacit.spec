# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for Tacit single-binary distribution."""

import os
from pathlib import Path

from PyInstaller.utils.hooks import copy_metadata

block_cipher = None
root = os.path.dirname(os.path.abspath(SPEC))


def _without_installation_records(entries):
    """Drop install-layout manifests whose hashes encode virtualenv paths."""
    return [entry for entry in entries if not entry[0].replace("\\", "/").endswith(".dist-info/RECORD")]


schema_smoke_hook_path = Path(workpath) / "tacit_release_schema_smoke_hook.py"
schema_smoke_hook_path.write_text(
    """from __future__ import annotations

import json
import os

if os.environ.get("TACIT_RELEASE_SCHEMA_SMOKE") == "1":
    from tacit.investigation_contract import (
        SCHEMA_VERSION,
        load_investigation_contract_schema,
    )

    schema = load_investigation_contract_schema()
    print(
        json.dumps(
            {
                "schema_title": schema.get("title"),
                "schema_type": schema.get("type"),
                "schema_version": SCHEMA_VERSION,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )
    raise SystemExit(0)
""",
    encoding="utf-8",
)
schema_smoke_hook = str(schema_smoke_hook_path)

metadata_smoke_hook_path = Path(workpath) / "tacit_release_metadata_smoke_hook.py"
metadata_smoke_hook_path.write_text(
    """from __future__ import annotations

import importlib.metadata
import json
import os

if os.environ.get("TACIT_RELEASE_METADATA_SMOKE") == "1":
    distribution = "tacit-ai"
    matching = [
        entry_point
        for entry_point in importlib.metadata.entry_points(group="console_scripts")
        if entry_point.name == "tacit"
    ]
    if len(matching) != 1:
        raise RuntimeError("frozen Tacit distribution must expose exactly one console script")
    print(
        json.dumps(
            {
                "console_script": matching[0].value,
                "distribution": distribution,
                "version": importlib.metadata.version(distribution),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )
    raise SystemExit(0)
""",
    encoding="utf-8",
)
metadata_smoke_hook = str(metadata_smoke_hook_path)

a = Analysis(
    [os.path.join(root, "tacit", "cli.py")],
    pathex=[root],
    binaries=[],
    datas=copy_metadata("tacit-ai")
    + [
        (os.path.join(root, "tacit", "data"), "tacit/data"),
        (os.path.join(root, "tacit", "schemas"), "tacit/schemas"),
        (os.path.join(root, "tacit", "static"), "tacit/static"),
    ],
    hiddenimports=[
        "tacit.main",
        "tacit.cli",
        "tacit.config",
        "tacit.data",
        "tacit.pipeline",
        "tacit.feedback",
        "tacit.ranking",
        "tacit.cache",
        "tacit.validation",
        "tacit.agents",
        "tacit.agents.intent",
        "tacit.agents.providers",
        "tacit.agents.providers.registry",
        "tacit.agents.providers.anthropic",
        "tacit.agents.providers.bedrock",
        "tacit.agents.providers.openai_provider",
        "tacit.agents.providers.ollama",
        "tacit.archetypes",
        "tacit.archetypes.engine",
        "tacit.archetypes.templates",
        "tacit.grafana",
        "tacit.grafana.client",
        "tacit.grafana.adapters",
        "tacit.grafana.adapters.prometheus",
        "tacit.grafana.adapters.cloudwatch",
        "tacit.grafana.adapters.influxdb",
        "tacit.grafana.adapters.graphite",
        "tacit.grafana.adapters.signalfx",
        "tacit.signalfx",
        "tacit.signalfx.client",
        "tacit.signalfx.discovery",
        "tacit.signalfx.publisher",
        "tacit.history",
        "tacit.investigation_contract",
        "tacit.context",
        "tacit.integrations",
        "tacit.integrations.slack",
        "tacit.models",
        "tacit.models.schemas",
        "uvicorn",
        "uvicorn.logging",
        "uvicorn.loops",
        "uvicorn.loops.auto",
        "uvicorn.protocols",
        "uvicorn.protocols.http",
        "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets",
        "uvicorn.protocols.websockets.auto",
        "uvicorn.lifespan",
        "uvicorn.lifespan.on",
        "click",
        "rich",
        "yaml",
        "httpx",
        "pydantic",
        "pydantic_settings",
        "structlog",
        "fastapi",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[schema_smoke_hook, metadata_smoke_hook],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
a.datas = _without_installation_records(a.datas)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="tacit",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
