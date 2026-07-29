from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from tacit.cli import cli
from tacit.config import Settings
from tacit.grounding_benchmark import _contract_for_case, load_grounding_corpus
from tacit.history import InvestigationStore
from tacit.investigation_bundle import build_investigation_bundle, export_investigation_bundle
from tacit.investigation_contract import InvestigationRunType


def _contract(investigation_id: str = "inv-a", *, tenant_id: str = "tenant-a"):
    contract = _contract_for_case(load_grounding_corpus()[0])
    return contract.model_copy(
        update={
            "investigation": contract.investigation.model_copy(update={"id": investigation_id}),
            "request": contract.request.model_copy(
                update={
                    "scope": contract.request.scope.model_copy(update={"tenant_id": tenant_id}),
                }
            ),
        }
    )


def _seed_legacy_history(
    db_path: Path,
    contract_tenants: dict[str, str | None],
    *,
    retain_tenant_column: bool = False,
) -> dict[str, str]:
    store = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    investigation_ids: dict[str, str] = {}
    for label, contract_tenant in contract_tenants.items():
        investigation_id = store.start(f"Legacy {label}")
        investigation_ids[label] = investigation_id
        scope = {} if contract_tenant is None else {"tenant_id": contract_tenant}
        with store._conn() as conn:
            conn.execute(
                """INSERT INTO investigation_revisions (
                       investigation_id, revision, parent_revision, schema_version, contract_json,
                       input_fingerprint, output_fingerprint, engine_version, created_at, reason
                   ) VALUES (?, 1, NULL, '1.0', ?, 'input', 'output', 'legacy', ?, 'fixture')""",
                (investigation_id, json.dumps({"request": {"scope": scope}}), time.time()),
            )
            conn.execute(
                "UPDATE investigations SET current_revision=1 WHERE id=?",
                (investigation_id,),
            )
    with store._conn() as conn:
        conn.execute("DROP INDEX IF EXISTS idx_inv_tenant_started")
        if not retain_tenant_column:
            conn.execute("ALTER TABLE investigations DROP COLUMN tenant_id")
    return investigation_ids


def test_pinned_migration_assigns_ownerless_contracts_and_preserves_explicit_owners(tmp_path):
    db_path = tmp_path / "legacy-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {
            "absent": None,
            "empty": "",
            "default": "default",
            "explicit": "tenant-b",
        },
    )

    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert store.get(investigation_ids["absent"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["empty"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["default"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["explicit"]) is None
    with store._conn() as conn:
        explicit_owner = conn.execute(
            "SELECT tenant_id FROM investigations WHERE id=?",
            (investigation_ids["explicit"],),
        ).fetchone()["tenant_id"]
    assert explicit_owner == "tenant-b"


def test_wildcard_migration_rejects_ownerless_contracts_before_schema_mutation(tmp_path):
    db_path = tmp_path / "ownerless-history.db"
    _seed_legacy_history(
        db_path,
        {"ownerless": "default", "owned": "tenant-b"},
    )

    with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
        )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
        tenant_index = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_inv_tenant_started'"
        ).fetchone()
    assert "tenant_id" not in columns
    assert tenant_index is None


def test_wildcard_migration_preserves_explicit_contract_owners(tmp_path):
    db_path = tmp_path / "owned-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {"tenant-a": "tenant-a", "tenant-b": "tenant-b"},
    )

    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )

    assert store.get(investigation_ids["tenant-a"], tenant_id="tenant-a")["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["tenant-b"], tenant_id="tenant-b")["tenant_id"] == "tenant-b"


def test_pinned_migration_repairs_partial_default_placeholders(tmp_path):
    db_path = tmp_path / "partial-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {"placeholder": "default", "owned": "tenant-b"},
        retain_tenant_column=True,
    )

    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert store.get(investigation_ids["placeholder"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["owned"]) is None
    with store._conn() as conn:
        explicit_owner = conn.execute(
            "SELECT tenant_id FROM investigations WHERE id=?",
            (investigation_ids["owned"],),
        ).fetchone()["tenant_id"]
    assert explicit_owner == "tenant-b"


def test_wildcard_migration_rejects_partial_default_placeholders(tmp_path):
    db_path = tmp_path / "partial-ownerless-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {"placeholder": "default", "owned": "tenant-b"},
        retain_tenant_column=True,
    )

    with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
        )

    with sqlite3.connect(db_path) as conn:
        placeholder = conn.execute(
            "SELECT tenant_id FROM investigations WHERE id=?",
            (investigation_ids["placeholder"],),
        ).fetchone()
        tenant_index = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_inv_tenant_started'"
        ).fetchone()
    assert placeholder == ("default",)
    assert tenant_index is None


def test_complete_schema_default_history_requires_and_records_a_migration_owner(tmp_path):
    db_path = tmp_path / "previously-migrated-history.db"
    original = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    investigation_id = original.start("Previously migrated")
    with original._conn() as conn:
        conn.execute("DROP TABLE investigation_tenant_assignments")

    with pytest.raises(RuntimeError, match="unconfirmed default-tenant ownership"):
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
        )

    pinned = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    assert pinned.get(investigation_id, tenant_id="tenant-a")["tenant_id"] == "tenant-a"

    wildcard = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
    assert wildcard.get(investigation_id, tenant_id="tenant-a") is not None
    assert wildcard.get(investigation_id, tenant_id="default") is None


def test_history_schema_migration_rolls_back_tenant_changes_on_failure(tmp_path):
    db_path = tmp_path / "rollback-history.db"
    _seed_legacy_history(db_path, {"ownerless": None})

    class FailingHistoryStore(InvestigationStore):
        def _backfill_legacy_tenants(
            self,
            conn: sqlite3.Connection,
            *,
            tenant_column_existed: bool,
        ) -> None:
            super()._backfill_legacy_tenants(
                conn,
                tenant_column_existed=tenant_column_existed,
            )
            raise RuntimeError("forced migration failure")

    with pytest.raises(RuntimeError, match="forced migration failure"):
        FailingHistoryStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
        tenant_index = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_inv_tenant_started'"
        ).fetchone()
    assert "tenant_id" not in columns
    assert tenant_index is None


def test_compare_revisions_scopes_both_contract_lookups(monkeypatch):
    contract = _contract()
    calls: list[tuple[int | None, str | None]] = []
    store = object.__new__(InvestigationStore)

    def get_contract(
        investigation_id: str,
        revision: int | None = None,
        *,
        tenant_id: str | None = None,
    ):
        assert investigation_id == "inv-a"
        calls.append((revision, tenant_id))
        return contract

    monkeypatch.setattr(store, "get_contract", get_contract)

    comparison = store.compare_revisions("inv-a", 1, 2, tenant_id="tenant-a")

    assert comparison is not None
    assert calls == [(1, "tenant-a"), (2, "tenant-a")]


class _TrackingBundleStore:
    def __init__(self) -> None:
        self.contract = _contract()
        self.calls: list[tuple[str, str | None]] = []

    def get_contract(self, _investigation_id, _revision=None, *, tenant_id=None):
        self.calls.append(("contract", tenant_id))
        return self.contract

    def get_snapshot(self, _investigation_id, _revision=None, *, tenant_id=None):
        self.calls.append(("snapshot", tenant_id))
        return None

    def list_revisions(self, _investigation_id, *, tenant_id=None):
        self.calls.append(("revisions", tenant_id))
        return [{"revision": self.contract.investigation.revision}]

    def compare_revisions(self, _investigation_id, _left, _right, *, tenant_id=None):
        self.calls.append(("compare", tenant_id))
        return {}


def test_bundle_build_and_export_scope_every_lookup(tmp_path):
    build_store = _TrackingBundleStore()
    build_investigation_bundle(build_store, "inv-a", tenant_id="tenant-a")  # type: ignore[arg-type]
    assert build_store.calls == [
        ("contract", "tenant-a"),
        ("snapshot", "tenant-a"),
        ("revisions", "tenant-a"),
    ]

    export_store = _TrackingBundleStore()
    output = tmp_path / "bundle.tar.gz"
    export_investigation_bundle(
        export_store,  # type: ignore[arg-type]
        "inv-a",
        output,
        tenant_id="tenant-a",
    )
    assert output.exists()
    assert export_store.calls == [
        ("contract", "tenant-a"),
        ("snapshot", "tenant-a"),
        ("revisions", "tenant-a"),
    ]


def test_bundle_requires_a_concrete_tenant_for_wildcard_history(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*")
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    store.persist_contract_revision(_contract(investigation_id, tenant_id="tenant-a"))

    with pytest.raises(ValueError, match="tenant"):
        build_investigation_bundle(store, investigation_id)

    assert build_investigation_bundle(store, investigation_id, tenant_id="tenant-a")


def test_history_stats_rejects_cross_tenant_requests_in_pinned_runtime(tmp_path):
    store = InvestigationStore(
        db_path=tmp_path / "history.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    store.start("Tenant A investigation", tenant_id="tenant-a")

    assert store.stats()["total"] == 1
    with pytest.raises(ValueError, match="Tenant access denied"):
        store.stats(tenant_id="tenant-b")


def test_history_child_records_require_and_enforce_the_selected_tenant(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*")
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    store.persist_contract_revision(_contract(investigation_id, tenant_id="tenant-a"))
    run_id = store.start_run(investigation_id, run_type=InvestigationRunType.REPLAY, base_revision=1)
    store.append_event(investigation_id, run_id, "tenant_a_event", {})
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Tenant A correction",
        tenant_id="tenant-a",
    )
    assert candidate is not None

    with pytest.raises(ValueError, match="tenant"):
        store.list_runs(investigation_id)
    assert store.list_runs(investigation_id, tenant_id="tenant-b") == []
    assert store.list_events(investigation_id, tenant_id="tenant-b") == []
    assert store.list_knowledge_candidates(investigation_id, tenant_id="tenant-b") == []
    assert (
        store.review_knowledge_candidate(
            investigation_id,
            candidate.id,
            approved=True,
            reviewed_by="tenant-b-reviewer",
            tenant_id="tenant-b",
        )
        is None
    )
    assert store.apply_knowledge_candidate(investigation_id, candidate.id, tenant_id="tenant-b") is None

    own_candidates = store.list_knowledge_candidates(investigation_id, tenant_id="tenant-a")
    assert own_candidates[0].status.value == "pending_review"
    assert store.list_runs(investigation_id, tenant_id="tenant-a")[-1]["run_id"] == run_id
    assert any(
        event["event_type"] == "tenant_a_event" for event in store.list_events(investigation_id, tenant_id="tenant-a")
    )


def test_wildcard_history_store_requires_a_tenant_for_direct_reads(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*")
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    with pytest.raises(ValueError, match="tenant"):
        store.start("Ownerless investigation")
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    store.persist_contract_revision(_contract(investigation_id, tenant_id="tenant-a"))

    direct_reads = [
        lambda: store.get(investigation_id),
        lambda: store.get_by_dashboard("dashboard-a"),
        lambda: store.list_recent(),
        lambda: store.get_contract(investigation_id),
        lambda: store.get_snapshot(investigation_id),
        lambda: store.list_revisions(investigation_id),
        lambda: store.compare_revisions(investigation_id, 1, 1),
    ]
    for direct_read in direct_reads:
        with pytest.raises(ValueError, match="tenant"):
            direct_read()


def test_legacy_migration_is_scoped_before_reading_or_persisting(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*")
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    investigation_id = store.start("Tenant A legacy investigation", tenant_id="tenant-a")
    store.finish(investigation_id, status="success")

    assert store.migrate_legacy_investigation(investigation_id, tenant_id="tenant-b") is None
    assert store.get_contract(investigation_id, tenant_id="tenant-a") is None

    migrated = store.migrate_legacy_investigation(investigation_id, tenant_id="tenant-a")
    assert migrated is not None
    assert migrated.request.scope.tenant_id == "tenant-a"


class _FakeHistory:
    def __init__(self) -> None:
        self.contract = _contract()
        self.calls: list[tuple[str, str | None]] = []

    def list_recent(self, **kwargs):
        self.calls.append(("list", kwargs.get("tenant_id")))
        return []

    def get(self, _investigation_id, *, tenant_id=None):
        self.calls.append(("show", tenant_id))
        return {"id": "inv-a", "tenant_id": tenant_id, "status": "success", "prompt": "Prompt"}

    def get_contract(self, _investigation_id, _revision=None, *, tenant_id=None):
        self.calls.append(("contract", tenant_id))
        return self.contract

    def compare_revisions(self, _investigation_id, _left, _right, *, tenant_id=None):
        self.calls.append(("compare", tenant_id))
        return {"same_output": True}

    def replay_contract(self, _investigation_id, _revision=None, *, tenant_id=None, **_kwargs):
        self.calls.append(("replay", tenant_id))
        return self.contract


class _FakeStores:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.history_store = _FakeHistory()

    def history(self):
        return self.history_store

    def knowledge(self):
        raise AssertionError("exact replay must not resolve current knowledge")


@pytest.mark.parametrize(
    "arguments",
    [
        ["history", "list"],
        ["history", "show", "inv-a"],
        ["history", "contract", "inv-a"],
        ["history", "compare", "inv-a", "1", "2"],
        ["history", "export", "inv-a"],
        ["history", "replay", "inv-a"],
    ],
)
def test_history_cli_requires_tenant_in_wildcard_mode(monkeypatch, arguments):
    stores = _FakeStores(Settings(_env_file=None, knowledge_tenant_id="*"))
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code != 0
    assert "--tenant is required" in result.output
    assert stores.history_store.calls == []


def test_history_cli_scopes_all_commands_to_selected_tenant(tmp_path, monkeypatch):
    stores = _FakeStores(Settings(_env_file=None, knowledge_tenant_id="*"))
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    exported: list[str | None] = []

    def fake_export(_store, _investigation_id, _output, *, revision=None, tenant_id=None):
        assert revision is None
        exported.append(tenant_id)

    monkeypatch.setattr("tacit.investigation_bundle.export_investigation_bundle", fake_export)
    runner = CliRunner()
    commands = [
        ["history", "list", "--tenant", "tenant-a"],
        ["history", "show", "inv-a", "--tenant", "tenant-a"],
        ["history", "contract", "inv-a", "--tenant", "tenant-a"],
        ["history", "compare", "inv-a", "1", "2", "--tenant", "tenant-a"],
        [
            "history",
            "export",
            "inv-a",
            "--tenant",
            "tenant-a",
            "--output",
            str(tmp_path / "bundle.tar.gz"),
        ],
        ["history", "replay", "inv-a", "--tenant", "tenant-a"],
    ]

    for command in commands:
        result = runner.invoke(cli, command)
        assert result.exit_code == 0, result.output

    assert stores.history_store.calls == [
        ("list", "tenant-a"),
        ("show", "tenant-a"),
        ("contract", "tenant-a"),
        ("compare", "tenant-a"),
        ("contract", "tenant-a"),
        ("show", "tenant-a"),
        ("replay", "tenant-a"),
    ]
    assert exported == ["tenant-a"]


def test_history_export_requires_export_permission(tmp_path, monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    called = False

    def fake_export(*_args: Any, **_kwargs: Any):
        nonlocal called
        called = True

    monkeypatch.setattr("tacit.investigation_bundle.export_investigation_bundle", fake_export)

    result = CliRunner().invoke(
        cli,
        ["history", "export", "inv-a", "--output", str(tmp_path / "bundle.tar.gz")],
    )

    assert result.exit_code != 0
    assert "knowledge.export" in result.output
    assert called is False


def test_assessment_export_requires_export_permission(tmp_path, monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    called = False

    def fake_export(*_args: Any, **_kwargs: Any):
        nonlocal called
        called = True

    monkeypatch.setattr("tacit.export_report.export_assessment_report", fake_export)

    result = CliRunner().invoke(
        cli,
        ["export-report", "--output", str(tmp_path / "assessment.tar.gz")],
    )

    assert result.exit_code != 0
    assert "knowledge.export" in result.output
    assert called is False


def test_assessment_export_requires_read_permission(tmp_path, monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.export",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    called = False

    def fake_export(*_args: Any, **_kwargs: Any):
        nonlocal called
        called = True

    monkeypatch.setattr("tacit.export_report.export_assessment_report", fake_export)

    result = CliRunner().invoke(
        cli,
        ["export-report", "--output", str(tmp_path / "assessment.tar.gz")],
    )

    assert result.exit_code != 0
    assert "knowledge.read" in result.output
    assert called is False


def test_history_cli_rejects_pinned_tenant_override_before_lookup(monkeypatch):
    stores = _FakeStores(Settings(_env_file=None, knowledge_tenant_id="tenant-a"))
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)

    result = CliRunner().invoke(
        cli,
        ["history", "list", "--tenant", "tenant-b"],
    )

    assert result.exit_code != 0
    assert "Tenant access denied" in result.output
    assert stores.history_store.calls == []
