from __future__ import annotations

import sqlite3
from bisect import bisect_right
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from tacit.artifact_learning import (
    IncidentExtractor,
    RunbookExtractor,
    _reconcile_stale_artifact_knowledge,
    artifact_from_text,
    learn_artifact,
    learn_incident_dir,
    learn_incident_file,
    learn_runbook_dir,
    learn_runbook_file,
)
from tacit.config import Settings
from tacit.signals import SignalStore


class _DescriptorOnlySignalStore:
    """Delegate behavior while exposing ownership only through the public descriptor."""

    def __init__(self, delegate: SignalStore):
        self._delegate = delegate
        self.runtime_settings = delegate.runtime_settings
        self.runtime_ownership = delegate.runtime_ownership
        self.private_accesses: list[str] = []

    def __getattr__(self, name: str):
        if name == "database_path":
            raise AttributeError(name)
        if name.startswith("_"):
            self.private_accesses.append(name)
            raise AssertionError(f"private ownership probe: {name}")
        return getattr(self._delegate, name)


class _DescriptorOnlyKnowledgeService:
    """Delegate governance without exposing its private signal-store accessor."""

    def __init__(self, delegate):
        self._delegate = delegate
        self.runtime_settings = delegate.runtime_settings
        self.runtime_ownership = delegate.runtime_ownership
        self.private_accesses: list[str] = []

    def __getattr__(self, name: str):
        if name == "database_path":
            raise AttributeError(name)
        if name == "_signal_store":
            self.private_accesses.append(name)
            raise AssertionError(f"private ownership probe: {name}")
        return getattr(self._delegate, name)


def _artifact(body: str):
    return artifact_from_text(
        artifact_type="runbook",
        title="Checkout Runbook",
        body_text=body,
        external_id="checkout-runbook",
        source_vendor="test",
    )


def test_artifact_learning_requires_apply_before_persistence(tmp_path):
    db_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_permissions="knowledge.read,knowledge.review",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            RunbookExtractor(),
            runtime_settings=runtime_settings,
        )

    assert not db_path.exists()


def test_artifact_learning_requires_review_before_creating_storage(tmp_path):
    db_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_permissions="knowledge.read,knowledge.apply",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.review"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            RunbookExtractor(),
            runtime_settings=runtime_settings,
        )

    assert not db_path.exists()


def test_direct_artifact_learning_uses_descriptor_only_store_without_global_fallback(
    tmp_path,
    monkeypatch,
):
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(tmp_path / "signals.db"),
        knowledge_tenant_id="tenant-a",
    )
    real_store = SignalStore(runtime_settings.signals_db_path, runtime_settings=runtime_settings)
    store = _DescriptorOnlySignalStore(real_store)

    def forbidden_global_store():
        raise AssertionError("descriptor-owned artifact learning consulted a process-global store")

    monkeypatch.setattr("tacit.artifact_learning.get_signal_store", forbidden_global_store)
    monkeypatch.setattr("tacit.dashboard_ingest.service.get_signal_store", forbidden_global_store)

    result = learn_artifact(
        _artifact("## Checks\n- check Redis misses"),
        RunbookExtractor(),
        runtime_settings=runtime_settings,
        store=store,
        tenant_id="tenant-a",
    )

    assert result["change_state"] == "created"
    assert result["knowledge_candidate_ids"]
    assert store.private_accesses == []


def test_direct_artifact_learning_never_probes_injected_service_private_store(
    tmp_path,
    monkeypatch,
):
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    database_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(database_path),
        knowledge_tenant_id="tenant-a",
    )
    real_store = SignalStore(database_path, runtime_settings=runtime_settings)
    store = _DescriptorOnlySignalStore(real_store)
    service = _DescriptorOnlyKnowledgeService(
        KnowledgeService(
            KnowledgeRepository(database_path),
            signal_store=real_store,
            runtime_settings=runtime_settings,
        )
    )

    def forbidden_global_store():
        raise AssertionError("descriptor-owned artifact learning consulted a process-global store")

    monkeypatch.setattr("tacit.artifact_learning.get_signal_store", forbidden_global_store)
    result = learn_artifact(
        _artifact("## Checks\n- check Redis misses"),
        RunbookExtractor(),
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=service,
        tenant_id="tenant-a",
    )

    assert result["knowledge_candidate_ids"]
    assert store.private_accesses == []
    assert service.private_accesses == []


def test_runbook_extractor_emits_evidence_requirement_for_check():
    result = RunbookExtractor().extract(_artifact("## Checks\n- check Redis misses"))

    assert len(result.evidence_requirements) == 1
    assert result.evidence_requirements[0].evidence_kind == "cache_misses"
    assert result.evidence_requirements[0].observation_state == "indeterminate"


def test_runbook_dependency_hint_is_not_evidence_requirement():
    result = RunbookExtractor().extract(_artifact("## Dependencies\ncheckout-api depends on redis-cart"))

    assert len(result.dependency_hints) == 1
    assert result.dependency_hints[0].source_entity == "checkout-api"
    assert result.dependency_hints[0].target_entity == "redis-cart"
    assert result.dependency_hints[0].source_type == "runbook"
    assert result.evidence_requirements == []


def test_runbook_dependency_target_strips_trailing_sentence_punctuation():
    result = RunbookExtractor().extract(_artifact("## Dependencies\ncheckout-api depends on redis-cart."))

    assert len(result.dependency_hints) == 1
    assert result.dependency_hints[0].source_entity == "checkout-api"
    assert result.dependency_hints[0].target_entity == "redis-cart"


def test_runbook_dependency_preserves_leading_digit_entity():
    result = RunbookExtractor().extract(_artifact("## Dependencies\n3ds-gateway depends on auth-db"))

    assert len(result.dependency_hints) == 1
    assert result.dependency_hints[0].source_entity == "3ds-gateway"
    assert result.dependency_hints[0].target_entity == "auth-db"


def test_runbook_dependency_section_shorthand_uses_artifact_entity():
    result = RunbookExtractor().extract(_artifact("## Dependencies\n- calls redis-cart"))

    assert len(result.dependency_hints) == 1
    assert result.dependency_hints[0].source_entity == "Checkout"
    assert result.dependency_hints[0].target_entity == "redis-cart"
    assert result.evidence_requirements == []


def test_runbook_dependency_section_adverb_shorthand_uses_artifact_entity():
    result = RunbookExtractor().extract(_artifact("## Dependencies\n- also depends on redis-cart"))

    assert len(result.dependency_hints) == 1
    assert result.dependency_hints[0].source_entity == "Checkout"
    assert result.dependency_hints[0].target_entity == "redis-cart"
    assert result.evidence_requirements == []


def test_runbook_ownership_hint_is_not_evidence_requirement():
    result = RunbookExtractor().extract(_artifact("## Escalation\n- escalate to Payments"))

    assert len(result.ownership_hints) == 1
    assert result.ownership_hints[0].owner == "Payments"
    assert result.ownership_hints[0].source_type == "runbook"
    assert result.evidence_requirements == []


def test_runbook_owner_colon_label_emits_ownership_hint():
    result = RunbookExtractor().extract(_artifact("## Owners\n- Owner: Payments\n- Maintainer: SRE"))

    assert [hint.owner for hint in result.ownership_hints] == ["Payments", "SRE"]
    assert all(hint.hint_kind == "owner_label" for hint in result.ownership_hints)
    assert result.evidence_requirements == []


def test_runbook_mitigation_is_ignored_as_non_evidential():
    result = RunbookExtractor().extract(_artifact("## Checks\n- restart Redis"))

    assert result.evidence_requirements == []
    assert result.warnings == ["ignored_mitigation:restart Redis"]


def test_runbook_causal_claim_does_not_emit_dependency_hint():
    result = RunbookExtractor().extract(_artifact("## Notes\n- Root cause: checkout-api calls redis-cart"))

    assert result.dependency_hints == []
    assert result.evidence_requirements == []
    assert result.warnings == ["ignored_causal_claim:Root cause: checkout-api calls redis-cart"]


@pytest.mark.parametrize("extractor", [RunbookExtractor(), IncidentExtractor()])
def test_extractors_scan_full_lines_for_causal_claims_before_excerpt_truncation(extractor):
    line = "checkout-api depends on redis-cart " + ("context " * 300) + "root cause"

    result = extractor.extract(_artifact(line))

    assert result.dependency_hints == []
    assert len(result.warnings) == 1
    assert result.warnings[0].startswith("ignored_causal_claim:checkout-api depends on redis-cart")
    assert len(result.warnings[0].split(":", 1)[1]) == 2000


def test_runbook_causal_claim_suppresses_continuation_dependency_hint():
    result = RunbookExtractor().extract(
        _artifact("## Notes\n- Root cause: checkout-api saturation\n- checkout-api depends on redis-cart")
    )

    assert result.dependency_hints == []
    assert result.evidence_requirements == []
    assert result.warnings == [
        "ignored_causal_claim:Root cause: checkout-api saturation",
        "ignored_causal_claim:checkout-api depends on redis-cart",
    ]


def test_runbook_causal_section_does_not_emit_following_dependency_hint():
    result = RunbookExtractor().extract(
        _artifact("## RCA\n- checkout-api depends on redis-cart\n## Checks\n- check checkout_latency_seconds")
    )

    assert result.dependency_hints == []
    assert len(result.evidence_requirements) == 1
    assert result.evidence_requirements[0].signal_hint == "checkout_latency_seconds"
    assert result.warnings == [
        "ignored_causal_claim:## RCA",
        "ignored_causal_claim:checkout-api depends on redis-cart",
    ]


def test_runbook_resolution_section_does_not_emit_dependency_hint():
    result = RunbookExtractor().extract(
        _artifact("## Resolution\n- checkout-api depends on redis-cart\n## Checks\n- check checkout_latency_seconds")
    )

    assert result.dependency_hints == []
    assert len(result.evidence_requirements) == 1
    assert result.evidence_requirements[0].signal_hint == "checkout_latency_seconds"
    assert result.warnings == ["ignored_causal_claim:checkout-api depends on redis-cart"]


def test_runbook_ignored_text_is_not_indexed(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact(
        "\n".join(
            [
                "## Checks",
                "- check checkout_latency_seconds",
                "- Root cause: redis-cart",
                "- restart Redis",
            ]
        )
    )

    result = learn_artifact(artifact, RunbookExtractor())

    assert result["warnings"] == [
        "ignored_causal_claim:Root cause: redis-cart",
        "ignored_causal_claim:restart Redis",
    ]
    assert store.search_learning_context("checkout_latency_seconds")
    assert store.search_learning_context("redis") == []


def test_missing_signal_requirement_is_indeterminate():
    result = RunbookExtractor().extract(_artifact("## Checks\n- check DB latency"))

    assert len(result.evidence_requirements) == 1
    assert result.evidence_requirements[0].signal_hint is None
    assert result.evidence_requirements[0].observation_state == "indeterminate"


def test_dotted_metric_names_are_extracted_as_candidates():
    result = RunbookExtractor().extract(_artifact("## Checks\n- check system.cpu.user"))

    assert len(result.evidence_requirements) == 1
    assert result.evidence_requirements[0].signal_hint == "system.cpu.user"
    assert len(result.signal_mapping_candidates) == 1
    assert result.signal_mapping_candidates[0].candidate_metric == "system.cpu.user"


def test_artifact_ids_include_source_vendor():
    pagerduty = artifact_from_text(
        artifact_type="incident",
        title="INC-123",
        body_text="observed checkout_errors_total",
        external_id="INC-123",
        source_vendor="pagerduty",
    )
    jira = artifact_from_text(
        artifact_type="incident",
        title="INC-123",
        body_text="observed checkout_errors_total",
        external_id="INC-123",
        source_vendor="jira",
    )

    assert pagerduty.id != jira.id


def test_repeated_check_lines_persist_as_distinct_extractions(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Checks\n- check redis_cache_misses_total\n- check redis_cache_misses_total")

    result = learn_artifact(artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(artifact.id)

    assert result["change_state"] == "created"
    assert len(rows["evidence_requirements"]) == 2
    assert rows["evidence_requirements"][0]["id"] != rows["evidence_requirements"][1]["id"]


def test_dry_run_does_not_open_signal_store(monkeypatch):
    def fail_store():
        raise AssertionError("dry-run should not open the signal store")

    monkeypatch.setattr("tacit.artifact_learning.get_signal_store", fail_store)

    result = learn_artifact(_artifact("## Checks\n- check redis_cache_misses_total"), RunbookExtractor(), dry_run=True)

    assert result["dry_run"] is True
    assert result["summary"]["evidence_requirements"] == 1


def test_artifact_signal_candidates_do_not_create_active_mappings(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    result = learn_artifact(artifact, RunbookExtractor())

    assert result["summary"]["signal_mapping_candidates"] == 1
    assert result["mappings_created"] == 0
    rows = store.list_artifact_extractions(artifact.id)
    assert len(rows["signal_mapping_candidates"]) == 1
    assert rows["signal_mapping_candidates"][0]["candidate_metric"] == "redis_cache_misses_total"
    assert store.get_signal_type("cache_misses") is None


def test_incident_extractor_preserves_observed_evidence_without_learning_root_cause():
    result = IncidentExtractor().extract(
        artifact_from_text(
            artifact_type="incident",
            title="INC-482 checkout latency",
            body_text="\n".join(
                [
                    "## Symptoms",
                    "- observed redis_cache_misses_total above normal",
                    "## Investigation References",
                    "- See INC-481 and checkout runbook",
                    "## Resolution",
                    "- Root cause: redis-cart",
                ]
            ),
            external_id="INC-482",
            source_vendor="test",
        )
    )

    assert len(result.evidence_requirements) == 1
    assert result.evidence_requirements[0].source_type == "incident"
    assert result.evidence_requirements[0].observation_state == "observed"
    assert result.evidence_requirements[0].signal_hint == "redis_cache_misses_total"
    assert len(result.signal_mapping_candidates) == 1
    assert result.dependency_hints == []
    assert result.warnings == ["ignored_causal_claim:Root cause: redis-cart"]


def test_incident_colon_evidence_labels_emit_observed_requirements():
    result = IncidentExtractor().extract(
        artifact_from_text(
            artifact_type="incident",
            title="INC-908 checkout labels",
            body_text="Evidence: checkout_errors_total spike\nSignal: checkout_latency_seconds elevated",
            external_id="INC-908",
            source_vendor="test",
        )
    )

    assert [row.subject for row in result.evidence_requirements] == [
        "checkout_errors_total spike",
        "checkout_latency_seconds elevated",
    ]
    assert [row.signal_hint for row in result.evidence_requirements] == [
        "checkout_errors_total",
        "checkout_latency_seconds",
    ]
    assert all(row.observation_state == "observed" for row in result.evidence_requirements)


def test_incident_ignored_rca_text_is_not_indexed(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-900 checkout errors",
        body_text="## Evidence\n- observed checkout_errors_total spike\n## Resolution\n- Root cause: redis-cart",
        external_id="INC-900",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == ["ignored_causal_claim:Root cause: redis-cart"]
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_artifact_learning_requires_explicit_tenant_for_wildcard_config(monkeypatch):
    monkeypatch.setattr("tacit.config.settings.api_auth_enabled", True)
    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "*")

    with pytest.raises(ValueError, match="tenant_id is required"):
        learn_artifact(_artifact("## Checks\n- check Redis misses"), RunbookExtractor(), dry_run=True)


def test_artifact_learning_enforces_supplied_runtime_tenant():
    with pytest.raises(ValueError, match="Tenant access denied"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            RunbookExtractor(),
            dry_run=True,
            runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
            tenant_id="tenant-b",
        )


def test_artifact_tenant_denial_precedes_store_initialization(tmp_path):
    db_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_tenant_id="tenant-a",
    )

    with pytest.raises(ValueError, match="Tenant access denied"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            RunbookExtractor(),
            runtime_settings=runtime_settings,
            tenant_id="tenant-b",
        )

    assert not db_path.exists()


@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_learning_requires_read_before_extraction_or_store(tmp_path, dry_run):
    db_path = tmp_path / "read-protected-artifacts.db"

    class ForbiddenExtractor:
        def extract(self, _artifact):
            raise AssertionError("artifact extracted before knowledge.read authorization")

    with pytest.raises(PermissionError, match="Missing permission: knowledge.read"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            ForbiddenExtractor(),
            dry_run=dry_run,
            runtime_settings=Settings(
                _env_file=None,
                knowledge_permissions="knowledge.review,knowledge.apply",
                signals_db_path=str(db_path),
            ),
        )

    assert not db_path.exists()


def test_artifact_reingestion_without_read_discloses_nothing_and_changes_nothing(tmp_path):
    db_path = tmp_path / "reingestion-read-boundary.db"
    permissive_settings = Settings(_env_file=None)
    store = SignalStore(db_path=db_path, runtime_settings=permissive_settings)
    artifact = _artifact("## Checks\n- check Redis misses")
    learned = learn_artifact(artifact, RunbookExtractor(), store=store)
    assert learned["knowledge_candidate_ids"]
    with store._conn() as conn:
        conn.execute(
            "UPDATE evidence_requirements SET review_state='trusted' WHERE tenant_id=? AND artifact_id=?",
            ("default", artifact.id),
        )
        candidate_count = int(conn.execute("SELECT COUNT(*) FROM knowledge_candidates").fetchone()[0])
    before = deepcopy(store.list_artifact_extractions(artifact.id, tenant_id="default"))

    class ForbiddenExtractor:
        def extract(self, _artifact):
            raise AssertionError("artifact re-extracted before knowledge.read authorization")

    restricted_settings = permissive_settings.model_copy(
        update={"knowledge_permissions": "knowledge.review,knowledge.apply"}
    )
    restricted_store = SignalStore(db_path=db_path, runtime_settings=restricted_settings)
    with pytest.raises(PermissionError, match="Missing permission: knowledge.read"):
        learn_artifact(
            artifact,
            ForbiddenExtractor(),
            runtime_settings=restricted_settings,
            store=restricted_store,
        )

    assert restricted_store.list_artifact_extractions(artifact.id, tenant_id="default") == before
    with restricted_store._conn() as conn:
        assert int(conn.execute("SELECT COUNT(*) FROM knowledge_candidates").fetchone()[0]) == candidate_count


@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_learning_rejects_explicit_and_store_settings_disagreement_before_extraction(
    tmp_path,
    dry_run,
):
    from tacit.knowledge.repository import KnowledgeRepository

    db_path = tmp_path / "split-runtime.db"
    restricted_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_permissions="knowledge.review,knowledge.apply",
    )
    permissive_settings = restricted_settings.model_copy(
        update={"knowledge_permissions": "knowledge.read,knowledge.review,knowledge.apply"}
    )
    store = SignalStore(db_path=db_path, runtime_settings=restricted_settings)

    class ForbiddenExtractor:
        def extract(self, _artifact):
            raise AssertionError("artifact extracted before runtime composition validation")

    with pytest.raises(ValueError, match="runtime settings must match"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            ForbiddenExtractor(),
            dry_run=dry_run,
            runtime_settings=permissive_settings,
            store=store,
        )

    assert KnowledgeRepository(db_path).list_candidates(limit=None) == []


@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_learning_rejects_an_unowned_injected_store_before_extraction(tmp_path, dry_run):
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    backing_store = SignalStore(
        db_path=tmp_path / "unowned-store.db",
        runtime_settings=runtime_settings,
    )

    class UnownedStore:
        _db_path = backing_store._db_path

        def __getattr__(self, name):
            if name in {"runtime_settings", "settings", "_runtime_settings", "_settings"}:
                raise AttributeError(name)
            return getattr(backing_store, name)

    class ForbiddenExtractor:
        def extract(self, _artifact):
            raise AssertionError("artifact extracted before runtime ownership validation")

    with pytest.raises(ValueError, match="require explicit runtime settings"):
        learn_artifact(
            _artifact("## Checks\n- check Redis misses"),
            ForbiddenExtractor(),
            dry_run=dry_run,
            store=UnownedStore(),
        )


@pytest.mark.parametrize("learner", [learn_runbook_file, learn_incident_file])
@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_file_learning_honors_restricted_service_before_file_access(
    tmp_path,
    monkeypatch,
    learner,
    dry_run,
):
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    restricted_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.review,knowledge.apply",
    )
    service = KnowledgeService(
        KnowledgeRepository(
            tmp_path / "restricted-service.db",
            runtime_settings=restricted_settings,
        ),
        runtime_settings=restricted_settings,
    )

    def forbidden_read_text(_path):
        raise AssertionError("artifact file read before service authorization")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    with pytest.raises(PermissionError, match="Missing permission: knowledge.read"):
        learner(
            tmp_path / "restricted.md",
            dry_run=dry_run,
            knowledge_service=service,
        )


@pytest.mark.parametrize("learner", [learn_runbook_file, learn_incident_file])
@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("disagreement", ["settings", "database"])
def test_artifact_file_learning_rejects_store_service_disagreement_before_file_access(
    tmp_path,
    monkeypatch,
    learner,
    dry_run,
    disagreement,
):
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    runtime_settings = Settings(_env_file=None)
    store_path = tmp_path / "artifact-store.db"
    store = SignalStore(db_path=store_path, runtime_settings=runtime_settings)
    service_settings = (
        runtime_settings.model_copy(
            update={
                "knowledge_tenant_id": "tenant-a",
                "signals_db_path": str(store_path),
            }
        )
        if disagreement == "settings"
        else runtime_settings
    )
    if disagreement == "settings":
        from tacit.runtime_ownership import runtime_descriptor_for_store

        class SplitSettingsService:
            runtime_settings = service_settings
            database_path = store_path
            runtime_ownership = runtime_descriptor_for_store(
                component="split_settings_service",
                runtime_settings=service_settings,
                database_role="signals",
                database_path=store_path,
            )

        service = SplitSettingsService()
    else:
        repository_path = tmp_path / "knowledge-service.db"
        service = KnowledgeService(
            KnowledgeRepository(repository_path, runtime_settings=service_settings),
            runtime_settings=service_settings,
        )

    def forbidden_read_text(_path):
        raise AssertionError("artifact file read before runtime composition validation")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    expected = "runtime settings must match" if disagreement == "settings" else "same database"
    with pytest.raises(ValueError, match=expected):
        learner(
            tmp_path / "split.md",
            dry_run=dry_run,
            store=store,
            knowledge_service=service,
        )


@pytest.mark.parametrize("learner", [learn_runbook_dir, learn_incident_dir])
@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_directory_learning_rejects_store_service_disagreement_before_traversal(
    tmp_path,
    monkeypatch,
    learner,
    dry_run,
):
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    runtime_settings = Settings(_env_file=None)
    store = SignalStore(db_path=tmp_path / "directory-store.db", runtime_settings=runtime_settings)
    service = KnowledgeService(
        KnowledgeRepository(
            tmp_path / "directory-service.db",
            runtime_settings=runtime_settings,
        ),
        runtime_settings=runtime_settings,
    )

    def forbidden_rglob(_path, _pattern):
        raise AssertionError("artifact directory traversed before runtime composition validation")

    monkeypatch.setattr(Path, "rglob", forbidden_rglob)
    with pytest.raises(ValueError, match="same database"):
        learner(
            tmp_path / "split-directory",
            dry_run=dry_run,
            store=store,
            knowledge_service=service,
        )


@pytest.mark.parametrize("learner", [learn_runbook_file, learn_incident_file])
@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_file_learning_requires_read_before_file_access(tmp_path, monkeypatch, learner, dry_run):
    db_path = tmp_path / "file-read-protected.db"

    def forbidden_read_text(_path):
        raise AssertionError("artifact file read before knowledge.read authorization")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    with pytest.raises(PermissionError, match="Missing permission: knowledge.read"):
        learner(
            tmp_path / "restricted.md",
            dry_run=dry_run,
            runtime_settings=Settings(
                _env_file=None,
                knowledge_permissions="knowledge.review,knowledge.apply",
                signals_db_path=str(db_path),
            ),
        )

    assert not db_path.exists()


@pytest.mark.parametrize("learner", [learn_runbook_dir, learn_incident_dir])
@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_directory_learning_requires_read_before_traversal(tmp_path, monkeypatch, learner, dry_run):
    db_path = tmp_path / "directory-read-protected.db"

    def forbidden_rglob(_path, _pattern):
        raise AssertionError("artifact directory traversed before knowledge.read authorization")

    monkeypatch.setattr(Path, "rglob", forbidden_rglob)
    with pytest.raises(PermissionError, match="Missing permission: knowledge.read"):
        learner(
            tmp_path / "restricted",
            dry_run=dry_run,
            runtime_settings=Settings(
                _env_file=None,
                knowledge_permissions="knowledge.review,knowledge.apply",
                signals_db_path=str(db_path),
            ),
        )

    assert not db_path.exists()


@pytest.mark.parametrize("learner", [learn_runbook_dir, learn_incident_dir])
def test_artifact_directory_tenant_denial_precedes_store_initialization(tmp_path, learner):
    source_dir = tmp_path / "artifacts"
    source_dir.mkdir()
    db_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_tenant_id="tenant-a",
    )

    with pytest.raises(ValueError, match="Tenant access denied"):
        learner(
            source_dir,
            runtime_settings=runtime_settings,
            tenant_id="tenant-b",
        )

    assert not db_path.exists()


@pytest.mark.parametrize("learner", [learn_runbook_dir, learn_incident_dir])
@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_directory_limit_fails_before_source_reads_or_persistence(
    tmp_path,
    monkeypatch,
    learner,
    dry_run,
):
    source_dir = tmp_path / "artifacts"
    source_dir.mkdir()
    (source_dir / "one.md").write_text("service: checkout", encoding="utf-8")
    (source_dir / "two.md").write_text("service: payments", encoding="utf-8")
    runtime_settings = Settings(
        _env_file=None,
        artifact_learning_directory_file_limit=1,
    )
    store = SignalStore(tmp_path / "bounded-artifacts.db", runtime_settings=runtime_settings)

    def forbidden_read_text(_path, *_args, **_kwargs):
        raise AssertionError("artifact source read before directory limit validation")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    with pytest.raises(ValueError, match="configured file limit"):
        learner(
            source_dir,
            dry_run=dry_run,
            runtime_settings=runtime_settings,
            store=store,
        )

    assert store.list_learned_artifacts(tenant_id="default") == []


def test_incident_resolution_section_body_is_not_indexed(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-906 checkout resolution",
        body_text="## Evidence\n- observed checkout_errors_total spike\n## Resolution\n- redis-cart saturated",
        external_id="INC-906",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == ["ignored_causal_claim:redis-cart saturated"]
    extractions = store.list_artifact_extractions(artifact.id)
    assert len(extractions["evidence_requirements"]) == 1
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_incident_plain_text_causal_label_suppresses_following_claim(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-904 checkout errors",
        body_text="Evidence:\nobserved checkout_errors_total spike\nRoot cause:\nredis-cart",
        external_id="INC-904",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == ["ignored_causal_claim:Root cause:", "ignored_causal_claim:redis-cart"]
    extractions = store.list_artifact_extractions(artifact.id)
    assert len(extractions["evidence_requirements"]) == 1
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_incident_causal_claim_suppresses_continuation_dependency_hint(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-907 checkout errors",
        body_text=(
            "## Evidence\n"
            "- observed checkout_errors_total spike\n"
            "- Root cause: checkout-api saturation\n"
            "- checkout-api depends on redis-cart"
        ),
        external_id="INC-907",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == [
        "ignored_causal_claim:Root cause: checkout-api saturation",
        "ignored_causal_claim:checkout-api depends on redis-cart",
    ]
    extractions = store.list_artifact_extractions(artifact.id)
    assert len(extractions["evidence_requirements"]) == 1
    assert extractions["dependency_hints"] == []
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_incident_fix_heading_suppresses_following_dependency_hint(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-909 checkout fix",
        body_text="## Evidence\n- observed checkout_errors_total spike\n## Fix:\n- checkout-api depends on redis-cart",
        external_id="INC-909",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == [
        "ignored_causal_claim:## Fix:",
        "ignored_causal_claim:checkout-api depends on redis-cart",
    ]
    extractions = store.list_artifact_extractions(artifact.id)
    assert len(extractions["evidence_requirements"]) == 1
    assert extractions["dependency_hints"] == []
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_incident_leading_causal_labels_suppress_continuation_dependency_hint():
    for label in ("Triggered by:", "Due to:", "Postmortem conclusion:", "Resolved by:"):
        result = IncidentExtractor().extract(
            artifact_from_text(
                artifact_type="incident",
                title=f"INC {label}",
                body_text=(
                    f"## Evidence\n- observed checkout_errors_total spike\n- {label} deploy\n"
                    "- checkout-api depends on redis-cart"
                ),
                external_id=label,
                source_vendor="test",
            )
        )

        assert result.dependency_hints == []
        assert len(result.evidence_requirements) == 1
        assert result.warnings == [
            f"ignored_causal_claim:{label} deploy",
            "ignored_causal_claim:checkout-api depends on redis-cart",
        ]


def test_incident_rca_heading_suppresses_following_claims(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-901 checkout errors",
        body_text="## Evidence\n- observed checkout_errors_total spike\n## RCA\n- redis-cart",
        external_id="INC-901",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == ["ignored_causal_claim:## RCA", "ignored_causal_claim:redis-cart"]
    extractions = store.list_artifact_extractions(artifact.id)
    assert len(extractions["evidence_requirements"]) == 1
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_incident_causal_regex_heading_resets_previous_evidence_section(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = artifact_from_text(
        artifact_type="incident",
        title="INC-903 checkout errors",
        body_text="## Evidence\n- observed checkout_errors_total spike\n## Root Cause Analysis\n- redis-cart",
        external_id="INC-903",
        source_vendor="test",
    )

    result = learn_artifact(artifact, IncidentExtractor())

    assert result["warnings"] == [
        "ignored_causal_claim:## Root Cause Analysis",
        "ignored_causal_claim:redis-cart",
    ]
    extractions = store.list_artifact_extractions(artifact.id)
    assert len(extractions["evidence_requirements"]) == 1
    assert len(extractions["signal_mapping_candidates"]) == 1
    assert store.search_learning_context("checkout_errors_total")
    assert store.search_learning_context("redis") == []


def test_incident_root_cause_hyphen_claim_is_suppressed():
    result = IncidentExtractor().extract(
        artifact_from_text(
            artifact_type="incident",
            title="INC-902 checkout errors",
            body_text="Root-cause: redis-cart",
            external_id="INC-902",
            source_vendor="test",
        )
    )

    assert result.evidence_requirements == []
    assert result.signal_mapping_candidates == []
    assert result.warnings == ["ignored_causal_claim:Root-cause: redis-cart"]


def test_incident_observed_mitigation_word_evidence_is_preserved():
    result = IncidentExtractor().extract(
        artifact_from_text(
            artifact_type="incident",
            title="INC-905 checkout restarts",
            body_text="## Evidence\n- observed restart count increased\n- detected OOM kill events",
            external_id="INC-905",
            source_vendor="test",
        )
    )

    assert [row.subject for row in result.evidence_requirements] == [
        "restart count increased",
        "OOM kill events",
    ]
    assert [row.observation_state for row in result.evidence_requirements] == ["observed", "observed"]
    assert result.warnings == []


def test_dependency_target_is_searchable_as_service(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Dependencies\n- checkout-api depends on redis-cart")

    learn_artifact(artifact, RunbookExtractor())

    rows = store.search_learning_context("redis-cart", service="redis-cart")
    assert rows
    assert rows[0]["signal_type"] == "dependency"


def test_runbook_reingest_lifecycle_is_idempotent_and_updates_on_change(tmp_path, monkeypatch):
    clock = [1_700_000_000.0]
    monkeypatch.setattr("tacit.signals.store.time.time", lambda: clock[0])
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    first_artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    first = learn_artifact(first_artifact, RunbookExtractor())
    first_row = store.get_learned_artifact(first_artifact.id)
    assert first_row is not None

    clock[0] += 1
    second = learn_artifact(first_artifact, RunbookExtractor())
    second_row = store.get_learned_artifact(first_artifact.id)
    assert second_row is not None

    changed_artifact = _artifact("## Checks\n- check redis_cache_misses_total\n- check checkout_latency_seconds")
    clock[0] += 1
    changed = learn_artifact(changed_artifact, RunbookExtractor())
    changed_row = store.get_learned_artifact(changed_artifact.id)
    assert changed_row is not None

    assert first["change_state"] == "created"
    assert second["change_state"] == "skipped"
    assert changed["change_state"] == "updated"
    assert second_row["updated_at"] == first_row["updated_at"]
    assert changed_row["updated_at"] > second_row["updated_at"]


def test_artifact_title_change_rebuilds_title_derived_extractions(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    body = "## Dependencies\n- calls redis-cart\n## Queries\n- checkout_latency_seconds"
    first_artifact = artifact_from_text(
        artifact_type="runbook",
        title="Checkout Runbook",
        body_text=body,
        external_id="stable-runbook",
        source_vendor="test",
    )
    renamed_artifact = artifact_from_text(
        artifact_type="runbook",
        title="Payments Runbook",
        body_text=body,
        external_id="stable-runbook",
        source_vendor="test",
    )

    first = learn_artifact(first_artifact, RunbookExtractor())
    renamed = learn_artifact(renamed_artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(renamed_artifact.id)
    artifact_row = store.get_learned_artifact(renamed_artifact.id)

    assert first["change_state"] == "created"
    assert renamed["change_state"] == "updated"
    assert rows["dependency_hints"][0]["source_entity"] == "Payments"
    assert rows["signal_mapping_candidates"][0]["symptom"] == "Payments Runbook"
    assert artifact_row is not None
    assert artifact_row["title"] == "Payments Runbook"


def test_line_number_only_edit_preserves_reviewed_extraction_state(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    first_artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    learn_artifact(first_artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(first_artifact.id)
    evidence_id = rows["evidence_requirements"][0]["id"]
    with store._conn() as conn:
        conn.execute(
            "UPDATE evidence_requirements SET review_state = 'approved' WHERE id = ?",
            (evidence_id,),
        )

    shifted_artifact = _artifact("A harmless note\n## Checks\n- check redis_cache_misses_total")
    updated = learn_artifact(shifted_artifact, RunbookExtractor())
    shifted_rows = store.list_artifact_extractions(shifted_artifact.id)

    assert updated["change_state"] == "updated"
    assert shifted_rows["evidence_requirements"][0]["id"] == evidence_id
    assert shifted_rows["evidence_requirements"][0]["review_state"] == "approved"


def test_updated_reingest_preserves_reviewed_extraction_state(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    first_artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    first = learn_artifact(first_artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(first_artifact.id)
    evidence_id = rows["evidence_requirements"][0]["id"]
    with store._conn() as conn:
        conn.execute(
            "UPDATE evidence_requirements SET review_state = 'approved' WHERE id = ?",
            (evidence_id,),
        )

    changed_artifact = _artifact("## Checks\n- check redis_cache_misses_total\n- check checkout_latency_seconds")
    updated = learn_artifact(changed_artifact, RunbookExtractor())
    reviewed_rows = store.list_artifact_extractions(changed_artifact.id)
    review_states = {row["signal_hint"]: row["review_state"] for row in reviewed_rows["evidence_requirements"]}

    assert first["change_state"] == "created"
    assert updated["change_state"] == "updated"
    assert review_states["redis_cache_misses_total"] == "approved"
    assert review_states["checkout_latency_seconds"] == "candidate"


def test_skipped_reingest_rebuilds_missing_extractions(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    first = learn_artifact(artifact, RunbookExtractor())
    with store._conn() as conn:
        conn.execute("DELETE FROM evidence_requirements WHERE artifact_id = ?", (artifact.id,))

    second = learn_artifact(artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(artifact.id)

    assert first["change_state"] == "created"
    assert second["change_state"] == "skipped"
    assert len(rows["evidence_requirements"]) == 1


def test_skipped_reingest_replaces_equal_count_extractions_from_an_old_generation(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    first_artifact = _artifact("## Checks\n- check redis_cache_misses_total")
    next_artifact = _artifact("## Checks\n- check checkout_latency_seconds")
    learn_artifact(first_artifact, RunbookExtractor())
    first_rows = store.list_artifact_extractions(first_artifact.id)
    assert len(first_rows["evidence_requirements"]) == 1

    # Simulate the pre-fix crash window: the source generation committed while
    # the equal-sized extraction replacement did not.
    with store._conn() as conn:
        conn.execute(
            """UPDATE learned_artifacts SET fingerprint=?, body_text=?
               WHERE tenant_id='default' AND artifact_id=?""",
            (next_artifact.fingerprint, next_artifact.body_text, next_artifact.id),
        )

    retried = learn_artifact(next_artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(next_artifact.id)

    assert retried["change_state"] == "skipped"
    assert [row["signal_hint"] for row in rows["evidence_requirements"]] == ["checkout_latency_seconds"]
    assert rows["evidence_requirements"][0]["id"] != first_rows["evidence_requirements"][0]["id"]


def test_artifact_source_and_extraction_generation_roll_back_together(tmp_path, monkeypatch):
    tenant_canary = "PRIVATE-ARTIFACT-TENANT-CANARY"
    path_canary = "PRIVATE-ARTIFACT-PATH-CANARY"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id=tenant_canary)
    store = SignalStore(
        db_path=tmp_path / path_canary / "signals.db",
        runtime_settings=runtime_settings,
    )
    runtime_settings = store.runtime_settings
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    first_artifact = _artifact("## Checks\n- check redis_cache_misses_total")
    next_artifact = _artifact("## Checks\n- check checkout_latency_seconds")
    store.record_learned_artifact(
        tenant_id=tenant_canary,
        artifact_id=first_artifact.id,
        artifact_type="runbook",
        fingerprint=first_artifact.fingerprint,
        body_text=first_artifact.body_text,
    )
    store.replace_artifact_extractions(
        tenant_id=tenant_canary,
        artifact_id=first_artifact.id,
        evidence_requirements=[
            {
                "id": "baseline-evidence",
                "subject": "Checkout Runbook",
                "signal_hint": "redis_cache_misses_total",
            }
        ],
    )

    class FakeRepository:
        database_path = store.database_path

        @contextmanager
        def bind_transaction_connection(self, connection):
            yield connection

    class FakeKnowledgeService:
        pass

    knowledge_service = FakeKnowledgeService()
    knowledge_service.runtime_settings = store.runtime_settings
    knowledge_service.database_path = store.database_path
    knowledge_service.repository = FakeRepository()
    knowledge_service.runtime_ownership = store.runtime_ownership
    exception_canary = "PRIVATE-ARTIFACT-EXCEPTION-CANARY"

    def fail_replacement(**_kwargs):
        raise RuntimeError(exception_canary)

    monkeypatch.setattr(store, "replace_artifact_extractions", fail_replacement)
    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match=exception_canary):
            learn_artifact(
                next_artifact,
                RunbookExtractor(),
                store=store,
                runtime_settings=runtime_settings,
                tenant_id=tenant_canary,
                knowledge_service=knowledge_service,
            )

    rendered = repr(logs)
    assert tenant_canary not in rendered
    assert next_artifact.id not in rendered
    assert path_canary not in rendered
    assert exception_canary not in rendered
    diagnostic = next(entry for entry in logs if entry.get("event") == "artifact_authority_transaction_failed")
    assert diagnostic["reason_code"] == "artifact_authority_transaction_failed"
    assert diagnostic["exception_class"] == "RuntimeError"
    assert len(str(diagnostic["tenant_fingerprint"])) == 16
    assert len(str(diagnostic["artifact_fingerprint"])) == 16
    assert len(str(diagnostic["error_fingerprint"])) == 16
    assert "exc_info" not in diagnostic

    persisted = store.get_learned_artifact(first_artifact.id, tenant_id=tenant_canary)
    rows = store.list_artifact_extractions(first_artifact.id, tenant_id=tenant_canary)
    assert persisted is not None
    assert persisted["fingerprint"] == first_artifact.fingerprint
    assert [row["signal_hint"] for row in rows["evidence_requirements"]] == ["redis_cache_misses_total"]


def test_artifact_generation_rolls_back_when_governed_lifecycle_fails(tmp_path, monkeypatch):
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    runtime_settings = Settings(_env_file=None)
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=runtime_settings,
    )
    repository = KnowledgeRepository(store._db_path)
    service = KnowledgeService(
        repository,
        signal_store=store,
        runtime_settings=runtime_settings,
    )
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    def fail_lifecycle(**_kwargs):
        raise RuntimeError("simulated governed lifecycle failure")

    monkeypatch.setattr(service, "reconcile_source_lifecycle", fail_lifecycle)

    with pytest.raises(RuntimeError, match="simulated governed lifecycle failure"):
        learn_artifact(
            artifact,
            RunbookExtractor(),
            store=store,
            runtime_settings=runtime_settings,
            knowledge_service=service,
        )

    assert store.get_learned_artifact(artifact.id) is None
    assert store.list_artifact_extractions(artifact.id) == {
        "evidence_requirements": [],
        "ownership_hints": [],
        "dependency_hints": [],
        "signal_mapping_candidates": [],
    }
    assert repository.list_candidates(limit=None) == []
    with store._conn() as conn:
        indexed = conn.execute(
            """SELECT COUNT(*) FROM learning_context_fts
               WHERE tenant_id='default' AND source_kind='runbook' AND source_id=?""",
            (artifact.id,),
        ).fetchone()[0]
    assert indexed == 0


def test_artifact_generation_rolls_back_when_context_indexing_fails(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")
    index_context = store.index_artifact_context

    def fail_after_index_write(**kwargs):
        assert kwargs["strict"] is True
        index_context(**kwargs)
        raise sqlite3.OperationalError("simulated artifact FTS failure")

    monkeypatch.setattr(store, "index_artifact_context", fail_after_index_write)
    with pytest.raises(sqlite3.OperationalError, match="simulated artifact FTS failure"):
        learn_artifact(artifact, RunbookExtractor(), store=store)

    assert store.get_learned_artifact(artifact.id) is None
    assert store.list_artifact_extractions(artifact.id) == {
        "evidence_requirements": [],
        "ownership_hints": [],
        "dependency_hints": [],
        "signal_mapping_candidates": [],
    }
    with store._conn() as conn:
        assert (
            conn.execute(
                """SELECT COUNT(*) FROM learning_context_fts
               WHERE tenant_id='default' AND source_kind='runbook' AND source_id=?""",
                (artifact.id,),
            ).fetchone()[0]
            == 0
        )


def test_artifact_fanout_limit_is_checked_before_source_persistence(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_source_atomic_candidate_limit=1,
    )
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=runtime_settings,
    )
    artifact = _artifact("## Checks\n- check redis_cache_misses_total\n- check checkout_latency_seconds")

    with pytest.raises(ValueError, match=r"artifact produced \d+ candidates"):
        learn_artifact(
            artifact,
            RunbookExtractor(),
            store=store,
            runtime_settings=runtime_settings,
        )

    assert store.get_learned_artifact(artifact.id) is None
    assert store.list_artifact_extractions(artifact.id)["evidence_requirements"] == []


def test_skipped_reingest_preserves_reviewed_extraction_state(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    first = learn_artifact(artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(artifact.id)
    evidence_id = rows["evidence_requirements"][0]["id"]
    with store._conn() as conn:
        conn.execute(
            "UPDATE evidence_requirements SET review_state = 'approved' WHERE id = ?",
            (evidence_id,),
        )

    second = learn_artifact(artifact, RunbookExtractor())
    reviewed_rows = store.list_artifact_extractions(artifact.id)

    assert first["change_state"] == "created"
    assert second["change_state"] == "skipped"
    assert second["evidence_requirements"][0]["review_state"] == "approved"
    assert reviewed_rows["evidence_requirements"][0]["review_state"] == "approved"


def test_skipped_reingest_repairs_missing_index_without_resetting_review_state(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    learn_artifact(artifact, RunbookExtractor())
    rows = store.list_artifact_extractions(artifact.id)
    evidence_id = rows["evidence_requirements"][0]["id"]
    with store._conn() as conn:
        conn.execute(
            "UPDATE evidence_requirements SET review_state = 'approved' WHERE id = ?",
            (evidence_id,),
        )
        conn.execute(
            "DELETE FROM learning_context_fts WHERE source_kind = ? AND source_id = ?",
            (artifact.artifact_type, artifact.id),
        )

    second = learn_artifact(artifact, RunbookExtractor())
    search = store.search_learning_context(
        "redis_cache_misses_total",
        include_candidates=False,
    )

    assert second["change_state"] == "skipped"
    assert search
    assert search[0]["review_state"] == "approved"


def test_missing_runbook_marks_stale_not_deleted(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")
    store.record_learned_artifact(
        artifact_id=artifact.id,
        artifact_type=artifact.artifact_type,
        external_id=artifact.external_id,
        title=artifact.title,
        body_text=artifact.body_text,
        fingerprint=artifact.fingerprint,
    )

    marked = store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        authority_reconciler=lambda _conn, _artifact: None,
    )
    row = store.get_learned_artifact(artifact.id)

    assert marked == 1
    assert row is not None
    assert row["stale"] is True
    assert row["missing_since"] is not None


def test_list_learned_artifacts_omits_body_text(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")
    store.record_learned_artifact(
        artifact_id=artifact.id,
        artifact_type=artifact.artifact_type,
        source_vendor=artifact.source_vendor or "",
        external_id=artifact.external_id,
        title=artifact.title,
        body_text=artifact.body_text,
        fingerprint=artifact.fingerprint,
    )

    listed = store.list_learned_artifacts(artifact_type="runbook")
    detail = store.get_learned_artifact(artifact.id)

    assert listed
    assert "body_text" not in listed[0]
    assert detail is not None
    assert detail["body_text"] == artifact.body_text


def test_learned_artifacts_and_extractions_are_tenant_scoped(tmp_path):
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )
    for tenant_id, title in (("tenant-a", "Tenant A"), ("tenant-b", "Tenant B")):
        store.record_learned_artifact(
            tenant_id=tenant_id,
            artifact_id="shared-artifact",
            artifact_type="runbook",
            title=title,
            body_text=f"{title} body",
            fingerprint=f"fingerprint-{tenant_id}",
        )
        store.replace_artifact_extractions(
            tenant_id=tenant_id,
            artifact_id="shared-artifact",
            evidence_requirements=[
                {
                    "id": "shared-requirement",
                    "subject": title,
                    "evidence_kind": "latency",
                }
            ],
        )

    tenant_a = store.get_learned_artifact("shared-artifact", tenant_id="tenant-a")
    tenant_b = store.get_learned_artifact("shared-artifact", tenant_id="tenant-b")
    tenant_a_rows = store.list_artifact_extractions("shared-artifact", tenant_id="tenant-a")
    tenant_b_rows = store.list_artifact_extractions("shared-artifact", tenant_id="tenant-b")

    assert tenant_a is not None and tenant_a["title"] == "Tenant A"
    assert tenant_b is not None and tenant_b["title"] == "Tenant B"
    assert tenant_a_rows["evidence_requirements"][0]["subject"] == "Tenant A"
    assert tenant_b_rows["evidence_requirements"][0]["subject"] == "Tenant B"
    assert [row["tenant_id"] for row in store.list_learned_artifacts(tenant_id="tenant-a")] == ["tenant-a"]


def test_artifact_governance_uses_explicit_runtime_settings(tmp_path):
    from tacit.knowledge.repository import KnowledgeRepository

    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    backing_store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=runtime_settings,
    )

    class InjectedStore:
        _db_path = backing_store._db_path

        def __getattr__(self, name):
            if name == "_settings":
                raise AttributeError(name)
            return getattr(backing_store, name)

    result = learn_artifact(
        _artifact("## Checks\n- check redis_cache_misses_total"),
        RunbookExtractor(),
        runtime_settings=runtime_settings,
        store=InjectedStore(),
        tenant_id="tenant-a",
    )

    candidate_ids = result["knowledge_candidate_ids"]
    assert isinstance(candidate_ids, list)
    assert candidate_ids
    repository = KnowledgeRepository(backing_store._db_path)
    assert all(repository.get_candidate(candidate_id, "tenant-a") is not None for candidate_id in candidate_ids)


def test_artifact_governance_derives_settings_from_injected_store(tmp_path):
    from tacit.knowledge.repository import KnowledgeRepository

    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=runtime_settings,
    )

    result = learn_artifact(
        _artifact("## Checks\n- check redis_cache_misses_total"),
        RunbookExtractor(),
        store=store,
        tenant_id="tenant-a",
    )

    candidate_ids = result["knowledge_candidate_ids"]
    assert isinstance(candidate_ids, list)
    assert candidate_ids
    repository = KnowledgeRepository(store._db_path)
    assert all(repository.get_candidate(candidate_id, "tenant-a") is not None for candidate_id in candidate_ids)


def test_artifact_governance_constructs_store_from_explicit_settings(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    from tacit.knowledge.repository import KnowledgeRepository

    db_path = tmp_path / "scoped" / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="*",
        api_auth_enabled=True,
        signals_db_path=str(db_path),
    )

    def unexpected_global_store():
        raise AssertionError("explicit settings consulted the process-global signal store")

    monkeypatch.setattr("tacit.artifact_learning.get_signal_store", unexpected_global_store)
    result = learn_artifact(
        _artifact("## Checks\n- check redis_cache_misses_total"),
        RunbookExtractor(),
        runtime_settings=runtime_settings,
        tenant_id="tenant-a",
    )

    candidate_ids = result["knowledge_candidate_ids"]
    assert isinstance(candidate_ids, list)
    assert candidate_ids
    assert db_path.exists()
    repository = KnowledgeRepository(db_path)
    assert all(repository.get_candidate(candidate_id, "tenant-a") is not None for candidate_id in candidate_ids)


@pytest.mark.parametrize(
    ("configured_tenant", "expected_tenant"),
    [("default", "default"), ("tenant-a", "tenant-a")],
)
def test_legacy_artifact_rows_migrate_to_configured_tenant(
    tmp_path,
    configured_tenant: str,
    expected_tenant: str,
):
    db_path = tmp_path / f"legacy-signals-{configured_tenant}.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE learned_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, artifact_id TEXT NOT NULL UNIQUE,
                artifact_type TEXT NOT NULL, source_vendor TEXT NOT NULL DEFAULT '',
                source_instance TEXT NOT NULL DEFAULT '', external_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '', body_text TEXT NOT NULL DEFAULT '',
                provenance_url TEXT NOT NULL DEFAULT '', fingerprint TEXT NOT NULL DEFAULT '',
                stale INTEGER NOT NULL DEFAULT 0, missing_since REAL,
                first_seen_at REAL NOT NULL, last_seen_at REAL NOT NULL,
                updated_at REAL NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE evidence_requirements (
                id TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, subject TEXT NOT NULL DEFAULT '',
                evidence_kind TEXT NOT NULL DEFAULT '', target_entity TEXT, signal_hint TEXT,
                query_hint TEXT, priority INTEGER, source_artifact_id TEXT NOT NULL DEFAULT '',
                source_excerpt TEXT NOT NULL DEFAULT '', source_type TEXT NOT NULL DEFAULT '',
                confidence_prior REAL NOT NULL DEFAULT 0.5, review_state TEXT NOT NULL DEFAULT 'candidate',
                observation_state TEXT NOT NULL DEFAULT 'indeterminate', extraction_hash TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL
            );
            INSERT INTO learned_artifacts
                (artifact_id, artifact_type, title, body_text, first_seen_at, last_seen_at, updated_at, created_at)
            VALUES ('legacy-artifact', 'runbook', 'Legacy', 'legacy body', 1, 1, 1, 1);
            INSERT INTO evidence_requirements
                (id, artifact_id, subject, evidence_kind, created_at)
            VALUES ('legacy-requirement', 'legacy-artifact', 'checkout', 'latency', 1);
        """)

    store = SignalStore(
        db_path=db_path,
        runtime_settings=Settings(knowledge_tenant_id=configured_tenant),
    )
    artifact = store.get_learned_artifact("legacy-artifact", tenant_id=expected_tenant)
    extractions = store.list_artifact_extractions("legacy-artifact", tenant_id=expected_tenant)

    assert artifact is not None and artifact["tenant_id"] == expected_tenant
    assert extractions["evidence_requirements"][0]["tenant_id"] == expected_tenant
    wildcard_store = SignalStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )
    wildcard_store.record_learned_artifact(
        tenant_id="tenant-b",
        artifact_id="legacy-artifact",
        artifact_type="runbook",
        title="Tenant B",
    )
    assert wildcard_store.get_learned_artifact("legacy-artifact", tenant_id="tenant-b") is not None


def test_copied_artifact_bodies_share_lineage_despite_renamed_titles(tmp_path):
    from tacit.knowledge.enums import EntityKind
    from tacit.knowledge.models import Entity, KnowledgeScope
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    store = SignalStore(db_path=tmp_path / "signals.db")
    service = KnowledgeService(KnowledgeRepository(store._db_path))
    for entity_id, kind, name in (
        ("entity:service:checkout", EntityKind.SERVICE, "checkout"),
        ("entity:datastore:redis-session", EntityKind.DATASTORE, "redis-session"),
    ):
        service.register_entity(
            Entity(
                id=entity_id,
                kind=kind,
                canonical_name=name,
                scope=KnowledgeScope(),
                provenance_refs=[f"catalog:{name}"],
            )
        )
    artifacts = [
        artifact_from_text(
            artifact_type="runbook",
            title=title,
            body_text=(
                f"# {heading}\n"
                "checkout depends on redis-session for cache operations. "
                f"verify latency errors saturation and {traffic_word} before rollback"
            ),
            external_id=external_id,
            source_vendor="file",
            source_instance=source_instance,
        )
        for title, heading, external_id, source_instance, traffic_word in (
            ("Checkout Runbook", "Checkout Runbook", "/team-a/checkout.md", "/team-a", "traffic"),
            ("Renamed Copy", "Completely Different Title", "/team-b/copy.md", "/team-b", "throughput"),
        )
    ]
    candidate_ids = []
    for artifact in artifacts:
        result = learn_artifact(artifact, RunbookExtractor(), store=store)
        learned_candidate_ids = result["knowledge_candidate_ids"]
        assert isinstance(learned_candidate_ids, list)
        candidate_ids.extend(str(candidate_id) for candidate_id in learned_candidate_ids)

    dependency_ids = []
    for candidate_id in candidate_ids:
        stored_candidate = service.repository.get_candidate(candidate_id)
        assert stored_candidate is not None
        if stored_candidate.kind.value == "dependency":
            dependency_ids.append(candidate_id)
    for candidate_id in dependency_ids:
        service.review_candidate(candidate_id, approved=True, reviewer="operator")
    candidate = service.repository.get_candidate(dependency_ids[0])
    assert candidate is not None

    summary, _ = service.corroboration.analyze("default", candidate.proposition.proposition_key)
    lineage_groups = set()
    for candidate_id in dependency_ids:
        stored_candidate = service.repository.get_candidate(candidate_id)
        assert stored_candidate is not None
        lineage_groups.add(stored_candidate.evidence.items[0].lineage_group)

    assert len(lineage_groups) == 1
    assert summary.raw_source_count == 2
    assert summary.independent_source_count == 1


def test_missing_artifact_stale_marking_is_scoped_to_crawled_source(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    first = artifact_from_text(
        artifact_type="runbook",
        title="A",
        body_text="check redis_cache_misses_total",
        external_id="/tmp/team-a/a.md",
        source_vendor="file",
    )
    second = artifact_from_text(
        artifact_type="runbook",
        title="B",
        body_text="check checkout_latency_seconds",
        external_id="/tmp/team-b/b.md",
        source_vendor="file",
    )
    for artifact in (first, second):
        store.record_learned_artifact(
            artifact_id=artifact.id,
            artifact_type=artifact.artifact_type,
            source_vendor=artifact.source_vendor or "",
            external_id=artifact.external_id,
            title=artifact.title,
            body_text=artifact.body_text,
            fingerprint=artifact.fingerprint,
        )

    marked = store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        source_vendor="file",
        external_id_prefix="/tmp/team-a/",
        authority_reconciler=lambda _conn, _artifact: None,
    )

    first_row = store.get_learned_artifact(first.id)
    second_row = store.get_learned_artifact(second.id)
    assert marked == 1
    assert first_row is not None and first_row["stale"] is True
    assert second_row is not None and second_row["stale"] is False


def test_missing_artifact_stale_prefix_treats_like_metacharacters_literally(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    team_a = artifact_from_text(
        artifact_type="runbook",
        title="A",
        body_text="check redis_cache_misses_total",
        external_id="/tmp/runbooks/team_a/a.md",
        source_vendor="file",
    )
    team_xa = artifact_from_text(
        artifact_type="runbook",
        title="B",
        body_text="check checkout_latency_seconds",
        external_id="/tmp/runbooks/teamXa/b.md",
        source_vendor="file",
    )
    for artifact in (team_a, team_xa):
        store.record_learned_artifact(
            artifact_id=artifact.id,
            artifact_type=artifact.artifact_type,
            source_vendor=artifact.source_vendor or "",
            external_id=artifact.external_id,
            title=artifact.title,
            body_text=artifact.body_text,
            fingerprint=artifact.fingerprint,
        )

    marked = store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        source_vendor="file",
        external_id_prefix="/tmp/runbooks/team_a/",
        authority_reconciler=lambda _conn, _artifact: None,
    )

    team_a_row = store.get_learned_artifact(team_a.id)
    team_xa_row = store.get_learned_artifact(team_xa.id)
    assert marked == 1
    assert team_a_row is not None and team_a_row["stale"] is True
    assert team_xa_row is not None and team_xa_row["stale"] is False


def test_stale_artifact_removes_legacy_artifact_only_mappings(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")
    store.record_learned_artifact(
        artifact_id=artifact.id,
        artifact_type=artifact.artifact_type,
        source_vendor=artifact.source_vendor or "",
        external_id=artifact.external_id,
        title=artifact.title,
        body_text=artifact.body_text,
        fingerprint=artifact.fingerprint,
    )
    store.add_mapping(
        "cache_misses",
        "redis_cache_misses_total",
        confidence=0.4,
        source_type="runbook",
        source_refs=[artifact.id],
        review_state="candidate",
    )

    marked = store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        source_vendor=artifact.source_vendor or "",
        authority_reconciler=lambda _conn, _artifact: None,
    )

    signal_type = store.get_signal_type("cache_misses")
    assert marked == 1
    assert signal_type is not None
    assert signal_type["mappings"] == []


def test_stale_runbook_reappears_as_restored_and_reindexed(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    first = learn_artifact(artifact, RunbookExtractor())
    first_row = store.get_learned_artifact(artifact.id)
    assert first_row is not None

    marked = store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        authority_reconciler=lambda _conn, _artifact: None,
    )
    stale_row = store.get_learned_artifact(artifact.id)
    assert marked == 1
    assert stale_row is not None
    assert stale_row["stale"] is True

    restored = learn_artifact(artifact, RunbookExtractor())
    restored_row = store.get_learned_artifact(artifact.id)
    assert restored_row is not None

    assert first["change_state"] == "created"
    assert restored["change_state"] == "restored"
    assert restored_row["stale"] is False
    assert restored_row["missing_since"] is None
    assert restored_row["first_seen_at"] == first_row["first_seen_at"]
    if store._learning_index_available():
        assert store.search_learning_context("redis_cache_misses_total")


def test_stale_artifact_knowledge_reconciliation_pages_past_ten_thousand(monkeypatch, tmp_path):
    row_ids = [-(2**63), -19, 0, *range(1, 10_001), 100_003, 2**63 - 1]
    total = len(row_ids)
    requested_cursors: list[int | None] = []
    reconciled: list[str] = []
    owner_settings = Settings(_env_file=None, signals_db_path=str(tmp_path / "signals.db"))

    class FakeStore:
        runtime_settings = owner_settings
        database_path = Path(owner_settings.signals_db_path)

        @property
        def runtime_ownership(self):
            from tacit.runtime_ownership import runtime_descriptor_for_store

            return runtime_descriptor_for_store(
                component="fake_signal_store",
                runtime_settings=self.runtime_settings,
                database_role="signals",
                database_path=self.database_path,
            )

        def ensure_governed_projection_audit_current(self):
            return None

        def governed_projection_audit_is_current(self, _conn):
            return True

        @contextmanager
        def transaction(self):
            yield object()

        def list_unreconciled_stale_artifacts(self, *, tenant_id, artifact_type, limit, after_id):
            assert tenant_id == "tenant-a"
            assert artifact_type == "runbook"
            requested_cursors.append(after_id)
            start = 0 if after_id is None else bisect_right(row_ids, after_id)
            return [
                {
                    "id": row_id,
                    "artifact_id": f"artifact-{index}",
                    "missing_since": float(index + 1),
                }
                for index, row_id in enumerate(row_ids[start : start + limit], start=start)
            ]

        def mark_artifact_knowledge_reconciled(self, *, tenant_id, artifact_id, missing_since):
            assert tenant_id == "tenant-a"
            assert missing_since > 0
            return True

        def artifact_stale_generation_is_current(self, _conn, **_kwargs):
            return True

    class FakeRepository:
        database_path = Path(owner_settings.signals_db_path)

        @contextmanager
        def bind_transaction_connection(self, conn):
            yield conn

    class FakeKnowledgeService:
        runtime_settings = owner_settings
        repository = FakeRepository()
        database_path = repository.database_path

        @property
        def runtime_ownership(self):
            from tacit.runtime_ownership import runtime_descriptor_for_store

            return runtime_descriptor_for_store(
                component="fake_knowledge_service",
                runtime_settings=self.runtime_settings,
                database_role="signals",
                database_path=self.repository.database_path,
            )

        def reconcile_source_lifecycle(
            self,
            *,
            provenance_ref,
            tenant_id,
            source_stale,
            source_generation_guard,
        ):
            assert tenant_id == "tenant-a"
            assert source_stale is True
            reconciled.append(provenance_ref)

    service = FakeKnowledgeService()

    _reconcile_stale_artifact_knowledge(
        store=FakeStore(),
        tenant_id="tenant-a",
        artifact_type="runbook",
        knowledge_service=service,
    )

    expected_cursors = [None, *(row_ids[index - 1] for index in range(1_000, total, 1_000))]
    assert requested_cursors == expected_cursors
    assert len(reconciled) == total
    assert reconciled[-1] == f"prov_artifact:artifact-{total - 1}"


def test_runbook_directory_reuses_one_scoped_knowledge_service(tmp_path, monkeypatch):
    from tacit.dashboard_ingest import service as dashboard_service

    runbooks = tmp_path / "runbooks"
    runbooks.mkdir()
    (runbooks / "checkout.md").write_text("## Checks\n- check checkout_latency_seconds")
    (runbooks / "payments.md").write_text("## Checks\n- check payments_errors_total")
    store = SignalStore(db_path=tmp_path / "signals.db")
    service_creations = 0
    original_factory = dashboard_service._knowledge_service_for_store

    def counted_factory(*args, **kwargs):
        nonlocal service_creations
        service_creations += 1
        return original_factory(*args, **kwargs)

    monkeypatch.setattr(dashboard_service, "_knowledge_service_for_store", counted_factory)

    result = learn_runbook_dir(runbooks, store=store)

    assert result["artifacts_learned"] == 2
    assert service_creations == 1
