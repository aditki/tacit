from __future__ import annotations

import time

import pytest

from tacit.artifact_learning import IncidentExtractor, RunbookExtractor, artifact_from_text, learn_artifact
from tacit.signals import SignalStore


def _artifact(body: str):
    return artifact_from_text(
        artifact_type="runbook",
        title="Checkout Runbook",
        body_text=body,
        external_id="checkout-runbook",
        source_vendor="test",
    )


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


def test_runbook_ownership_hint_is_not_evidence_requirement():
    result = RunbookExtractor().extract(_artifact("## Escalation\n- escalate to Payments"))

    assert len(result.ownership_hints) == 1
    assert result.ownership_hints[0].owner == "Payments"
    assert result.ownership_hints[0].source_type == "runbook"
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
        "ignored_mitigation:restart Redis",
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
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    first_artifact = _artifact("## Checks\n- check redis_cache_misses_total")

    first = learn_artifact(first_artifact, RunbookExtractor())
    first_row = store.get_learned_artifact(first_artifact.id)
    assert first_row is not None

    time.sleep(0.001)
    second = learn_artifact(first_artifact, RunbookExtractor())
    second_row = store.get_learned_artifact(first_artifact.id)
    assert second_row is not None

    changed_artifact = _artifact("## Checks\n- check redis_cache_misses_total\n- check checkout_latency_seconds")
    time.sleep(0.001)
    changed = learn_artifact(changed_artifact, RunbookExtractor())
    changed_row = store.get_learned_artifact(changed_artifact.id)
    assert changed_row is not None

    assert first["change_state"] == "created"
    assert second["change_state"] == "skipped"
    assert changed["change_state"] == "updated"
    assert second_row["updated_at"] == first_row["updated_at"]
    assert changed_row["updated_at"] > second_row["updated_at"]


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

    marked = store.mark_missing_artifacts_stale(artifact_type="runbook", seen_artifact_ids=set())
    row = store.get_learned_artifact(artifact.id)

    assert marked == 1
    assert row is not None
    assert row["stale"] is True
    assert row["missing_since"] is not None


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

    marked = store.mark_missing_artifacts_stale(artifact_type="runbook", seen_artifact_ids=set())
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
