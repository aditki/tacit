"""Pipeline progress event emission tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from tacit.models.schemas import DashboardSpec
from tacit.pipeline.progress import (
    emit_progress,
    reset_progress_callback,
    set_progress_callback,
)
from tacit.pipeline.recording import PipelineRecorder


class TestEmitProgress:
    def test_noop_without_callback(self):
        emit_progress("intent", "passed", "ok")  # must not raise

    def test_emits_event_to_callback(self):
        events: list[dict] = []
        token = set_progress_callback(events.append)
        try:
            emit_progress("validation", "passed", "queries_validated", panels_before=8, final_panel_count=6)
        finally:
            reset_progress_callback(token)
        assert len(events) == 1
        ev = events[0]
        assert ev["stage"] == "validation"
        assert ev["status"] == "passed"
        assert ev["reason"] == "queries_validated"
        assert ev["details"]["panels_before"] == 8
        assert "ts" in ev

    def test_callback_errors_do_not_propagate(self):
        def broken(_ev):
            raise RuntimeError("listener died")

        token = set_progress_callback(broken)
        try:
            emit_progress("intent", "passed", "ok")  # must not raise
        finally:
            reset_progress_callback(token)

    def test_large_details_are_compacted(self):
        events: list[dict] = []
        token = set_progress_callback(events.append)
        try:
            emit_progress(
                "discovery",
                "passed",
                "ok",
                metrics=[f"metric_{i}" for i in range(100)],
                blob="x" * 5000,
            )
        finally:
            reset_progress_callback(token)
        details = events[0]["details"]
        assert len(details["metrics"]) <= 12
        assert len(details["blob"]) <= 300

    def test_reset_stops_emission(self):
        events: list[dict] = []
        token = set_progress_callback(events.append)
        reset_progress_callback(token)
        emit_progress("intent", "passed", "ok")
        assert events == []


class TestRecorderEmitsProgress:
    def test_stage_record_also_emits(self):
        events: list[dict] = []
        recorder = PipelineRecorder(MagicMock(), "inv-1", tenant_id="default")
        token = set_progress_callback(events.append)
        try:
            recorder.stage("compilation", "passed", "queries_compiled", panel_count=6, query_count=9)
        finally:
            reset_progress_callback(token)
        assert events and events[0]["stage"] == "compilation"
        assert events[0]["details"]["query_count"] == 9

    def test_history_failure_still_emits(self):
        history = MagicMock()
        history.record_stage.side_effect = RuntimeError("db locked")
        events: list[dict] = []
        recorder = PipelineRecorder(history, "inv-1", tenant_id="default")
        token = set_progress_callback(events.append)
        try:
            recorder.stage("binding", "passed", "ok")
        finally:
            reset_progress_callback(token)
        assert events and events[0]["stage"] == "binding"

    def test_validation_with_dropped_panels_emits_partial(self):
        events: list[dict] = []
        recorder = PipelineRecorder(MagicMock(), "inv-1", tenant_id="default")
        token = set_progress_callback(events.append)
        try:
            recorder.validation(["Panel dropped"], panels_before=4, final_panel_count=2)
        finally:
            reset_progress_callback(token)

        assert events and events[0]["stage"] == "validation"
        assert events[0]["status"] == "partial"
        assert events[0]["reason"] == "some_panels_rejected"
        assert events[0]["details"]["panels_dropped"] == 2

    def test_every_history_mutation_uses_the_recorder_tenant(self):
        history = MagicMock()
        recorder = PipelineRecorder(
            history,
            "inv-1",
            run_id="run-1",
            tenant_id="tenant-a",
        )
        intent = SimpleNamespace(
            summary="Checkout latency",
            domain="application",
            services=["checkout"],
            environments=[],
            keywords=["latency"],
            signals=[],
            problem_type="latency",
            archetypes=[],
            timerange="30m",
        )

        recorder.stage("intent", "passed", "intent_classified")
        recorder.event("custom_event", {"status": "passed"})
        recorder.intent(intent)
        recorder.selected_intent(intent, [], [])
        recorder.discovery(SimpleNamespace(datasource_types=["prometheus"], metric_catalog=[]))
        recorder.queries(DashboardSpec(title="Checkout", panels=[]), path_used="archetype")
        recorder.validation([], panels_before=0, final_panel_count=0)
        recorder.finish(status="success")

        for method_name in (
            "append_event",
            "record_intent",
            "record_discovery",
            "record_queries",
            "record_validation",
            "record_stage",
            "finish",
            "complete_run",
        ):
            calls = getattr(history, method_name).call_args_list
            assert calls, method_name
            assert all(call.kwargs["tenant_id"] == "tenant-a" for call in calls), method_name
