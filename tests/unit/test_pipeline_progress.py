"""Pipeline progress event emission tests."""

from __future__ import annotations

from unittest.mock import MagicMock

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
        recorder = PipelineRecorder(MagicMock(), "inv-1")
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
        recorder = PipelineRecorder(history, "inv-1")
        token = set_progress_callback(events.append)
        try:
            recorder.stage("binding", "passed", "ok")
        finally:
            reset_progress_callback(token)
        assert events and events[0]["stage"] == "binding"
