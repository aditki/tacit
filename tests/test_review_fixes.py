"""Regression tests for the dashboard-ingestion review fixes.

Covers:
  * #3 panel-level drilldown link capture in parse_dashboard_json
  * #4 strict boolean coercion of the `auto_approve` flag
  * #5 confidence-bound validation in SignalStore.add_mapping

NOTE: the project requires Python 3.11+ (uses enum.StrEnum). Run with the
project's 3.12 toolchain: `pytest tests/test_review_fixes.py`.
"""

from __future__ import annotations

import pytest

from dashforge.dashboard_ingest import parse_dashboard_json
from dashforge.signals import SignalStore


@pytest.fixture
def signal_store(tmp_path):
    return SignalStore(db_path=tmp_path / "test_signals.db")


# ── #5 confidence bounds ─────────────────────────────────────────────────────


class TestConfidenceBounds:
    def test_in_range_values_are_accepted(self, signal_store):
        # Boundaries (0.0 and 1.0) and a mid value are all valid and persisted.
        for pattern, conf in [("metric_lo", 0.0), ("metric_hi", 1.0), ("metric_mid", 0.5)]:
            assert isinstance(signal_store.add_mapping("sig", pattern, confidence=conf), int)
        patterns = {
            m["metric_pattern"]
            for m in signal_store.get_mappings_for_signal("sig", include_decayed=True)
        }
        assert {"metric_lo", "metric_hi", "metric_mid"} <= patterns

    def test_above_one_is_rejected(self, signal_store):
        # The classic 90-instead-of-0.9 typo must not be persisted.
        with pytest.raises(ValueError):
            signal_store.add_mapping("sig", "metric", confidence=90)

    def test_negative_is_rejected(self, signal_store):
        with pytest.raises(ValueError):
            signal_store.add_mapping("sig", "metric", confidence=-0.1)

    def test_rejected_value_is_not_stored(self, signal_store):
        with pytest.raises(ValueError):
            signal_store.add_mapping("sig", "metric", confidence=5.0)
        assert signal_store.get_mappings_for_signal("sig") == []


# ── #3 panel-level drilldown links ───────────────────────────────────────────


class TestPanelDrilldownLinks:
    def _dashboard(self, panel_links, dashboard_links=None):
        return {
            "dashboard": {
                "uid": "abc",
                "title": "Test",
                "links": dashboard_links or [],
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Latency",
                        "links": panel_links,
                        "targets": [{"expr": "rate(http_requests_total[5m])"}],
                    }
                ],
            }
        }

    def test_panel_link_url_is_captured(self):
        parsed = parse_dashboard_json(
            self._dashboard([{"title": "Drill", "url": "/d/xyz/other"}])
        )
        assert "/d/xyz/other" in parsed["drilldown_links"]

    def test_panel_link_attached_to_panel(self):
        parsed = parse_dashboard_json(
            self._dashboard([{"title": "Drill", "url": "/d/xyz/other"}])
        )
        panel = parsed["panels"][0]
        assert panel.get("links") == [{"title": "Drill", "url": "/d/xyz/other"}]

    def test_dashboard_level_links_still_captured(self):
        parsed = parse_dashboard_json(
            self._dashboard(
                panel_links=[],
                dashboard_links=[{"type": "link", "url": "/d/top/level"}],
            )
        )
        assert "/d/top/level" in parsed["drilldown_links"]

    def test_no_links_yields_empty(self):
        parsed = parse_dashboard_json(self._dashboard(panel_links=[]))
        assert parsed["drilldown_links"] == []

    def test_non_list_panel_links_are_ignored(self):
        parsed = parse_dashboard_json(self._dashboard(panel_links={"title": "bad"}))
        assert parsed["panels"][0].get("links") == []
        assert parsed["drilldown_links"] == []

    def test_non_dict_dashboard_links_are_ignored(self):
        parsed = parse_dashboard_json(
            self._dashboard(
                panel_links=[],
                dashboard_links=["not-a-link", {"type": "link", "url": "/d/ok"}],
            )
        )
        assert parsed["drilldown_links"] == ["/d/ok"]

    def test_empty_panel_links_are_not_persisted(self):
        parsed = parse_dashboard_json(self._dashboard(panel_links=[{}, {"title": "", "url": ""}]))
        assert parsed["panels"][0].get("links") == []
        assert parsed["drilldown_links"] == []


# ── #4 / strict request models ───────────────────────────────────────────────


class TestLearnDashboardRequest:
    def test_string_false_is_falsy(self):
        # The original footgun: the *string* "false" must not be truthy.
        from dashforge.models.schemas import LearnDashboardRequest

        req = LearnDashboardRequest(dashboard_uid="abc", auto_approve="false")
        assert req.auto_approve is False

    def test_native_bools(self):
        from dashforge.models.schemas import LearnDashboardRequest

        assert LearnDashboardRequest(dashboard_uid="abc", auto_approve=True).auto_approve is True
        assert LearnDashboardRequest(dashboard_uid="abc", auto_approve=False).auto_approve is False

    def test_string_true_is_explicitly_accepted(self):
        from dashforge.models.schemas import LearnDashboardRequest

        assert LearnDashboardRequest(dashboard_uid="abc", auto_approve="true").auto_approve is True

    def test_ambiguous_value_rejected(self):
        from pydantic import ValidationError

        from dashforge.models.schemas import LearnDashboardRequest

        with pytest.raises(ValidationError):
            LearnDashboardRequest(dashboard_uid="abc", auto_approve="maybe")

        with pytest.raises(ValidationError):
            LearnDashboardRequest(dashboard_uid="abc", auto_approve="yes")

        with pytest.raises(ValidationError):
            LearnDashboardRequest(dashboard_uid="abc", auto_approve=0)

    def test_empty_uid_rejected(self):
        from pydantic import ValidationError

        from dashforge.models.schemas import LearnDashboardRequest

        with pytest.raises(ValidationError):
            LearnDashboardRequest(dashboard_uid="   ")

    def test_unknown_field_rejected(self):
        from pydantic import ValidationError

        from dashforge.models.schemas import LearnDashboardRequest

        # Catches typos like "auto_aprove" that would otherwise silently default.
        with pytest.raises(ValidationError):
            LearnDashboardRequest(dashboard_uid="abc", auto_aprove=True)


class TestTeachSignalRequest:
    def test_valid_request_parses(self):
        from dashforge.models.schemas import TeachSignalRequest

        req = TeachSignalRequest(
            signal_type="queue_depth",
            metric_patterns=[{"pattern": "kafka_consumer_lag", "confidence": 0.9}],
        )
        assert req.metric_patterns[0].confidence == 0.9

    def test_out_of_range_confidence_rejected(self):
        from pydantic import ValidationError

        from dashforge.models.schemas import TeachSignalRequest

        # 90 instead of 0.9 must be rejected at the schema boundary.
        with pytest.raises(ValidationError):
            TeachSignalRequest(
                signal_type="queue_depth",
                metric_patterns=[{"pattern": "kafka_consumer_lag", "confidence": 90}],
            )

    def test_empty_pattern_rejected(self):
        from pydantic import ValidationError

        from dashforge.models.schemas import TeachSignalRequest

        with pytest.raises(ValidationError):
            TeachSignalRequest(
                signal_type="queue_depth",
                metric_patterns=[{"pattern": "   ", "confidence": 0.5}],
            )

    def test_empty_signal_type_rejected(self):
        from pydantic import ValidationError

        from dashforge.models.schemas import TeachSignalRequest

        with pytest.raises(ValidationError):
            TeachSignalRequest(signal_type="  ")
