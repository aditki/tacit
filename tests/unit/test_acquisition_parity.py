"""Frozen typed-outcome denominators for the acquisition regression."""

from __future__ import annotations

import importlib


def test_full_catalog_does_not_credit_missing_observations():
    harness = importlib.import_module("tests.eval.acquisition_parity")
    fixture = harness.load_fixture()
    result = harness.score(fixture, harness.catalog(fixture), [], [])
    assert result["retained_entries"] == 588
    assert result["available_categories"] == 6
    assert result["observed_categories"] == 0
    assert not result["passed"]


def test_original_300_projection_loses_all_requested_categories():
    harness = importlib.import_module("tests.eval.acquisition_parity")
    fixture = harness.load_fixture()
    result = harness.score(fixture, harness.catalog(fixture)[:300], [], [])
    assert result["available_categories"] == 0
    assert result["fixture_critical_available"] == 0
    assert not result["passed"]


def test_safe_validated_observations_score_separate_six_and_seven_denominators():
    harness = importlib.import_module("tests.eval.acquisition_parity")
    fixture = harness.load_fixture()
    observations = [
        {
            "resolution_metric": name,
            "datasource_uid": fixture["datasource_uid"],
            "valid_query": True,
            "non_empty": True,
            "survived": True,
            "outcome": "SUPPORTED_OBSERVATION",
        }
        for name in set(fixture["fixture_critical"]) | {"app_payment_transactions_total"}
    ]
    result = harness.score(fixture, harness.catalog(fixture), observations, [])
    assert result["passed"]
    assert (result["observed_categories"], result["category_denominator"]) == (6, 6)
    assert (result["fixture_critical_observed"], result["fixture_critical_denominator"]) == (7, 7)


def test_wrong_source_invalid_or_unsafe_outcomes_never_pass():
    harness = importlib.import_module("tests.eval.acquisition_parity")
    fixture = harness.load_fixture()
    observations = [
        {
            "resolution_metric": name,
            "datasource_uid": "wrong",
            "valid_query": True,
            "non_empty": True,
            "survived": True,
            "outcome": "SUPPORTED_OBSERVATION",
        }
        for name in fixture["fixture_critical"]
    ]
    result = harness.score(fixture, harness.catalog(fixture), observations, ["unsupported_cause"])
    assert not result["passed"]
    assert result["routing_errors"] == 7
    assert result["unsafe_assertions"] == 1
