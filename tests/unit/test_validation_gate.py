from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from tacit.config import settings
from tests import validate
from tests.eval.cold_isolation import LocalEvaluationEndpoints
from tests.eval.metric_matching import match_metric_sets

RATE_THRESHOLD_OPTIONS = (
    ("min_archetype_accuracy", "--min-archetype-accuracy"),
    ("min_archetype_soft_accuracy", "--min-archetype-soft-accuracy"),
    ("min_metric_recall", "--min-metric-recall"),
    ("min_critical_recall", "--min-critical-recall"),
    ("min_weighted_recall", "--min-weighted-recall"),
    ("min_signal_to_noise", "--min-signal-to-noise"),
    ("max_error_rate", "--max-error-rate"),
)
NON_FINITE_RATES = ("nan", "inf", "-inf")


def _case(prompt_id: str = "DF-001") -> validate.TestCase:
    return validate.TestCase(
        prompt_id=prompt_id,
        prompt="Investigate checkout latency",
        expected_archetype="latency_investigation",
        expected_metrics=["request_latency_seconds"],
        expected_datasources=["Prometheus"],
        difficulty="medium",
        validation_goal="critical evidence",
        critical_metrics=["request_latency_seconds"],
    )


def _pipeline_result(
    *,
    prompt_id: str = "DF-001",
    recall: float = 0.9,
    critical_recall: float = 0.9,
    weighted_recall: float = 0.9,
    signal_to_noise: float = 0.8,
    error: str = "",
) -> validate.PipelineResult:
    return validate.PipelineResult(
        prompt_id=prompt_id,
        expected_metrics=["request_latency_seconds"],
        found_metrics=["request_latency_seconds"] if not error else [],
        missing_metrics=[] if not error else ["request_latency_seconds"],
        extra_metrics=[],
        metric_recall=recall,
        dashboard_url="http://localhost:3000/d/validation",
        panel_count=1,
        latency_ms=10.0,
        critical_metrics_expected=["request_latency_seconds"],
        critical_metrics_found=["request_latency_seconds"] if not error else [],
        critical_metrics_missing=[] if not error else ["request_latency_seconds"],
        critical_recall=critical_recall,
        weighted_recall=weighted_recall,
        signal_to_noise=signal_to_noise,
        error=error,
    )


def _pipeline_gate_args(*, max_errors: int) -> argparse.Namespace:
    return argparse.Namespace(
        csv="cases.csv",
        mode="pipeline",
        review=False,
        api_url="http://127.0.0.1:8000",
        grafana_url="http://127.0.0.1:3000",
        api_key="",
        tenant="tenant-a",
        min_archetype_accuracy=0.0,
        min_archetype_soft_accuracy=0.0,
        min_metric_recall=0.0,
        min_critical_recall=0.75,
        min_weighted_recall=0.0,
        min_signal_to_noise=0.0,
        max_errors=max_errors,
        max_error_rate=1.0,
    )


def _critical_gate_outcome(
    monkeypatch: pytest.MonkeyPatch,
    results: list[validate.PipelineResult],
) -> tuple[dict[str, object], validate.ValidationGateResult]:
    async def pipeline_results(*_args, **_kwargs):
        return results

    monkeypatch.setattr(validate, "run_pipeline_validation", pipeline_results)
    cases = [_case(result.prompt_id) for result in results]
    return asyncio.run(
        validate._execute_validation(
            _pipeline_gate_args(max_errors=len(results)),
            cases,
            validate.SelectedEvaluationState(mode="external", fingerprint="external"),
        )
    )


def test_validation_gate_enforces_quality_thresholds_and_error_limits() -> None:
    thresholds = validate.ValidationThresholds(
        min_archetype_accuracy=0.85,
        min_archetype_soft_accuracy=0.9,
        min_metric_recall=0.75,
        min_critical_recall=0.8,
        min_weighted_recall=0.78,
        min_signal_to_noise=0.65,
        max_errors=0,
        max_error_rate=0.0,
    )
    passing = {
        "archetype": {"strict_accuracy": 0.9, "soft_accuracy": 0.95, "total": 100, "errors": 0},
        "pipeline": {
            "avg_metric_recall": 0.8,
            "avg_critical_recall": 0.85,
            "avg_weighted_recall": 0.82,
            "avg_signal_to_noise": 0.7,
            "total": 100,
            "errors": 0,
        },
    }

    assert validate.evaluate_validation_gate(passing, thresholds).passed is True

    failing = json.loads(json.dumps(passing))
    failing["pipeline"]["avg_signal_to_noise"] = 0.64
    failing["pipeline"]["errors"] = 1
    result = validate.evaluate_validation_gate(failing, thresholds)

    assert result.passed is False
    assert any("signal-to-noise" in failure.casefold() for failure in result.failures)
    assert any("errors" in failure.casefold() for failure in result.failures)


def test_critical_gate_counts_all_errored_critical_cases_and_fails_floor(monkeypatch) -> None:
    report, gate = _critical_gate_outcome(
        monkeypatch,
        [
            _pipeline_result(prompt_id="DF-001", critical_recall=0.0, error="provider unavailable"),
            _pipeline_result(prompt_id="DF-002", critical_recall=0.0, error="provider unavailable"),
        ],
    )
    pipeline = report["pipeline"]
    assert isinstance(pipeline, dict)

    assert (
        pipeline["critical_cases"],
        pipeline["avg_critical_recall"],
        gate.passed,
        any("critical recall" in failure.casefold() for failure in gate.failures),
    ) == (2, 0.0, False, True)


def test_critical_gate_counts_mixed_success_and_error_in_critical_floor(monkeypatch) -> None:
    report, gate = _critical_gate_outcome(
        monkeypatch,
        [
            _pipeline_result(prompt_id="DF-001", critical_recall=1.0),
            _pipeline_result(prompt_id="DF-002", critical_recall=0.0, error="provider unavailable"),
        ],
    )
    pipeline = report["pipeline"]
    assert isinstance(pipeline, dict)

    assert (
        pipeline["critical_cases"],
        pipeline["avg_critical_recall"],
        gate.passed,
        any("critical recall" in failure.casefold() for failure in gate.failures),
    ) == (2, 0.5, False, True)


@pytest.mark.parametrize(("field_name", "_cli_option"), RATE_THRESHOLD_OPTIONS)
@pytest.mark.parametrize("raw_rate", NON_FINITE_RATES)
def test_validation_thresholds_reject_non_finite_rates(
    field_name: str,
    _cli_option: str,
    raw_rate: str,
) -> None:
    with pytest.raises(ValueError, match="finite number between 0 and 1"):
        validate.ValidationThresholds(**cast(Any, {field_name: float(raw_rate)}))


@pytest.mark.parametrize(("_field_name", "cli_option"), RATE_THRESHOLD_OPTIONS)
@pytest.mark.parametrize("raw_rate", NON_FINITE_RATES)
def test_validation_cli_rejects_non_finite_rates_before_loading_the_dataset(
    _field_name: str,
    cli_option: str,
    raw_rate: str,
    capsys,
) -> None:
    with pytest.raises(SystemExit) as raised:
        validate.main(["missing.csv", f"{cli_option}={raw_rate}"])

    assert raised.value.code == 2
    error = capsys.readouterr().err
    assert cli_option in error
    assert "finite number between 0 and 1" in error


def test_pipeline_validation_sends_api_key_and_concrete_tenant(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    client_options: list[dict[str, object]] = []

    class Response:
        status_code = 503
        text = "unavailable"

    class Client:
        def __init__(self, *args, **kwargs):
            client_options.append(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, **kwargs):
            calls.append({"url": url, **kwargs})
            return Response()

    monkeypatch.setattr(httpx, "AsyncClient", Client)

    results = asyncio.run(
        validate.run_pipeline_validation(
            [_case()],
            "http://127.0.0.1:8000",
            "http://127.0.0.1:3000",
            api_key="test-api-key",
            tenant_id="tenant-a",
        )
    )

    assert len(results) == 1
    assert results[0].error.startswith("HTTP 503")
    assert client_options == [{"timeout": 180, "trust_env": False}]
    assert calls == [
        {
            "url": "http://127.0.0.1:8000/api/v1/chart",
            "json": {
                "prompt": "Investigate checkout latency",
                "user_id": "validation",
                "channel_id": "test",
                "tenant_id": "tenant-a",
            },
            "headers": {"X-API-Key": "test-api-key", "X-Tacit-Tenant": "tenant-a"},
        }
    ]


def test_pipeline_validation_uses_only_explicit_grafana_credentials(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class Response:
        def __init__(self, payload: dict[str, object]) -> None:
            self.status_code = 200
            self.text = ""
            self._payload = payload

        def json(self) -> dict[str, object]:
            return self._payload

    class Client:
        def __init__(self, *args, **kwargs):
            assert kwargs["trust_env"] is False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url, **kwargs):
            calls.append({"url": url, **kwargs})
            return Response(
                {
                    "dashboard_uid": "validation",
                    "dashboard_url": "http://127.0.0.1:3000/d/validation",
                    "panel_count": 1,
                }
            )

        async def get(self, url, **kwargs):
            calls.append({"url": url, **kwargs})
            return Response({"dashboard": {"panels": []}})

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    monkeypatch.setattr(settings, "grafana_api_key", "process-global-secret")
    monkeypatch.setattr(settings, "grafana_org_id", 99)

    results = asyncio.run(
        validate.run_pipeline_validation(
            [_case()],
            "http://127.0.0.1:8000",
            "http://127.0.0.1:3000",
            tenant_id="tenant-a",
            grafana_api_key="",
            grafana_org_id=7,
        )
    )

    assert results[0].error == ""
    assert calls[1]["headers"] == {"X-Grafana-Org-Id": "7"}
    assert "process-global-secret" not in json.dumps(calls)


def test_pipeline_validation_rejects_non_concrete_tenant_before_network(monkeypatch) -> None:
    calls: list[str] = []

    class ForbiddenClient:
        def __init__(self, *_args, **_kwargs):
            calls.append("client")
            raise AssertionError("network client constructed before tenant validation")

    monkeypatch.setattr(httpx, "AsyncClient", ForbiddenClient)

    with pytest.raises(ValueError, match="concrete tenant"):
        asyncio.run(
            validate.run_pipeline_validation(
                [_case()],
                "http://127.0.0.1:8000",
                "http://127.0.0.1:3000",
                tenant_id="*",
            )
        )

    assert calls == []


def test_classifier_exception_cannot_score_as_general_strict_or_soft_pass(monkeypatch) -> None:
    case = _case()
    case.expected_archetype = "general"

    async def fail_classification(*_args, **_kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr("tacit.agents.intent.classify_intent", fail_classification)

    result = asyncio.run(validate.run_archetype_validation([case], provider=object()))[0]

    assert result.error == "provider unavailable"
    assert result.passed is False
    assert result.any_match is False


@pytest.mark.parametrize("actual", ["", "unknown", "made_up_archetype", " general-ish "])
def test_unknown_archetype_outputs_cannot_score_as_general(actual, monkeypatch) -> None:
    case = _case()
    case.expected_archetype = "general"

    async def classify(*_args, **_kwargs):
        archetype = type("Archetype", (), {"type": actual, "confidence": 0.99})()
        intent = type("Intent", (), {"problem_type": actual, "archetypes": [archetype]})()
        return intent, object()

    monkeypatch.setattr("tacit.agents.intent.classify_intent", classify)

    result = asyncio.run(validate.run_archetype_validation([case], provider=object()))[0]

    assert result.error == ""
    assert result.passed is False
    assert result.any_match is False


def test_direct_archetype_validation_requires_an_exact_provider_before_classification(monkeypatch) -> None:
    calls: list[str] = []

    async def classify(*_args, **_kwargs):
        calls.append("classify")
        raise AssertionError("classification must not start")

    monkeypatch.setattr("tacit.agents.intent.classify_intent", classify)

    with pytest.raises(RuntimeError, match="owner-managed LLM provider"):
        asyncio.run(validate.run_archetype_validation([_case()]))

    assert calls == []


def test_metric_matching_is_one_to_one_and_prefers_exact_matches() -> None:
    matches = match_metric_sets(
        {"request_duration_seconds", "request_duration_seconds_bucket"},
        {"request_duration_seconds_bucket"},
    )

    assert matches == {"request_duration_seconds_bucket": "request_duration_seconds_bucket"}


def test_metric_matching_supports_only_explicit_histogram_suffixes() -> None:
    assert match_metric_sets(
        {"request_duration_seconds"},
        {"request_duration_seconds_bucket"},
    ) == {"request_duration_seconds": "request_duration_seconds_bucket"}
    assert match_metric_sets({"request_duration_seconds"}, {"request_duration"}) == {}
    assert match_metric_sets({"requests"}, {"requests_total"}) == {}


def test_metric_matching_keeps_every_derived_rate_within_unit_bounds() -> None:
    expected = {"request_duration_seconds", "request_duration_seconds_bucket"}
    found = {"request_duration_seconds_bucket"}
    matches = match_metric_sets(expected, found)

    recall = len(matches) / len(expected)
    signal_to_noise = len(set(matches.values())) / len(found)

    assert 0.0 <= recall <= 1.0
    assert 0.0 <= signal_to_noise <= 1.0


@pytest.mark.parametrize(
    "report",
    [
        {"archetype": {"strict_accuracy": 1.0, "soft_accuracy": 1.0, "total": 0, "errors": 0}},
        {
            "pipeline": {
                "avg_metric_recall": 1.0,
                "avg_critical_recall": 1.0,
                "avg_weighted_recall": 1.0,
                "avg_signal_to_noise": 1.0,
                "total": 0,
                "errors": 0,
            }
        },
    ],
)
def test_validation_gate_fails_closed_for_empty_scored_corpus(report) -> None:
    result = validate.evaluate_validation_gate(report, validate.ValidationThresholds())

    assert result.passed is False
    assert any("no results" in failure.casefold() for failure in result.failures)


def _create_state_database(path: Path, marker: str) -> None:
    conn = sqlite3.connect(path)
    try:
        with conn:
            conn.execute("CREATE TABLE benchmark_marker(value TEXT NOT NULL)")
            conn.execute("INSERT INTO benchmark_marker(value) VALUES (?)", (marker,))
    finally:
        conn.close()


def test_long_lived_state_uses_a_disposable_copy_and_preserves_source(tmp_path) -> None:
    source = tmp_path / "long-lived"
    source.mkdir()
    for name in validate.LONG_LIVED_STATE_DATABASES:
        _create_state_database(source / name, name)
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source.iterdir()}
    endpoints = LocalEvaluationEndpoints()

    with validate.evaluation_state("long-lived", source, endpoints=endpoints, tenant_id="tenant-a") as selected:
        assert selected.mode == "long-lived"
        assert selected.isolated_state is not None
        assert selected.isolated_state.settings.knowledge_tenant_id == "tenant-a"
        copied_workdir = selected.isolated_state.workdir
        assert copied_workdir != source
        assert copied_workdir.exists()
        for name in validate.LONG_LIVED_STATE_DATABASES:
            conn = sqlite3.connect(copied_workdir / name)
            try:
                marker = conn.execute("SELECT value FROM benchmark_marker").fetchone()[0]
            finally:
                conn.close()
            assert marker == name

    after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source.iterdir()}
    assert after == before
    assert not copied_workdir.exists()


def test_clean_state_is_disposable_and_identified() -> None:
    with validate.evaluation_state("clean", endpoints=LocalEvaluationEndpoints()) as selected:
        assert selected.mode == "clean"
        assert selected.fingerprint
        assert selected.isolated_state is not None
        workdir = selected.isolated_state.workdir
        assert workdir.exists()

    assert not workdir.exists()


@pytest.mark.parametrize("mode", ["clean", "long-lived"])
def test_isolated_validation_never_forwards_process_global_grafana_credentials(
    tmp_path,
    monkeypatch,
    mode,
) -> None:
    source = None
    if mode == "long-lived":
        source = tmp_path / "authority"
        source.mkdir()
        for name in validate.LONG_LIVED_STATE_DATABASES:
            _create_state_database(source / name, name)
    observed: list[tuple[str | None, int | None]] = []

    async def capture_pipeline(*_args, **kwargs):
        observed.append((kwargs.get("grafana_api_key"), kwargs.get("grafana_org_id")))
        return [_pipeline_result()]

    monkeypatch.setattr(settings, "grafana_api_key", "process-global-secret")
    monkeypatch.setattr(settings, "grafana_org_id", 99)
    monkeypatch.setattr(validate, "run_pipeline_validation", capture_pipeline)
    defaults = validate.ValidationThresholds()
    args = argparse.Namespace(
        csv="cases.csv",
        mode="pipeline",
        review=False,
        api_url="http://127.0.0.1:8000",
        grafana_url="http://127.0.0.1:3000",
        api_key="",
        tenant="tenant-a",
        **vars(defaults),
    )
    endpoints = LocalEvaluationEndpoints(grafana_url="http://127.0.0.1:3000")

    with validate.evaluation_state(
        mode,
        source,
        endpoints=endpoints,
        tenant_id="tenant-a",
    ) as selected:
        asyncio.run(validate._execute_validation(args, [_case()], selected))
        assert selected.isolated_state is not None
        expected_org_id = selected.isolated_state.settings.grafana_org_id

    assert observed == [("", expected_org_id)]


@pytest.mark.parametrize(
    ("endpoints", "tenant", "message"),
    [
        (
            LocalEvaluationEndpoints(grafana_url="https://production.example"),
            "tenant-a",
            "local loopback endpoint",
        ),
        (LocalEvaluationEndpoints(), "*", "concrete tenant"),
    ],
)
def test_long_lived_state_validates_endpoint_and_tenant_before_source_access(
    tmp_path,
    monkeypatch,
    endpoints,
    tenant,
    message,
) -> None:
    source_accesses: list[Path] = []

    def forbidden_source_access(source_dir: Path):
        source_accesses.append(source_dir)
        raise AssertionError("long-lived source accessed before evaluation admission")

    monkeypatch.setattr(validate, "_long_lived_state_sources", forbidden_source_access)

    with pytest.raises(ValueError, match=message):
        with validate.evaluation_state(
            "long-lived",
            tmp_path / "authority",
            endpoints=endpoints,
            tenant_id=tenant,
        ):
            raise AssertionError("invalid evaluation state entered")

    assert source_accesses == []


def test_validation_main_returns_nonzero_and_reports_state_on_gate_failure(tmp_path, monkeypatch, capsys) -> None:
    csv_path = tmp_path / "cases.csv"
    csv_path.write_text(
        "prompt_id,prompt,expected_archetype,expected_metrics,expected_datasources,difficulty,validation_goal\n"
        "DF-001,Investigate checkout latency,latency_investigation,request_latency_seconds,Prometheus,medium,gate\n"
    )
    output_path = tmp_path / "report.json"

    async def low_quality(*_args, **_kwargs):
        return [_pipeline_result(recall=0.1, critical_recall=0.1, weighted_recall=0.1, signal_to_noise=0.1)]

    monkeypatch.setattr(validate, "run_pipeline_validation", low_quality)
    exit_code = validate.main(
        [
            str(csv_path),
            "--mode",
            "pipeline",
            "--state",
            "external",
            "--output",
            str(output_path),
        ]
    )

    report = json.loads(output_path.read_text())
    assert exit_code == 1
    assert report["state"]["mode"] == "external"
    assert report["gate"]["passed"] is False
    assert "State    : external" in capsys.readouterr().out


def test_validation_cli_returns_nonzero_on_unexpected_exception(monkeypatch, capsys) -> None:
    def fail(_argv=None):
        raise RuntimeError("fixture exploded")

    monkeypatch.setattr(validate, "main", fail)

    assert validate.cli(["unused.csv"]) == 2
    assert "VALIDATION ERROR" in capsys.readouterr().err


def test_negative_max_errors_is_rejected_before_dataset_or_runtime_access(monkeypatch, capsys) -> None:
    accesses: list[str] = []

    def forbidden_dataset(_path):
        accesses.append("dataset")
        raise AssertionError("dataset accessed")

    monkeypatch.setattr(validate, "load_test_cases", forbidden_dataset)

    with pytest.raises(SystemExit) as raised:
        validate.main(["missing.csv", "--max-errors=-1"])

    assert raised.value.code == 2
    assert accesses == []
    assert "--max-errors" in capsys.readouterr().err
