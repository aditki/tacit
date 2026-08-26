from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tacit.agents.providers import registry as provider_registry
from tacit.config import settings
from tests.eval import gamma_diagnostic_harness, gate_harness, prompt_variation_harness


def _gamma_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    values = {
        "archive": tmp_path / "archive.tar.gz",
        "scenario": "gamma-scenario",
        "workdir": tmp_path / "output",
        "grafana_url": "http://127.0.0.1:3001",
        "vm_url": "http://127.0.0.1:8428",
        "ollama_url": "http://127.0.0.1:11434",
        "model": "gamma-model",
        "expect": "post-fix",
        "replace_all_series": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"vm_url": "https://production-vm.example"}, "VictoriaMetrics URL"),
        ({"replace_all_series": False}, "--replace-all-series"),
    ],
)
def test_gamma_rejects_unsafe_replacement_before_filesystem_or_network(
    tmp_path,
    monkeypatch,
    overrides,
    message,
) -> None:
    calls: list[str] = []

    class ForbiddenClient:
        def __init__(self, *args, **kwargs):
            calls.append("network")
            raise AssertionError("network client initialized before safety validation")

    monkeypatch.setattr(gamma_diagnostic_harness.httpx, "Client", ForbiddenClient)
    monkeypatch.setattr(
        gamma_diagnostic_harness,
        "build",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fixture build occurred before validation")),
    )
    args = _gamma_args(tmp_path, **overrides)

    with pytest.raises(ValueError, match=message):
        asyncio.run(gamma_diagnostic_harness.run(args))

    assert calls == []
    assert not args.workdir.exists()


def test_gamma_live_configuration_pins_all_remote_capabilities_to_loopback(tmp_path) -> None:
    config = gamma_diagnostic_harness._validate_live_run(_gamma_args(tmp_path))

    assert config.vm_url == "http://127.0.0.1:8428"
    assert config.endpoints.grafana_url == "http://127.0.0.1:3001"
    assert config.endpoints.llm_api_base == "http://127.0.0.1:11434"
    assert config.endpoints.llm_model == "gamma-model"


@pytest.mark.parametrize(
    ("provider", "api_base", "api_key"),
    [
        ("openai", "http://127.0.0.1:11434", ""),
        ("ollama", "https://api.example.test", ""),
        ("ollama", "http://127.0.0.1:11434", "production-secret"),
    ],
)
def test_prompt_variation_rejects_nonlocal_or_credentialed_providers(provider, api_base, api_key) -> None:
    with pytest.raises(ValueError):
        prompt_variation_harness._local_llm_endpoints(
            provider=provider,
            model="local-model",
            api_key=api_key,
            api_base=api_base,
        )


def test_prompt_variation_revalidates_injected_endpoints_before_corpus_access(tmp_path, monkeypatch) -> None:
    corpus = tmp_path / "must-not-be-read.json"
    reads: list[Path] = []

    def forbidden_read(path: Path, *args, **kwargs):
        reads.append(path)
        raise AssertionError("corpus was read before provider validation")

    monkeypatch.setattr(Path, "read_text", forbidden_read)

    with pytest.raises(ValueError, match="local loopback endpoint"):
        asyncio.run(
            prompt_variation_harness.run(
                1,
                corpus,
                endpoints=prompt_variation_harness.LocalEvaluationEndpoints(llm_model="local-model"),
            )
        )

    assert reads == []


def test_prompt_variation_uses_dependency_owned_provider_and_settings(tmp_path, monkeypatch) -> None:
    corpus = tmp_path / "prompt.json"
    corpus.write_text(
        json.dumps(
            {
                "role": "development",
                "prompts": [
                    {"class": "cache", "text": "checkout latency", "expected": {}},
                    {"class": "negative", "text": "rotate signing keys", "expected": {}},
                ],
            }
        )
    )
    production_provider = object()
    monkeypatch.setattr(provider_registry, "_provider", production_provider)
    monkeypatch.setattr(settings, "llm_provider", "openai")
    monkeypatch.setattr(settings, "llm_api_base", "https://api.openai.com")
    monkeypatch.setattr(settings, "llm_api_key", "production-secret")
    observed: list[tuple[object, object]] = []

    async def fake_classify(prompt, *, provider=None, runtime_settings=None):
        observed.append((provider, runtime_settings))
        return SimpleNamespace(), SimpleNamespace()

    monkeypatch.setattr(prompt_variation_harness, "classify_intent", fake_classify)
    monkeypatch.setattr(prompt_variation_harness, "_evaluate", lambda intent, item: (True, {}))
    endpoints = prompt_variation_harness._local_llm_endpoints(
        provider="ollama",
        model="local-model",
        api_key="",
        api_base="http://127.0.0.1:11434",
    )

    report = asyncio.run(prompt_variation_harness.run(1, corpus, endpoints=endpoints))

    assert report["provider"] == "ollama"
    assert report["model"] == "local-model"
    assert len(observed) == 2
    provider: Any = observed[0][0]
    runtime_settings: Any = observed[0][1]
    assert provider is not production_provider
    assert provider._base_url == "http://127.0.0.1:11434"
    assert runtime_settings.llm_provider == "ollama"
    assert runtime_settings.llm_api_key == ""
    assert provider_registry._provider is production_provider


@pytest.mark.parametrize(
    "prompts",
    [
        [],
        [{"class": "positive", "text": "checkout latency"}],
        [{"class": "negative", "text": "database saturation"}],
    ],
)
def test_prompt_variation_rejects_empty_or_one_sided_corpus_before_provider_access(
    tmp_path,
    monkeypatch,
    prompts,
) -> None:
    corpus = tmp_path / "prompt.json"
    corpus.write_text(json.dumps({"role": "development", "prompts": prompts}))
    provider_accesses: list[str] = []

    def forbidden_isolation(*_args, **_kwargs):
        provider_accesses.append("isolation")
        raise AssertionError("provider isolation entered for an invalid corpus")

    monkeypatch.setattr(prompt_variation_harness, "cold_isolation", forbidden_isolation)

    with pytest.raises(ValueError, match="positive and negative populations"):
        asyncio.run(
            prompt_variation_harness.run(
                1,
                corpus,
                endpoints=prompt_variation_harness.LocalEvaluationEndpoints(
                    llm_api_base="http://127.0.0.1:11434",
                    llm_model="local-model",
                ),
            )
        )

    assert provider_accesses == []


def test_prompt_variation_classifier_errors_are_failed_trials(tmp_path, monkeypatch) -> None:
    corpus = tmp_path / "prompt.json"
    corpus.write_text(
        json.dumps(
            {
                "role": "development",
                "prompts": [
                    {"class": "positive", "text": "checkout latency"},
                    {"class": "negative", "text": "database saturation"},
                ],
            }
        )
    )

    async def fail_classification(*_args, **_kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(prompt_variation_harness, "classify_intent", fail_classification)

    report = asyncio.run(
        prompt_variation_harness.run(
            1,
            corpus,
            endpoints=prompt_variation_harness.LocalEvaluationEndpoints(
                llm_api_base="http://127.0.0.1:11434",
                llm_model="local-model",
            ),
        )
    )

    assert report["passed"] is False
    assert report["positive_useful_rate"] == 0.0
    assert report["negative_correct_rate"] == 0.0
    assert all(row["failures"][0]["error"].startswith("RuntimeError") for row in report["results"])


def test_offline_gate_fails_closed_when_fixture_filtering_produces_no_corpus() -> None:
    report = {
        "classification": [],
        "cold_resolution": [],
        "learned_resolution": [],
        "learned_selection": [],
    }

    failures = gate_harness.gate_failures(report)

    assert any("classification corpus" in failure for failure in failures)
    assert any("cold resolution corpus" in failure for failure in failures)
    assert any("learned resolution corpus" in failure for failure in failures)
    assert any("learned selection corpus" in failure for failure in failures)


@pytest.mark.parametrize(
    ("labeled", "tn", "expected_population"),
    [(0, 1, "positive"), (1, 0, "negative")],
)
def test_offline_gate_requires_both_classification_populations(labeled, tn, expected_population) -> None:
    report = {
        "classification": [
            {
                "dataset": "one-sided",
                "precision": 1.0,
                "recall": 1.0,
                "coverage": 1.0,
                "labeled_signal_metrics": labeled,
                "tp": labeled,
                "fp": 0,
                "fn": 0,
                "tn": tn,
            }
        ],
        "cold_resolution": [{"dataset": "one-sided", "recall": 1.0, "total": 1}],
        "learned_resolution": [{"dataset": "one-sided", "recall": 1.0, "total": 1}],
        "learned_selection": [{"dataset": "one-sided", "passed": True}],
    }

    failures = gate_harness.gate_failures(report)

    assert any(expected_population in failure for failure in failures)


def test_offline_gate_runs_without_llm_or_grafana_configuration(monkeypatch) -> None:
    monkeypatch.setattr(settings, "grafana_url", "")
    monkeypatch.setattr(settings, "llm_api_base", "")
    monkeypatch.setattr(settings, "llm_api_key", "")

    report = gate_harness.run()

    assert gate_harness.gate_failures(report) == []
