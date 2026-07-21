"""Zero-key deterministic intent fallback tests."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from tacit.agents.intent_fallback import (
    heuristic_intent,
    zero_key_mode,
)

DEMO_PROMPT = (
    "checkout-service is in an incident: p95 latency is spiking after deploy, "
    "5xx errors are rising on payment routes, and requests are piling up."
)


def _settings(provider: str, key: str, api_base: str = "") -> SimpleNamespace:
    return SimpleNamespace(llm_provider=provider, llm_api_key=key, llm_api_base=api_base)


class TestZeroKeyMode:
    def test_key_required_provider_without_key(self):
        assert zero_key_mode(_settings("anthropic", "")) is True
        assert zero_key_mode(_settings("openai", "")) is True
        assert zero_key_mode(_settings("azure", "")) is True

    def test_key_required_provider_with_key(self):
        assert zero_key_mode(_settings("anthropic", "sk-test")) is False

    def test_local_and_iam_providers_never_zero_key(self):
        assert zero_key_mode(_settings("ollama", "")) is False
        assert zero_key_mode(_settings("bedrock", "")) is False

    def test_openai_compatible_base_is_not_zero_key(self):
        assert zero_key_mode(_settings("openai", "", "http://localhost:8001/v1")) is False


class TestHeuristicIntent:
    def test_demo_prompt_matches_expected_archetypes(self):
        intent = heuristic_intent(DEMO_PROMPT)
        types = [a.type for a in intent.archetypes]
        assert "latency_investigation" in types
        assert "error_spike" in types
        assert "deployment_regression" in types
        assert intent.problem_type == types[0]

    def test_demo_prompt_extracts_service(self):
        intent = heuristic_intent(DEMO_PROMPT)
        assert "checkout-service" in intent.services

    def test_single_word_service_phrase_extraction(self):
        intent = heuristic_intent("checkout service is throwing 500s")
        assert intent.services == ["checkout"]

    def test_plural_http_error_codes_match_error_spike(self):
        assert heuristic_intent("checkout service is throwing 500s").archetypes[0].type == "error_spike"
        assert heuristic_intent("checkout service is throwing 5xxs").archetypes[0].type == "error_spike"

    def test_single_word_service_preposition_extraction(self):
        intent = heuristic_intent("high CPU on checkout in the last 30 minutes")
        assert "checkout" in intent.services

    def test_operational_hyphen_terms_do_not_outrank_real_service(self):
        intent = heuristic_intent("cache-miss spikes on checkout")
        assert intent.services[0] == "checkout"
        assert "cache-miss" not in intent.services

    def test_single_word_service_extraction_ignores_timerange_word(self):
        intent = heuristic_intent("high CPU in the last 30 minutes")
        assert "last" not in intent.services

    def test_specific_archetype_wins_over_generic_resource_tie(self):
        assert heuristic_intent("memory leak in checkout").archetypes[0].type == "memory_leak_investigation"
        assert heuristic_intent("API throttling on checkout").archetypes[0].type == "rate_limiting_investigation"

    def test_timerange_extraction(self):
        intent = heuristic_intent("high CPU on checkout in the last 30 minutes")
        assert intent.timerange == "30m"
        assert intent.archetypes[0].type == "resource_saturation"

    def test_timerange_hours(self):
        intent = heuristic_intent("errors in the past 6 hours")
        assert intent.timerange == "6h"

    def test_timerange_default(self):
        assert heuristic_intent("something is wrong").timerange == "1h"

    def test_explicit_environment_is_captured_without_defaulting(self):
        production = heuristic_intent("checkout latency on checkout in production")
        assert production.environments == ["production"]
        assert production.services == ["checkout"]
        assert heuristic_intent("checkout latency on checkout env:us-east-prod").environments == ["us-east-prod"]
        assert heuristic_intent("checkout latency").environments == []

    def test_unmatched_prompt_falls_back_to_golden_signals(self):
        intent = heuristic_intent("something feels off with the flux capacitor")
        assert intent.archetypes[0].type == "golden_signals"
        assert intent.archetypes[0].confidence < 0.5

    def test_always_returns_at_least_one_archetype(self):
        assert heuristic_intent("").archetypes

    def test_operational_stopwords_not_treated_as_services(self):
        intent = heuristic_intent("error_rate and queue_depth are elevated on payment-api")
        assert "payment-api" in intent.services
        assert "error_rate" not in intent.services
        assert "queue_depth" not in intent.services

    def test_confidence_bounded(self):
        intent = heuristic_intent(DEMO_PROMPT)
        assert all(0.0 <= a.confidence <= 0.95 for a in intent.archetypes)


class TestClassifyIntentFallbackRouting:
    def test_unconfigured_provider_routes_to_heuristic(self):
        from tacit.agents.intent import classify_intent

        class UnconfiguredProvider:
            is_configured = False

            async def chat_json(self, *args, **kwargs):  # pragma: no cover
                raise AssertionError("LLM must not be called in zero-key mode")

        runtime_settings = SimpleNamespace(
            intent_fallback_enabled=True,
            llm_provider="openai",
            llm_api_key="",
            llm_api_base="",
        )
        intent, usage = asyncio.run(
            classify_intent(DEMO_PROMPT, provider=UnconfiguredProvider(), runtime_settings=runtime_settings)
        )
        assert usage.total_tokens == 0
        assert intent.archetypes
        assert "checkout-service" in intent.services

    def test_provider_init_failure_routes_to_heuristic(self):
        from tacit.agents.intent import classify_intent

        fake_settings = SimpleNamespace(
            intent_fallback_enabled=True,
            llm_provider="openai",
            llm_api_key="",
            llm_api_base="",
        )
        with (
            patch("tacit.config.settings", fake_settings),
            patch("tacit.agents.llm.get_provider", side_effect=ValueError("boom")),
        ):
            intent, usage = asyncio.run(classify_intent("high cpu on checkout"))
        assert usage.total_tokens == 0
        assert intent.archetypes[0].type == "resource_saturation"

    def test_provider_init_failure_does_not_hide_ollama_errors(self):
        from tacit.agents.intent import classify_intent

        fake_settings = SimpleNamespace(
            intent_fallback_enabled=True,
            llm_provider="ollama",
            llm_api_key="",
            llm_api_base="",
        )
        with (
            patch("tacit.config.settings", fake_settings),
            patch("tacit.agents.llm.get_provider", side_effect=ValueError("ollama offline")),
        ):
            with pytest.raises(ValueError, match="ollama offline"):
                asyncio.run(classify_intent("high cpu on checkout"))

    def test_runtime_zero_key_settings_enable_fallback_even_when_globals_do_not(self):
        from tacit.agents.intent import classify_intent

        class UnconfiguredProvider:
            is_configured = False

            async def chat_json(self, *args, **kwargs):  # pragma: no cover
                raise AssertionError("zero-key sentinel must not be called")

        runtime_settings = SimpleNamespace(
            intent_fallback_enabled=True,
            llm_provider="openai",
            llm_api_key="",
            llm_api_base="",
        )
        global_settings = SimpleNamespace(
            intent_fallback_enabled=False,
            llm_provider="openai",
            llm_api_key="sk-real",
            llm_api_base="",
        )
        with patch("tacit.config.settings", global_settings):
            intent, usage = asyncio.run(
                classify_intent(
                    "high cpu on checkout",
                    provider=UnconfiguredProvider(),
                    runtime_settings=runtime_settings,
                )
            )
        assert usage.total_tokens == 0
        assert intent.archetypes[0].type == "resource_saturation"
