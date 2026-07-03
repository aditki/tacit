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


def _settings(provider: str, key: str) -> SimpleNamespace:
    return SimpleNamespace(llm_provider=provider, llm_api_key=key)


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

    def test_timerange_extraction(self):
        intent = heuristic_intent("high CPU on checkout in the last 30 minutes")
        assert intent.timerange == "30m"
        assert intent.archetypes[0].type == "resource_saturation"

    def test_timerange_hours(self):
        intent = heuristic_intent("errors in the past 6 hours")
        assert intent.timerange == "6h"

    def test_timerange_default(self):
        assert heuristic_intent("something is wrong").timerange == "1h"

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

        intent, usage = asyncio.run(classify_intent(DEMO_PROMPT, provider=UnconfiguredProvider()))
        assert usage.total_tokens == 0
        assert intent.archetypes
        assert "checkout-service" in intent.services

    def test_provider_init_failure_routes_to_heuristic(self):
        from tacit.agents.intent import classify_intent

        with patch("tacit.agents.llm.get_provider", side_effect=ValueError("boom")):
            intent, usage = asyncio.run(classify_intent("high cpu on checkout"))
        assert usage.total_tokens == 0
        assert intent.archetypes[0].type == "resource_saturation"

    def test_provider_init_failure_does_not_hide_ollama_errors(self):
        from tacit.agents.intent import classify_intent

        fake_settings = SimpleNamespace(
            intent_fallback_enabled=True,
            llm_provider="ollama",
            llm_api_key="",
        )
        with (
            patch("tacit.config.settings", fake_settings),
            patch("tacit.agents.llm.get_provider", side_effect=ValueError("ollama offline")),
        ):
            with pytest.raises(ValueError, match="ollama offline"):
                asyncio.run(classify_intent("high cpu on checkout"))
