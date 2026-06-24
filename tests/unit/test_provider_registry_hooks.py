from __future__ import annotations

from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.agents.providers.registry import (
    create_provider,
    get_provider,
    register_provider_factory,
    reset_provider_for_tests,
)
from tacit.config import Settings, settings


class DummyProvider(LLMProvider):
    async def chat_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> LLMResult:
        return LLMResult("")


class OtherDummyProvider(LLMProvider):
    async def chat_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> LLMResult:
        return LLMResult('{"other": true}')

    async def chat_text(self, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> LLMResult:
        return LLMResult("other")


def test_register_provider_factory_and_reset(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "unit-test")
    register_provider_factory("unit-test", DummyProvider)
    reset_provider_for_tests()

    try:
        first = get_provider()
        second = get_provider()

        assert isinstance(first, DummyProvider)
        assert first is second

        reset_provider_for_tests()
        third = get_provider()
        assert isinstance(third, DummyProvider)
        assert third is not first
    finally:
        reset_provider_for_tests()


def test_register_provider_factory_invalidates_cached_provider(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "unit-test")
    register_provider_factory("unit-test", DummyProvider)

    try:
        first = get_provider()
        register_provider_factory("unit-test", OtherDummyProvider)
        second = get_provider()

        assert isinstance(first, DummyProvider)
        assert isinstance(second, OtherDummyProvider)
        assert second is not first
    finally:
        reset_provider_for_tests()


def test_create_provider_passes_runtime_settings(monkeypatch):
    seen: list[Settings] = []

    def factory(runtime_settings: Settings):
        seen.append(runtime_settings)
        return DummyProvider()

    runtime_settings = Settings(llm_provider="runtime-provider", llm_model="runtime-model")
    register_provider_factory("runtime-provider", factory)

    try:
        provider = create_provider(runtime_settings)
        assert isinstance(provider, DummyProvider)
        assert seen == [runtime_settings]
    finally:
        reset_provider_for_tests()
