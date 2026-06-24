"""Provider registry — resolves LLM_PROVIDER config to a concrete implementation."""

from __future__ import annotations

import inspect

import structlog

from tacit.agents.providers.base import LLMProvider
from tacit.config import Settings, settings

logger = structlog.get_logger()

_provider: LLMProvider | None = None


def _anthropic_provider(runtime_settings: Settings | None = None) -> LLMProvider:
    from tacit.agents.providers.anthropic import AnthropicProvider

    return AnthropicProvider(runtime_settings=runtime_settings)


def _openai_provider(runtime_settings: Settings | None = None) -> LLMProvider:
    from tacit.agents.providers.openai_provider import OpenAIProvider

    return OpenAIProvider(runtime_settings=runtime_settings)


def _azure_provider(runtime_settings: Settings | None = None) -> LLMProvider:
    from tacit.agents.providers.openai_provider import AzureOpenAIProvider

    return AzureOpenAIProvider(runtime_settings=runtime_settings)


def _bedrock_provider(runtime_settings: Settings | None = None) -> LLMProvider:
    from tacit.agents.providers.bedrock import BedrockProvider

    return BedrockProvider(runtime_settings=runtime_settings)


def _ollama_provider(runtime_settings: Settings | None = None) -> LLMProvider:
    from tacit.agents.providers.ollama import OllamaProvider

    return OllamaProvider(runtime_settings=runtime_settings)


_PROVIDER_FACTORIES = {
    "anthropic": _anthropic_provider,
    "openai": _openai_provider,
    "azure": _azure_provider,
    "bedrock": _bedrock_provider,
    "ollama": _ollama_provider,
}


def register_provider_factory(name: str, factory) -> None:
    """Register or override an LLM provider factory."""
    _PROVIDER_FACTORIES[name.lower()] = factory
    reset_provider_for_tests()


def reset_provider_for_tests() -> None:
    """Clear the cached provider singleton."""
    global _provider
    _provider = None


def _call_factory(factory, runtime_settings: Settings) -> LLMProvider:
    try:
        accepts_settings = bool(inspect.signature(factory).parameters)
    except (TypeError, ValueError):
        accepts_settings = False
    if accepts_settings:
        return factory(runtime_settings)
    return factory()


def create_provider(runtime_settings: Settings | None = None) -> LLMProvider:
    """Create an LLMProvider from an explicit settings object."""
    runtime_settings = runtime_settings or settings
    name = runtime_settings.llm_provider.lower()
    factory = _PROVIDER_FACTORIES.get(name)
    if factory is None:
        supported = ", ".join(sorted(_PROVIDER_FACTORIES))
        raise ValueError(f"Unknown LLM_PROVIDER={name!r}. Supported: {supported}")
    provider = _call_factory(factory, runtime_settings)
    logger.info("llm_provider_init", provider=name, model=runtime_settings.llm_model)
    return provider


def get_provider() -> LLMProvider:
    """Return the singleton LLMProvider based on settings.llm_provider."""
    global _provider
    if _provider is not None:
        return _provider

    _provider = create_provider(settings)
    return _provider
