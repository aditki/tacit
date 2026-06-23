"""Provider registry — resolves LLM_PROVIDER config to a concrete implementation."""

from __future__ import annotations

import structlog

from dashforge.agents.providers.base import LLMProvider
from dashforge.config import settings

logger = structlog.get_logger()

_provider: LLMProvider | None = None


def _anthropic_provider() -> LLMProvider:
    from dashforge.agents.providers.anthropic import AnthropicProvider

    return AnthropicProvider()


def _openai_provider() -> LLMProvider:
    from dashforge.agents.providers.openai_provider import OpenAIProvider

    return OpenAIProvider()


def _azure_provider() -> LLMProvider:
    from dashforge.agents.providers.openai_provider import AzureOpenAIProvider

    return AzureOpenAIProvider()


def _bedrock_provider() -> LLMProvider:
    from dashforge.agents.providers.bedrock import BedrockProvider

    return BedrockProvider()


def _ollama_provider() -> LLMProvider:
    from dashforge.agents.providers.ollama import OllamaProvider

    return OllamaProvider()


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


def reset_provider_for_tests() -> None:
    """Clear the cached provider singleton."""
    global _provider
    _provider = None


def get_provider() -> LLMProvider:
    """Return the singleton LLMProvider based on settings.llm_provider."""
    global _provider
    if _provider is not None:
        return _provider

    name = settings.llm_provider.lower()
    factory = _PROVIDER_FACTORIES.get(name)
    if factory is None:
        supported = ", ".join(sorted(_PROVIDER_FACTORIES))
        raise ValueError(f"Unknown LLM_PROVIDER={name!r}. Supported: {supported}")

    _provider = factory()
    logger.info("llm_provider_init", provider=name, model=settings.llm_model)
    return _provider
