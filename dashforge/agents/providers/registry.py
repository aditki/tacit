"""Provider registry — resolves LLM_PROVIDER config to a concrete implementation."""

from __future__ import annotations

import structlog

from dashforge.agents.providers.base import LLMProvider
from dashforge.config import settings

logger = structlog.get_logger()

_provider: LLMProvider | None = None


def get_provider() -> LLMProvider:
    """Return the singleton LLMProvider based on settings.llm_provider."""
    global _provider
    if _provider is not None:
        return _provider

    name = settings.llm_provider.lower()

    if name == "anthropic":
        from dashforge.agents.providers.anthropic import AnthropicProvider

        _provider = AnthropicProvider()

    elif name == "openai":
        from dashforge.agents.providers.openai_provider import OpenAIProvider

        _provider = OpenAIProvider()

    elif name == "azure":
        from dashforge.agents.providers.openai_provider import AzureOpenAIProvider

        _provider = AzureOpenAIProvider()

    elif name == "bedrock":
        from dashforge.agents.providers.bedrock import BedrockProvider

        _provider = BedrockProvider()

    elif name == "ollama":
        from dashforge.agents.providers.ollama import OllamaProvider

        _provider = OllamaProvider()

    else:
        raise ValueError(f"Unknown LLM_PROVIDER={name!r}. " f"Supported: anthropic, openai, azure, bedrock, ollama")

    logger.info("llm_provider_init", provider=name, model=settings.llm_model)
    return _provider
