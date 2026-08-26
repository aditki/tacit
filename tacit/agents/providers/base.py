"""Abstract base for LLM providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from tacit.config import Settings
from tacit.runtime_ownership import (
    RuntimeOwnershipDescriptor,
    copy_runtime_settings,
    runtime_descriptor_for_provider,
    snapshot_runtime_settings,
)


@dataclass
class TokenUsage:
    """Token usage from a single LLM call."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def __add__(self, other: TokenUsage) -> TokenUsage:
        return TokenUsage(
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
        )


@dataclass
class LLMResult:
    """Raw LLM response text + token usage metadata."""

    text: str
    usage: TokenUsage = field(default_factory=TokenUsage)


class LLMProvider(ABC):
    """Interface every LLM backend must implement."""

    def __init__(self, runtime_settings: Settings | None = None, *, component: str = "llm_provider") -> None:
        if runtime_settings is None:
            return
        self._runtime_settings = snapshot_runtime_settings(runtime_settings)
        self._runtime_ownership = runtime_descriptor_for_provider(
            component=component,
            runtime_settings=self._runtime_settings,
            capability="llm",
        )

    @property
    def runtime_settings(self) -> Settings:
        """Return the immutable configuration identity used by this provider."""
        try:
            runtime_settings = self._runtime_settings
        except AttributeError as exc:
            raise RuntimeError("LLM provider has no runtime ownership") from exc
        return copy_runtime_settings(runtime_settings)

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return the provider's public runtime ownership descriptor."""
        try:
            return self._runtime_ownership
        except AttributeError as exc:
            raise RuntimeError("LLM provider has no runtime ownership") from exc

    @property
    def is_configured(self) -> bool:
        """False when the provider is missing required credentials.

        Providers that need an API key override this; local providers
        (Ollama) and IAM-based providers (Bedrock) stay True.
        """
        return True

    @abstractmethod
    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        """Return a raw JSON string from the model.

        Implementations should instruct the model to respond with valid JSON
        (via native JSON-mode, tool-use, or prompt engineering).
        """

    @abstractmethod
    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        """Return plain text from the model."""

    async def close(self) -> None:
        """Release any underlying network clients."""
