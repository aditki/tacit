"""Abstract base for LLM providers."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast

from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
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
        factory_event_loop: asyncio.AbstractEventLoop | None
        try:
            factory_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            factory_event_loop = None
        self._factory_event_loop = factory_event_loop
        self._foreign_loop_rejection = False
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

    def validate_factory_event_loop(self, expected_loop: asyncio.AbstractEventLoop | None) -> None:
        """Reject products created under a different live event-loop owner."""
        owner_loop = getattr(self, "_factory_event_loop", None)
        if owner_loop is None or expected_loop is None or owner_loop is expected_loop:
            return
        self._foreign_loop_rejection = True
        raise RuntimeOwnershipError("Pipeline provider runtime ownership mismatch: incompatible event loop affinity")

    async def retire_rejected(self, *, reason_code: str) -> bool:
        """Asynchronously return a rejected product to its creating loop."""
        del reason_code
        if not getattr(self, "_foreign_loop_rejection", False):
            return False
        owner_loop = getattr(self, "_factory_event_loop", None)
        if owner_loop is None or owner_loop.is_closed() or not owner_loop.is_running():
            raise RuntimeOwnershipError("Rejected provider event loop owner is unavailable")
        retirement = asyncio.run_coroutine_threadsafe(self.close(), owner_loop)
        await asyncio.wrap_future(retirement)
        return True

    def bind_lifecycle_invoker(
        self,
        *,
        owner: object,
        invoke: Callable[[Callable[[], Awaitable[Any]]], Awaitable[Any]],
        close_invoke: Callable[[Callable[[], Awaitable[Any]]], Awaitable[Any]] | None = None,
    ) -> None:
        """Bind every async operation to the event loop that owns this product."""
        if getattr(self, "_lifecycle_invocation_revoked", False):
            raise RuntimeOwnershipError("LLM provider lifecycle authority was revoked")
        existing_owner = getattr(self, "_lifecycle_invocation_owner", None)
        if existing_owner is owner:
            return
        if existing_owner is not None:
            raise RuntimeOwnershipError("LLM provider is already bound to another lifecycle owner")

        chat_json = self.chat_json
        chat_text = self.chat_text
        close = self.close

        async def invoke_chat_json(
            system_prompt: str,
            user_prompt: str,
            temperature: float = 0.2,
        ) -> LLMResult:
            return cast(
                LLMResult,
                await invoke(lambda: chat_json(system_prompt, user_prompt, temperature)),
            )

        async def invoke_chat_text(
            system_prompt: str,
            user_prompt: str,
            temperature: float = 0.3,
        ) -> LLMResult:
            return cast(
                LLMResult,
                await invoke(lambda: chat_text(system_prompt, user_prompt, temperature)),
            )

        async def deny_consumer_close() -> None:
            raise RuntimeOwnershipError("Managed LLM providers can only be closed by their runtime owner")

        object.__setattr__(self, "chat_json", invoke_chat_json)
        object.__setattr__(self, "chat_text", invoke_chat_text)
        object.__setattr__(self, "close", deny_consumer_close)
        self._lifecycle_invocation_owner = owner
        self._lifecycle_close_operation = close
        self._lifecycle_close_invoke = close_invoke or invoke

    async def close_from_lifecycle(self, *, owner: object) -> None:
        """Close a managed provider only for its exact generation owner."""
        if getattr(self, "_lifecycle_invocation_revoked", False):
            raise RuntimeOwnershipError("LLM provider lifecycle authority was revoked")
        if getattr(self, "_lifecycle_invocation_owner", None) is not owner:
            raise RuntimeOwnershipError("LLM provider close authority does not match its runtime owner")
        close = cast(Callable[[], Awaitable[Any]], getattr(self, "_lifecycle_close_operation"))
        invoke = cast(
            Callable[[Callable[[], Awaitable[Any]]], Awaitable[Any]],
            getattr(self, "_lifecycle_close_invoke"),
        )
        await invoke(close)

    def revoke_lifecycle_invoker(self, *, owner: object) -> None:
        """Fence operations and sever generation references after final cleanup."""
        if getattr(self, "_lifecycle_invocation_revoked", False):
            return
        if getattr(self, "_lifecycle_invocation_owner", None) is not owner:
            raise RuntimeOwnershipError("LLM provider revoke authority does not match its runtime owner")

        async def denied_chat_json(
            _system_prompt: str,
            _user_prompt: str,
            _temperature: float = 0.2,
        ) -> LLMResult:
            raise RuntimeOwnershipError("Pipeline provider generation is no longer active")

        async def denied_chat_text(
            _system_prompt: str,
            _user_prompt: str,
            _temperature: float = 0.3,
        ) -> LLMResult:
            raise RuntimeOwnershipError("Pipeline provider generation is no longer active")

        async def denied_close() -> None:
            raise RuntimeOwnershipError("Managed LLM providers can only be closed by their runtime owner")

        object.__setattr__(self, "chat_json", denied_chat_json)
        object.__setattr__(self, "chat_text", denied_chat_text)
        object.__setattr__(self, "close", denied_close)
        self._lifecycle_invocation_owner = None
        del self._lifecycle_close_operation
        del self._lifecycle_close_invoke
        self._lifecycle_invocation_revoked = True

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
