"""Abstract base for context providers (knowledge base integrations)."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, cast

from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
from tacit.models.schemas import ContextChunk, Intent
from tacit.runtime_ownership import (
    RuntimeOwnershipDescriptor,
    copy_runtime_settings,
    runtime_descriptor_for_provider,
    snapshot_runtime_settings,
)


class ContextProvider(ABC):
    """Interface every knowledge-base backend must implement.

    A context provider retrieves relevant documentation, runbooks, service
    catalogs, or past incident data given a classified intent.  The returned
    ContextChunks are injected into downstream agent prompts so the LLM has
    domain-specific knowledge it wouldn't otherwise have.
    """

    def __init__(self, runtime_settings: Settings | None = None, *, component: str = "context_provider") -> None:
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
            capability="context",
        )

    @property
    def runtime_settings(self) -> Settings:
        """Return the immutable configuration identity used by this provider."""
        try:
            runtime_settings = self._runtime_settings
        except AttributeError as exc:
            raise RuntimeError("context provider has no runtime ownership") from exc
        return copy_runtime_settings(runtime_settings)

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return the provider's public runtime ownership descriptor."""
        try:
            return self._runtime_ownership
        except AttributeError as exc:
            raise RuntimeError("context provider has no runtime ownership") from exc

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable provider name for logging."""

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
            raise RuntimeOwnershipError("Context provider lifecycle authority was revoked")
        existing_owner = getattr(self, "_lifecycle_invocation_owner", None)
        if existing_owner is owner:
            return
        if existing_owner is not None:
            raise RuntimeOwnershipError("Context provider is already bound to another lifecycle owner")

        query = self.query
        close = self.close

        async def invoke_query(
            intent: Intent,
            max_chunks: int = 10,
        ) -> list[ContextChunk]:
            return cast(list[ContextChunk], await invoke(lambda: query(intent, max_chunks)))

        async def deny_consumer_close() -> None:
            raise RuntimeOwnershipError("Managed context providers can only be closed by their runtime owner")

        object.__setattr__(self, "query", invoke_query)
        object.__setattr__(self, "close", deny_consumer_close)
        self._lifecycle_invocation_owner = owner
        self._lifecycle_close_operation = close
        self._lifecycle_close_invoke = close_invoke or invoke

    async def close_from_lifecycle(self, *, owner: object) -> None:
        """Close a managed provider only for its exact generation owner."""
        if getattr(self, "_lifecycle_invocation_revoked", False):
            raise RuntimeOwnershipError("Context provider lifecycle authority was revoked")
        if getattr(self, "_lifecycle_invocation_owner", None) is not owner:
            raise RuntimeOwnershipError("Context provider close authority does not match its runtime owner")
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
            raise RuntimeOwnershipError("Context provider revoke authority does not match its runtime owner")

        async def denied_query(
            _intent: Intent,
            _max_chunks: int = 10,
        ) -> list[ContextChunk]:
            raise RuntimeOwnershipError("Pipeline provider generation is no longer active")

        async def denied_close() -> None:
            raise RuntimeOwnershipError("Managed context providers can only be closed by their runtime owner")

        object.__setattr__(self, "query", denied_query)
        object.__setattr__(self, "close", denied_close)
        self._lifecycle_invocation_owner = None
        del self._lifecycle_close_operation
        del self._lifecycle_close_invoke
        self._lifecycle_invocation_revoked = True

    @abstractmethod
    async def query(
        self,
        intent: Intent,
        max_chunks: int = 10,
    ) -> list[ContextChunk]:
        """Retrieve context chunks relevant to the intent.

        Args:
            intent: Classified intent from the Intent Agent.
            max_chunks: Maximum number of chunks to return.

        Returns:
            List of ContextChunk objects, sorted by relevance (best first).
        """

    async def close(self) -> None:
        """Release any underlying network clients."""
