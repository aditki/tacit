"""Abstract base for context providers (knowledge base integrations)."""

from __future__ import annotations

from abc import ABC, abstractmethod

from tacit.config import Settings
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
