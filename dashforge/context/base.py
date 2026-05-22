"""Abstract base for context providers (knowledge base integrations)."""
from __future__ import annotations

from abc import ABC, abstractmethod

from dashforge.models.schemas import ContextChunk, Intent


class ContextProvider(ABC):
    """Interface every knowledge-base backend must implement.

    A context provider retrieves relevant documentation, runbooks, service
    catalogs, or past incident data given a classified intent.  The returned
    ContextChunks are injected into downstream agent prompts so the LLM has
    domain-specific knowledge it wouldn't otherwise have.
    """

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
