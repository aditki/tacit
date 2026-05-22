"""Abstract base for LLM providers."""
from __future__ import annotations

from abc import ABC, abstractmethod


class LLMProvider(ABC):
    """Interface every LLM backend must implement."""

    @abstractmethod
    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> str:
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
    ) -> str:
        """Return plain text from the model."""
