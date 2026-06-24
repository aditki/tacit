"""Registry that resolves CONTEXT_PROVIDER config to a concrete implementation."""

from __future__ import annotations

import structlog

from dashforge.config import Settings, settings
from dashforge.context.base import ContextProvider

logger = structlog.get_logger()

_provider: ContextProvider | None = None
_initialized = False


def create_context_provider(runtime_settings: Settings | None = None) -> ContextProvider | None:
    """Create the configured context provider, or None if disabled.

    Returns None when context_provider is 'none' or '' (the default).
    """
    runtime_settings = runtime_settings or settings
    name = runtime_settings.context_provider.lower().strip()

    if not name or name == "none":
        logger.info("context_provider_disabled")
        return None

    if name == "mcp":
        from dashforge.context.mcp_provider import MCPProvider

        provider: ContextProvider | None = MCPProvider(runtime_settings=runtime_settings)

    elif name == "a2a":
        from dashforge.context.a2a_provider import A2AProvider

        provider = A2AProvider(runtime_settings=runtime_settings)

    elif name == "rag_api":
        from dashforge.context.rag_api_provider import RAGAPIProvider

        provider = RAGAPIProvider(runtime_settings=runtime_settings)

    else:
        logger.warning("unknown_context_provider", provider=name)
        return None

    logger.info("context_provider_init", provider=name)
    return provider


def get_context_provider() -> ContextProvider | None:
    """Return the singleton context provider based on global settings."""
    global _provider, _initialized

    if _initialized:
        return _provider

    _initialized = True
    _provider = create_context_provider(settings)
    return _provider
