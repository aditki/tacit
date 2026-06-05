"""Registry that resolves CONTEXT_PROVIDER config to a concrete implementation."""

from __future__ import annotations

import structlog

from dashforge.config import settings
from dashforge.context.base import ContextProvider

logger = structlog.get_logger()

_provider: ContextProvider | None = None
_initialized = False


def get_context_provider() -> ContextProvider | None:
    """Return the configured context provider, or None if disabled.

    Returns None when context_provider is 'none' or '' (the default).
    """
    global _provider, _initialized

    if _initialized:
        return _provider

    _initialized = True
    name = settings.context_provider.lower().strip()

    if not name or name == "none":
        logger.info("context_provider_disabled")
        return None

    if name == "mcp":
        from dashforge.context.mcp_provider import MCPProvider

        _provider = MCPProvider()

    elif name == "a2a":
        from dashforge.context.a2a_provider import A2AProvider

        _provider = A2AProvider()

    elif name == "rag_api":
        from dashforge.context.rag_api_provider import RAGAPIProvider

        _provider = RAGAPIProvider()

    else:
        logger.warning("unknown_context_provider", provider=name)
        return None

    logger.info("context_provider_init", provider=name)
    return _provider
