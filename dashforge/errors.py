"""DashForge domain exception taxonomy."""

from __future__ import annotations


class DashForgeError(Exception):
    """Base class for errors raised by DashForge domain code."""


class RecoverableDashForgeError(DashForgeError):
    """A failure that can be recorded and degraded without failing the process."""


class FatalPipelineError(DashForgeError):
    """A pipeline failure that should stop request processing."""


class BackendUnavailable(RecoverableDashForgeError):
    """A dashboard backend or datasource is unavailable."""


class HistoryWriteFailed(RecoverableDashForgeError):
    """A best-effort history write failed."""


class PipelineStageError(RecoverableDashForgeError):
    """A recoverable stage-level failure."""


class EvidenceResolutionError(PipelineStageError):
    """Evidence resolution failed without invalidating the whole request."""
