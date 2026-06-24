"""Tacit domain exception taxonomy."""

from __future__ import annotations


class TacitError(Exception):
    """Base class for errors raised by Tacit domain code."""


class RecoverableTacitError(TacitError):
    """A failure that can be recorded and degraded without failing the process."""


class FatalPipelineError(TacitError):
    """A pipeline failure that should stop request processing."""


class BackendUnavailable(RecoverableTacitError):
    """A dashboard backend or datasource is unavailable."""


class HistoryWriteFailed(RecoverableTacitError):
    """A best-effort history write failed."""


class PipelineStageError(RecoverableTacitError):
    """A recoverable stage-level failure."""


class EvidenceResolutionError(PipelineStageError):
    """Evidence resolution failed without invalidating the whole request."""
