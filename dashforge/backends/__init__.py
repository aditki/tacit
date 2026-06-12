"""Backend adapter registry.

Returns the list of active backends based on config. The pipeline iterates
over these instead of using vendor-specific if/else branches.
"""

from __future__ import annotations

from dashforge.backends.base import DashboardBackend, DashboardFeatures, DiscoveryStatus, PublishResult
from dashforge.config import settings

__all__ = [
    "DashboardBackend",
    "DashboardFeatures",
    "DiscoveryStatus",
    "PublishResult",
    "get_active_backends",
]


def get_active_backends() -> list[DashboardBackend]:
    """Instantiate backends that are enabled in config.

    Order matters: the first backend is considered "primary" and determines
    the query language used for archetype compilation.
    When both Grafana and SignalFx are enabled, Grafana comes first because
    PromQL is the most broadly supported language.
    """
    backends: list[DashboardBackend] = []

    if settings.grafana_enabled:
        from dashforge.backends.grafana import GrafanaBackend

        backends.append(GrafanaBackend())

    if settings.signalfx_enabled and settings.signalfx_api_token:
        from dashforge.backends.signalfx import SignalFxBackend

        backends.append(SignalFxBackend())

    return backends
