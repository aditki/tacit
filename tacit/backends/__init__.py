"""Backend adapter registry.

Returns the list of active backends based on config. The pipeline iterates
over these instead of using vendor-specific if/else branches.
"""

from __future__ import annotations

from tacit.backends.base import AlertFeatures, DashboardBackend, DashboardFeatures, DiscoveryStatus, PublishResult
from tacit.config import Settings, settings

__all__ = [
    "DashboardBackend",
    "DashboardFeatures",
    "AlertFeatures",
    "DiscoveryStatus",
    "PublishResult",
    "get_active_backends",
]


def get_active_backends(runtime_settings: Settings | None = None) -> list[DashboardBackend]:
    """Instantiate backends that are enabled in config.

    Order matters: the first backend is considered "primary" and determines
    the query language used for archetype compilation.
    When both Grafana and SignalFx are enabled, Grafana comes first because
    PromQL is the most broadly supported language.
    """
    config = runtime_settings or settings
    backends: list[DashboardBackend] = []

    if config.grafana_enabled:
        from tacit.backends.grafana import GrafanaBackend

        backends.append(GrafanaBackend(runtime_settings=config))

    if config.signalfx_enabled and config.signalfx_api_token:
        from tacit.backends.signalfx import SignalFxBackend

        backends.append(SignalFxBackend(runtime_settings=config))

    return backends
