"""Process-lifetime lifecycle fence isolation for unit fault matrices."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import tacit.api.optional_integrations as optional_integrations_module
import tacit.pipeline_admission as pipeline_admission_module


@pytest.fixture(autouse=True)
def _isolate_process_lifetime_lifecycle_fences(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """Model a fresh process per test without adding production reset APIs."""
    production_fatal_registry = pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY
    with production_fatal_registry._lock:
        fatal_records_before = dict(production_fatal_registry._records)
        fatal_overflow_before = production_fatal_registry._overflow

    production_optional_owners = optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    production_optional_fences = optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    with optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_LOCK:
        optional_owners_before = dict(production_optional_owners)
        optional_fences_before = set(production_optional_fences)

    isolated_fatal_registry = pipeline_admission_module._ProcessRuntimeFatalRegistry(limit=1024)
    isolated_optional_owners: dict[
        tuple[str, str],
        optional_integrations_module._OptionalIntegrationExecutionState,
    ] = {}
    isolated_optional_fences: set[tuple[str, str]] = set()
    monkeypatch.setattr(
        pipeline_admission_module,
        "_PROCESS_RUNTIME_FATAL_REGISTRY",
        isolated_fatal_registry,
    )
    monkeypatch.setattr(
        optional_integrations_module,
        "_OPTIONAL_INTEGRATION_EXECUTION_OWNERS",
        isolated_optional_owners,
    )
    monkeypatch.setattr(
        optional_integrations_module,
        "_FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS",
        isolated_optional_fences,
    )

    yield

    assert all(state.finished.is_set() for state in isolated_optional_owners.values())
    with production_fatal_registry._lock:
        assert production_fatal_registry._records == fatal_records_before
        assert production_fatal_registry._overflow is fatal_overflow_before
    with optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_LOCK:
        assert production_optional_owners == optional_owners_before
        assert production_optional_fences == optional_fences_before
