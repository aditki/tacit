from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import httpx
import pytest

from tacit.backends.grafana import GrafanaBackend
from tacit.backends.signalfx import SignalFxBackend
from tacit.config import Settings
from tacit.grafana.client import GrafanaClient
from tacit.integrations.pagerduty import PagerDutyClient
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.runtime_ownership import (
    RuntimeAvailability,
    RuntimeDatabaseIdentity,
    RuntimeOwner,
    RuntimeOwnershipDescriptor,
    RuntimeOwnershipError,
    RuntimeOwnershipMismatchError,
    RuntimeRemoteIdentity,
    RuntimeTenantPolicy,
    adapt_third_party_runtime_owner,
    canonical_remote_endpoint,
    canonical_signalfx_realm,
    credential_fingerprint,
    get_runtime_ownership,
    require_compatible_runtime_ownership,
    resolve_runtime_settings,
    runtime_descriptor_for_remote,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
    snapshot_runtime_settings,
)
from tacit.runtime_stores import RuntimeStores
from tacit.signalfx.client import SignalFxClient


def _settings(tmp_path, **updates: object) -> Settings:
    values: dict[str, Any] = {
        "_env_file": None,
        "history_db_path": str(tmp_path / "state" / "history.db"),
        "feedback_db_path": str(tmp_path / "state" / "feedback.db"),
        "signals_db_path": str(tmp_path / "state" / "signals.db"),
        "knowledge_tenant_id": "tenant-a",
        "knowledge_permissions": "knowledge.read,knowledge.review",
        "grafana_url": "https://grafana.example.test/",
        "grafana_org_id": 7,
        "grafana_api_key": "grafana-secret",
        "signalfx_realm": "eu0",
        "signalfx_api_token": "signalfx-secret",
        "pagerduty_base_url": "https://api.pagerduty.test/",
        "pagerduty_api_token": "pagerduty-secret",
    }
    values.update(updates)
    return Settings(**values)


def _isolated_settings(**updates: object) -> Settings:
    values: dict[str, Any] = {"_env_file": None, **updates}
    return Settings(**values)


def test_equivalent_runtime_owners_succeed_without_initializing_storage(tmp_path):
    first_settings = _settings(tmp_path)
    equivalent_settings = _settings(tmp_path)
    stores = RuntimeStores(first_settings)

    selected = require_compatible_runtime_ownership(
        boundary="runtime-test",
        descriptors=(
            get_runtime_ownership(stores),
            runtime_descriptor_from_settings(
                equivalent_settings,
                component="equivalent-settings",
            ),
        ),
    )

    assert selected.settings_identity == get_runtime_ownership(stores).settings_identity
    assert not (tmp_path / "state").exists()


@pytest.mark.parametrize(
    ("dimension", "other"),
    [
        ("settings", {"log_level": "DEBUG"}),
        ("tenant", {"knowledge_tenant_id": "tenant-b"}),
        ("permission", {"knowledge_permissions": "knowledge.read"}),
    ],
)
def test_settings_tenant_and_permission_mismatches_fail(tmp_path, dimension, other):
    expected = runtime_descriptor_from_settings(_settings(tmp_path), component="expected")
    actual = runtime_descriptor_from_settings(_settings(tmp_path, **other), component="actual")

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="runtime-test",
            descriptors=(expected, actual),
        )

    assert dimension in exc_info.value.dimensions
    assert not (tmp_path / "state").exists()


def test_permission_identity_preserves_enforcement_case_sensitivity(tmp_path):
    expected = runtime_descriptor_from_settings(
        _settings(tmp_path, knowledge_permissions="knowledge.read"),
        component="expected",
    )
    actual = runtime_descriptor_from_settings(
        _settings(tmp_path, knowledge_permissions="KNOWLEDGE.READ"),
        component="actual",
    )

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="permission-case",
            descriptors=(expected, actual),
        )

    assert "permission" in exc_info.value.dimensions
    assert expected.settings_identity != actual.settings_identity


def test_settings_reject_tenant_key_names_that_lookup_would_not_match():
    with pytest.raises(ValueError, match="tenant key name"):
        Settings(
            _env_file=None,
            api_auth_enabled=True,
            knowledge_tenant_id="*",
            knowledge_tenant_api_keys={" tenant-a ": "tenant-a-secret"},
        )


def test_database_identity_mismatch_fails_without_touching_either_path(tmp_path):
    runtime_settings = _settings(tmp_path)
    expected = runtime_descriptor_for_store(
        component="expected-signals",
        runtime_settings=runtime_settings,
        database_role="signals",
        database_path=tmp_path / "expected" / "signals.db",
    )
    actual = runtime_descriptor_for_store(
        component="actual-signals",
        runtime_settings=runtime_settings,
        database_role="signals",
        database_path=tmp_path / "actual" / "signals.db",
    )

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="runtime-test",
            descriptors=(expected, actual),
        )

    assert "database" in exc_info.value.dimensions
    assert not (tmp_path / "expected").exists()
    assert not (tmp_path / "actual").exists()


@pytest.mark.parametrize(
    ("field", "value", "dimension"),
    [
        ("endpoint", "https://other.example.test", "endpoint"),
        ("account", "99", "account"),
        ("credential_fingerprint", credential_fingerprint("other-secret"), "credential"),
    ],
)
def test_effective_remote_identity_mismatches_fail(tmp_path, field, value, dimension):
    runtime_settings = _settings(tmp_path)
    expected_remote = RuntimeRemoteIdentity(
        provider="grafana",
        endpoint="https://grafana.example.test",
        account="7",
        credential_fingerprint=credential_fingerprint("grafana-secret"),
    )
    actual_remote = replace(expected_remote, **{field: value})
    expected = runtime_descriptor_for_remote(
        component="expected-grafana",
        runtime_settings=runtime_settings,
        remote=expected_remote,
    )
    actual = runtime_descriptor_for_remote(
        component="actual-grafana",
        runtime_settings=runtime_settings,
        remote=actual_remote,
    )

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="runtime-test",
            descriptors=(expected, actual),
        )

    assert dimension in exc_info.value.dimensions


def test_ownerless_injected_component_fails_and_compatibility_is_explicit(tmp_path):
    class OwnerlessTacitComponent:
        pass

    with pytest.raises(RuntimeOwnershipError, match="public runtime ownership descriptor"):
        get_runtime_ownership(OwnerlessTacitComponent())

    runtime_settings = _settings(tmp_path)
    adapted = adapt_third_party_runtime_owner(
        component="third-party-store",
        owner=OwnerlessTacitComponent(),
        runtime_settings=runtime_settings,
        database_role="signals",
        database_path=tmp_path / "third-party" / "signals.db",
    )

    assert (
        adapted.settings_identity
        == runtime_descriptor_from_settings(
            runtime_settings,
            component="settings",
        ).settings_identity
    )
    assert not (tmp_path / "third-party").exists()


def test_descriptor_serialization_never_exposes_raw_secrets(tmp_path):
    runtime_settings = _settings(
        tmp_path,
        api_auth_key="http-auth-secret",
        knowledge_tenant_api_keys={"tenant-a": "tenant-auth-secret"},
        llm_api_key="llm-secret",
        context_api_key="context-secret",
    )
    descriptor = runtime_descriptor_from_settings(runtime_settings, component="runtime")
    serialized = json.dumps(asdict(descriptor), default=str, sort_keys=True)

    for secret in (
        "http-auth-secret",
        "tenant-auth-secret",
        "llm-secret",
        "context-secret",
        "grafana-secret",
        "signalfx-secret",
        "pagerduty-secret",
    ):
        assert secret not in serialized
        assert secret not in repr(descriptor)


def test_explicit_unavailable_owner_is_distinct_from_absent_owner():
    absent = RuntimeOwnershipDescriptor.absent(component="optional-store")
    unavailable = RuntimeOwnershipDescriptor.unavailable(
        component="optional-store",
        reason="configured store could not be opened",
    )

    assert absent.availability is RuntimeAvailability.ABSENT
    assert unavailable.availability is RuntimeAvailability.UNAVAILABLE
    assert absent != unavailable

    with pytest.raises(RuntimeOwnershipError, match="explicitly unavailable"):
        require_compatible_runtime_ownership(
            boundary="runtime-test",
            descriptors=(unavailable,),
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("settings_identity", credential_fingerprint("settings-identity")),
        (
            "tenant_policy",
            RuntimeTenantPolicy(
                mode="pinned",
                tenant_id="tenant-a",
                permissions=("knowledge.read",),
                api_auth_enabled=False,
            ),
        ),
        ("databases", (RuntimeDatabaseIdentity(role="history", path=Path("state.db")),)),
        (
            "remotes",
            (RuntimeRemoteIdentity(provider="grafana", endpoint="https://grafana.example.test"),),
        ),
        ("cache_namespace", "runtime-cache:available"),
        ("admission_namespace", "runtime-admission:available"),
        ("settings_identity", ""),
        ("cache_namespace", ""),
        ("admission_namespace", ""),
    ],
)
def test_runtime_ownership_non_available_states_reject_identity_dimensions(
    field_name,
    value,
):
    with pytest.raises(RuntimeOwnershipError, match="must not expose runtime identity"):
        RuntimeOwnershipDescriptor(
            component=f"invalid-{field_name}",
            availability=RuntimeAvailability.ABSENT,
            **{field_name: value},
        )
    with pytest.raises(RuntimeOwnershipError, match="must not expose runtime identity"):
        RuntimeOwnershipDescriptor(
            component=f"invalid-{field_name}",
            availability=RuntimeAvailability.UNAVAILABLE,
            availability_reason="dependency_unavailable",
            **{field_name: value},
        )


def test_absent_and_unavailable_runtime_ownership_reason_invariants():
    with pytest.raises(RuntimeOwnershipError, match="absent runtime owners must not include a reason"):
        RuntimeOwnershipDescriptor(
            component="invalid-absent",
            availability=RuntimeAvailability.ABSENT,
            availability_reason="not configured",
        )

    with pytest.raises(RuntimeOwnershipError, match="unavailable runtime owners require a reason"):
        RuntimeOwnershipDescriptor(
            component="invalid-unavailable",
            availability=RuntimeAvailability.UNAVAILABLE,
        )

    unavailable = RuntimeOwnershipDescriptor.unavailable(
        component="bounded-unavailable",
        reason="Store could not be opened! " * 20,
    )

    assert unavailable.availability_reason
    assert len(unavailable.availability_reason) <= 96
    assert unavailable.availability_reason == unavailable.availability_reason.casefold()
    assert set(unavailable.availability_reason) <= set("abcdefghijklmnopqrstuvwxyz0123456789_.:-")


def test_compatibility_rejects_absent_owner_instead_of_filtering_it(tmp_path):
    available = runtime_descriptor_from_settings(
        _settings(tmp_path),
        component="available-owner",
    )
    absent = RuntimeOwnershipDescriptor.absent(component="optional-store")

    with pytest.raises(RuntimeOwnershipError, match="was not supplied"):
        require_compatible_runtime_ownership(
            boundary="runtime-test",
            descriptors=(available, absent),
        )

    assert not (tmp_path / "state").exists()


def test_runtime_tenant_policy_rejects_unauthenticated_wildcard_mode():
    with pytest.raises(RuntimeOwnershipError, match="requires API authentication"):
        RuntimeTenantPolicy(
            mode="wildcard",
            tenant_id="*",
            permissions=("knowledge.read",),
            api_auth_enabled=False,
        )


def test_runtime_tenant_policy_rejects_duplicate_credential_fingerprints_without_disclosure():
    duplicate_secret = "same-secret-for-two-tenants"
    duplicate_fingerprint = credential_fingerprint(duplicate_secret)

    with pytest.raises(RuntimeOwnershipError, match="unique non-empty key") as exc_info:
        RuntimeTenantPolicy(
            mode="wildcard",
            tenant_id="*",
            permissions=("knowledge.read",),
            api_auth_enabled=True,
            tenant_credential_fingerprints=(
                ("tenant-a", duplicate_fingerprint),
                ("tenant-b", duplicate_fingerprint),
            ),
        )

    assert duplicate_secret not in str(exc_info.value)
    assert duplicate_fingerprint not in str(exc_info.value)


@pytest.mark.parametrize("boundary", ["snapshot", "descriptor"])
def test_runtime_boundary_revalidates_model_construct_wildcard_auth_before_storage(
    tmp_path,
    boundary,
):
    database_path = tmp_path / "must-not-exist" / "history.db"
    payload = _settings(tmp_path, history_db_path=str(database_path)).model_dump()
    payload.update(knowledge_tenant_id="*", api_auth_enabled=False)
    bypassed = Settings.model_construct(**payload)

    with pytest.raises(RuntimeOwnershipError, match="requires API authentication"):
        if boundary == "snapshot":
            snapshot_runtime_settings(bypassed)
        else:
            runtime_descriptor_from_settings(bypassed, component="bypassed-settings")

    assert not database_path.parent.exists()


@pytest.mark.parametrize("boundary", ["snapshot", "descriptor"])
def test_runtime_boundary_revalidates_model_construct_duplicate_tenant_secrets(
    tmp_path,
    boundary,
):
    duplicate_secret = "same-secret-for-two-tenants"
    database_path = tmp_path / "must-not-exist" / "history.db"
    payload = _settings(tmp_path, history_db_path=str(database_path)).model_dump()
    payload.update(
        knowledge_tenant_id="*",
        api_auth_enabled=True,
        knowledge_tenant_api_keys={
            "tenant-a": duplicate_secret,
            "tenant-b": duplicate_secret,
        },
    )
    bypassed = Settings.model_construct(**payload)

    with pytest.raises(RuntimeOwnershipError, match="unique non-empty key") as exc_info:
        if boundary == "snapshot":
            snapshot_runtime_settings(bypassed)
        else:
            runtime_descriptor_from_settings(bypassed, component="bypassed-settings")

    assert duplicate_secret not in str(exc_info.value)
    assert not database_path.parent.exists()


def test_namespace_mismatch_is_an_ownership_mismatch(tmp_path):
    expected = runtime_descriptor_from_settings(_settings(tmp_path), component="expected")
    actual = replace(expected, component="actual", cache_namespace="cache:other")

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="runtime-test",
            descriptors=(expected, actual),
        )

    assert "cache_namespace" in exc_info.value.dimensions


@pytest.mark.asyncio
async def test_remote_descriptor_inspection_and_rejection_make_zero_requests(tmp_path):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={})

    runtime_settings = _settings(tmp_path)
    first = PagerDutyClient(
        runtime_settings=runtime_settings,
        transport=httpx.MockTransport(handler),
    )
    second = PagerDutyClient(
        api_token="different-secret",
        runtime_settings=runtime_settings,
        transport=httpx.MockTransport(handler),
    )
    try:
        first_descriptor = get_runtime_ownership(first)
        second_descriptor = get_runtime_ownership(second)
        with pytest.raises(RuntimeOwnershipMismatchError):
            require_compatible_runtime_ownership(
                boundary="pagerduty-test",
                descriptors=(first_descriptor, second_descriptor),
            )
    finally:
        await first.close()
        await second.close()

    assert requests == []
    assert not (tmp_path / "state").exists()


def test_database_identities_are_role_scoped(tmp_path):
    runtime_settings = _settings(tmp_path)
    history = runtime_descriptor_for_store(
        component="history",
        runtime_settings=runtime_settings,
        database_role="history",
        database_path=tmp_path / "history.db",
    )
    signals = runtime_descriptor_for_store(
        component="signals",
        runtime_settings=runtime_settings,
        database_role="signals",
        database_path=tmp_path / "signals.db",
    )

    require_compatible_runtime_ownership(
        boundary="distinct-store-roles",
        descriptors=(history, signals),
    )
    assert history.databases == (RuntimeDatabaseIdentity(role="history", path=(tmp_path / "history.db").resolve()),)


@pytest.mark.parametrize(
    "descriptors",
    [
        (
            RuntimeOwnershipDescriptor(
                component="history-only",
                databases=(RuntimeDatabaseIdentity(role="history", path=Path("history.db")),),
            ),
            RuntimeOwnershipDescriptor(
                component="signals-only",
                databases=(RuntimeDatabaseIdentity(role="signals", path=Path("signals.db")),),
            ),
        ),
        (
            RuntimeOwnershipDescriptor(
                component="database-only",
                databases=(RuntimeDatabaseIdentity(role="history", path=Path("history.db")),),
            ),
            RuntimeOwnershipDescriptor(
                component="remote-only",
                remotes=(
                    RuntimeRemoteIdentity(
                        provider="grafana",
                        endpoint="https://grafana.example.test",
                    ),
                ),
            ),
        ),
    ],
)
def test_disconnected_runtime_owners_fail_closed_without_side_effects(tmp_path, monkeypatch, descriptors):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="disconnected-runtime",
            descriptors=descriptors,
        )

    assert "ownership_graph" in exc_info.value.dimensions
    assert not (tmp_path / "history.db").exists()
    assert not (tmp_path / "signals.db").exists()


def test_runtime_owners_may_form_one_connected_identity_graph(tmp_path):
    runtime_settings = _settings(tmp_path)
    root = runtime_descriptor_from_settings(runtime_settings, component="root")
    history = RuntimeOwnershipDescriptor(
        component="history-adapter",
        databases=(
            RuntimeDatabaseIdentity(
                role="history",
                path=tmp_path / "state" / "history.db",
            ),
        ),
    )
    grafana = RuntimeOwnershipDescriptor(
        component="grafana-adapter",
        remotes=(
            RuntimeRemoteIdentity(
                provider="grafana",
                endpoint="https://grafana.example.test",
                account="7",
                credential_fingerprint=credential_fingerprint("grafana-secret"),
            ),
        ),
    )

    require_compatible_runtime_ownership(
        boundary="connected-runtime",
        descriptors=(history, root, grafana),
    )

    assert not (tmp_path / "state").exists()


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://alice:secret@grafana.example.test",
        "https://token@api.pagerduty.test",
        "alice:secret@grafana.example.test/api",
        "user:supersecret@example.test/api?token=anothersecret",
        "https://grafana.example.test/api?api_key=secret",
        "grafana.example.test/api?token=secret",
        "https://grafana.example.test:not-a-port",
    ],
)
def test_canonical_remote_endpoint_rejects_embedded_credentials_without_echoing_them(endpoint):
    with pytest.raises(RuntimeOwnershipError, match="remote endpoint") as exc_info:
        canonical_remote_endpoint(endpoint)

    assert "secret" not in str(exc_info.value)
    assert "token" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
    "endpoint",
    [
        "grafana.example.test",
        "//grafana.example.test",
        "ftp://grafana.example.test",
        "https://",
        " https://grafana.example.test",
        "https://grafana.example.test ",
        "https://grafana.example.test\n",
        "https://grafana example.test",
        "https://grafana.example.test?",
        "https://grafana.example.test#",
        "https://-grafana.example.test",
        "https://grafana..example.test",
        "https://999.999.999.999",
        "https://grafana.example.test:0",
        "https://grafana.example.test:70000",
        "http://example.com:",
    ],
)
def test_canonical_remote_endpoint_rejects_malformed_nonabsolute_endpoints_without_echo(endpoint):
    with pytest.raises(RuntimeOwnershipError, match="remote endpoint") as exc_info:
        canonical_remote_endpoint(endpoint)

    assert endpoint not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
    ("endpoint", "expected"),
    [
        ("HTTPS://GRAFANA.EXAMPLE.TEST:443/", "https://grafana.example.test"),
        ("http://GRAFANA.EXAMPLE.TEST:80/api/", "http://grafana.example.test/api"),
        ("https://GRAFANA.EXAMPLE.TEST:8443/api///", "https://grafana.example.test:8443/api"),
    ],
)
def test_canonical_remote_endpoint_preserves_valid_equivalence(endpoint, expected):
    assert canonical_remote_endpoint(endpoint) == expected


@pytest.mark.parametrize(
    ("realm", "expected"),
    [
        ("US1", "us1"),
        ("eu0", "eu0"),
        ("private-1", "private-1"),
    ],
)
def test_canonical_signalfx_realm_accepts_one_dns_label_case_insensitively(realm, expected):
    assert canonical_signalfx_realm(realm) == expected


@pytest.mark.parametrize(
    "realm",
    [
        "",
        " us1",
        "us1 ",
        "us\t1",
        "us\n1",
        "evil.com",
        "evil/realm",
        "evil@realm",
        "evil:realm",
        "-us1",
        "us1-",
        "uſ1",
    ],
)
def test_canonical_signalfx_realm_rejects_unsafe_values_without_echoing_them(realm):
    with pytest.raises(RuntimeOwnershipError, match="SignalFx realm is invalid") as exc_info:
        canonical_signalfx_realm(realm)

    if realm:
        assert realm not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_settings_and_descriptors_share_signalfx_realm_validation(tmp_path):
    with pytest.raises(ValueError, match="SignalFx realm is invalid"):
        _settings(tmp_path, signalfx_realm="evil.com")

    bypassed_settings = _settings(tmp_path).model_copy(update={"signalfx_realm": "evil.com/@api"})
    with pytest.raises(RuntimeOwnershipError, match="SignalFx realm is invalid"):
        runtime_descriptor_from_settings(bypassed_settings, component="unsafe-settings")


def test_signalfx_realm_override_rejects_token_redirect_before_http_client_construction(
    tmp_path,
    monkeypatch,
):
    constructions: list[dict[str, Any]] = []

    def record_construction(**kwargs):
        constructions.append(kwargs)
        return object()

    monkeypatch.setattr(httpx, "AsyncClient", record_construction)

    with pytest.raises(RuntimeOwnershipError, match="SignalFx realm is invalid"):
        SignalFxClient(
            realm="evil.com/@api",
            runtime_settings=_settings(tmp_path),
        )

    assert constructions == []


@pytest.mark.parametrize(
    ("store_factory", "path_field"),
    [
        ("tacit.history.InvestigationStore", "history_db_path"),
        ("tacit.feedback.FeedbackStore", "feedback_db_path"),
        ("tacit.signals.store.SignalStore", "signals_db_path"),
    ],
)
@pytest.mark.parametrize("explicit_path", [False, True])
def test_invalid_runtime_endpoint_fails_before_store_path_creation(
    tmp_path,
    store_factory,
    path_field,
    explicit_path,
):
    from importlib import import_module

    module_name, class_name = store_factory.rsplit(".", 1)
    store_type = getattr(import_module(module_name), class_name)
    database_path = tmp_path / "not-created" / "runtime.db"
    runtime_settings = _settings(
        tmp_path,
        grafana_url="https://user:secret@grafana.example.test",
        **{path_field: str(database_path)},
    )

    with pytest.raises(RuntimeOwnershipError, match="remote endpoint credentials are not allowed"):
        store_type(database_path if explicit_path else None, runtime_settings=runtime_settings)

    assert not database_path.parent.exists()


@pytest.mark.parametrize("owner_kind", ["runtime_stores", "knowledge_service"])
def test_invalid_runtime_endpoint_fails_before_lazy_owner_storage(tmp_path, owner_kind):
    database_path = tmp_path / "not-created" / "signals.db"
    runtime_settings = _settings(
        tmp_path,
        signals_db_path=str(database_path),
        grafana_url="https://user:secret@grafana.example.test",
    )

    with pytest.raises(RuntimeOwnershipError, match="remote endpoint credentials are not allowed"):
        if owner_kind == "runtime_stores":
            RuntimeStores(runtime_settings)
        else:
            KnowledgeService(runtime_settings=runtime_settings)

    assert not database_path.parent.exists()


@pytest.mark.parametrize("client_kind", ["grafana", "pagerduty", "signalfx"])
def test_remote_clients_reject_url_userinfo_before_http_client_construction(
    tmp_path,
    monkeypatch,
    client_kind,
):
    constructions: list[dict[str, Any]] = []

    def record_construction(**kwargs):
        constructions.append(kwargs)
        return object()

    monkeypatch.setattr(httpx, "AsyncClient", record_construction)
    runtime_settings = _settings(tmp_path)

    expected_error = (
        "SignalFx realm is invalid" if client_kind == "signalfx" else "remote endpoint credentials are not allowed"
    )
    with pytest.raises(RuntimeOwnershipError, match=expected_error):
        if client_kind == "grafana":
            GrafanaClient(
                base_url="https://alice:secret@grafana.example.test",
                runtime_settings=runtime_settings,
            )
        elif client_kind == "pagerduty":
            PagerDutyClient(
                base_url="https://alice:secret@api.pagerduty.test",
                runtime_settings=runtime_settings,
            )
        else:
            SignalFxClient(
                realm="alice:secret@signalfx.example.test",
                runtime_settings=runtime_settings,
            )

    assert constructions == []


@pytest.mark.parametrize(
    ("client_kind", "endpoint"),
    [
        ("grafana", "grafana.example.test"),
        ("pagerduty", "ftp://api.pagerduty.test"),
        ("signalfx", "bad realm"),
    ],
)
def test_remote_clients_reject_malformed_endpoint_before_http_client_construction(
    tmp_path,
    monkeypatch,
    client_kind,
    endpoint,
):
    constructions: list[dict[str, Any]] = []

    def record_construction(**kwargs):
        constructions.append(kwargs)
        return object()

    monkeypatch.setattr(httpx, "AsyncClient", record_construction)
    runtime_settings = _settings(tmp_path)

    expected_error = "SignalFx realm is invalid" if client_kind == "signalfx" else "remote endpoint"
    with pytest.raises(RuntimeOwnershipError, match=expected_error):
        if client_kind == "grafana":
            GrafanaClient(base_url=endpoint, runtime_settings=runtime_settings)
        elif client_kind == "pagerduty":
            PagerDutyClient(base_url=endpoint, runtime_settings=runtime_settings)
        else:
            SignalFxClient(realm=endpoint, runtime_settings=runtime_settings)

    assert constructions == []


@pytest.mark.parametrize("client_kind", ["grafana", "pagerduty"])
@pytest.mark.parametrize("endpoint", ["", "   "])
def test_remote_clients_reject_explicit_empty_endpoint_before_http_client_construction(
    tmp_path,
    monkeypatch,
    client_kind,
    endpoint,
):
    constructions: list[dict[str, Any]] = []

    def record_construction(**kwargs):
        constructions.append(kwargs)
        return object()

    monkeypatch.setattr(httpx, "AsyncClient", record_construction)
    runtime_settings = _settings(tmp_path)

    with pytest.raises(RuntimeOwnershipError, match="remote endpoint"):
        if client_kind == "grafana":
            GrafanaClient(base_url=endpoint, runtime_settings=runtime_settings)
        else:
            PagerDutyClient(base_url=endpoint, runtime_settings=runtime_settings)

    assert constructions == []


@pytest.mark.parametrize("client_kind", ["grafana", "pagerduty"])
def test_remote_clients_use_configured_endpoint_only_when_override_is_none(
    tmp_path,
    monkeypatch,
    client_kind,
):
    constructions: list[dict[str, Any]] = []

    def record_construction(**kwargs):
        constructions.append(kwargs)
        return object()

    monkeypatch.setattr(httpx, "AsyncClient", record_construction)
    runtime_settings = _settings(tmp_path)

    client = (
        GrafanaClient(base_url=None, runtime_settings=runtime_settings)
        if client_kind == "grafana"
        else PagerDutyClient(base_url=None, runtime_settings=runtime_settings)
    )

    expected = (
        canonical_remote_endpoint(runtime_settings.grafana_url)
        if client_kind == "grafana"
        else canonical_remote_endpoint(runtime_settings.pagerduty_base_url)
    )
    assert client.base_url == expected
    assert constructions[0]["base_url"] == expected


def test_runtime_descriptor_rejects_cross_role_database_path_reuse(tmp_path):
    shared = tmp_path / "shared.db"

    with pytest.raises(RuntimeOwnershipError, match="roles must use distinct files"):
        RuntimeOwnershipDescriptor(
            component="invalid-combined-store",
            databases=(
                RuntimeDatabaseIdentity(role="history", path=shared),
                RuntimeDatabaseIdentity(role="feedback", path=shared),
            ),
        )


@pytest.mark.parametrize(
    "constructor",
    [
        lambda: RuntimeDatabaseIdentity(role=" ", path=Path("state.db")),
        lambda: RuntimeRemoteIdentity(provider=" ", endpoint="https://example.test"),
        lambda: RuntimeRemoteIdentity(
            provider="grafana",
            endpoint="https://example.test",
            credential_fingerprint="raw-secret",
        ),
        lambda: RuntimeOwnershipDescriptor(
            component=" ",
            databases=(RuntimeDatabaseIdentity(role="history", path=Path("state.db")),),
        ),
        lambda: RuntimeOwnershipDescriptor(
            component="runtime",
            settings_identity="not-a-fingerprint",
        ),
        lambda: RuntimeTenantPolicy(
            mode="wildcard",
            tenant_id="tenant-a",
            permissions=(),
            api_auth_enabled=True,
        ),
    ],
)
def test_malformed_runtime_descriptor_identity_raises_typed_error(constructor):
    with pytest.raises(RuntimeOwnershipError):
        constructor()


def test_conflicting_runtime_descriptor_identity_raises_typed_error(tmp_path):
    with pytest.raises(RuntimeOwnershipError, match="conflicting database role"):
        RuntimeOwnershipDescriptor(
            component="conflicting-databases",
            databases=(
                RuntimeDatabaseIdentity(role="signals", path=tmp_path / "first.db"),
                RuntimeDatabaseIdentity(role="signals", path=tmp_path / "second.db"),
            ),
        )

    with pytest.raises(RuntimeOwnershipError, match="conflicting remote provider"):
        RuntimeOwnershipDescriptor(
            component="conflicting-remotes",
            remotes=(
                RuntimeRemoteIdentity(provider="grafana", endpoint="https://first.example.test"),
                RuntimeRemoteIdentity(provider="grafana", endpoint="https://second.example.test"),
            ),
        )


def test_runtime_composition_rejects_cross_role_canonical_path_reuse(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shared = tmp_path / "shared.db"
    history = RuntimeOwnershipDescriptor(
        component="history-store",
        cache_namespace="shared-runtime",
        databases=(RuntimeDatabaseIdentity(role="history", path="shared.db"),),
    )
    feedback = RuntimeOwnershipDescriptor(
        component="feedback-store",
        cache_namespace="shared-runtime",
        databases=(RuntimeDatabaseIdentity(role="feedback", path=shared),),
    )

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="cross-role-collision",
            descriptors=(history, feedback),
        )

    assert "database_role_collision" in exc_info.value.dimensions
    assert not shared.exists()


def test_runtime_composition_rejects_symlink_equivalent_cross_role_paths(tmp_path):
    real_dir = tmp_path / "real"
    alias_dir = tmp_path / "alias"
    real_dir.mkdir()
    alias_dir.symlink_to(real_dir, target_is_directory=True)
    history = RuntimeOwnershipDescriptor(
        component="history-store",
        cache_namespace="shared-runtime",
        databases=(RuntimeDatabaseIdentity(role="history", path=real_dir / "shared.db"),),
    )
    signals = RuntimeOwnershipDescriptor(
        component="signals-store",
        cache_namespace="shared-runtime",
        databases=(RuntimeDatabaseIdentity(role="signals", path=alias_dir / "shared.db"),),
    )

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        require_compatible_runtime_ownership(
            boundary="symlink-cross-role-collision",
            descriptors=(history, signals),
        )

    assert "database_role_collision" in exc_info.value.dimensions
    assert not (real_dir / "shared.db").exists()


def test_runtime_composition_rejects_hard_linked_cross_role_paths(tmp_path):
    history_path = tmp_path / "history.db"
    feedback_path = tmp_path / "feedback.db"
    history_path.touch()
    feedback_path.hardlink_to(history_path)
    history = RuntimeOwnershipDescriptor(
        component="history-store",
        cache_namespace="shared-runtime",
        databases=(RuntimeDatabaseIdentity(role="history", path=history_path),),
    )

    with pytest.raises(RuntimeOwnershipError, match="distinct files"):
        RuntimeOwnershipDescriptor(
            component="cross-role-owner",
            cache_namespace="shared-runtime",
            databases=(
                history.databases[0],
                RuntimeDatabaseIdentity(role="feedback", path=feedback_path),
            ),
        )


def test_runtime_composition_allows_same_role_same_path(tmp_path):
    shared = tmp_path / "history.db"
    first = RuntimeOwnershipDescriptor(
        component="first-history",
        databases=(RuntimeDatabaseIdentity(role="history", path=shared),),
    )
    second = RuntimeOwnershipDescriptor(
        component="second-history",
        databases=(RuntimeDatabaseIdentity(role="history", path=shared),),
    )

    require_compatible_runtime_ownership(
        boundary="same-role-same-path",
        descriptors=(first, second),
    )

    assert not shared.exists()


def test_semantically_equivalent_settings_have_one_runtime_identity(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    relative = _settings(
        tmp_path,
        history_db_path="state/history.db",
        feedback_db_path="state/feedback.db",
        signals_db_path="state/signals.db",
        knowledge_tenant_id=" tenant-a ",
        knowledge_permissions=" knowledge.review, knowledge.read,knowledge.review ",
        grafana_url="HTTPS://GRAFANA.EXAMPLE.TEST:443/",
        pagerduty_base_url="HTTPS://API.PAGERDUTY.TEST:443/",
        signalfx_realm="EU0",
    )
    absolute = _settings(
        tmp_path,
        history_db_path=str(tmp_path / "state" / "history.db"),
        feedback_db_path=str(tmp_path / "state" / "feedback.db"),
        signals_db_path=str(tmp_path / "state" / "signals.db"),
        knowledge_tenant_id="tenant-a",
        knowledge_permissions="knowledge.read,knowledge.review",
        grafana_url="https://grafana.example.test",
        pagerduty_base_url="https://api.pagerduty.test",
        signalfx_realm="eu0",
    )

    relative_descriptor = runtime_descriptor_from_settings(relative, component="relative")
    absolute_descriptor = runtime_descriptor_from_settings(absolute, component="absolute")

    assert relative_descriptor.settings_identity == absolute_descriptor.settings_identity
    assert relative_descriptor.tenant_policy == absolute_descriptor.tenant_policy
    assert relative_descriptor.databases == absolute_descriptor.databases
    assert relative_descriptor.remotes == absolute_descriptor.remotes
    assert relative_descriptor.cache_namespace == absolute_descriptor.cache_namespace
    assert relative_descriptor.admission_namespace == absolute_descriptor.admission_namespace


def test_runtime_stores_freezes_settings_identity_and_relative_paths(tmp_path, monkeypatch):
    original_cwd = tmp_path / "owner"
    later_cwd = tmp_path / "later"
    original_cwd.mkdir()
    later_cwd.mkdir()
    monkeypatch.chdir(original_cwd)
    runtime_settings = _isolated_settings(
        history_db_path="state/history.db",
        feedback_db_path="state/feedback.db",
        signals_db_path="state/signals.db",
        learned_archetypes_quarantine_path="state/quarantine",
        evaluation_results_dir="state/evaluations",
        knowledge_tenant_id="tenant-a",
    )
    stores = RuntimeStores(runtime_settings)
    original_descriptor = stores.runtime_ownership

    runtime_settings.knowledge_tenant_id = "tenant-b"
    runtime_settings.history_db_path = "other/history.db"
    returned_settings = stores.runtime_settings
    returned_settings.knowledge_tenant_id = "tenant-c"
    returned_settings.history_db_path = "returned/history.db"
    stores.settings.knowledge_tenant_id = "tenant-d"
    monkeypatch.chdir(later_cwd)

    assert stores.runtime_ownership == original_descriptor
    assert stores.runtime_settings.knowledge_tenant_id == "tenant-a"
    assert stores.runtime_settings.history_db_path == str(original_cwd / "state" / "history.db")
    assert stores.runtime_settings.feedback_db_path == str(original_cwd / "state" / "feedback.db")
    assert stores.runtime_settings.signals_db_path == str(original_cwd / "state" / "signals.db")
    assert stores.runtime_settings.learned_archetypes_quarantine_path == str(original_cwd / "state" / "quarantine")
    assert stores.runtime_settings.evaluation_results_dir == str(original_cwd / "state" / "evaluations")
    assert stores.history().database_path == original_cwd / "state" / "history.db"
    assert not (later_cwd / "state" / "history.db").exists()


@pytest.mark.parametrize(
    ("store_path", "store_factory"),
    [
        ("history.db", "tacit.history.InvestigationStore"),
        ("feedback.db", "tacit.feedback.FeedbackStore"),
        ("signals.db", "tacit.signals.store.SignalStore"),
    ],
)
def test_direct_stores_freeze_settings_and_relative_database_paths(
    tmp_path,
    monkeypatch,
    store_path,
    store_factory,
):
    from importlib import import_module

    module_name, class_name = store_factory.rsplit(".", 1)
    store_type = getattr(import_module(module_name), class_name)
    original_cwd = tmp_path / "owner"
    later_cwd = tmp_path / "later"
    original_cwd.mkdir(exist_ok=True)
    later_cwd.mkdir(exist_ok=True)
    monkeypatch.chdir(original_cwd)
    runtime_settings = _isolated_settings(knowledge_tenant_id="tenant-a")
    store = store_type(Path("state") / store_path, runtime_settings=runtime_settings)
    original_descriptor = store.runtime_ownership

    runtime_settings.knowledge_tenant_id = "tenant-b"
    returned_settings = store.runtime_settings
    returned_settings.knowledge_tenant_id = "tenant-c"
    monkeypatch.chdir(later_cwd)

    assert store.runtime_ownership == original_descriptor
    assert store.runtime_settings.knowledge_tenant_id == "tenant-a"
    assert store.database_path == original_cwd / "state" / store_path


@pytest.mark.parametrize("owner_kind", ["grafana", "signalfx", "pagerduty"])
def test_remote_clients_freeze_settings_and_descriptor(tmp_path, monkeypatch, owner_kind):
    class FakeAsyncClient:
        def __init__(self, **_kwargs):
            pass

        async def aclose(self):
            pass

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    runtime_settings = _settings(tmp_path)
    owner: GrafanaClient | SignalFxClient | PagerDutyClient
    if owner_kind == "grafana":
        owner = GrafanaClient(runtime_settings=runtime_settings)
    elif owner_kind == "signalfx":
        owner = SignalFxClient(runtime_settings=runtime_settings)
    else:
        owner = PagerDutyClient(runtime_settings=runtime_settings)
    original_descriptor = owner.runtime_ownership

    runtime_settings.knowledge_tenant_id = "tenant-b"
    returned_settings = owner.runtime_settings
    returned_settings.knowledge_tenant_id = "tenant-c"

    assert owner.runtime_ownership == original_descriptor
    assert owner.runtime_settings.knowledge_tenant_id == "tenant-a"


@pytest.mark.parametrize("backend_kind", ["grafana", "signalfx"])
def test_backends_freeze_explicit_settings(tmp_path, monkeypatch, backend_kind):
    class FakeAsyncClient:
        def __init__(self, **_kwargs):
            pass

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    runtime_settings = _settings(tmp_path)
    backend: GrafanaBackend | SignalFxBackend
    if backend_kind == "grafana":
        backend = GrafanaBackend(runtime_settings=runtime_settings)
    else:
        backend = SignalFxBackend(runtime_settings=runtime_settings)
    original_descriptor = backend.runtime_ownership

    runtime_settings.knowledge_tenant_id = "tenant-b"
    runtime_settings.knowledge_permissions = "knowledge.read"

    assert backend.runtime_ownership == original_descriptor


def test_knowledge_service_freezes_settings_and_public_settings_view(tmp_path):
    runtime_settings = _settings(tmp_path)
    (tmp_path / "state").mkdir()
    service = KnowledgeService(
        KnowledgeRepository(
            tmp_path / "state" / "signals.db",
            runtime_settings=runtime_settings,
        ),
        runtime_settings=runtime_settings,
    )
    original_descriptor = service.runtime_ownership

    runtime_settings.knowledge_tenant_id = "tenant-b"
    returned_settings = service.runtime_settings
    returned_settings.knowledge_tenant_id = "tenant-c"

    assert service.runtime_ownership == original_descriptor
    assert service.runtime_settings.knowledge_tenant_id == "tenant-a"


def test_knowledge_service_rejects_repository_settings_database_split(tmp_path):
    runtime_settings = _settings(tmp_path)
    repository = KnowledgeRepository(tmp_path / "authority.db")

    with pytest.raises(RuntimeOwnershipError, match="runtime settings and explicit signals database path"):
        KnowledgeService(repository, runtime_settings=runtime_settings)


def test_knowledge_service_adopts_explicit_repository_when_settings_path_is_omitted(tmp_path):
    runtime_settings = _isolated_settings(knowledge_tenant_id="tenant-a")
    repository = KnowledgeRepository(
        tmp_path / "authority.db",
        runtime_settings=runtime_settings,
    )
    service = KnowledgeService(
        repository,
        runtime_settings=runtime_settings,
    )

    assert service.runtime_settings.signals_db_path == str(repository.database_path)
    assert service.runtime_ownership.databases == (
        RuntimeDatabaseIdentity(role="signals", path=repository.database_path),
    )


def test_knowledge_repository_anchors_explicit_relative_path_before_schema_initialization(
    tmp_path,
    monkeypatch,
):
    owner_cwd = tmp_path / "owner"
    later_cwd = tmp_path / "later"
    (owner_cwd / "state").mkdir(parents=True)
    (later_cwd / "state").mkdir(parents=True)
    monkeypatch.chdir(owner_cwd)

    repository = KnowledgeRepository(Path("state") / "signals.db")
    assert repository.database_path == owner_cwd / "state" / "signals.db"

    monkeypatch.chdir(later_cwd)

    assert repository.list_candidates() == []
    assert repository.database_path == owner_cwd / "state" / "signals.db"
    assert not (later_cwd / "state" / "signals.db").exists()


def test_knowledge_service_rejects_ownerless_repository_before_path_property_side_effects(tmp_path):
    touched_path = tmp_path / "must-not-exist" / "signals.db"

    class OwnerlessRepository:
        @property
        def database_path(self):
            touched_path.parent.mkdir(parents=True)
            return touched_path

    with pytest.raises(RuntimeOwnershipError, match="public runtime ownership descriptor"):
        KnowledgeService(
            OwnerlessRepository(),  # type: ignore[arg-type]
            runtime_settings=_isolated_settings(),
        )

    assert not touched_path.parent.exists()


def test_knowledge_service_composes_repository_settings_and_signal_store_from_descriptors(tmp_path):
    database_path = tmp_path / "authority.db"
    runtime_settings = _isolated_settings(
        signals_db_path=str(database_path),
        knowledge_tenant_id="tenant-a",
    )

    class DescriptorRepository:
        def __init__(self):
            self.runtime_ownership = RuntimeOwnershipDescriptor(
                component="descriptor-repository",
                databases=(RuntimeDatabaseIdentity(role="signals", path=database_path),),
            )

        @property
        def database_path(self):
            raise AssertionError("repository database_path was read instead of its descriptor")

    class DescriptorSignalStore:
        def __init__(self):
            self.runtime_ownership = runtime_descriptor_for_store(
                component="descriptor-signal-store",
                runtime_settings=runtime_settings,
                database_role="signals",
                database_path=database_path,
            )
            self.runtime_settings = runtime_settings

        @property
        def database_path(self):
            raise AssertionError("signal store database_path was read instead of its descriptor")

    service = KnowledgeService(
        DescriptorRepository(),  # type: ignore[arg-type]
        signal_store=DescriptorSignalStore(),
        runtime_settings=runtime_settings,
    )

    assert service.database_path == database_path
    assert service.runtime_ownership.databases == (RuntimeDatabaseIdentity(role="signals", path=database_path),)


def test_knowledge_service_rejects_descriptor_graph_before_injected_property_side_effects(tmp_path):
    repository_path = tmp_path / "repository" / "signals.db"
    signal_path = tmp_path / "signal" / "signals.db"
    runtime_settings = _isolated_settings(
        signals_db_path=str(signal_path),
        knowledge_tenant_id="tenant-a",
    )

    class DescriptorRepository:
        def __init__(self):
            self.runtime_ownership = RuntimeOwnershipDescriptor(
                component="descriptor-repository",
                databases=(RuntimeDatabaseIdentity(role="signals", path=repository_path),),
            )

        @property
        def database_path(self):
            repository_path.parent.mkdir(parents=True)
            return repository_path

    class DescriptorSignalStore:
        def __init__(self):
            self.runtime_ownership = runtime_descriptor_for_store(
                component="descriptor-signal-store",
                runtime_settings=runtime_settings,
                database_role="signals",
                database_path=signal_path,
            )

        @property
        def runtime_settings(self):
            signal_path.parent.mkdir(parents=True)
            return runtime_settings

        @property
        def database_path(self):
            signal_path.parent.mkdir(parents=True)
            return signal_path

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        KnowledgeService(
            DescriptorRepository(),  # type: ignore[arg-type]
            signal_store=DescriptorSignalStore(),
            runtime_settings=runtime_settings,
        )

    assert "database" in exc_info.value.dimensions
    assert exc_info.value.components == (
        "knowledge_service_settings",
        "descriptor-repository",
        "descriptor-signal-store",
    )
    assert not repository_path.parent.exists()
    assert not signal_path.parent.exists()


@pytest.mark.parametrize(
    ("store_factory", "database_role", "conflicting_field"),
    [
        ("tacit.history.InvestigationStore", "history", "feedback_db_path"),
        ("tacit.history.InvestigationStore", "history", "signals_db_path"),
        ("tacit.feedback.FeedbackStore", "feedback", "history_db_path"),
        ("tacit.feedback.FeedbackStore", "feedback", "signals_db_path"),
        ("tacit.signals.store.SignalStore", "signals", "history_db_path"),
        ("tacit.signals.store.SignalStore", "signals", "feedback_db_path"),
    ],
)
def test_direct_store_explicit_path_adoption_revalidates_complete_database_role_map(
    tmp_path,
    monkeypatch,
    store_factory,
    database_role,
    conflicting_field,
):
    from importlib import import_module

    module_name, class_name = store_factory.rsplit(".", 1)
    store_type = getattr(import_module(module_name), class_name)
    database_path = tmp_path / "must-not-exist" / "shared.db"
    monkeypatch.chdir(tmp_path)
    runtime_settings = _isolated_settings(**{conflicting_field: str(database_path)})

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        store_type(
            Path("must-not-exist") / "shared.db",
            runtime_settings=runtime_settings,
        )

    assert not database_path.parent.exists()


def test_snapshot_runtime_settings_revalidates_adopted_database_path_without_side_effects(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "must-not-exist" / "shared.db"
    monkeypatch.chdir(tmp_path)
    runtime_settings = _isolated_settings(
        history_db_path=str(database_path),
    )

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        snapshot_runtime_settings(
            runtime_settings,
            database_role="signals",
            database_path=Path("must-not-exist") / "shared.db",
        )

    assert not database_path.parent.exists()


def test_legacy_owner_path_adoption_revalidates_complete_database_role_map(tmp_path):
    database_path = tmp_path / "must-not-exist" / "shared.db"
    runtime_settings = _isolated_settings(history_db_path=str(database_path))
    owner = RuntimeOwner(
        name="signals-owner",
        supplied=True,
        descriptor=RuntimeOwnershipDescriptor(
            component="signals-owner",
            databases=(RuntimeDatabaseIdentity(role="signals", path=database_path),),
        ),
    )

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        resolve_runtime_settings(
            boundary="test-owner-adoption",
            explicit_settings=runtime_settings,
            owners=(owner,),
            fallback_settings=runtime_settings,
        )

    assert not database_path.parent.exists()
