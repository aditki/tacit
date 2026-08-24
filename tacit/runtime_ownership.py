"""Typed, side-effect-free runtime ownership identities.

The descriptor in this module is the public composition contract for Tacit
components. It contains only canonical paths, non-secret endpoint/account
identities, and one-way fingerprints. Constructing or comparing descriptors
must never initialize a store or contact a remote service.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

import structlog

import tacit.config as config_module
from tacit.config import (
    Settings,
    canonical_knowledge_permissions,
    canonical_knowledge_tenant_id,
    canonical_sqlite_role_paths,
    validate_distinct_sqlite_role_paths,
    validated_knowledge_tenant_api_keys,
)
from tacit.config import (
    canonical_signalfx_realm as _canonical_signalfx_realm,
)
from tacit.errors import RuntimeOwnershipError

logger = structlog.get_logger()

_FINGERPRINT_PREFIX = "sha256:"
_SECRET_FIELD_MARKERS = (
    "api_key",
    "api_token",
    "access_key",
    "password",
    "secret",
    "signing_key",
    "tenant_api_keys",
    "token",
)
_REASON_CODE_RE = re.compile(r"[^a-z0-9_.:-]+")
_DATABASE_SETTING_DEFAULTS = {
    "history_db_path": config_module.DEFAULT_HISTORY_DB_PATH,
    "feedback_db_path": config_module.DEFAULT_FEEDBACK_DB_PATH,
    "signals_db_path": config_module.DEFAULT_SIGNALS_DB_PATH,
}
_DATABASE_ROLE_SETTING_FIELDS = {
    "history": "history_db_path",
    "feedback": "feedback_db_path",
    "signals": "signals_db_path",
}
_PATH_SETTING_FIELDS = (
    "evaluation_results_dir",
    "learned_archetypes_quarantine_path",
)
_ENDPOINT_SETTING_FIELDS = (
    "context_a2a_agent_url",
    "context_mcp_server_url",
    "context_rag_api_url",
    "grafana_public_url",
    "grafana_url",
    "llm_api_base",
    "pagerduty_base_url",
)
_CASEFOLD_SETTING_FIELDS = (
    "context_provider",
    "llm_bedrock_region",
    "llm_provider",
    "log_level",
    "signalfx_realm",
)
_STRIPPED_SETTING_FIELDS = ("learned_archetypes_tenant_id",)


class RuntimeOwnershipMismatchError(RuntimeOwnershipError):
    """Raised when supplied runtime owners identify different compositions."""

    def __init__(
        self,
        boundary: str,
        dimensions: set[str],
        components: tuple[str, ...],
        *,
        message: str | None = None,
    ) -> None:
        self.boundary = boundary
        self.dimensions = frozenset(dimensions)
        self.components = components
        joined = ", ".join(sorted(self.dimensions))
        super().__init__(message or f"{boundary} runtime ownership mismatch: {joined}")


class RuntimeAvailability(StrEnum):
    """Whether an owner was supplied and can be consumed."""

    ABSENT = "absent"
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class RuntimeTenantPolicy:
    """Tenant and semantic-permission identity without credential disclosure."""

    mode: str
    tenant_id: str
    permissions: tuple[str, ...]
    api_auth_enabled: bool
    tenant_credential_fingerprints: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.mode, str):
            raise RuntimeOwnershipError("runtime tenant policy mode is invalid")
        if not isinstance(self.tenant_id, str):
            raise RuntimeOwnershipError("runtime tenant identity is invalid")
        if not isinstance(self.permissions, tuple) or any(
            not isinstance(permission, str) for permission in self.permissions
        ):
            raise RuntimeOwnershipError("runtime permission identity is invalid")
        if not isinstance(self.tenant_credential_fingerprints, tuple) or any(
            not isinstance(item, tuple)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], str)
            for item in self.tenant_credential_fingerprints
        ):
            raise RuntimeOwnershipError("runtime tenant credential identity is invalid")
        try:
            tenant_id = canonical_knowledge_tenant_id(self.tenant_id)
            permission_identity = canonical_knowledge_permissions(",".join(self.permissions))
            credential_names = validated_knowledge_tenant_api_keys(
                {tenant: fingerprint for tenant, fingerprint in self.tenant_credential_fingerprints}
            )
        except ValueError as exc:
            raise RuntimeOwnershipError(str(exc)) from exc
        if self.mode not in {"pinned", "wildcard"}:
            raise RuntimeOwnershipError("runtime tenant policy mode is invalid")
        expected_mode = "wildcard" if tenant_id == "*" else "pinned"
        if self.mode != expected_mode:
            raise RuntimeOwnershipError("runtime tenant policy mode conflicts with tenant identity")
        if not isinstance(self.api_auth_enabled, bool):
            raise RuntimeOwnershipError("runtime tenant authentication policy is invalid")
        if len(credential_names) != len(self.tenant_credential_fingerprints):
            raise RuntimeOwnershipError("runtime tenant credential identity is duplicated")
        credentials = tuple(sorted(self.tenant_credential_fingerprints))
        if any(not _is_fingerprint(fingerprint) and fingerprint != "none" for _tenant, fingerprint in credentials):
            raise RuntimeOwnershipError("runtime tenant credential identity must be a non-secret fingerprint")
        if expected_mode == "wildcard" and not self.api_auth_enabled:
            raise RuntimeOwnershipError("Wildcard knowledge tenancy requires API authentication")
        non_empty_fingerprints = [fingerprint for _tenant, fingerprint in credentials if fingerprint != "none"]
        if expected_mode == "wildcard" and len(non_empty_fingerprints) != len(set(non_empty_fingerprints)):
            raise RuntimeOwnershipError("knowledge_tenant_api_keys must use a unique non-empty key per tenant")
        permissions = tuple(permission_identity.split(",")) if permission_identity else ()
        object.__setattr__(self, "tenant_id", tenant_id)
        object.__setattr__(self, "permissions", permissions)
        object.__setattr__(self, "tenant_credential_fingerprints", credentials)


@dataclass(frozen=True, slots=True)
class RuntimeDatabaseIdentity:
    """One role-scoped SQLite identity.

    Different roles are intentionally allowed to use different files. Two
    owners conflict only when they claim the same role with different paths.
    """

    role: str
    path: Path

    def __post_init__(self) -> None:
        if not isinstance(self.role, str):
            raise RuntimeOwnershipError("runtime database role is invalid")
        object.__setattr__(self, "role", self.role.strip().casefold())
        object.__setattr__(self, "path", _canonical_path(self.path))
        if not self.role:
            raise RuntimeOwnershipError("runtime database role is required")


@dataclass(frozen=True, slots=True)
class RuntimeRemoteIdentity:
    """Effective remote identity after client overrides have been applied."""

    provider: str
    endpoint: str
    account: str = ""
    credential_fingerprint: str = "none"

    def __post_init__(self) -> None:
        if not isinstance(self.provider, str):
            raise RuntimeOwnershipError("runtime remote provider is invalid")
        if not isinstance(self.credential_fingerprint, str):
            raise RuntimeOwnershipError("runtime credential identity is invalid")
        object.__setattr__(self, "provider", self.provider.strip().casefold())
        object.__setattr__(self, "endpoint", canonical_remote_endpoint(self.endpoint))
        object.__setattr__(self, "account", str(self.account).strip().casefold())
        if not self.provider:
            raise RuntimeOwnershipError("runtime remote provider is required")
        fingerprint = self.credential_fingerprint
        if fingerprint != "none" and not (
            fingerprint.startswith(_FINGERPRINT_PREFIX)
            and len(fingerprint) == len(_FINGERPRINT_PREFIX) + 64
            and all(character in "0123456789abcdef" for character in fingerprint[len(_FINGERPRINT_PREFIX) :])
        ):
            raise RuntimeOwnershipError("runtime credential identity must be a non-secret fingerprint")


@dataclass(frozen=True, slots=True)
class RuntimeOwnershipDescriptor:
    """Immutable public identity for one composed runtime component."""

    component: str
    availability: RuntimeAvailability = RuntimeAvailability.AVAILABLE
    settings_identity: str | None = None
    tenant_policy: RuntimeTenantPolicy | None = None
    databases: tuple[RuntimeDatabaseIdentity, ...] = ()
    remotes: tuple[RuntimeRemoteIdentity, ...] = ()
    cache_namespace: str | None = None
    admission_namespace: str | None = None
    availability_reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.component, str):
            raise RuntimeOwnershipError("runtime ownership component is invalid")
        if not isinstance(self.availability, RuntimeAvailability):
            raise RuntimeOwnershipError("runtime availability identity is invalid")
        if self.tenant_policy is not None and not isinstance(self.tenant_policy, RuntimeTenantPolicy):
            raise RuntimeOwnershipError("runtime tenant policy identity is invalid")
        if not isinstance(self.databases, tuple) or any(
            not isinstance(database, RuntimeDatabaseIdentity) for database in self.databases
        ):
            raise RuntimeOwnershipError("runtime database identity is invalid")
        if not isinstance(self.remotes, tuple) or any(
            not isinstance(remote, RuntimeRemoteIdentity) for remote in self.remotes
        ):
            raise RuntimeOwnershipError("runtime remote identity is invalid")
        if not isinstance(self.availability_reason, str):
            raise RuntimeOwnershipError("runtime availability reason is invalid")
        component = self.component.strip()
        if not component:
            raise RuntimeOwnershipError("runtime ownership component is required")
        object.__setattr__(self, "component", component)

        exposes_identity = (
            self.settings_identity is not None
            or self.tenant_policy is not None
            or bool(self.databases)
            or bool(self.remotes)
            or self.cache_namespace is not None
            or self.admission_namespace is not None
        )
        reason = _reason_code(self.availability_reason)
        if self.availability is not RuntimeAvailability.AVAILABLE:
            if exposes_identity:
                raise RuntimeOwnershipError("non-available runtime owners must not expose runtime identity")
            if self.availability is RuntimeAvailability.ABSENT:
                if reason:
                    raise RuntimeOwnershipError("absent runtime owners must not include a reason")
            elif not reason:
                raise RuntimeOwnershipError("unavailable runtime owners require a reason")
            object.__setattr__(self, "availability_reason", reason)
            return

        object.__setattr__(self, "databases", _unique_databases(self.databases))
        object.__setattr__(self, "remotes", _unique_remotes(self.remotes))
        object.__setattr__(self, "availability_reason", reason)

        if self.settings_identity is not None and (
            not isinstance(self.settings_identity, str) or not _is_fingerprint(self.settings_identity)
        ):
            raise RuntimeOwnershipError("settings identity must be a non-secret fingerprint")
        for namespace_name, namespace in (
            ("cache", self.cache_namespace),
            ("admission", self.admission_namespace),
        ):
            if namespace is not None and (not isinstance(namespace, str) or not namespace):
                raise RuntimeOwnershipError(f"runtime {namespace_name} namespace is invalid")
        if self.availability is RuntimeAvailability.AVAILABLE and not any(
            (
                self.settings_identity,
                self.tenant_policy,
                self.databases,
                self.remotes,
                self.cache_namespace,
                self.admission_namespace,
            )
        ):
            raise RuntimeOwnershipError("available runtime owners must expose at least one identity dimension")

    @classmethod
    def absent(cls, *, component: str) -> RuntimeOwnershipDescriptor:
        """Represent an owner that was not supplied."""
        return cls(component=component, availability=RuntimeAvailability.ABSENT)

    @classmethod
    def unavailable(cls, *, component: str, reason: str) -> RuntimeOwnershipDescriptor:
        """Represent an explicitly supplied owner that cannot be consumed."""
        return cls(
            component=component,
            availability=RuntimeAvailability.UNAVAILABLE,
            availability_reason=reason,
        )


@runtime_checkable
class RuntimeOwnershipProvider(Protocol):
    """Public protocol implemented by Tacit-owned runtime components."""

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return the component's immutable ownership descriptor."""
        ...


def credential_fingerprint(secret: str | bytes | None) -> str:
    """Return a stable one-way credential identity without exposing the secret."""
    if secret is None or secret == "" or secret == b"":
        return "none"
    if not isinstance(secret, (str, bytes)):
        raise RuntimeOwnershipError("runtime credential value is invalid")
    raw = secret if isinstance(secret, bytes) else secret.encode("utf-8")
    digest = hashlib.sha256(b"tacit-runtime-credential\0" + raw).hexdigest()
    return f"{_FINGERPRINT_PREFIX}{digest}"


def canonical_remote_endpoint(value: str) -> str:
    """Canonicalize a credential-free remote base endpoint."""
    raw = str(value or "")
    if (
        not raw
        or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in raw)
        or "\\" in raw
    ):
        raise RuntimeOwnershipError("remote endpoint is invalid")
    try:
        parsed = urlsplit(raw)
        username = parsed.username
        password = parsed.password
        hostname = parsed.hostname
        parsed_port = parsed.port
    except ValueError:
        raise RuntimeOwnershipError("remote endpoint is invalid") from None
    if username is not None or password is not None or "@" in parsed.netloc:
        raise RuntimeOwnershipError("remote endpoint credentials are not allowed")
    if parsed.netloc.endswith(":"):
        raise RuntimeOwnershipError("remote endpoint is invalid")
    if "?" in raw or "#" in raw:
        raise RuntimeOwnershipError("remote endpoint must not include a query or fragment")
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or not parsed.netloc or hostname is None:
        raise RuntimeOwnershipError("remote endpoint is invalid")
    canonical_host = _canonical_remote_host(hostname)
    if parsed_port == 0:
        raise RuntimeOwnershipError("remote endpoint is invalid")
    if ":" in canonical_host:
        canonical_host = f"[{canonical_host}]"
    default_port = (scheme, parsed_port) in {("http", 80), ("https", 443)}
    port = f":{parsed_port}" if parsed_port is not None and not default_port else ""
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, f"{canonical_host}{port}", path, "", ""))


def canonical_signalfx_realm(value: str) -> str:
    """Return one canonical SignalFx realm as a typed runtime identity."""
    try:
        return _canonical_signalfx_realm(value)
    except ValueError:
        raise RuntimeOwnershipError("SignalFx realm is invalid") from None


def _canonical_remote_host(hostname: str) -> str:
    """Return a validated ASCII DNS or IP host without disclosing failures."""
    try:
        if ":" in hostname:
            return str(ipaddress.IPv6Address(hostname))
        if re.fullmatch(r"[0-9.]+", hostname):
            return str(ipaddress.IPv4Address(hostname))
        ascii_host = hostname.encode("idna").decode("ascii").casefold()
    except (UnicodeError, ValueError):
        raise RuntimeOwnershipError("remote endpoint host is invalid") from None

    labels = ascii_host[:-1].split(".") if ascii_host.endswith(".") else ascii_host.split(".")
    if (
        not labels
        or len(ascii_host.rstrip(".")) > 253
        or any(
            not label or len(label) > 63 or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label) is None
            for label in labels
        )
    ):
        raise RuntimeOwnershipError("remote endpoint host is invalid")
    return ascii_host


def snapshot_runtime_settings(
    runtime_settings: Settings,
    *,
    database_role: str | None = None,
    database_path: str | Path | None = None,
) -> Settings:
    """Capture settings for one owner without retaining caller-owned state.

    Configured persistence paths are anchored because their meaning otherwise
    changes with the process CWD. Endpoint and policy equivalence belongs to the
    immutable descriptor; retaining those configured values avoids rewriting
    caller-visible settings behavior.
    """
    if not isinstance(runtime_settings, Settings):
        raise RuntimeOwnershipError("runtime settings owner is invalid")
    _tenant_policy(runtime_settings)
    if (database_role is None) != (database_path is None):
        raise RuntimeOwnershipError("database role and path must be supplied together")
    updates: dict[str, Any] = {}
    for field_name in _DATABASE_SETTING_DEFAULTS:
        configured = str(getattr(runtime_settings, field_name) or "").strip()
        updates[field_name] = str(_canonical_path(configured)) if configured else ""
    for field_name in _PATH_SETTING_FIELDS:
        configured = str(getattr(runtime_settings, field_name) or "").strip()
        if configured:
            updates[field_name] = str(_canonical_path(configured))
    if database_role is not None and database_path is not None:
        if not isinstance(database_role, str):
            raise RuntimeOwnershipError("runtime database role is invalid")
        normalized_role = database_role.strip().casefold()
        role_field = _DATABASE_ROLE_SETTING_FIELDS.get(normalized_role)
        if role_field is None:
            raise RuntimeOwnershipError(f"unsupported runtime database role: {normalized_role}")
        actual_path = _canonical_path(database_path)
        configured = str(getattr(runtime_settings, role_field) or "").strip()
        if configured and _canonical_path(configured) != actual_path:
            raise RuntimeOwnershipError(f"runtime settings and explicit {normalized_role} database path must match")
        updates[role_field] = str(actual_path)
    effective_role_paths = {
        role: updates.get(field_name, getattr(runtime_settings, field_name))
        for role, field_name in _DATABASE_ROLE_SETTING_FIELDS.items()
    }
    try:
        canonical_sqlite_role_paths(effective_role_paths)
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    return runtime_settings.model_copy(deep=True, update=updates)


def copy_runtime_settings(runtime_settings: Settings) -> Settings:
    """Return a detached public copy of an owner's settings snapshot."""
    if not isinstance(runtime_settings, Settings):
        raise RuntimeOwnershipError("runtime settings owner is invalid")
    return runtime_settings.model_copy(deep=True)


def runtime_descriptor_from_settings(
    runtime_settings: Settings,
    *,
    component: str,
) -> RuntimeOwnershipDescriptor:
    """Describe a complete settings owner without initializing its resources."""
    if not isinstance(runtime_settings, Settings):
        raise RuntimeOwnershipError("runtime settings owner is invalid")
    tenant_policy = _tenant_policy(runtime_settings)
    settings_identity = _settings_identity(runtime_settings)
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=settings_identity,
        tenant_policy=tenant_policy,
        databases=(
            RuntimeDatabaseIdentity(
                role="history",
                path=Path(runtime_settings.history_db_path or config_module.DEFAULT_HISTORY_DB_PATH),
            ),
            RuntimeDatabaseIdentity(
                role="feedback",
                path=Path(runtime_settings.feedback_db_path or config_module.DEFAULT_FEEDBACK_DB_PATH),
            ),
            RuntimeDatabaseIdentity(
                role="signals",
                path=Path(runtime_settings.signals_db_path or config_module.DEFAULT_SIGNALS_DB_PATH),
            ),
        ),
        remotes=_settings_remotes(runtime_settings),
        cache_namespace=f"runtime-cache:{settings_identity}",
        admission_namespace=f"runtime-admission:{settings_identity}",
    )


def runtime_descriptor_for_store(
    *,
    component: str,
    runtime_settings: Settings,
    database_role: str,
    database_path: str | Path,
) -> RuntimeOwnershipDescriptor:
    """Describe one settings-backed persistence owner."""
    base = runtime_descriptor_from_settings(runtime_settings, component=component)
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity,
        tenant_policy=base.tenant_policy,
        databases=(RuntimeDatabaseIdentity(role=database_role, path=Path(database_path)),),
        cache_namespace=base.cache_namespace,
        admission_namespace=base.admission_namespace,
    )


def runtime_descriptor_for_remote(
    *,
    component: str,
    runtime_settings: Settings,
    remote: RuntimeRemoteIdentity,
) -> RuntimeOwnershipDescriptor:
    """Describe a client using its effective, post-override remote identity."""
    base = runtime_descriptor_from_settings(runtime_settings, component=component)
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity,
        tenant_policy=base.tenant_policy,
        remotes=(remote,),
        cache_namespace=base.cache_namespace,
        admission_namespace=base.admission_namespace,
    )


def get_runtime_ownership(owner: object | None, *, component: str | None = None) -> RuntimeOwnershipDescriptor:
    """Read the public descriptor from a Tacit owner.

    This strict API never probes private fields. Third-party objects must be
    adapted explicitly with :func:`adapt_third_party_runtime_owner`.
    """
    if owner is None:
        return RuntimeOwnershipDescriptor.absent(component=component or "runtime-owner")
    if isinstance(owner, RuntimeOwnershipDescriptor):
        return owner
    descriptor = getattr(owner, "runtime_ownership", None)
    if not isinstance(descriptor, RuntimeOwnershipDescriptor):
        owner_name = component or type(owner).__name__
        raise RuntimeOwnershipError(f"{owner_name} must expose a public runtime ownership descriptor")
    return descriptor


def adapt_third_party_runtime_owner(
    *,
    component: str,
    owner: object,
    runtime_settings: Settings | None = None,
    database_role: str | None = None,
    database_path: str | Path | None = None,
    remote: RuntimeRemoteIdentity | None = None,
    availability: RuntimeAvailability = RuntimeAvailability.AVAILABLE,
) -> RuntimeOwnershipDescriptor:
    """Explicit compatibility adapter for non-Tacit components.

    The adapter accepts only public values supplied by the caller. It never
    inspects private attributes, initializes persistence, or contacts a remote.
    """
    del owner
    if availability is RuntimeAvailability.ABSENT:
        return RuntimeOwnershipDescriptor.absent(component=component)
    if availability is RuntimeAvailability.UNAVAILABLE:
        return RuntimeOwnershipDescriptor.unavailable(component=component, reason="third_party_unavailable")

    if database_role is None and database_path is not None:
        raise RuntimeOwnershipError("third-party database paths require an explicit role")
    if database_role is not None and database_path is None:
        raise RuntimeOwnershipError("third-party database roles require an explicit path")

    base = (
        runtime_descriptor_from_settings(runtime_settings, component=component)
        if runtime_settings is not None
        else None
    )
    databases = (
        (RuntimeDatabaseIdentity(role=database_role, path=Path(database_path)),)
        if database_role is not None and database_path is not None
        else ()
    )
    remotes = (remote,) if remote is not None else ()
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity if base is not None else None,
        tenant_policy=base.tenant_policy if base is not None else None,
        databases=databases,
        remotes=remotes,
        cache_namespace=base.cache_namespace if base is not None else None,
        admission_namespace=base.admission_namespace if base is not None else None,
    )


def require_compatible_runtime_ownership(
    *,
    boundary: str,
    descriptors: tuple[RuntimeOwnershipDescriptor, ...],
) -> RuntimeOwnershipDescriptor:
    """Require every supplied descriptor to identify one compatible runtime."""
    absent = [item.component for item in descriptors if item.availability is RuntimeAvailability.ABSENT]
    if absent:
        logger.warning("runtime_owner_absent", boundary=boundary, components=absent)
        raise RuntimeOwnershipError(f"{boundary} runtime owner was not supplied")
    unavailable = [item.component for item in descriptors if item.availability is RuntimeAvailability.UNAVAILABLE]
    if unavailable:
        logger.warning("runtime_owner_unavailable", boundary=boundary, components=unavailable)
        raise RuntimeOwnershipError(f"{boundary} runtime owner is explicitly unavailable")

    available = tuple(item for item in descriptors if item.availability is RuntimeAvailability.AVAILABLE)
    if not available:
        raise RuntimeOwnershipError(f"{boundary} requires an available runtime owner")

    dimensions: set[str] = set()
    for index, expected in enumerate(available):
        for actual in available[index + 1 :]:
            dimensions.update(_mismatch_dimensions(expected, actual))
    claimed_database_paths: dict[str, Path] = {}
    for descriptor in available:
        for database in descriptor.databases:
            if database.role in _DATABASE_ROLE_SETTING_FIELDS:
                claimed_database_paths.setdefault(database.role, database.path)
    try:
        validate_distinct_sqlite_role_paths(claimed_database_paths)
    except ValueError:
        dimensions.add("database_role_collision")
    if not _ownership_graph_is_connected(available):
        dimensions.add("ownership_graph")
    if dimensions:
        components = tuple(item.component for item in available)
        logger.warning(
            "runtime_owner_mismatch",
            boundary=boundary,
            components=components,
            dimensions=sorted(dimensions),
        )
        raise RuntimeOwnershipMismatchError(boundary, dimensions, components)
    return available[0]


def require_remote_runtime_ownership(
    *,
    boundary: str,
    descriptor: RuntimeOwnershipDescriptor,
    provider: str,
    settings_descriptor: RuntimeOwnershipDescriptor | None = None,
) -> RuntimeOwnershipDescriptor:
    """Require one available owner with the expected remote identity."""
    descriptors = (descriptor,) if settings_descriptor is None else (settings_descriptor, descriptor)
    require_compatible_runtime_ownership(boundary=boundary, descriptors=descriptors)
    expected_provider = str(provider or "").strip().casefold()
    if not expected_provider or len(descriptor.remotes) != 1 or descriptor.remotes[0].provider != expected_provider:
        components = tuple(item.component for item in descriptors)
        logger.warning(
            "runtime_remote_owner_missing",
            boundary=boundary,
            components=components,
            provider=expected_provider,
        )
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"remote"},
            components,
            message=f"{boundary} runtime owner must expose the expected remote identity as the sole provider",
        )
    return descriptor


def resolve_remote_runtime_settings(
    *,
    boundary: str,
    owner: object,
    provider: str,
    explicit_settings: Settings | None = None,
) -> Settings:
    """Resolve one executable remote owner without process-global fallback."""
    descriptor = get_runtime_ownership(owner, component=f"{provider}_remote_owner")
    require_remote_runtime_ownership(
        boundary=boundary,
        descriptor=descriptor,
        provider=provider,
    )
    owner_settings = getattr(owner, "runtime_settings", None)
    if not isinstance(owner_settings, Settings):
        raise RuntimeOwnershipError(f"{boundary} requires a public runtime settings owner")
    selected = snapshot_runtime_settings(owner_settings)
    if explicit_settings is not None:
        explicit = snapshot_runtime_settings(explicit_settings)
        require_remote_runtime_ownership(
            boundary=boundary,
            descriptor=descriptor,
            provider=provider,
            settings_descriptor=runtime_descriptor_from_settings(
                explicit,
                component=f"{boundary}_settings",
            ),
        )
        selected = explicit
    return selected


# Compatibility facade used by pre-S2 composition call sites. New code should
# consume RuntimeOwnershipDescriptor directly through get_runtime_ownership().
@dataclass(frozen=True)
class RuntimeOwner:
    """Legacy settings carrier around the public typed descriptor."""

    name: str
    supplied: bool
    settings: Settings | None = field(default=None, repr=False)
    database_path: Path | None = None
    descriptor: RuntimeOwnershipDescriptor | None = None


def describe_runtime_owner(name: str, owner: Any | None) -> RuntimeOwner:
    """Describe an owner for legacy call sites without private-field probing."""
    if owner is None:
        return RuntimeOwner(
            name=name,
            supplied=False,
            descriptor=RuntimeOwnershipDescriptor.absent(component=name),
        )

    settings_owner = _public_settings(owner)
    database_path = _public_database_path(owner)
    try:
        descriptor = get_runtime_ownership(owner, component=name)
    except RuntimeOwnershipError:
        if settings_owner is None and database_path is None:
            raise
        descriptor = adapt_third_party_runtime_owner(
            component=name,
            owner=owner,
            runtime_settings=settings_owner,
            database_role="persistence" if database_path is not None else None,
            database_path=database_path,
        )
    return RuntimeOwner(
        name=name,
        supplied=True,
        settings=settings_owner,
        database_path=database_path,
        descriptor=descriptor,
    )


def resolve_runtime_settings(
    *,
    boundary: str,
    explicit_settings: Settings | None,
    owners: tuple[RuntimeOwner, ...] = (),
    fallback_settings: Settings,
) -> Settings:
    """Resolve one settings owner for legacy composition call sites."""
    configured: list[tuple[str, Settings]] = []
    if explicit_settings is not None:
        configured.append(("explicit", explicit_settings))
    for owner in owners:
        if owner.settings is not None:
            configured.append((owner.name, owner.settings))

    supplied_owners = [owner.name for owner in owners if owner.supplied]
    if supplied_owners and not configured:
        logger.warning("runtime_settings_owner_missing", boundary=boundary, owners=supplied_owners)
        raise RuntimeOwnershipError(f"{boundary} injected owners require explicit runtime settings or a settings owner")

    if configured:
        configured = [
            (name, _settings_with_owner_databases(value, owners, boundary=boundary)) for name, value in configured
        ]
    active_settings = (
        configured[0][1] if configured else _settings_with_owner_databases(fallback_settings, owners, boundary=boundary)
    )
    expected_identity = _settings_identity(active_settings)
    mismatched = [name for name, value in configured[1:] if _settings_identity(value) != expected_identity]
    if mismatched:
        logger.warning(
            "runtime_settings_owner_mismatch",
            boundary=boundary,
            owners=[name for name, _value in configured],
            mismatched_owners=mismatched,
        )
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"settings"},
            tuple(name for name, _value in configured),
            message=f"{boundary} runtime settings must match across all supplied owners",
        )
    return active_settings


def _settings_with_owner_databases(
    runtime_settings: Settings,
    owners: tuple[RuntimeOwner, ...],
    *,
    boundary: str,
) -> Settings:
    """Adopt supplied database identities when settings leave their paths unset."""
    active = snapshot_runtime_settings(runtime_settings)
    databases: dict[str, Path] = {}
    components: list[str] = []
    for owner in owners:
        descriptor = owner.descriptor
        if not owner.supplied or descriptor is None:
            continue
        components.append(owner.name)
        for database in descriptor.databases:
            existing = databases.get(database.role)
            if existing is not None and existing != database.path:
                raise RuntimeOwnershipMismatchError(
                    boundary,
                    {"database"},
                    tuple(components),
                    message=f"{boundary} persistence owners must use the same database",
                )
            databases[database.role] = database.path

    updates: dict[str, str] = {}
    for role, database_path in databases.items():
        field_name = _DATABASE_ROLE_SETTING_FIELDS.get(role)
        if field_name is None:
            continue
        configured = str(getattr(active, field_name) or "").strip()
        if configured and _canonical_path(configured) != database_path:
            raise RuntimeOwnershipMismatchError(
                boundary,
                {"database"},
                tuple(["settings", *components]),
                message=f"{boundary} settings and persistence owners must use the same database",
            )
        updates[field_name] = str(database_path)
    if not updates:
        return active
    return snapshot_runtime_settings(active.model_copy(deep=True, update=updates))


def require_same_database(*, boundary: str, owners: tuple[RuntimeOwner, ...]) -> None:
    """Require supplied legacy persistence owners to identify one database."""
    supplied = [owner for owner in owners if owner.supplied]
    if len(supplied) < 2:
        return
    missing = [owner.name for owner in supplied if owner.database_path is None]
    if missing:
        logger.warning(
            "runtime_database_owner_missing",
            boundary=boundary,
            owners=[owner.name for owner in supplied],
            missing_database_owners=missing,
        )
        raise RuntimeOwnershipError(f"{boundary} persistence owners must expose their database paths")
    expected = supplied[0].database_path
    mismatched = [owner.name for owner in supplied[1:] if owner.database_path != expected]
    if mismatched:
        logger.warning(
            "runtime_database_owner_mismatch",
            boundary=boundary,
            owners=[owner.name for owner in supplied],
            mismatched_owners=mismatched,
        )
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"database"},
            tuple(owner.name for owner in supplied),
            message=f"{boundary} persistence owners must use the same database",
        )


def _settings_identity(runtime_settings: Settings) -> str:
    payload = runtime_settings.model_dump(mode="json")
    for field_name, default_path in _DATABASE_SETTING_DEFAULTS.items():
        configured = str(payload.get(field_name) or "").strip()
        payload[field_name] = str(_canonical_path(configured or default_path))
    for field_name in _PATH_SETTING_FIELDS:
        configured = str(payload.get(field_name) or "").strip()
        if configured:
            payload[field_name] = str(_canonical_path(configured))
    for field_name in _ENDPOINT_SETTING_FIELDS:
        configured = str(payload.get(field_name) or "").strip()
        if configured:
            payload[field_name] = canonical_remote_endpoint(configured)
    if not payload.get("grafana_public_url"):
        payload["grafana_public_url"] = payload.get("grafana_url", "")
    for field_name in _CASEFOLD_SETTING_FIELDS:
        payload[field_name] = str(payload.get(field_name) or "").strip().casefold()
    for field_name in _STRIPPED_SETTING_FIELDS:
        payload[field_name] = str(payload.get(field_name) or "").strip()
    try:
        payload["knowledge_tenant_id"] = canonical_knowledge_tenant_id(payload.get("knowledge_tenant_id"))
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    payload["knowledge_permissions"] = _canonical_permissions(str(payload.get("knowledge_permissions") or ""))
    tenant_api_keys = payload.get("knowledge_tenant_api_keys")
    if isinstance(tenant_api_keys, Mapping):
        try:
            payload["knowledge_tenant_api_keys"] = validated_knowledge_tenant_api_keys(tenant_api_keys)
        except ValueError as exc:
            raise RuntimeOwnershipError(str(exc)) from exc
    sanitized = {key: _sanitize_setting(key, value) for key, value in sorted(payload.items())}
    encoded = json.dumps(sanitized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{_FINGERPRINT_PREFIX}{hashlib.sha256(encoded).hexdigest()}"


def _sanitize_setting(name: str, value: Any) -> Any:
    normalized_name = name.casefold()
    if any(marker in normalized_name for marker in _SECRET_FIELD_MARKERS):
        if isinstance(value, Mapping):
            return {
                str(key): credential_fingerprint(str(item))
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        return credential_fingerprint(str(value) if value is not None else None)
    if isinstance(value, Mapping):
        return {
            str(key): _sanitize_setting(str(key), item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        return [_sanitize_setting(name, item) for item in value]
    return value


def _tenant_policy(runtime_settings: Settings) -> RuntimeTenantPolicy:
    try:
        tenant_id = canonical_knowledge_tenant_id(runtime_settings.knowledge_tenant_id)
        raw_tenant_api_keys = runtime_settings.knowledge_tenant_api_keys
        if not isinstance(raw_tenant_api_keys, Mapping):
            raise ValueError("knowledge_tenant_api_keys must be a tenant-key mapping")
        tenant_api_keys = validated_knowledge_tenant_api_keys(raw_tenant_api_keys)
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    permission_identity = _canonical_permissions(runtime_settings.knowledge_permissions)
    permissions = tuple(permission_identity.split(",")) if permission_identity else ()
    credentials = tuple(
        sorted(
            (
                tenant,
                credential_fingerprint(secret),
            )
            for tenant, secret in tenant_api_keys.items()
        )
    )
    return RuntimeTenantPolicy(
        mode="wildcard" if tenant_id == "*" else "pinned",
        tenant_id=tenant_id,
        permissions=permissions,
        api_auth_enabled=runtime_settings.api_auth_enabled,
        tenant_credential_fingerprints=credentials,
    )


def _settings_remotes(runtime_settings: Settings) -> tuple[RuntimeRemoteIdentity, ...]:
    signalfx_realm = canonical_signalfx_realm(runtime_settings.signalfx_realm)
    return (
        RuntimeRemoteIdentity(
            provider="grafana",
            endpoint=runtime_settings.grafana_url,
            account=str(runtime_settings.grafana_org_id),
            credential_fingerprint=credential_fingerprint(runtime_settings.grafana_api_key),
        ),
        RuntimeRemoteIdentity(
            provider="pagerduty",
            endpoint=runtime_settings.pagerduty_base_url,
            credential_fingerprint=credential_fingerprint(runtime_settings.pagerduty_api_token),
        ),
        RuntimeRemoteIdentity(
            provider="signalfx",
            endpoint=f"https://api.{signalfx_realm}.signalfx.com",
            account=signalfx_realm,
            credential_fingerprint=credential_fingerprint(runtime_settings.signalfx_api_token),
        ),
    )


def _mismatch_dimensions(
    expected: RuntimeOwnershipDescriptor,
    actual: RuntimeOwnershipDescriptor,
) -> set[str]:
    dimensions: set[str] = set()
    if (
        expected.settings_identity is not None
        and actual.settings_identity is not None
        and expected.settings_identity != actual.settings_identity
    ):
        dimensions.add("settings")

    if expected.tenant_policy is not None and actual.tenant_policy is not None:
        expected_tenant = (
            expected.tenant_policy.mode,
            expected.tenant_policy.tenant_id,
            expected.tenant_policy.api_auth_enabled,
            expected.tenant_policy.tenant_credential_fingerprints,
        )
        actual_tenant = (
            actual.tenant_policy.mode,
            actual.tenant_policy.tenant_id,
            actual.tenant_policy.api_auth_enabled,
            actual.tenant_policy.tenant_credential_fingerprints,
        )
        if expected_tenant != actual_tenant:
            dimensions.add("tenant")
        if expected.tenant_policy.permissions != actual.tenant_policy.permissions:
            dimensions.add("permission")

    expected_databases = {item.role: item.path for item in expected.databases}
    actual_databases = {item.role: item.path for item in actual.databases}
    if any(
        expected_databases[role] != actual_databases[role]
        for role in expected_databases.keys() & actual_databases.keys()
    ):
        dimensions.add("database")

    expected_remotes = {item.provider: item for item in expected.remotes}
    actual_remotes = {item.provider: item for item in actual.remotes}
    for provider in expected_remotes.keys() & actual_remotes.keys():
        expected_remote = expected_remotes[provider]
        actual_remote = actual_remotes[provider]
        if expected_remote.endpoint != actual_remote.endpoint:
            dimensions.add("endpoint")
        if expected_remote.account != actual_remote.account:
            dimensions.add("account")
        if expected_remote.credential_fingerprint != actual_remote.credential_fingerprint:
            dimensions.add("credential")

    if (
        expected.cache_namespace is not None
        and actual.cache_namespace is not None
        and expected.cache_namespace != actual.cache_namespace
    ):
        dimensions.add("cache_namespace")
    if (
        expected.admission_namespace is not None
        and actual.admission_namespace is not None
        and expected.admission_namespace != actual.admission_namespace
    ):
        dimensions.add("admission_namespace")
    return dimensions


def _ownership_graph_is_connected(descriptors: tuple[RuntimeOwnershipDescriptor, ...]) -> bool:
    if len(descriptors) < 2:
        return True
    visited = {0}
    pending = [0]
    while pending:
        current = pending.pop()
        for index, candidate in enumerate(descriptors):
            if index in visited:
                continue
            if _shares_identity_dimension(descriptors[current], candidate):
                visited.add(index)
                pending.append(index)
    return len(visited) == len(descriptors)


def _shares_identity_dimension(
    expected: RuntimeOwnershipDescriptor,
    actual: RuntimeOwnershipDescriptor,
) -> bool:
    if expected.settings_identity is not None and expected.settings_identity == actual.settings_identity:
        return True
    if expected.tenant_policy is not None and expected.tenant_policy == actual.tenant_policy:
        return True
    if set(expected.databases) & set(actual.databases):
        return True
    if set(expected.remotes) & set(actual.remotes):
        return True
    if expected.cache_namespace is not None and expected.cache_namespace == actual.cache_namespace:
        return True
    return expected.admission_namespace is not None and expected.admission_namespace == actual.admission_namespace


def _canonical_path(value: str | Path) -> Path:
    try:
        return Path(value).expanduser().resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise RuntimeOwnershipError("runtime database path is invalid") from exc


def _canonical_permissions(value: object) -> str:
    try:
        return canonical_knowledge_permissions(value)
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc


def _unique_databases(values: tuple[RuntimeDatabaseIdentity, ...]) -> tuple[RuntimeDatabaseIdentity, ...]:
    by_role: dict[str, RuntimeDatabaseIdentity] = {}
    for value in values:
        if not isinstance(value, RuntimeDatabaseIdentity):
            raise RuntimeOwnershipError("runtime database identity is invalid")
        existing = by_role.get(value.role)
        if existing is not None and existing.path != value.path:
            raise RuntimeOwnershipError(f"runtime descriptor contains conflicting database role: {value.role}")
        by_role[value.role] = value
    try:
        validate_distinct_sqlite_role_paths(
            {role: value.path for role, value in by_role.items() if role in _DATABASE_ROLE_SETTING_FIELDS}
        )
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    return tuple(by_role[role] for role in sorted(by_role))


def _unique_remotes(values: tuple[RuntimeRemoteIdentity, ...]) -> tuple[RuntimeRemoteIdentity, ...]:
    by_provider: dict[str, RuntimeRemoteIdentity] = {}
    for value in values:
        if not isinstance(value, RuntimeRemoteIdentity):
            raise RuntimeOwnershipError("runtime remote identity is invalid")
        existing = by_provider.get(value.provider)
        if existing is not None and existing != value:
            raise RuntimeOwnershipError(f"runtime descriptor contains conflicting remote provider: {value.provider}")
        by_provider[value.provider] = value
    return tuple(by_provider[provider] for provider in sorted(by_provider))


def _is_fingerprint(value: str) -> bool:
    return (
        value.startswith(_FINGERPRINT_PREFIX)
        and len(value) == len(_FINGERPRINT_PREFIX) + 64
        and all(character in "0123456789abcdef" for character in value[len(_FINGERPRINT_PREFIX) :])
    )


def _reason_code(value: str) -> str:
    normalized = _REASON_CODE_RE.sub("_", str(value or "").strip().casefold()).strip("_")
    return normalized[:96]


def _public_settings(owner: object) -> Settings | None:
    runtime_settings = getattr(owner, "runtime_settings", None)
    if isinstance(runtime_settings, Settings):
        return runtime_settings
    owner_settings = getattr(owner, "settings", None)
    return owner_settings if isinstance(owner_settings, Settings) else None


def _public_database_path(owner: object) -> Path | None:
    value = getattr(owner, "database_path", None)
    return _canonical_path(value) if value is not None else None
