"""Standalone cold-isolation runtime for evaluation runs.

Lifts the ``isolated_learning_runtime`` pytest fixture into a plain context
manager that any evaluation harness (or a manual run) can use to guarantee a
*cold* baseline: fresh signal/history/feedback stores seeded only from the
packaged ``signals.yaml``, no learned archetypes, and cleared in-memory caches.

Without this, learned mappings, runtime archetypes, feedback-driven metric
quality, and the LLM/metric caches accumulate across runs and silently
contaminate cold-recall measurements (as observed during ClickStack testing).

Usage
-----
    from tests.eval.cold_isolation import cold_isolation

    with cold_isolation() as state:
        response = await run_pipeline(request, state.dependencies)

CLI
---
    python -m tests.eval.cold_isolation --verify
        Enters an isolated runtime and prints the baseline (mappings loaded,
        archetypes registered, cache sizes) so you can confirm a clean slate.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import tempfile
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from tacit.archetypes.generated.schema import ArchetypeRetrievalMode

DISABLED_LOCAL_ENDPOINT = "http://127.0.0.1:9"
_COLD_ISOLATION_LOCK = threading.RLock()
_COLD_ISOLATION_OWNER: tuple[int, int | None] | None = None
_COLD_ISOLATION_DEPTH = 0

COLD_ENV_CREDENTIAL_NAMES = (
    "ANTHROPIC_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_CONFIG_FILE",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_DEFAULT_REGION",
    "AWS_DEFAULT_PROFILE",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_SESSION_TOKEN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AZURE_OPENAI_API_KEY",
    "CONTEXT_API_KEY",
    "GRAFANA_API_KEY",
    "OPENAI_API_KEY",
    "OPENAI_ORG_ID",
    "OPENAI_PROJECT_ID",
    "PAGERDUTY_API_TOKEN",
    "SIGNALFX_API_TOKEN",
    "SLACK_APP_TOKEN",
    "SLACK_BOT_TOKEN",
    "SLACK_SIGNING_SECRET",
    "TACIT_API_KEY",
)
COLD_ENV_PROXY_NAMES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


@dataclass
class IsolatedState:
    """Handles to the fresh stores backing an isolated run."""

    workdir: Path
    settings: Any
    runtime_stores: Any
    dependencies: Any
    signal_store: Any
    history_store: Any
    feedback_store: Any
    archetypes_path: Path
    signal_mappings_loaded: int


@dataclass(frozen=True)
class LocalEvaluationEndpoints:
    """Explicit local capabilities an evaluation is allowed to consume."""

    grafana_url: str | None = None
    llm_api_base: str | None = None
    llm_model: str = ""


@dataclass(frozen=True)
class _CacheState:
    store: Any
    total_weight: int
    hits: int
    misses: int


def require_local_endpoint(value: str, label: str) -> str:
    """Accept only explicit loopback or local-container HTTP endpoints."""
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname or ""
        _ = parsed.port
    except ValueError:
        hostname = ""
        parsed = urlsplit("")
    local_hostname = hostname.casefold() in {
        "localhost",
        "host.docker.internal",
        "host.containers.internal",
    }
    try:
        local_address = ip_address(hostname).is_loopback
    except ValueError:
        local_address = False
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or not (local_hostname or local_address)
    ):
        raise ValueError(f"{label} must be an explicit local loopback endpoint")
    return value


def validate_local_evaluation_endpoints(
    endpoints: LocalEvaluationEndpoints | None,
) -> LocalEvaluationEndpoints:
    """Validate every selected capability without creating evaluation state."""
    selected = endpoints or LocalEvaluationEndpoints()
    if selected.grafana_url:
        require_local_endpoint(selected.grafana_url, "Evaluation Grafana URL")
    if selected.llm_api_base:
        require_local_endpoint(selected.llm_api_base, "Evaluation LLM URL")
    if selected.llm_model and not selected.llm_api_base:
        raise ValueError("Evaluation LLM model requires an explicit local LLM endpoint")
    return selected


def validate_evaluation_tenant(tenant_id: str) -> str:
    """Return one concrete tenant before isolation creates process state."""
    from tacit.config import canonical_knowledge_tenant_id

    concrete_tenant = canonical_knowledge_tenant_id(tenant_id)
    if concrete_tenant == "*":
        raise ValueError("Cold isolation requires a concrete tenant")
    return concrete_tenant


def _capture_cache(cache: Any) -> _CacheState:
    with cache._lock:
        return _CacheState(
            store=cache._store.copy(),
            total_weight=cache._total_weight,
            hits=cache._hits,
            misses=cache._misses,
        )


def _restore_cache(cache: Any, state: _CacheState) -> None:
    with cache._lock:
        cache._store.clear()
        cache._store.update(state.store)
        cache._total_weight = state.total_weight
        cache._hits = state.hits
        cache._misses = state.misses


def _capture_global_caches() -> tuple[_CacheState, _CacheState]:
    from tacit.cache import llm_cache, metric_cache

    return _capture_cache(metric_cache), _capture_cache(llm_cache)


def _restore_global_caches(states: tuple[_CacheState, _CacheState]) -> None:
    from tacit.cache import llm_cache, metric_cache

    metric_state, llm_state = states
    _restore_cache(metric_cache, metric_state)
    _restore_cache(llm_cache, llm_state)


def _reset_caches(scoped_llm_cache: Any | None = None) -> None:
    """Clear metric, compatibility LLM, and optional runtime-owned caches."""
    try:
        from tacit.cache import llm_cache, metric_cache

        metric_cache.invalidate()
        metric_cache.reset_stats()
        llm_cache.invalidate()
        llm_cache.reset_stats()
        if scoped_llm_cache is not None and scoped_llm_cache is not llm_cache:
            scoped_llm_cache.invalidate()
            scoped_llm_cache.reset_stats()
    except Exception:
        pass


def _default_settings_values(settings_type: Any) -> dict[str, Any]:
    """Materialize model defaults so ambient env/YAML cannot refill fields."""
    return {name: field.get_default(call_default_factory=True) for name, field in settings_type.model_fields.items()}


def _isolated_settings(
    source: Any,
    settings_type: Any,
    base: Path,
    endpoints: LocalEvaluationEndpoints | None,
    tenant_id: str,
) -> Any:
    """Build a deny-by-default eval configuration from explicit allowlisted inputs."""
    selected = validate_local_evaluation_endpoints(endpoints)
    grafana_url = (
        require_local_endpoint(selected.grafana_url, "Evaluation Grafana URL")
        if selected.grafana_url
        else DISABLED_LOCAL_ENDPOINT
    )
    llm_api_base = (
        require_local_endpoint(selected.llm_api_base, "Evaluation LLM URL")
        if selected.llm_api_base
        else DISABLED_LOCAL_ENDPOINT
    )
    values = _default_settings_values(settings_type)
    values.update(
        {
            "history_db_path": str(base / "history.db"),
            "feedback_db_path": str(base / "feedback.db"),
            "signals_db_path": str(base / "signals.db"),
            "knowledge_tenant_id": tenant_id,
            "knowledge_tenant_api_keys": {},
            "knowledge_permissions": str(settings_type.model_fields["knowledge_permissions"].default),
            "api_auth_enabled": False,
            "api_auth_key": "",
            "api_cors_allowed_origins": "",
            # Network capabilities are disabled unless a harness selects them.
            "grafana_enabled": selected.grafana_url is not None,
            "grafana_url": grafana_url,
            "grafana_public_url": "",
            "grafana_api_key": "",
            "llm_provider": "ollama",
            "llm_api_base": llm_api_base,
            "llm_model": selected.llm_model or str(settings_type.model_fields["llm_model"].default),
            "llm_api_key": "",
            # Every other remote integration is disabled at composition time.
            "llm_azure_api_version": "",
            "llm_azure_deployment": "",
            "llm_bedrock_region": "",
            "llm_bedrock_model_id": "",
            "llm_bedrock_role_arn": "",
            "llm_aws_access_key_id": "",
            "llm_aws_secret_access_key": "",
            "signalfx_enabled": False,
            "signalfx_api_token": "",
            "pagerduty_api_token": "",
            "pagerduty_base_url": "http://127.0.0.1:9",
            "slack_bot_token": "",
            "slack_app_token": "",
            "slack_signing_secret": "",
            "context_provider": "none",
            "context_api_key": "",
            "context_mcp_server_url": "",
            "context_a2a_agent_url": "",
            "context_rag_api_url": "",
            # Generated artifacts remain shadow-disabled in a cold measurement.
            "learned_archetypes_generation_enabled": False,
            "learned_archetypes_automatic_registration_enabled": False,
            "learned_archetypes_normal_retrieval_enabled": False,
            "learned_archetypes_retrieval_mode": ArchetypeRetrievalMode.CURATED_ONLY,
            "learned_archetypes_quarantine_path": str(base / "generated-archetypes-quarantine"),
            "learned_archetypes_tenant_id": tenant_id,
            "learning_auto_register_archetype": False,
        }
    )
    return source.model_copy(deep=True, update=values)


def _remove_ambient_credentials() -> dict[str, str]:
    """Remove SDK-discovered credentials until the isolated graph is released."""
    previous: dict[str, str] = {}
    for name in COLD_ENV_CREDENTIAL_NAMES:
        value = os.environ.pop(name, None)
        if value is not None:
            previous[name] = value
    return previous


def _restore_ambient_credentials(previous: dict[str, str]) -> None:
    for name in COLD_ENV_CREDENTIAL_NAMES:
        os.environ.pop(name, None)
    os.environ.update(previous)


def _remove_ambient_proxies() -> dict[str, str]:
    """Remove proxy routing that could redirect explicit local capabilities."""
    previous: dict[str, str] = {}
    for name in COLD_ENV_PROXY_NAMES:
        value = os.environ.pop(name, None)
        if value is not None:
            previous[name] = value
    return previous


def _restore_ambient_proxies(previous: dict[str, str]) -> None:
    for name in COLD_ENV_PROXY_NAMES:
        os.environ.pop(name, None)
    os.environ.update(previous)


def _evaluation_dependencies(runtime_settings: Any, runtime_stores: Any) -> Any:
    """Build local-only clients without changing production proxy semantics."""
    from tacit.agents.providers.ollama import OllamaProvider
    from tacit.backends.grafana import GrafanaBackend
    from tacit.dependencies import build_pipeline_dependencies, declare_backend_factory
    from tacit.grafana.client import GrafanaClient
    from tacit.runtime_ownership import declare_runtime_factory, runtime_descriptor_for_provider

    def local_llm_provider() -> OllamaProvider:
        return OllamaProvider(runtime_settings=runtime_settings, trust_env=False)

    def local_backends() -> list[Any]:
        if not runtime_settings.grafana_enabled:
            return []
        client = GrafanaClient(runtime_settings=runtime_settings, trust_env=False)
        return [GrafanaBackend(client=client, runtime_settings=runtime_settings)]

    declared_llm_factory = declare_runtime_factory(
        local_llm_provider,
        ownership=runtime_descriptor_for_provider(
            component="cold_isolation_llm_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    declared_backend_factory = declare_backend_factory(
        local_backends,
        runtime_settings=runtime_settings,
        component="evaluation_backend_factory",
    )
    return build_pipeline_dependencies(
        runtime_settings,
        stores=runtime_stores,
        backend_factory=declared_backend_factory,
        llm_provider_factory=declared_llm_factory,
    )


def _cold_isolation_owner() -> tuple[int, int | None]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return threading.get_ident(), id(task) if task is not None else None


@contextlib.contextmanager
def _serialized_cold_isolation() -> Iterator[None]:
    """Serialize threads and fail closed on cross-task reentry."""
    global _COLD_ISOLATION_DEPTH, _COLD_ISOLATION_OWNER

    owner = _cold_isolation_owner()
    with _COLD_ISOLATION_LOCK:
        if _COLD_ISOLATION_OWNER not in {None, owner}:
            raise RuntimeError("cold isolation cannot overlap between asynchronous tasks")
        _COLD_ISOLATION_OWNER = owner
        _COLD_ISOLATION_DEPTH += 1
        try:
            yield
        finally:
            _COLD_ISOLATION_DEPTH -= 1
            if _COLD_ISOLATION_DEPTH == 0:
                _COLD_ISOLATION_OWNER = None


@contextlib.contextmanager
def _cold_isolation_unlocked(
    workdir: str | os.PathLike[str] | None,
    endpoints: LocalEvaluationEndpoints | None,
    tenant_id: str,
) -> Iterator[IsolatedState]:
    """Context manager yielding an isolated, cold runtime.

    On enter: builds one explicit dependency graph in ``workdir`` (a temp dir
    if omitted), loads only the packaged signal taxonomy, points archetype
    loading at an empty learned-archetypes file, and clears caches. Evaluation
    callers pass ``state.dependencies`` into the pipeline. On exit, the
    ``TACIT_ARCHETYPES_PATH`` env var is restored and archetypes are reloaded.
    """
    from tacit.config import Settings, settings
    from tacit.runtime_stores import RuntimeStores

    prior_cache_state = _capture_global_caches()
    tmp_ctx: tempfile.TemporaryDirectory[str] | None = None
    if workdir is None:
        tmp_ctx = tempfile.TemporaryDirectory(prefix="tacit-cold-")
        base = Path(tmp_ctx.name)
    else:
        base = Path(workdir)
        base.mkdir(parents=True, exist_ok=True)

    prior_credentials = _remove_ambient_credentials()
    prior_proxies = _remove_ambient_proxies()
    prior_arch_path = os.environ.get("TACIT_ARCHETYPES_PATH")
    dependencies: Any | None = None
    templates: Any | None = None
    try:
        isolated_settings = _isolated_settings(settings, Settings, base, endpoints, tenant_id)
        runtime_stores = RuntimeStores(isolated_settings)
        signal_store = runtime_stores.signals()
        history_store = runtime_stores.history()
        feedback_store = runtime_stores.feedback()
        dependencies = _evaluation_dependencies(isolated_settings, runtime_stores)
        mappings_loaded = int(signal_store.stats(tenant_id=tenant_id)["metric_mappings"])
        archetypes_path = base / "learned_archetypes.yaml"
        os.environ["TACIT_ARCHETYPES_PATH"] = str(archetypes_path)

        import tacit.archetypes.templates as templates_module

        templates = templates_module
        templates.reload_archetypes()
        _reset_caches(dependencies.llm_cache)

        state = IsolatedState(
            workdir=base,
            settings=isolated_settings,
            runtime_stores=runtime_stores,
            dependencies=dependencies,
            signal_store=signal_store,
            history_store=history_store,
            feedback_store=feedback_store,
            archetypes_path=archetypes_path,
            signal_mappings_loaded=mappings_loaded,
        )
        yield state
    finally:
        try:
            if prior_arch_path is None:
                os.environ.pop("TACIT_ARCHETYPES_PATH", None)
            else:
                os.environ["TACIT_ARCHETYPES_PATH"] = prior_arch_path
            if templates is not None:
                templates.reload_archetypes()
            if dependencies is not None:
                dependencies.llm_cache.invalidate()
                dependencies.llm_cache.reset_stats()
        finally:
            try:
                _restore_ambient_proxies(prior_proxies)
            finally:
                try:
                    _restore_ambient_credentials(prior_credentials)
                    _restore_global_caches(prior_cache_state)
                finally:
                    if tmp_ctx is not None:
                        tmp_ctx.cleanup()


@contextlib.contextmanager
def cold_isolation(
    workdir: str | os.PathLike[str] | None = None,
    *,
    endpoints: LocalEvaluationEndpoints | None = None,
    tenant_id: str = "default",
) -> Iterator[IsolatedState]:
    """Yield one cold runtime while serializing process-global eval state."""
    selected_endpoints = validate_local_evaluation_endpoints(endpoints)
    concrete_tenant = validate_evaluation_tenant(tenant_id)
    with _serialized_cold_isolation():
        with _cold_isolation_unlocked(workdir, selected_endpoints, concrete_tenant) as state:
            yield state


def _verify() -> int:
    """Enter an isolated runtime and print the baseline; non-zero on a dirty slate."""
    from tacit.cache import metric_cache

    with cold_isolation() as state:
        import tacit.archetypes.templates as templates

        archetype_count = len(getattr(templates, "ALL_ARCHETYPES", []))
        learned = [a for a in getattr(templates, "ALL_ARCHETYPES", []) if "learned" in getattr(a, "tags", [])]
        print("cold-isolation baseline:")
        print(f"  workdir              : {state.workdir}")
        print(f"  signal mappings      : {state.signal_mappings_loaded} (packaged taxonomy)")
        print(f"  archetypes loaded    : {archetype_count}")
        print(f"  learned archetypes   : {len(learned)} (expect 0)")
        print(f"  metric cache size    : {metric_cache.size} (expect 0)")
        scoped_llm_cache = state.dependencies.llm_cache
        print(f"  llm cache size       : {scoped_llm_cache.size} (expect 0)")
        ok = (
            state.signal_mappings_loaded > 0
            and len(learned) == 0
            and metric_cache.size == 0
            and scoped_llm_cache.size == 0
        )
        print(f"  CLEAN BASELINE       : {ok}")
        return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Cold-isolation runtime for Tacit eval runs.")
    parser.add_argument("--verify", action="store_true", help="Print the baseline and exit non-zero if not clean.")
    args = parser.parse_args()
    if args.verify:
        return _verify()
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
