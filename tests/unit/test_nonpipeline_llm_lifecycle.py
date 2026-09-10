"""Lifecycle matrix for LLM work outside the investigation pipeline."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from tacit import cli as cli_module
from tacit.agents.providers import registry
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.agents.providers.http_transport import LLMSDKHTTPClientConstruction
from tacit.config import Settings
from tacit.dependencies import (
    PipelineDependencies,
    build_pipeline_dependencies,
    managed_nonpipeline_llm_provider,
)
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline_admission import RuntimeRootDrainStartupError
from tacit.runtime_ownership import RuntimeOwnershipMismatchError
from tacit.runtime_stores import RuntimeStores
from tests import validate

_OWNER_MANAGED_PROVIDERS = ("openai", "azure", "anthropic", "ollama")


class _LifecycleProbeProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings, observations: dict[str, Any]) -> None:
        # Mirrors the cloud SDK construction guard without allocating a client.
        LLMSDKHTTPClientConstruction.begin()
        super().__init__(runtime_settings, component="nonpipeline_lifecycle_probe")
        observations.setdefault("providers", []).append(self)
        observations.setdefault("construction_threads", []).append(threading.get_ident())
        observations.setdefault("construction_loops", []).append(self._factory_event_loop)
        self._observations = observations

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        del system_prompt, user_prompt, temperature
        self._observations.setdefault("used", []).append(self)
        self._observations.setdefault("use_threads", []).append(threading.get_ident())
        if self._observations.get("fail_use"):
            raise RuntimeError("synthetic provider use failure")
        return LLMResult(
            text=json.dumps(
                {
                    "summary": "Investigate checkout latency",
                    "domain": "application",
                    "services": ["checkout"],
                    "signals": ["metrics"],
                    "keywords": ["latency"],
                    "timerange": "1h",
                    "problem_type": "latency_investigation",
                    "archetypes": [{"type": "latency_investigation", "confidence": 0.99}],
                }
            )
        )

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        del system_prompt, user_prompt, temperature
        self._observations.setdefault("used", []).append(self)
        self._observations.setdefault("use_threads", []).append(threading.get_ident())
        if self._observations.get("fail_use"):
            raise RuntimeError("synthetic provider use failure")
        return LLMResult(text="Assessment narrative")

    async def close(self) -> None:
        self._observations.setdefault("closed", []).append(self)
        self._observations.setdefault("close_threads", []).append(threading.get_ident())
        if self._observations.get("fail_close"):
            raise RuntimeError("synthetic provider close failure")


def _settings(tmp_path: Path, provider_name: str) -> Settings:
    values: dict[str, Any] = {
        "llm_provider": provider_name,
        "llm_api_key": "nonpipeline-provider-test-key",
        "llm_api_base": "https://llm-owner.example",
        "llm_model": "test-model",
        "history_db_path": str(tmp_path / f"{provider_name}-history.db"),
        "feedback_db_path": str(tmp_path / f"{provider_name}-feedback.db"),
        "signals_db_path": str(tmp_path / f"{provider_name}-signals.db"),
    }
    if provider_name == "azure":
        values.update(
            {
                "llm_azure_api_version": "2024-06-01",
                "llm_azure_deployment": "test-deployment",
            }
        )
    return Settings(_env_file=None, **values)


def _install_probe_factory(
    monkeypatch: pytest.MonkeyPatch,
    runtime_settings: Settings,
    observations: dict[str, Any],
) -> None:
    def factory(settings: Settings) -> LLMProvider:
        assert settings.llm_provider == runtime_settings.llm_provider
        return _LifecycleProbeProvider(settings, observations)

    monkeypatch.setitem(registry._PROVIDER_FACTORIES, runtime_settings.llm_provider, factory)


def _case_file(tmp_path: Path) -> Path:
    csv_path = tmp_path / "cases.csv"
    csv_path.write_text(
        "prompt_id,prompt,expected_archetype,expected_metrics,expected_datasources,difficulty,validation_goal\n"
        "DF-001,Investigate checkout latency,latency_investigation,request_latency_seconds,Prometheus,medium,gate\n",
        encoding="utf-8",
    )
    return csv_path


def _assessment_report() -> dict[str, Any]:
    return {
        "inventory": {
            "dashboards_ingested": 0,
            "alerts_ingested": 0,
            "runbooks": 0,
            "incidents": 0,
            "signal_types": 0,
            "metric_mappings": 0,
            "artifacts_stale": 0,
            "alerts_stale": 0,
        },
        "services": {"known": 0, "missing_ownership": 0},
        "coverage": {
            "knowledge_coverage_pct": 0.0,
            "signal_types_with_trusted_mapping": 0,
            "signal_types_total": 0,
            "signal_types_candidate_only": 0,
        },
        "quality": {
            "duplicate_groups": 0,
            "alerts_without_owner_attribution": 0,
            "runbooks_without_matching_signals": 0,
            "incident_rca_claims_rejected_or_ignored": 0,
            "incident_rca_claims_unreviewed_over_30d": 0,
            "unresolved_evidence_claims": 0,
            "artifacts_with_zero_extractions": 0,
            "avg_extractions_per_artifact": 0.0,
        },
        "activity": {"investigations_total": 0, "success_rate_pct": 0.0, "archetype_path": 0, "freeform_path": 0},
        "readiness": {"level": "Low", "score": 0, "reasons": []},
    }


def _assert_runtime_released(stores: RuntimeStores) -> None:
    admission = stores.pipeline_admission()
    assert admission.in_flight == 0
    assert admission.retained == 0
    assert admission.blocking_in_flight == 0
    assert admission.service_owner_in_flight == 0
    assert admission.execution_graph.root_owner_count == 0
    assert admission.execution_graph.provider_manager_count == 0


def _dependencies(
    runtime_settings: Settings,
    stores: RuntimeStores,
) -> PipelineDependencies:
    return build_pipeline_dependencies(runtime_settings, stores=stores)


@pytest.mark.parametrize("provider_name", _OWNER_MANAGED_PROVIDERS)
def test_external_validation_uses_one_owner_realized_provider(
    provider_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, provider_name)
    observations: dict[str, Any] = {}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)
    monkeypatch.setattr("tacit.agents.llm.get_provider", lambda: pytest.fail("global provider used"))

    exit_code = validate.main(
        [str(_case_file(tmp_path)), "--mode", "archetype", "--state", "external"],
        runtime_stores=stores,
    )

    assert exit_code == 0
    assert observations["construction_loops"] == [None]
    assert observations["construction_threads"] != [threading.get_ident()]
    assert observations["used"] == observations["providers"]
    assert observations["closed"] == observations["providers"]
    assert observations["use_threads"] == observations["close_threads"]
    _assert_runtime_released(stores)


@pytest.mark.parametrize("provider_name", _OWNER_MANAGED_PROVIDERS)
def test_assessment_llm_uses_exact_owner_realized_provider(
    provider_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, provider_name)
    observations: dict[str, Any] = {}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)
    captured: list[LLMProvider] = []

    async def narrative(_report: dict[str, Any], *, provider: LLMProvider) -> str:
        captured.append(provider)
        return (await provider.chat_text("system", "user")).text

    monkeypatch.setattr(cli_module, "_cli_knowledge_read_context", lambda _tenant: (stores, "default"))
    monkeypatch.setattr("tacit.assess.build_assessment", lambda **_kwargs: _assessment_report())
    monkeypatch.setattr("tacit.assess.narrate_assessment", narrative)
    result = CliRunner().invoke(cli_module.cli, ["assess", "--llm"])

    assert result.exit_code == 0, result.output
    assert captured == observations["providers"]
    assert observations["construction_loops"] == [None]
    assert observations["construction_threads"] != [threading.get_ident()]
    assert observations["used"] == observations["providers"]
    assert observations["closed"] == observations["providers"]
    assert "Assessment narrative" in result.output
    _assert_runtime_released(stores)


def test_assessment_llm_failure_is_nonzero_and_still_retires_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "openai")
    observations: dict[str, Any] = {}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)

    async def failed_narrative(_report: dict[str, Any], *, provider: LLMProvider) -> str:
        assert provider is observations["providers"][0]
        raise RuntimeError("narrative failed")

    monkeypatch.setattr(cli_module, "_cli_knowledge_read_context", lambda _tenant: (stores, "default"))
    monkeypatch.setattr("tacit.assess.build_assessment", lambda **_kwargs: _assessment_report())
    monkeypatch.setattr("tacit.assess.narrate_assessment", failed_narrative)
    result = CliRunner().invoke(cli_module.cli, ["assess", "--llm"])

    assert result.exit_code != 0
    assert observations["closed"] == observations["providers"]
    _assert_runtime_released(stores)


def test_assessment_rejects_conflicting_output_modes_before_storage_access(monkeypatch: pytest.MonkeyPatch) -> None:
    accesses: list[str] = []

    def forbidden_context(_tenant: str | None):
        accesses.append("storage")
        raise AssertionError("storage accessed")

    monkeypatch.setattr(cli_module, "_cli_knowledge_read_context", forbidden_context)

    result = CliRunner().invoke(cli_module.cli, ["assess", "--json", "--llm"])

    assert result.exit_code == 2
    assert accesses == []


def test_nonpipeline_owner_releases_runtime_when_acquire_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "openai")
    observations: dict[str, Any] = {}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)
    dependencies = _dependencies(runtime_settings, stores)

    async def fail_acquire(_self: PipelineDependencies) -> Any:
        raise RuntimeError("synthetic provider acquire failure")

    monkeypatch.setattr(PipelineDependencies, "acquire_resources", fail_acquire)

    with pytest.raises(RuntimeError, match="provider acquire failure"):
        with managed_nonpipeline_llm_provider(runtime_settings, dependencies=dependencies):
            pytest.fail("acquire failure yielded a provider")

    assert observations.get("providers", []) == []
    _assert_runtime_released(stores)


def test_nonpipeline_owner_releases_runtime_when_construction_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "openai")
    observations: dict[str, Any] = {}
    stores = RuntimeStores(runtime_settings)

    def fail_construction(_settings: Settings) -> LLMProvider:
        LLMSDKHTTPClientConstruction.begin()
        observations.setdefault("construction_threads", []).append(threading.get_ident())
        try:
            observations.setdefault("construction_loops", []).append(asyncio.get_running_loop())
        except RuntimeError:
            observations.setdefault("construction_loops", []).append(None)
        raise RuntimeError("synthetic provider construction failure")

    monkeypatch.setitem(registry._PROVIDER_FACTORIES, "openai", fail_construction)

    with pytest.raises(RuntimeError, match="provider construction failure"):
        with managed_nonpipeline_llm_provider(runtime_settings, runtime_stores=stores):
            pytest.fail("construction failure yielded a provider")

    assert observations["construction_loops"] == [None]
    assert observations["construction_threads"] != [threading.get_ident()]
    _assert_runtime_released(stores)


def test_nonpipeline_owner_closes_constructed_provider_rejected_by_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "openai")
    foreign_settings = Settings(
        _env_file=None,
        **{
            **runtime_settings.model_dump(),
            "llm_api_base": "https://foreign-owner.example",
        },
    )
    observations: dict[str, Any] = {}
    stores = RuntimeStores(runtime_settings)

    def mismatched_factory(_settings: Settings) -> LLMProvider:
        return _LifecycleProbeProvider(foreign_settings, observations)

    monkeypatch.setitem(registry._PROVIDER_FACTORIES, "openai", mismatched_factory)

    with pytest.raises(RuntimeOwnershipMismatchError):
        with managed_nonpipeline_llm_provider(runtime_settings, runtime_stores=stores):
            pytest.fail("rejected construction yielded a provider")

    assert observations["closed"] == observations["providers"]
    _assert_runtime_released(stores)


def test_nonpipeline_owner_releases_provider_and_runtime_when_use_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "ollama")
    observations: dict[str, Any] = {"fail_use": True}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)

    with pytest.raises(RuntimeError, match="provider use failure"):
        with managed_nonpipeline_llm_provider(runtime_settings, runtime_stores=stores) as provider:
            asyncio.run(provider.chat_text("system", "user"))

    assert observations["closed"] == observations["providers"]
    _assert_runtime_released(stores)


def test_nonpipeline_owner_revokes_provider_and_releases_runtime_when_close_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "openai")
    observations: dict[str, Any] = {"fail_close": True}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)

    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        with managed_nonpipeline_llm_provider(runtime_settings, runtime_stores=stores):
            pass

    assert len(observations["closed"]) == 2
    provider = observations["providers"][0]
    assert getattr(provider, "_lifecycle_invocation_revoked", False) is True
    assert stores.pipeline_admission().runtime_fatal_circuit is not None
    _assert_runtime_released(stores)


def test_nonpipeline_owner_retries_root_drain_startup_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, "openai")
    observations: dict[str, Any] = {}
    _install_probe_factory(monkeypatch, runtime_settings, observations)
    stores = RuntimeStores(runtime_settings)
    dependencies = _dependencies(runtime_settings, stores)
    original_stop = PipelineDependencies.stop_runtime_root
    release_calls = 0

    async def flaky_stop(
        self: PipelineDependencies,
        handle: Any,
        *,
        wait_for_drain: bool = True,
    ) -> None:
        nonlocal release_calls
        release_calls += 1
        if self is dependencies and release_calls == 1:
            raise RuntimeRootDrainStartupError("synthetic root drain startup failure")
        await original_stop(self, handle, wait_for_drain=wait_for_drain)

    monkeypatch.setattr(PipelineDependencies, "stop_runtime_root", flaky_stop)

    with managed_nonpipeline_llm_provider(runtime_settings, dependencies=dependencies):
        pass

    assert release_calls == 2
    assert observations["closed"] == observations["providers"]
    _assert_runtime_released(stores)


def test_nonpipeline_owner_runs_bedrock_through_operation_scoped_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit.agents.providers.bedrock import BedrockProvider

    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_model="anthropic.claude-sonnet-4-20250514-v1:0",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        llm_bedrock_region="us-east-1",
        llm_aws_access_key_id="nonpipeline-owner-access-key",
        llm_aws_secret_access_key="nonpipeline-owner-secret",
        history_db_path=str(tmp_path / "bedrock-history.db"),
        feedback_db_path=str(tmp_path / "bedrock-feedback.db"),
        signals_db_path=str(tmp_path / "bedrock-signals.db"),
    )
    stores = RuntimeStores(runtime_settings)
    construction_threads: list[int] = []
    operation_threads: list[str] = []
    original_init = BedrockProvider.__init__

    def observed_init(self: BedrockProvider, *args: Any, **kwargs: Any) -> None:
        construction_threads.append(threading.get_ident())
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()
        original_init(self, *args, **kwargs)

    def execute_without_network(self: BedrockProvider, *_args: Any, **_kwargs: Any) -> LLMResult:
        operation_threads.append(threading.current_thread().name)
        return LLMResult(text="bedrock assessment")

    monkeypatch.setattr(BedrockProvider, "__init__", observed_init)
    monkeypatch.setattr(BedrockProvider, "_execute_converse_operation", execute_without_network)

    with managed_nonpipeline_llm_provider(runtime_settings, runtime_stores=stores) as provider:
        assert isinstance(provider, BedrockProvider)
        result = asyncio.run(provider.chat_text("system", "user"))

    assert result.text == "bedrock assessment"
    assert construction_threads != [threading.get_ident()]
    assert operation_threads == ["tacit-lifecycle-blocking-work"]
    assert provider._closed.is_set()
    _assert_runtime_released(stores)


def test_nonpipeline_owner_constructs_and_closes_ollama_off_caller_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit.agents.providers import ollama as ollama_module
    from tacit.agents.providers.ollama import OllamaProvider

    runtime_settings = _settings(tmp_path, "ollama")
    stores = RuntimeStores(runtime_settings)
    construction_threads: list[int] = []
    construction_loops: list[asyncio.AbstractEventLoop | None] = []
    use_threads: list[int] = []
    close_threads: list[int] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {"message": {"content": "ollama assessment"}}

    class Client:
        def __init__(self, **_kwargs: Any) -> None:
            construction_threads.append(threading.get_ident())
            try:
                construction_loops.append(asyncio.get_running_loop())
            except RuntimeError:
                construction_loops.append(None)

        async def post(self, *_args: Any, **_kwargs: Any) -> Response:
            use_threads.append(threading.get_ident())
            return Response()

        async def aclose(self) -> None:
            close_threads.append(threading.get_ident())

    monkeypatch.setattr(
        ollama_module,
        "create_llm_sdk_http_client",
        lambda *_args, **_kwargs: Client(),
    )

    with managed_nonpipeline_llm_provider(runtime_settings, runtime_stores=stores) as provider:
        assert isinstance(provider, OllamaProvider)
        result = asyncio.run(provider.chat_text("system", "user"))

    assert result.text == "ollama assessment"
    assert construction_loops == [None]
    assert construction_threads != [threading.get_ident()]
    assert use_threads == close_threads
    _assert_runtime_released(stores)


def test_bedrock_permission_guidance_names_runtime_api_actions() -> None:
    documentation = Path("docs/vendor-permissions.md").read_text(encoding="utf-8")
    remediation = cli_module._BEDROCK_DOCTOR_REMEDIATION["model/access/configuration"]

    assert "bedrock:Converse" not in documentation
    assert "bedrock:Converse" not in remediation
    assert "bedrock:InvokeModel" in documentation
    assert "bedrock:InvokeModel" in remediation
    assert "bedrock:InvokeModelWithResponseStream" in documentation
