"""CLI credential parity and secret-file persistence invariants."""

from __future__ import annotations

import os
import stat
import sys
import threading
import types
from collections.abc import Callable
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from tacit.agents.providers.base import LLMResult
from tacit.config import Settings


def test_doctor_reloads_one_settings_snapshot_for_every_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated embedded invocations must not reuse import-time datasource settings."""
    from tacit import cli as cli_module
    from tacit import config as config_module

    stale_settings = Settings.model_validate(
        {
            "grafana_url": "https://stale-grafana.example.test",
            "grafana_api_key": "stale-grafana-token",
            "signalfx_enabled": False,
            "signalfx_api_token": "stale-signalfx-token",
            "signalfx_realm": "stale-realm",
            "llm_provider": "anthropic",
            "llm_api_key": "sk-ant-stale",
            "intent_fallback_enabled": False,
        }
    )
    monkeypatch.setattr(config_module, "settings", stale_settings)

    config_file = tmp_path / "config.yaml"
    monkeypatch.setattr(cli_module, "CONFIG_FILE", config_file)
    monkeypatch.setattr(cli_module, "TACIT_HOME", tmp_path / ".tacit")
    monkeypatch.setenv("TACIT_CONFIG", str(config_file))
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_API_KEY", "")
    monkeypatch.setenv("LLM_API_BASE", "")
    monkeypatch.setattr(cli_module, "_check_archetypes", lambda: True)
    monkeypatch.setattr(
        "tacit.assess.build_assessment",
        lambda **_kwargs: {
            "inventory": {
                "dashboards_ingested": 0,
                "alerts_ingested": 0,
                "runbooks": 0,
                "incidents": 0,
            },
            "readiness": {"level": "Low"},
        },
    )

    settings_snapshots: list[Settings] = []
    real_create_settings = config_module.create_settings

    def observed_create_settings() -> Settings:
        runtime_settings = real_create_settings()
        settings_snapshots.append(runtime_settings)
        return runtime_settings

    monkeypatch.setattr(config_module, "create_settings", observed_create_settings)

    probe_settings: dict[str, list[Settings]] = {
        "grafana": [],
        "datasources": [],
        "signalfx": [],
        "llm": [],
        "zero_key": [],
    }

    def observe_probe(name: str, probe: Callable[[Settings], bool]) -> Callable[[Settings], bool]:
        def observed(runtime_settings: Settings) -> bool:
            probe_settings[name].append(runtime_settings)
            return probe(runtime_settings)

        return observed

    monkeypatch.setattr(
        cli_module,
        "_check_grafana",
        observe_probe("grafana", cli_module._check_grafana),
    )
    monkeypatch.setattr(
        cli_module,
        "_check_datasources",
        observe_probe("datasources", cli_module._check_datasources),
    )
    monkeypatch.setattr(
        cli_module,
        "_check_signalfx",
        observe_probe("signalfx", cli_module._check_signalfx),
    )
    monkeypatch.setattr(cli_module, "_check_llm", observe_probe("llm", cli_module._check_llm))
    monkeypatch.setattr(
        cli_module,
        "_llm_zero_key_mode",
        observe_probe("zero_key", cli_module._llm_zero_key_mode),
    )

    remote_calls: list[dict[str, object]] = []

    class SuccessfulResponse:
        status_code = 200

        def __init__(self, path: str) -> None:
            self._path = path

        def json(self) -> object:
            if self._path == "/api/org":
                return {"name": "Fresh"}
            if self._path == "/api/datasources":
                return [{"type": "prometheus"}]
            return {"count": 1}

    def observed_remote_get(**kwargs: object) -> SuccessfulResponse:
        remote_calls.append(kwargs)
        return SuccessfulResponse(str(kwargs["path"]))

    monkeypatch.setattr(cli_module, "_credentialed_remote_get", observed_remote_get)

    runner = CliRunner()
    outputs: list[str] = []
    for generation in ("one", "two"):
        config_file.write_text(
            "\n".join(
                [
                    "grafana:",
                    f"  url: https://grafana-{generation}.example.test",
                    "signalfx:",
                    "  enabled: true",
                    f"  realm: {generation}",
                    "llm:",
                    "  provider: openai",
                    f"  model: model-{generation}",
                    "intent_fallback_enabled: true",
                ]
            ),
            encoding="utf-8",
        )
        monkeypatch.setenv("GRAFANA_API_KEY", f"grafana-token-{generation}")
        monkeypatch.setenv("SIGNALFX_API_TOKEN", f"signalfx-token-{generation}")
        monkeypatch.setenv("LLM_MODEL", f"model-{generation}")

        result = runner.invoke(cli_module.cli, ["doctor"])

        assert result.exit_code == 0, result.output
        assert "llm (zero-key mode)" in result.output
        outputs.append(result.output)

    assert len(settings_snapshots) == 2
    assert all(len(observed) == 2 for observed in probe_settings.values())
    for generation_index in range(2):
        invocation_settings = [observed[generation_index] for observed in probe_settings.values()]
        assert all(current is invocation_settings[0] for current in invocation_settings)
    assert probe_settings["llm"][0] is not probe_settings["llm"][1]
    assert [settings.llm_model for settings in probe_settings["llm"]] == ["model-one", "model-two"]
    observed_calls = {
        (str(call["base_url"]), str(call["path"]), tuple(sorted(dict(call["headers"]).items())))
        for call in remote_calls
    }
    assert observed_calls == {
        (
            "https://grafana-one.example.test",
            "/api/org",
            (("Authorization", "Bearer grafana-token-one"),),
        ),
        (
            "https://grafana-one.example.test",
            "/api/datasources",
            (("Authorization", "Bearer grafana-token-one"),),
        ),
        (
            "https://api.one.signalfx.com",
            "/v2/metric",
            (("X-SF-TOKEN", "signalfx-token-one"),),
        ),
        (
            "https://grafana-two.example.test",
            "/api/org",
            (("Authorization", "Bearer grafana-token-two"),),
        ),
        (
            "https://grafana-two.example.test",
            "/api/datasources",
            (("Authorization", "Bearer grafana-token-two"),),
        ),
        (
            "https://api.two.signalfx.com",
            "/v2/metric",
            (("X-SF-TOKEN", "signalfx-token-two"),),
        ),
    }
    assert len(remote_calls) == len(observed_calls)
    assert all("stale" not in str(call) for call in remote_calls)
    assert all("Fatal: llm" not in output for output in outputs)


def _bedrock_settings() -> Settings:
    return Settings.model_validate(
        {
            "llm_provider": "bedrock",
            "llm_model": "anthropic.claude-sonnet-4-20250514-v1:0",
            "llm_bedrock_region": "us-east-1",
            "llm_aws_access_key_id": "AKIAEXPLICIT",
            "llm_aws_secret_access_key": "explicit-secret",
            "pipeline_max_concurrent": 1,
            "pipeline_max_queued": 0,
        }
    )


def _profile_environment(tmp_path: Path, profile_body: str) -> dict[str, str]:
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    credentials_path.write_text("", encoding="utf-8")
    config_path.write_text(f"[profile runtime]\n{profile_body}", encoding="utf-8")
    return {
        "HOME": str(tmp_path),
        "AWS_PROFILE": "runtime",
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_path),
        "AWS_CONFIG_FILE": str(config_path),
    }


def test_bedrock_setup_guidance_lists_only_admitted_credential_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    from tacit import cli as cli_module

    messages: list[str] = []

    def prompt(text: str, default: str = "") -> str:
        messages.append(text)
        if text.startswith("Provider"):
            return "bedrock"
        return default

    def confirm(text: str, default: bool = True) -> bool:
        del default
        messages.append(text)
        return False

    monkeypatch.setattr(cli_module, "_prompt", prompt)
    monkeypatch.setattr(cli_module, "_confirm", confirm)
    monkeypatch.setattr(cli_module, "_info", messages.append)

    cli_module._interactive_setup()

    rendered = "\n".join(messages).casefold()
    assert "instance profile" not in rendered
    assert "default chain" not in rendered
    assert "environment credentials" in rendered
    assert "static profile" in rendered
    assert "web identity" in rendered
    assert "credential_process" in rendered
    assert "not supported" in rendered


def test_bedrock_doctor_runs_the_production_plan_on_an_admitted_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    from tacit import cli as cli_module
    from tacit.agents.providers.bedrock import BedrockProvider

    caller_thread = threading.get_ident()
    observed: dict[str, object] = {}
    messages: list[str] = []

    def execute(self: BedrockProvider, *args: object, **kwargs: object) -> LLMResult:
        del args, kwargs
        lifecycle = self._pipeline_lifecycle
        observed["worker_thread"] = threading.get_ident()
        observed["admitted"] = bool(lifecycle and lifecycle.current_thread_owns_blocking_capacity())
        return LLMResult(text="ok")

    monkeypatch.setattr("tacit.runtime_ownership.capture_bedrock_environment", lambda: {})
    monkeypatch.setattr(BedrockProvider, "_execute_converse_operation", execute)
    monkeypatch.setattr(cli_module, "_success", messages.append)

    assert cli_module._check_llm(_bedrock_settings()) is True
    assert observed == {"worker_thread": observed["worker_thread"], "admitted": True}
    assert observed["worker_thread"] != caller_thread
    assert all("AKIAEXPLICIT" not in message and "explicit-secret" not in message for message in messages)


class _DoctorNoCredentialsError(Exception):
    pass


class _DoctorAccessDeniedException(Exception):
    response = {"Error": {"Code": "AccessDeniedException"}}


class _DoctorEndpointConnectionError(Exception):
    pass


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (
            _DoctorNoCredentialsError("private-credential-canary"),
            "LLM: bedrock validation failed (credentials). Check the admitted AWS credential source "
            "and, when configured, the role trust policy.",
        ),
        (
            _DoctorAccessDeniedException("private-access-canary"),
            "LLM: bedrock validation failed (model/access/configuration). Check LLM_BEDROCK_MODEL_ID, "
            "LLM_BEDROCK_REGION, model access, and bedrock:InvokeModel permission.",
        ),
        (
            _DoctorEndpointConnectionError("https://private-endpoint.invalid/secret"),
            "LLM: bedrock validation failed (timeout/unavailable). Check network reachability and "
            "Bedrock availability in the configured region, then retry.",
        ),
    ],
    ids=("credentials", "model-access-configuration", "timeout-unavailable"),
)
def test_bedrock_doctor_reports_bounded_reason_specific_secret_safe_remediation(
    failure: Exception,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module
    from tacit.agents.providers.bedrock import BedrockProvider

    messages: list[str] = []

    def fail_operation(self: BedrockProvider, *args: object, **kwargs: object) -> LLMResult:
        del self, args, kwargs
        raise failure

    monkeypatch.setattr("tacit.runtime_ownership.capture_bedrock_environment", lambda: {})
    monkeypatch.setattr(BedrockProvider, "_execute_converse_operation", fail_operation)
    monkeypatch.setattr(cli_module, "_fail", messages.append)

    assert cli_module._check_llm(_bedrock_settings()) is False
    assert messages == [expected]
    assert len(messages[0]) <= 240
    assert "private-" not in messages[0]
    assert "invalid/secret" not in messages[0]


@pytest.mark.parametrize(
    ("operation_name", "error_code"),
    [
        ("AssumeRole", "AccessDenied"),
        ("AssumeRoleWithWebIdentity", "InvalidIdentityToken"),
    ],
    ids=("assume-role-access-denied", "web-identity-token-rejected"),
)
def test_bedrock_doctor_classifies_sts_operation_and_code_as_role_trust_failure(
    operation_name: str,
    error_code: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from botocore.exceptions import ClientError

    from tacit import cli as cli_module
    from tacit.agents.providers.bedrock import BedrockProvider

    failure = ClientError(
        {
            "Error": {
                "Code": error_code,
                "Message": "private-role-and-identity-canary",
            }
        },
        operation_name,
    )
    messages: list[str] = []

    def fail_operation(self: BedrockProvider, *args: object, **kwargs: object) -> LLMResult:
        del self, args, kwargs
        raise failure

    monkeypatch.setattr("tacit.runtime_ownership.capture_bedrock_environment", lambda: {})
    monkeypatch.setattr(BedrockProvider, "_execute_converse_operation", fail_operation)
    monkeypatch.setattr(cli_module, "_fail", messages.append)

    assert cli_module._check_llm(_bedrock_settings()) is False
    assert messages == [
        "LLM: bedrock validation failed (role/trust). Check the configured role ARN, role trust "
        "policy, and STS assume-role permission."
    ]
    assert "private-role-and-identity-canary" not in messages[0]
    assert len(messages[0]) <= 240


@pytest.mark.parametrize(
    "environment_factory",
    [
        lambda tmp_path: _profile_environment(tmp_path, "credential_process = /tmp/get-creds\n"),
        lambda tmp_path: _profile_environment(
            tmp_path,
            "sso_start_url = https://example.invalid/start\n"
            "sso_region = us-east-1\n"
            "sso_account_id = 123456789012\n"
            "sso_role_name = ReadOnly\n",
        ),
        lambda tmp_path: (
            _profile_environment(tmp_path, "") | {"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/v2/credentials"}
        ),
        lambda tmp_path: _profile_environment(tmp_path, "") | {"AWS_EC2_METADATA_DISABLED": "false"},
    ],
    ids=("credential-process", "sso", "ecs", "imds"),
)
def test_bedrock_doctor_rejects_unmodeled_sources_before_boto3_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment_factory,
) -> None:
    from tacit import cli as cli_module

    touched: list[str] = []

    class Boto3Trap(types.ModuleType):
        def __getattr__(self, name: str) -> object:
            touched.append(name)
            raise AssertionError(f"boto3 accessed before credential admission: {name}")

    environment = environment_factory(tmp_path)
    runtime_settings = Settings.model_validate(
        {
            "llm_provider": "bedrock",
            "llm_model": "anthropic.claude-sonnet-4-20250514-v1:0",
            "llm_bedrock_region": "us-east-1",
        }
    )
    monkeypatch.setattr("tacit.runtime_ownership.capture_bedrock_environment", lambda: environment)
    monkeypatch.setitem(sys.modules, "boto3", Boto3Trap("boto3"))

    assert cli_module._check_llm(runtime_settings) is False
    assert touched == []


def test_atomic_secret_writer_sets_mode_before_first_byte(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tacit import cli as cli_module

    observed_modes: list[int] = []
    real_write = os.write

    def checked_write(fd: int, data: bytes) -> int:
        observed_modes.append(stat.S_IMODE(os.fstat(fd).st_mode))
        return real_write(fd, data)

    monkeypatch.setattr(cli_module.os, "write", checked_write)
    destination = tmp_path / ".env"

    cli_module._atomic_write_secret_file(destination, "TOKEN=secret\n")

    assert observed_modes
    assert set(observed_modes) == {0o600}
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600


def test_init_routes_secret_persistence_through_atomic_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    writes: list[tuple[Path, str]] = []
    monkeypatch.setattr(cli_module, "TACIT_HOME", tmp_path)
    monkeypatch.setattr(cli_module, "CONFIG_FILE", tmp_path / "config.yaml")

    def durable_write(path: Path, content: str) -> object:
        writes.append((path, content))
        return cli_module.SecretFileWriteOutcome.DURABLE

    monkeypatch.setattr(cli_module, "_atomic_write_secret_file", durable_write)

    result = CliRunner().invoke(cli_module.cli, ["init", "--non-interactive"])

    assert result.exit_code == 0, result.output
    assert writes == [
        (
            tmp_path / ".env",
            "# Tacit secrets — generated by `tacit init`\n",
        )
    ]


def test_init_warns_without_retrying_when_secret_is_published_but_not_directory_synced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    writes: list[tuple[Path, str]] = []
    warnings: list[str] = []
    monkeypatch.setattr(cli_module, "TACIT_HOME", tmp_path)
    monkeypatch.setattr(cli_module, "CONFIG_FILE", tmp_path / "config.yaml")

    def uncertain_write(path: Path, content: str) -> object:
        writes.append((path, content))
        return cli_module.SecretFileWriteOutcome.PUBLISHED_DURABILITY_UNCERTAIN

    monkeypatch.setattr(cli_module, "_atomic_write_secret_file", uncertain_write)
    monkeypatch.setattr(cli_module, "_warn", warnings.append)

    result = CliRunner().invoke(cli_module.cli, ["init", "--non-interactive"])

    assert result.exit_code == 0, result.output
    assert len(writes) == 1
    assert len(warnings) == 1
    assert "already visible" in warnings[0]
    assert "do not retry automatically" in warnings[0].casefold()


def test_secret_update_rejects_symlink_without_touching_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    target = tmp_path / "target"
    target.write_text("CANARY=unchanged\n", encoding="utf-8")
    (tmp_path / ".env").symlink_to(target)
    monkeypatch.setattr(cli_module, "TACIT_HOME", tmp_path)

    with pytest.raises(click.ClickException, match="symlink"):
        cli_module._update_env({"TOKEN": "secret"})

    assert target.read_text(encoding="utf-8") == "CANARY=unchanged\n"


def test_atomic_secret_writer_preserves_original_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    destination = tmp_path / ".env"
    destination.write_text("TOKEN=old\n", encoding="utf-8")
    destination.chmod(0o600)

    def fail_replace(source: str | bytes | os.PathLike[str] | os.PathLike[bytes], target: object) -> None:
        del source, target
        raise OSError("injected replace failure")

    monkeypatch.setattr(cli_module.os, "replace", fail_replace)

    with pytest.raises(OSError, match="injected replace failure"):
        cli_module._atomic_write_secret_file(destination, "TOKEN=new\n")

    assert destination.read_text(encoding="utf-8") == "TOKEN=old\n"
    assert list(tmp_path.glob("..env.*.tmp")) == []


def test_atomic_secret_writer_preserves_original_when_file_sync_fails_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    destination = tmp_path / ".env"
    destination.write_text("TOKEN=old\n", encoding="utf-8")
    destination.chmod(0o600)
    real_fsync = os.fsync
    replace_calls: list[tuple[object, object]] = []

    def fail_file_sync(fd: int) -> None:
        if stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("injected file fsync failure")
        real_fsync(fd)

    def record_replace(source: object, target: object) -> None:
        replace_calls.append((source, target))

    monkeypatch.setattr(cli_module.os, "fsync", fail_file_sync)
    monkeypatch.setattr(cli_module.os, "replace", record_replace)

    with pytest.raises(OSError, match="injected file fsync failure"):
        cli_module._atomic_write_secret_file(destination, "TOKEN=new\n")

    assert replace_calls == []
    assert destination.read_text(encoding="utf-8") == "TOKEN=old\n"
    assert list(tmp_path.glob("..env.*.tmp")) == []


def test_atomic_secret_writer_reports_uncertain_after_parent_directory_open_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    destination = tmp_path / ".env"
    destination.write_text("TOKEN=old\n", encoding="utf-8")
    destination.chmod(0o600)
    real_open = os.open
    real_replace = os.replace
    replace_calls: list[tuple[object, object]] = []

    def fail_parent_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if os.fsdecode(path) == os.fspath(tmp_path):
            raise OSError("injected directory open failure")
        if dir_fd is None:
            return real_open(path, flags, mode)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    def record_replace(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        target: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        replace_calls.append((source, target))
        real_replace(source, target)

    monkeypatch.setattr(cli_module.os, "open", fail_parent_open)
    monkeypatch.setattr(cli_module.os, "replace", record_replace)

    outcome = cli_module._atomic_write_secret_file(destination, "TOKEN=new\n")

    assert outcome is cli_module.SecretFileWriteOutcome.PUBLISHED_DURABILITY_UNCERTAIN
    assert len(replace_calls) == 1
    assert destination.read_text(encoding="utf-8") == "TOKEN=new\n"
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert list(tmp_path.glob("..env.*.tmp")) == []


def test_atomic_secret_writer_reports_uncertain_after_parent_directory_sync_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    destination = tmp_path / ".env"
    destination.write_text("TOKEN=old\n", encoding="utf-8")
    destination.chmod(0o600)
    real_fsync = os.fsync
    real_replace = os.replace
    replace_calls: list[tuple[object, object]] = []

    def fail_directory_sync(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("injected directory fsync failure")
        real_fsync(fd)

    def record_replace(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        target: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        replace_calls.append((source, target))
        real_replace(source, target)

    monkeypatch.setattr(cli_module.os, "fsync", fail_directory_sync)
    monkeypatch.setattr(cli_module.os, "replace", record_replace)

    outcome = cli_module._atomic_write_secret_file(destination, "TOKEN=new\n")

    assert outcome is cli_module.SecretFileWriteOutcome.PUBLISHED_DURABILITY_UNCERTAIN
    assert len(replace_calls) == 1
    assert destination.read_text(encoding="utf-8") == "TOKEN=new\n"
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert list(tmp_path.glob("..env.*.tmp")) == []


def test_update_env_preserves_existing_values_through_atomic_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tacit import cli as cli_module

    destination = tmp_path / ".env"
    destination.write_text("# old\nFIRST=one\nSECOND=old\n", encoding="utf-8")
    destination.chmod(0o644)
    monkeypatch.setattr(cli_module, "TACIT_HOME", tmp_path)

    cli_module._update_env({"SECOND": "two", "THIRD": "three"})

    assert destination.read_text(encoding="utf-8").splitlines() == [
        "# Tacit secrets",
        "FIRST=one",
        "SECOND=two",
        "THIRD=three",
    ]
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
