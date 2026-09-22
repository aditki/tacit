#!/usr/bin/env python3
"""Fail-closed authorization for a release publication mutation."""

from __future__ import annotations

import csv
import hashlib
import http.client
import io
import json
import math
import os
import re
import stat
import subprocess
import sys
import time
import zipfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from release_github_api import (
    ReleaseGitHubAPIError,
    authenticated_github_request,
    build_github_api_opener,
    validate_github_api_url,
)
from release_quality_contract import (
    ARCHIVED_STATE_MANIFEST,
    FINGERPRINT_PATTERN,
    STATE_SNAPSHOT_LIMITS,
    ReleaseQualityContractError,
    validate_admitted_state_manifest,
    validate_concrete_tenant,
    validate_state_snapshot_limits,
)

MAX_ACTIONS_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_QUALITY_ARCHIVE_BYTES = 25 * 1024 * 1024
MAX_QUALITY_EXPANDED_BYTES = 50 * 1024 * 1024
MAX_QUALITY_MEMBER_BYTES = 20 * 1024 * 1024
MAX_QUALITY_CORPUS_BYTES = 1024 * 1024
IDEMPOTENT_RETRY_DELAYS = (0.25, 1.0)
TRANSIENT_HTTP_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
SHA_PATTERN = re.compile(r"[0-9a-f]{40}\Z")
DIGEST_PATTERN = re.compile(r"sha256:([0-9a-f]{64})\Z")
REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
QUALITY_WORKFLOW = "release-quality.yml"
QUALITY_WORKFLOW_PATH = ".github/workflows/release-quality.yml"
QUALITY_WORKFLOW_RUN_PATH = f"{QUALITY_WORKFLOW_PATH}@main"
QUALITY_CORPUS_PATH = "tests/tacit_validation_prompts.csv"
QUALITY_EVIDENCE_PATH = "release-quality-evidence.json"
QUALITY_STATE_MANIFEST_PATH = ARCHIVED_STATE_MANIFEST
QUALITY_REPORT_PATHS = {
    "clean": "clean-report.json",
    "long-lived": "long-lived-report.json",
}
QUALITY_ARCHIVE_FILES = {
    QUALITY_EVIDENCE_PATH,
    QUALITY_CORPUS_PATH,
    QUALITY_STATE_MANIFEST_PATH,
    *QUALITY_REPORT_PATHS.values(),
}
QUALITY_PROMPT_COUNT = 100
QUALITY_MIN_LLM_REQUESTS = 400
QUALITY_MAX_LLM_REQUESTS = 500
QUALITY_THRESHOLDS = {
    "min_archetype_accuracy": 0.85,
    "min_archetype_soft_accuracy": 0.90,
    "min_metric_recall": 0.75,
    "min_critical_recall": 0.80,
    "min_weighted_recall": 0.78,
    "min_signal_to_noise": 0.65,
    "max_errors": 0,
    "max_error_rate": 0.0,
}
_CLEAN_STATE_FINGERPRINT = hashlib.sha256(b"tacit-validation-state-v1:clean").hexdigest()
_ARCHETYPE_ALIASES = {
    "latency_investigation": "latency_investigation",
    "slow_requests": "latency_investigation",
    "high_latency": "latency_investigation",
    "p99_spike": "latency_investigation",
    "error_spike": "error_spike",
    "5xx_errors": "error_spike",
    "error_rate": "error_spike",
    "failed_requests": "error_spike",
    "golden_signals": "golden_signals",
    "sre_overview": "golden_signals",
    "service_health": "golden_signals",
    "service_overview": "golden_signals",
    "resource_saturation": "resource_saturation",
    "high_cpu": "resource_saturation",
    "high_memory": "resource_saturation",
    "oom": "resource_saturation",
    "memory_leak": "resource_saturation",
    "cpu_throttling": "resource_saturation",
    "general": "general",
}
_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count", "_created")


class ReleaseAuthorizationError(RuntimeError):
    """The release no longer satisfies a publication invariant."""


class _TransientGitRead(ReleaseAuthorizationError):
    """An idempotent Git read may be retried."""


def _retry_idempotent[T](
    operation: Callable[[], T],
    *,
    label: str,
    retryable: Callable[[Exception], bool],
) -> T:
    attempts = len(IDEMPOTENT_RETRY_DELAYS) + 1
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:
            if not retryable(exc):
                raise
            if attempt == attempts - 1:
                raise ReleaseAuthorizationError(f"{label} failed after {attempts} attempts: {exc}") from exc
            time.sleep(IDEMPOTENT_RETRY_DELAYS[attempt])
    raise AssertionError("bounded retry loop did not return or raise")


def _git_once(repository: Path, *arguments: str) -> str:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise _TransientGitRead(f"could not run git: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "git command failed"
        if arguments and arguments[0] == "fetch":
            raise _TransientGitRead(detail)
        raise ReleaseAuthorizationError(detail)
    return result.stdout.strip()


def _git(repository: Path, *arguments: str) -> str:
    is_idempotent_read = bool(arguments) and arguments[0] in {"fetch", "rev-parse"}
    if not is_idempotent_read:
        return _git_once(repository, *arguments)
    return _retry_idempotent(
        lambda: _git_once(repository, *arguments),
        label="idempotent Git read",
        retryable=lambda exc: isinstance(exc, _TransientGitRead),
    )


def _validate_sha(value: str) -> str:
    if SHA_PATTERN.fullmatch(value) is None:
        raise ReleaseAuthorizationError("GITHUB_SHA is not a full lowercase commit SHA")
    return value


def prove_fresh_tag_on_main(
    repository: Path,
    *,
    expected_sha: str,
    tag_name: str,
) -> None:
    expected_sha = _validate_sha(expected_sha)
    tag_ref = f"refs/tags/{tag_name}"
    _git(repository, "check-ref-format", tag_ref)

    if _git(repository, "rev-parse", "HEAD") != expected_sha:
        raise ReleaseAuthorizationError("checked-out release commit does not match GITHUB_SHA")

    _git(repository, "update-ref", "-d", tag_ref)
    _git(
        repository,
        "fetch",
        "--force",
        "--no-tags",
        "origin",
        "+refs/heads/main:refs/remotes/origin/main",
        f"+{tag_ref}:{tag_ref}",
    )
    if _git(repository, "rev-parse", f"{tag_ref}^{{commit}}") != expected_sha:
        raise ReleaseAuthorizationError("release tag no longer resolves to GITHUB_SHA")

    if _git(repository, "rev-parse", "refs/remotes/origin/main^{commit}") != expected_sha:
        raise ReleaseAuthorizationError("release commit is not the current origin/main tip")


def _is_transient_http_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in TRANSIENT_HTTP_STATUS_CODES
    return isinstance(exc, (URLError, OSError, http.client.HTTPException))


def _load_actions_response_once(request: Request) -> dict[str, Any]:
    opener = build_github_api_opener(allow_https_redirects=False)
    with opener.open(request, timeout=30) as response:
        declared = response.headers.get("Content-Length")
        if declared is not None:
            try:
                declared_size = int(declared)
            except ValueError as exc:
                raise ReleaseAuthorizationError("CI authorization response has an invalid Content-Length") from exc
            if declared_size < 0 or declared_size > MAX_ACTIONS_RESPONSE_BYTES:
                raise ReleaseAuthorizationError("CI authorization response exceeded the size limit")
        payload = response.read(MAX_ACTIONS_RESPONSE_BYTES + 1)

    if len(payload) > MAX_ACTIONS_RESPONSE_BYTES:
        raise ReleaseAuthorizationError("CI authorization response exceeded the size limit")
    try:
        loaded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseAuthorizationError("CI authorization response is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise ReleaseAuthorizationError("CI authorization response is not an object")
    return loaded


def _load_actions_response(request: Request) -> dict[str, Any]:
    try:
        return _retry_idempotent(
            lambda: _load_actions_response_once(request),
            label="GitHub authorization read",
            retryable=_is_transient_http_error,
        )
    except ReleaseAuthorizationError:
        raise
    except (HTTPError, URLError, OSError) as exc:
        raise ReleaseAuthorizationError(f"could not verify CI authorization: {exc}") from exc


def require_successful_main_ci(
    *,
    expected_sha: str,
    repository_slug: str,
    token: str,
    api_url: str,
) -> None:
    expected_sha = _validate_sha(expected_sha)
    if REPOSITORY_PATTERN.fullmatch(repository_slug) is None:
        raise ReleaseAuthorizationError("GITHUB_REPOSITORY is invalid")
    if not token:
        raise ReleaseAuthorizationError("GITHUB_TOKEN is required")

    try:
        api_url = validate_github_api_url(api_url)
    except ReleaseGitHubAPIError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    query = urlencode(
        {
            "head_sha": expected_sha,
            "branch": "main",
            "event": "push",
            "status": "completed",
            "per_page": 100,
        }
    )
    url = f"{api_url.rstrip('/')}/repos/{repository_slug}" f"/actions/workflows/ci.yml/runs?{query}"
    try:
        request = authenticated_github_request(
            url,
            api_url=api_url,
            token=token,
            accept="application/vnd.github+json",
        )
    except ReleaseGitHubAPIError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    runs = _load_actions_response(request).get("workflow_runs")
    if not isinstance(runs, list):
        raise ReleaseAuthorizationError("CI authorization response has no workflow run list")
    accepted = any(
        isinstance(run, dict)
        and run.get("head_sha") == expected_sha
        and run.get("head_branch") == "main"
        and run.get("event") == "push"
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
        for run in runs
    )
    if not accepted:
        raise ReleaseAuthorizationError(
            "The tagged commit has no successful completed main CI run; publication is denied"
        )


def _positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReleaseAuthorizationError(f"{label} is invalid")
    return value


def _load_json_member(payload: bytes, label: str) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        loaded: dict[str, Any] = {}
        for key, value in pairs:
            if key in loaded:
                raise ValueError(f"duplicate JSON key {key}")
            loaded[key] = value
        return loaded

    try:
        loaded = json.loads(
            payload,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ReleaseAuthorizationError(f"{label} is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise ReleaseAuthorizationError(f"{label} is not an object")
    return loaded


def _canonical_quality_corpus_digest(repository: Path) -> str:
    corpus = repository / QUALITY_CORPUS_PATH
    if corpus.is_symlink() or not corpus.is_file():
        raise ReleaseAuthorizationError("tagged commit has no regular release-quality corpus")
    try:
        size = corpus.stat().st_size
        if size < 1 or size > MAX_QUALITY_CORPUS_BYTES:
            raise ReleaseAuthorizationError("tagged release-quality corpus exceeds the size limit")
        digest = hashlib.sha256()
        with corpus.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise ReleaseAuthorizationError("tagged release-quality corpus could not be read") from exc
    return digest.hexdigest()


def _validate_loopback_evidence_endpoint(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) > 500:
        raise ReleaseAuthorizationError(f"{label} is invalid")
    from urllib.parse import urlsplit

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ReleaseAuthorizationError(f"{label} is invalid") from exc
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or (parsed.hostname or "").casefold() not in {"localhost", "127.0.0.1", "::1"}
        or parsed.username is not None
        or parsed.password is not None
        or (port is not None and not 1 <= port <= 65535)
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ReleaseAuthorizationError(f"{label} is not a loopback HTTP(S) endpoint")
    return value


def _quality_archive_members(archive_payload: bytes) -> dict[str, bytes]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(archive_payload))
    except zipfile.BadZipFile as exc:
        raise ReleaseAuthorizationError("release-quality artifact is not a valid ZIP archive") from exc
    with archive:
        infos = archive.infolist()
        names: list[str] = []
        total_size = 0
        for info in infos:
            path = PurePosixPath(info.filename)
            if (
                not info.filename
                or "\\" in info.filename
                or path.is_absolute()
                or any(part in {"", ".", ".."} for part in path.parts)
            ):
                raise ReleaseAuthorizationError("release-quality artifact contains an unsafe path")
            mode = info.external_attr >> 16
            if info.is_dir():
                if info.filename != "tests/" or info.file_size != 0:
                    raise ReleaseAuthorizationError("release-quality artifact contains an unsafe directory")
                continue
            if stat.S_ISLNK(mode) or info.flag_bits & 0x1:
                raise ReleaseAuthorizationError("release-quality artifact contains an unsafe member")
            if info.file_size < 0 or info.file_size > MAX_QUALITY_MEMBER_BYTES:
                raise ReleaseAuthorizationError("release-quality artifact member exceeds the size limit")
            total_size += info.file_size
            if total_size > MAX_QUALITY_EXPANDED_BYTES:
                raise ReleaseAuthorizationError("release-quality artifact exceeds the expanded size limit")
            names.append(info.filename)
        if len(names) != len(set(names)):
            raise ReleaseAuthorizationError("release-quality artifact contains duplicate files")
        if set(names) != QUALITY_ARCHIVE_FILES:
            raise ReleaseAuthorizationError("release-quality artifact contains missing or unexpected files")
        try:
            return {name: archive.read(name) for name in names}
        except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
            raise ReleaseAuthorizationError("release-quality artifact could not be read") from exc


def _text(value: object, label: str, *, allow_empty: bool = False, max_length: int = 10_000) -> str:
    if not isinstance(value, str) or len(value) > max_length or (not allow_empty and not value):
        raise ReleaseAuthorizationError(f"{label} is invalid")
    return value


def _finite_number(
    value: object,
    label: str,
    *,
    minimum: float = 0.0,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReleaseAuthorizationError(f"{label} must be a finite number")
    number = float(value)
    if not math.isfinite(number) or number < minimum or (maximum is not None and number > maximum):
        raise ReleaseAuthorizationError(f"{label} must be a finite number in range")
    return number


def _validate_quality_tenant(value: object, label: str) -> str:
    try:
        return validate_concrete_tenant(value)
    except ReleaseQualityContractError as exc:
        raise ReleaseAuthorizationError(f"{label} tenant is invalid") from exc


def _string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list):
        raise ReleaseAuthorizationError(f"{label} is invalid")
    result = [_text(item, label, max_length=500) for item in value]
    if len(result) != len(set(result)):
        raise ReleaseAuthorizationError(f"{label} contains duplicate values")
    return result


def _corpus_metric_list(row: dict[str, str], field: str, separator: str) -> list[str]:
    value = row.get(field)
    if value is None:
        raise ReleaseAuthorizationError(f"release-quality corpus is missing {field}")
    metrics = [item.strip() for item in value.split(separator) if item.strip()]
    if field == "expected_metrics" and not metrics:
        raise ReleaseAuthorizationError("release-quality corpus has an empty expected metric set")
    if len(metrics) != len(set(metrics)):
        raise ReleaseAuthorizationError(f"release-quality corpus has duplicate {field}")
    return metrics


def _match_metric_sets(expected: set[str], found: set[str]) -> dict[str, str]:
    matches: dict[str, str] = {}
    available = set(found)
    for metric in sorted(expected & available):
        matches[metric] = metric
        available.remove(metric)
    for metric in sorted(expected - matches.keys()):
        derived = next(
            (candidate for suffix in _HISTOGRAM_SUFFIXES if (candidate := f"{metric}{suffix}") in available),
            None,
        )
        if derived is not None:
            matches[metric] = derived
            available.remove(derived)
    return matches


def _require_reported(section: dict[str, Any], key: str, expected: object, label: str) -> None:
    actual = section.get(key)
    if isinstance(expected, float):
        actual = _finite_number(actual, label, maximum=1.0)
    elif isinstance(expected, int) and (isinstance(actual, bool) or not isinstance(actual, int)):
        raise ReleaseAuthorizationError(f"{label} is invalid")
    if actual != expected:
        raise ReleaseAuthorizationError(f"{label} does not match independently derived measurements")


def _quality_threshold_failure(state: str, section: str, failures: list[str]) -> None:
    if failures:
        raise ReleaseAuthorizationError(f"{state} {section} quality threshold failed: {', '.join(failures)}")


def _validate_archetype_measurements(
    section: dict[str, Any],
    *,
    state: str,
    corpus_rows: list[dict[str, str]],
) -> None:
    details = section.get("details")
    if not isinstance(details, list) or len(details) != QUALITY_PROMPT_COUNT:
        raise ReleaseAuthorizationError(f"{state} quality report has incomplete archetype measurements")

    strict_passed = 0
    soft_passed = 0
    errors = 0
    for index, (detail, row) in enumerate(zip(details, corpus_rows, strict=True)):
        label = f"{state} archetype measurement {index + 1}"
        if not isinstance(detail, dict) or detail.get("prompt_id") != row["prompt_id"]:
            raise ReleaseAuthorizationError(f"{label} does not match the audited corpus")
        expected = _text(detail.get("expected"), f"{label} expected")
        actual = _text(detail.get("actual"), f"{label} actual")
        if expected != row["expected_archetype"].strip():
            raise ReleaseAuthorizationError(f"{label} changed the expected archetype")
        error = _text(detail.get("error"), f"{label} error", allow_empty=True)
        _finite_number(detail.get("latency_ms"), f"{label} latency")
        candidates = detail.get("archetypes")
        if not isinstance(candidates, list):
            raise ReleaseAuthorizationError(f"{label} archetypes are invalid")
        candidate_types: list[str] = []
        candidate_confidences: list[float] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                raise ReleaseAuthorizationError(f"{label} archetypes are invalid")
            candidate_types.append(_text(candidate.get("type"), f"{label} archetype type", max_length=200))
            candidate_confidences.append(
                _finite_number(candidate.get("confidence"), f"{label} archetype confidence", maximum=1.0)
            )
        top_confidence = _finite_number(
            detail.get("top_confidence"),
            f"{label} top confidence",
            maximum=1.0,
        )
        expected_top = round(candidate_confidences[0], 3) if candidate_confidences else 0.0
        if top_confidence != expected_top:
            raise ReleaseAuthorizationError(f"{label} top confidence is not derived from candidates")

        expected_normalized = _ARCHETYPE_ALIASES.get(expected)
        actual_normalized = _ARCHETYPE_ALIASES.get(actual)
        derived_strict = not error and expected_normalized is not None and expected_normalized == actual_normalized
        derived_soft = derived_strict or (
            expected_normalized is not None
            and any(_ARCHETYPE_ALIASES.get(candidate_type) == expected_normalized for candidate_type in candidate_types)
        )
        if detail.get("passed") is not derived_strict or detail.get("any_match") is not derived_soft:
            raise ReleaseAuthorizationError(f"{label} result flags are not independently derived")
        strict_passed += int(derived_strict)
        soft_passed += int(derived_soft)
        errors += int(bool(error))

    strict_accuracy = strict_passed / QUALITY_PROMPT_COUNT
    soft_accuracy = soft_passed / QUALITY_PROMPT_COUNT
    failures = []
    if strict_accuracy < QUALITY_THRESHOLDS["min_archetype_accuracy"]:
        failures.append(f"strict accuracy {strict_accuracy:.4f}")
    if soft_accuracy < QUALITY_THRESHOLDS["min_archetype_soft_accuracy"]:
        failures.append(f"soft accuracy {soft_accuracy:.4f}")
    if errors > QUALITY_THRESHOLDS["max_errors"]:
        failures.append(f"errors {errors}")
    if errors / QUALITY_PROMPT_COUNT > QUALITY_THRESHOLDS["max_error_rate"]:
        failures.append(f"error rate {errors / QUALITY_PROMPT_COUNT:.4f}")
    _quality_threshold_failure(state, "archetype", failures)

    for key, aggregate_expected in {
        "total": QUALITY_PROMPT_COUNT,
        "strict_passed": strict_passed,
        "soft_passed": soft_passed,
        "failed": QUALITY_PROMPT_COUNT - strict_passed,
        "errors": errors,
        "strict_accuracy": round(strict_accuracy, 4),
        "soft_accuracy": round(soft_accuracy, 4),
    }.items():
        _require_reported(section, key, aggregate_expected, f"{state} archetype aggregate {key}")
    _finite_number(section.get("avg_latency_ms"), f"{state} archetype aggregate latency")
    _finite_number(
        section.get("avg_top_confidence"),
        f"{state} archetype aggregate confidence",
        maximum=1.0,
    )


def _validate_pipeline_measurements(
    section: dict[str, Any],
    *,
    state: str,
    corpus_rows: list[dict[str, str]],
) -> None:
    details = section.get("details")
    if not isinstance(details, list) or len(details) != QUALITY_PROMPT_COUNT:
        raise ReleaseAuthorizationError(f"{state} quality report has incomplete pipeline measurements")

    measurements: list[dict[str, Any]] = []
    for index, (detail, row) in enumerate(zip(details, corpus_rows, strict=True)):
        label = f"{state} pipeline measurement {index + 1}"
        if not isinstance(detail, dict) or detail.get("prompt_id") != row["prompt_id"]:
            raise ReleaseAuthorizationError(f"{label} does not match the audited corpus")
        expected_metrics = _corpus_metric_list(row, "expected_metrics", ",")
        critical_metrics = _corpus_metric_list(row, "critical_metrics", ";")
        if not set(critical_metrics).issubset(expected_metrics):
            raise ReleaseAuthorizationError("release-quality corpus critical metrics are not expected metrics")
        found = _string_list(detail.get("found_metrics"), f"{label} found metrics")
        if found != sorted(found):
            raise ReleaseAuthorizationError(f"{label} found metrics are not canonical")
        matches = _match_metric_sets(set(expected_metrics), set(found))
        matched = set(matches)
        matched_found = set(matches.values())
        missing = sorted(set(expected_metrics) - matched)
        extra = sorted(set(found) - matched_found)
        critical_matches = _match_metric_sets(set(critical_metrics), set(found))
        critical_found = sorted(critical_matches)
        critical_missing = sorted(set(critical_metrics) - set(critical_matches))
        recall = len(matched) / len(expected_metrics)
        error = _text(detail.get("error"), f"{label} error", allow_empty=True)
        critical_recall = (
            len(critical_matches) / len(critical_metrics) if critical_metrics else (0.0 if error else recall)
        )
        if critical_metrics:
            supporting = set(expected_metrics) - set(critical_metrics)
            supporting_matched = matched - set(critical_metrics)
            maximum_weight = len(critical_metrics) + len(supporting) * 0.4
            weighted_recall = (len(critical_matches) + len(supporting_matched) * 0.4) / maximum_weight
        else:
            weighted_recall = recall
        signal_to_noise = len(matched_found) / len(found) if found else 0.0

        for key, numeric_expected in {
            "metric_recall": round(recall, 4),
            "critical_recall": round(critical_recall, 4),
            "weighted_recall": round(weighted_recall, 4),
            "signal_to_noise": round(signal_to_noise, 4),
        }.items():
            _require_reported(detail, key, numeric_expected, f"{label} {key}")
        for key, list_expected in {
            "missing_metrics": missing,
            "extra_metrics": extra,
            "critical_metrics_expected": critical_metrics,
            "critical_metrics_found": critical_found,
            "critical_metrics_missing": critical_missing,
        }.items():
            if _string_list(detail.get(key), f"{label} {key}") != list_expected:
                raise ReleaseAuthorizationError(f"{label} {key} is not independently derived")
        panel_count = detail.get("panel_count")
        if isinstance(panel_count, bool) or not isinstance(panel_count, int) or panel_count < 0:
            raise ReleaseAuthorizationError(f"{label} panel count is invalid")
        _text(detail.get("dashboard_url"), f"{label} dashboard URL", allow_empty=True, max_length=2_000)
        _finite_number(detail.get("latency_ms"), f"{label} latency")
        measurements.append(
            {
                "metric_recall": recall,
                "critical_recall": critical_recall,
                "weighted_recall": weighted_recall,
                "signal_to_noise": signal_to_noise,
                "critical": bool(critical_metrics),
                "error": bool(error),
            }
        )

    valid = [measurement for measurement in measurements if not measurement["error"]]
    errors = QUALITY_PROMPT_COUNT - len(valid)
    critical = [measurement for measurement in measurements if measurement["critical"]]

    def average(key: str, values: list[dict[str, Any]]) -> float:
        return sum(float(item[key]) for item in values) / len(values) if values else 0.0

    metric_recall = average("metric_recall", valid)
    critical_recall = (
        sum(float(item["critical_recall"]) for item in critical if not item["error"]) / len(critical)
        if critical
        else 0.0
    )
    weighted_recall = average("weighted_recall", valid)
    signal_to_noise = average("signal_to_noise", valid)
    failures = []
    for value, threshold_key, name in (
        (metric_recall, "min_metric_recall", "metric recall"),
        (weighted_recall, "min_weighted_recall", "weighted recall"),
        (signal_to_noise, "min_signal_to_noise", "signal-to-noise"),
    ):
        if value < QUALITY_THRESHOLDS[threshold_key]:
            failures.append(f"{name} {value:.4f}")
    if critical and critical_recall < QUALITY_THRESHOLDS["min_critical_recall"]:
        failures.append(f"critical recall {critical_recall:.4f}")
    if errors > QUALITY_THRESHOLDS["max_errors"]:
        failures.append(f"errors {errors}")
    if errors / QUALITY_PROMPT_COUNT > QUALITY_THRESHOLDS["max_error_rate"]:
        failures.append(f"error rate {errors / QUALITY_PROMPT_COUNT:.4f}")
    _quality_threshold_failure(state, "pipeline", failures)

    for key, expected in {
        "total": QUALITY_PROMPT_COUNT,
        "critical_cases": len(critical),
        "succeeded": len(valid),
        "errors": errors,
        "avg_metric_recall": round(metric_recall, 4),
        "avg_critical_recall": round(critical_recall, 4),
        "avg_weighted_recall": round(weighted_recall, 4),
        "avg_signal_to_noise": round(signal_to_noise, 4),
    }.items():
        _require_reported(section, key, expected, f"{state} pipeline aggregate {key}")
    _finite_number(section.get("avg_latency_ms"), f"{state} pipeline aggregate latency")


def _validate_quality_report(
    report: dict[str, Any],
    *,
    state: str,
    expected_fingerprint: str,
    expected_tenant: str,
    corpus_rows: list[dict[str, str]],
) -> None:
    if report.get("dataset") != QUALITY_CORPUS_PATH:
        raise ReleaseAuthorizationError(f"{state} quality report used the wrong corpus")
    if report.get("prompt_count") != QUALITY_PROMPT_COUNT or report.get("mode") != "all":
        raise ReleaseAuthorizationError(f"{state} quality report must cover exactly 100 prompts in all mode")
    selected_state = report.get("state")
    if not isinstance(selected_state, dict) or selected_state.get("mode") != state:
        raise ReleaseAuthorizationError(f"{state} quality report has the wrong state identity")
    if selected_state.get("fingerprint") != expected_fingerprint:
        raise ReleaseAuthorizationError(f"{state} quality report has the wrong state fingerprint")
    if _validate_quality_tenant(selected_state.get("tenant"), f"{state} quality report") != expected_tenant:
        raise ReleaseAuthorizationError(f"{state} quality report has the wrong tenant")
    gate = report.get("gate")
    if not isinstance(gate, dict) or not isinstance(gate.get("passed"), bool):
        raise ReleaseAuthorizationError(f"{state} quality report gate metadata is invalid")
    if not isinstance(gate.get("failures"), list) or gate.get("thresholds") != QUALITY_THRESHOLDS:
        raise ReleaseAuthorizationError(f"{state} quality report changed the release thresholds")
    archetype = report.get("archetype")
    pipeline = report.get("pipeline")
    if not isinstance(archetype, dict) or not isinstance(pipeline, dict):
        raise ReleaseAuthorizationError(f"{state} quality report has incomplete measurements")
    _validate_archetype_measurements(archetype, state=state, corpus_rows=corpus_rows)
    _validate_pipeline_measurements(pipeline, state=state, corpus_rows=corpus_rows)


def _validate_llm_budget(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {
        "max_requests",
        "attempted_requests",
        "forwarded_requests",
        "rejected_requests",
        "exhausted",
    }:
        raise ReleaseAuthorizationError("release-quality LLM request budget evidence is invalid")
    counts: dict[str, int] = {}
    for key in ("max_requests", "attempted_requests", "forwarded_requests", "rejected_requests"):
        item = value.get(key)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ReleaseAuthorizationError("release-quality LLM request budget evidence is invalid")
        counts[key] = item
    if (
        counts["max_requests"] != QUALITY_MAX_LLM_REQUESTS
        or counts["forwarded_requests"] < QUALITY_MIN_LLM_REQUESTS
        or counts["forwarded_requests"] > counts["max_requests"]
        or counts["attempted_requests"] != counts["forwarded_requests"] + counts["rejected_requests"]
        or counts["rejected_requests"] != 0
        or value.get("exhausted") is not False
    ):
        raise ReleaseAuthorizationError("release-quality LLM request budget did not complete within its hard limit")


def validate_release_quality_archive(
    archive_payload: bytes,
    *,
    expected_archive_digest: str,
    expected_corpus_digest: str,
    expected_sha: str,
    expected_repository: str,
    expected_run_id: int,
    expected_run_attempt: int,
) -> dict[str, Any]:
    """Validate the bounded quality artifact and both complete result reports."""
    expected_sha = _validate_sha(expected_sha)
    digest_match = DIGEST_PATTERN.fullmatch(expected_archive_digest)
    if digest_match is None or hashlib.sha256(archive_payload).hexdigest() != digest_match.group(1):
        raise ReleaseAuthorizationError("release-quality artifact digest does not match GitHub metadata")
    members = _quality_archive_members(archive_payload)
    evidence = _load_json_member(members[QUALITY_EVIDENCE_PATH], "release-quality evidence")
    if evidence.get("schema_version") != 3:
        raise ReleaseAuthorizationError("release-quality evidence schema is unsupported")
    if evidence.get("repository") != expected_repository:
        raise ReleaseAuthorizationError("release-quality evidence repository does not match")
    if evidence.get("commit_sha") != expected_sha:
        raise ReleaseAuthorizationError("release-quality evidence commit SHA does not match")
    if evidence.get("workflow") != QUALITY_WORKFLOW_PATH:
        raise ReleaseAuthorizationError("release-quality evidence workflow does not match")
    if evidence.get("workflow_run_id") != expected_run_id:
        raise ReleaseAuthorizationError("release-quality evidence run ID does not match")
    if evidence.get("workflow_run_attempt") != expected_run_attempt:
        raise ReleaseAuthorizationError("release-quality evidence run attempt does not match")

    evaluation = evidence.get("evaluation")
    if not isinstance(evaluation, dict) or evaluation.get("mode") != "all":
        raise ReleaseAuthorizationError("release-quality evidence evaluation identity is invalid")
    model = evaluation.get("model")
    if not isinstance(model, str) or not model.strip() or len(model) > 200:
        raise ReleaseAuthorizationError("release-quality evidence model identity is invalid")
    _validate_loopback_evidence_endpoint(evaluation.get("llm_endpoint"), "release-quality LLM endpoint")
    _validate_loopback_evidence_endpoint(evaluation.get("grafana_endpoint"), "release-quality Grafana endpoint")
    evaluation_tenant = _validate_quality_tenant(evaluation.get("tenant"), "release-quality evidence evaluation")
    _validate_llm_budget(evidence.get("llm_budget"))

    corpus = evidence.get("corpus")
    corpus_payload = members[QUALITY_CORPUS_PATH]
    if not 1 <= len(corpus_payload) <= MAX_QUALITY_CORPUS_BYTES:
        raise ReleaseAuthorizationError("release-quality corpus exceeds the size limit")
    if (
        FINGERPRINT_PATTERN.fullmatch(expected_corpus_digest) is None
        or hashlib.sha256(corpus_payload).hexdigest() != expected_corpus_digest
    ):
        raise ReleaseAuthorizationError("release-quality corpus does not match the tagged commit")
    if not isinstance(corpus, dict) or corpus.get("path") != QUALITY_CORPUS_PATH:
        raise ReleaseAuthorizationError("release-quality evidence corpus identity is invalid")
    if corpus.get("prompt_count") != QUALITY_PROMPT_COUNT:
        raise ReleaseAuthorizationError("release-quality evidence must cover exactly 100 prompts")
    if corpus.get("sha256") != hashlib.sha256(corpus_payload).hexdigest():
        raise ReleaseAuthorizationError("release-quality corpus digest does not match")
    try:
        reader = csv.DictReader(io.StringIO(corpus_payload.decode("utf-8-sig"), newline=""))
        required_columns = {
            "prompt_id",
            "prompt",
            "expected_archetype",
            "expected_metrics",
            "expected_datasources",
            "difficulty",
            "validation_goal",
            "critical_metrics",
        }
        if not required_columns.issubset(reader.fieldnames or []):
            raise ReleaseAuthorizationError("release-quality corpus is missing required columns")
        rows = list(reader)
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ReleaseAuthorizationError("release-quality corpus is not valid CSV") from exc
    if any(None in row for row in rows):
        raise ReleaseAuthorizationError("release-quality corpus contains extra columns")
    prompt_ids = [value.strip() if isinstance(value := row.get("prompt_id"), str) else "" for row in rows]
    if len(prompt_ids) != QUALITY_PROMPT_COUNT:
        raise ReleaseAuthorizationError("release-quality corpus must contain exactly 100 prompts")
    if any(not prompt_id for prompt_id in prompt_ids) or len(set(prompt_ids)) != QUALITY_PROMPT_COUNT:
        raise ReleaseAuthorizationError("release-quality corpus prompt IDs are invalid")
    for row in rows:
        archetype_value = row.get("expected_archetype")
        expected_archetype = archetype_value.strip() if isinstance(archetype_value, str) else ""
        if expected_archetype not in _ARCHETYPE_ALIASES:
            raise ReleaseAuthorizationError("release-quality corpus expected archetype is invalid")
        _corpus_metric_list(row, "expected_metrics", ",")
        critical_metrics = _corpus_metric_list(row, "critical_metrics", ";")
        if not set(critical_metrics).issubset(_corpus_metric_list(row, "expected_metrics", ",")):
            raise ReleaseAuthorizationError("release-quality corpus critical metrics are not expected metrics")

    long_lived_state = evidence.get("long_lived_state")
    if not isinstance(long_lived_state, dict):
        raise ReleaseAuthorizationError("release-quality long-lived state identity is missing")
    long_lived_fingerprint = long_lived_state.get("fingerprint")
    if not isinstance(long_lived_fingerprint, str) or FINGERPRINT_PATTERN.fullmatch(long_lived_fingerprint) is None:
        raise ReleaseAuthorizationError("release-quality long-lived state fingerprint is invalid")
    if (
        _validate_quality_tenant(long_lived_state.get("tenant"), "release-quality long-lived state")
        != evaluation_tenant
    ):
        raise ReleaseAuthorizationError("release-quality long-lived state tenant does not match evaluation")
    try:
        state_limits = validate_state_snapshot_limits(
            long_lived_state.get("limits"),
            require_release_limits=True,
        )
    except ReleaseQualityContractError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    if state_limits != STATE_SNAPSHOT_LIMITS:
        raise ReleaseAuthorizationError("release-quality state size contract does not match the release contract")
    state_manifest_metadata = long_lived_state.get("manifest")
    state_manifest_payload = members[QUALITY_STATE_MANIFEST_PATH]
    if (
        not isinstance(state_manifest_metadata, dict)
        or state_manifest_metadata.get("path") != QUALITY_STATE_MANIFEST_PATH
        or state_manifest_metadata.get("sha256") != hashlib.sha256(state_manifest_payload).hexdigest()
    ):
        raise ReleaseAuthorizationError("release-quality long-lived state manifest digest does not match")
    state_manifest = _load_json_member(state_manifest_payload, "release-quality long-lived state manifest")
    try:
        manifest_tenant, manifest_fingerprint, manifest_limits = validate_admitted_state_manifest(
            state_manifest,
            expected_tenant=evaluation_tenant,
            expected_fingerprint=long_lived_fingerprint,
            require_release_limits=True,
        )
    except ReleaseQualityContractError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    if (
        manifest_tenant != evaluation_tenant
        or manifest_fingerprint != long_lived_fingerprint
        or manifest_limits != state_limits
    ):
        raise ReleaseAuthorizationError("release-quality long-lived state manifest does not match evidence")

    report_metadata = evidence.get("reports")
    if not isinstance(report_metadata, dict) or set(report_metadata) != set(QUALITY_REPORT_PATHS):
        raise ReleaseAuthorizationError("release-quality report metadata is incomplete")
    fingerprints = {"clean": _CLEAN_STATE_FINGERPRINT, "long-lived": long_lived_fingerprint}
    for state, path in QUALITY_REPORT_PATHS.items():
        metadata = report_metadata.get(state)
        report_payload = members[path]
        if (
            not isinstance(metadata, dict)
            or metadata.get("path") != path
            or metadata.get("sha256") != hashlib.sha256(report_payload).hexdigest()
        ):
            raise ReleaseAuthorizationError(f"{state} quality report digest does not match")
        report = _load_json_member(report_payload, f"{state} quality report")
        _validate_quality_report(
            report,
            state=state,
            expected_fingerprint=fingerprints[state],
            expected_tenant=evaluation_tenant,
            corpus_rows=rows,
        )
    return evidence


def _download_actions_artifact_once(request: Request) -> bytes:
    opener = build_github_api_opener(allow_https_redirects=True)
    with opener.open(request, timeout=30) as response:
        declared = response.headers.get("Content-Length")
        if declared is not None:
            try:
                declared_size = int(declared)
            except ValueError as exc:
                raise ReleaseAuthorizationError("release-quality artifact has an invalid Content-Length") from exc
            if declared_size < 0 or declared_size > MAX_QUALITY_ARCHIVE_BYTES:
                raise ReleaseAuthorizationError("release-quality artifact exceeds the size limit")
        payload = response.read(MAX_QUALITY_ARCHIVE_BYTES + 1)
    if len(payload) > MAX_QUALITY_ARCHIVE_BYTES:
        raise ReleaseAuthorizationError("release-quality artifact exceeds the size limit")
    return payload


def _download_actions_artifact(request: Request) -> bytes:
    try:
        return _retry_idempotent(
            lambda: _download_actions_artifact_once(request),
            label="release-quality evidence download",
            retryable=_is_transient_http_error,
        )
    except ReleaseAuthorizationError:
        raise
    except (HTTPError, URLError, OSError) as exc:
        raise ReleaseAuthorizationError(f"could not download release-quality evidence: {exc}") from exc


def require_successful_release_quality(
    *,
    expected_sha: str,
    repository_slug: str,
    token: str,
    api_url: str,
) -> None:
    """Require exact-SHA quality success plus an independently checked artifact."""
    expected_sha = _validate_sha(expected_sha)
    if REPOSITORY_PATTERN.fullmatch(repository_slug) is None:
        raise ReleaseAuthorizationError("GITHUB_REPOSITORY is invalid")
    if not token:
        raise ReleaseAuthorizationError("GITHUB_TOKEN is required")
    try:
        api_url = validate_github_api_url(api_url)
    except ReleaseGitHubAPIError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc

    query = urlencode(
        {
            "head_sha": expected_sha,
            "branch": "main",
            "event": "workflow_dispatch",
            "status": "completed",
            "per_page": 100,
        }
    )
    runs_url = f"{api_url.rstrip('/')}/repos/{repository_slug}/actions/workflows/" f"{QUALITY_WORKFLOW}/runs?{query}"
    try:
        runs_request = authenticated_github_request(
            runs_url,
            api_url=api_url,
            token=token,
            accept="application/vnd.github+json",
        )
    except ReleaseGitHubAPIError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    runs = _load_actions_response(runs_request).get("workflow_runs")
    if not isinstance(runs, list):
        raise ReleaseAuthorizationError("release-quality authorization response has no workflow run list")
    accepted = [
        run
        for run in runs
        if isinstance(run, dict)
        and run.get("head_sha") == expected_sha
        and run.get("head_branch") == "main"
        and run.get("event") == "workflow_dispatch"
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
        and run.get("path") == QUALITY_WORKFLOW_RUN_PATH
    ]
    if not accepted:
        raise ReleaseAuthorizationError(
            "The tagged commit has no successful completed exact-SHA main release-quality run; publication is denied"
        )
    selected = max(
        accepted,
        key=lambda run: (
            run.get("id") if isinstance(run.get("id"), int) else -1,
            run.get("run_attempt") if isinstance(run.get("run_attempt"), int) else -1,
        ),
    )
    run_id = _positive_integer(selected.get("id"), "release-quality workflow run ID")
    run_attempt = _positive_integer(selected.get("run_attempt"), "release-quality workflow run attempt")

    artifacts_url = f"{api_url.rstrip('/')}/repos/{repository_slug}/actions/runs/{run_id}/artifacts?per_page=100"
    try:
        artifacts_request = authenticated_github_request(
            artifacts_url,
            api_url=api_url,
            token=token,
            accept="application/vnd.github+json",
        )
    except ReleaseGitHubAPIError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    artifacts = _load_actions_response(artifacts_request).get("artifacts")
    if not isinstance(artifacts, list):
        raise ReleaseAuthorizationError("release-quality authorization response has no artifact list")
    expected_name = f"release-quality-evidence-{expected_sha}-{run_id}-{run_attempt}"
    matching = [item for item in artifacts if isinstance(item, dict) and item.get("name") == expected_name]
    if len(matching) != 1:
        raise ReleaseAuthorizationError("release-quality run has missing or duplicate evidence artifacts")
    artifact = matching[0]
    if artifact.get("expired") is not False:
        raise ReleaseAuthorizationError("release-quality evidence artifact is expired")
    size = _positive_integer(artifact.get("size_in_bytes"), "release-quality artifact size")
    if size > MAX_QUALITY_ARCHIVE_BYTES:
        raise ReleaseAuthorizationError("release-quality artifact exceeds the size limit")
    digest = artifact.get("digest")
    download_url = artifact.get("archive_download_url")
    workflow_run = artifact.get("workflow_run")
    if not isinstance(digest, str) or DIGEST_PATTERN.fullmatch(digest) is None:
        raise ReleaseAuthorizationError("release-quality artifact digest is missing or invalid")
    if (
        not isinstance(workflow_run, dict)
        or workflow_run.get("id") != run_id
        or workflow_run.get("head_sha") != expected_sha
        or workflow_run.get("head_branch") != "main"
    ):
        raise ReleaseAuthorizationError("release-quality artifact workflow identity does not match")
    if not isinstance(download_url, str):
        raise ReleaseAuthorizationError("release-quality artifact download URL is missing")
    try:
        download_request = authenticated_github_request(
            download_url,
            api_url=api_url,
            token=token,
            accept="application/vnd.github+json",
        )
    except ReleaseGitHubAPIError as exc:
        raise ReleaseAuthorizationError(str(exc)) from exc
    archive_payload = _download_actions_artifact(download_request)
    validate_release_quality_archive(
        archive_payload,
        expected_archive_digest=digest,
        expected_corpus_digest=_canonical_quality_corpus_digest(Path.cwd()),
        expected_sha=expected_sha,
        expected_repository=repository_slug,
        expected_run_id=run_id,
        expected_run_attempt=run_attempt,
    )


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ReleaseAuthorizationError(f"{name} is required")
    return value


def main() -> int:
    try:
        expected_sha = _required_environment("GITHUB_SHA")
        prove_fresh_tag_on_main(
            Path.cwd(),
            expected_sha=expected_sha,
            tag_name=_required_environment("GITHUB_REF_NAME"),
        )
        require_successful_main_ci(
            expected_sha=expected_sha,
            repository_slug=_required_environment("GITHUB_REPOSITORY"),
            token=_required_environment("GITHUB_TOKEN"),
            api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        )
        require_successful_release_quality(
            expected_sha=expected_sha,
            repository_slug=_required_environment("GITHUB_REPOSITORY"),
            token=_required_environment("GITHUB_TOKEN"),
            api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        )
    except ReleaseAuthorizationError as exc:
        print(f"Release publication authorization failed: {exc}", file=sys.stderr)
        return 1
    print("Release publication authorized by exact-SHA CI and release-quality evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
