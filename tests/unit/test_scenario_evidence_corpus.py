"""Frozen scenario provenance and recorded-baseline/decision replay gates.

This suite consumes a checked-in capture artifact. It never acquires telemetry,
starts a model or manufactures a missing baseline investigation.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import re
import socket
import sqlite3
import subprocess
import tarfile
from collections import Counter
from copy import deepcopy
from datetime import datetime
from itertools import zip_longest
from pathlib import Path, PurePosixPath

from tests.eval import evidence_decisions as decisions

FIXTURE = Path(__file__).parents[1] / "eval/fixtures/scenario_tev_corpus_v1.tar.gz"
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_EXPANDED_BYTES = 256 * 1024 * 1024
MAX_MEMBERS = 1000
MAX_MEMBER_BYTES = 32 * 1024 * 1024
ROOT = "scenario_tev_corpus_v1/"


def _octal(field: bytes) -> int:
    value = field.rstrip(b"\0 ").lstrip(b" ")
    assert value and all(character in b"01234567" for character in value)
    return int(value, 8)


def _members() -> dict[str, bytes]:
    assert FIXTURE.is_file(), "The frozen scenario capture artifact must exist before this gate can pass"
    assert not FIXTURE.is_symlink() and FIXTURE.stat().st_size <= MAX_ARCHIVE_BYTES
    with gzip.open(FIXTURE, "rb") as source:
        expanded = source.read(MAX_EXPANDED_BYTES + 1)
    assert len(expanded) <= MAX_EXPANDED_BYTES and len(expanded) % 512 == 0
    result: dict[str, bytes] = {}
    offset = 0
    while offset + 512 <= len(expanded):
        header = expanded[offset : offset + 512]
        if header == bytes(512):
            # Physical inspection prevents convenience archive iteration from
            # hiding extension headers, concatenated members or trailing input.
            assert len(expanded) - offset >= 1024
            assert not any(expanded[offset:])
            break
        assert header[257:263] == b"ustar\0" and header[263:265] == b"00"
        assert header[156:157] in (
            b"0",
            b"\0",
        ), "Only regular physical USTAR members are admitted"
        assert _octal(header[148:156]) == sum(header[:148]) + 8 * 32 + sum(header[156:])
        name = header[:100].split(b"\0", 1)[0].decode("utf-8")
        prefix = header[345:500].split(b"\0", 1)[0].decode("utf-8")
        name = prefix + "/" + name if prefix else name
        selected = PurePosixPath(name)
        assert name and not selected.is_absolute() and ".." not in selected.parts
        assert str(selected) == name and "\\" not in name and "\0" not in name
        assert name not in result
        size = _octal(header[124:136])
        assert size <= MAX_MEMBER_BYTES
        start, end = offset + 512, offset + 512 + size
        assert end <= len(expanded)
        result[name] = expanded[start:end]
        assert len(result) <= MAX_MEMBERS
        offset = start + ((size + 511) // 512) * 512
    else:
        raise AssertionError("USTAR termination is missing")
    assert result
    return result


def test_scenario_corpus_is_a_bounded_regular_ustar_artifact():
    members = _members()
    assert all(
        not name.endswith((".gguf", ".safetensors", ".prom", ".env", ".sqlite", ".db", ".log")) for name in members
    )
    assert all(
        not any(part in {".git", ".ollama", ".herald", "runtime", "__pycache__"} for part in PurePosixPath(name).parts)
        for name in members
    )
    files = _files()
    licenses = _json(files, "data_licenses.json")
    gamma = licenses["gamma"]
    assert gamma["license"] == "Apache-2.0"
    assert (
        gamma["license_evidence_url"]
        == "https://www.kaggle.com/api/v1/datasets/view/gagansomashekar/microservices-bottleneck-detection-dataset"
    )
    assert gamma["license_sha256"] == _sha(files[gamma["license_path"]])
    assert b"Apache License" in files[gamma["license_path"]]
    assert gamma["notice_sha256"] == _sha(files[gamma["notice_path"]])
    assert licenses["clickstack"]["raw_timeseries_included"] is False
    assert licenses["clickstack"]["redistribution_license_verified"] is False
    assert _json(files, "stack_teardown.json")["owned_resources_remaining"] == 0
    assert _json(files, "stack_teardown.json")["all_13_runtime_roots_terminal_zero"] is True
    inventory = _json(files, "publication_inventory.json")
    assert inventory["raw_source_mutations"] is False
    assert {item["path"] for item in inventory["members"]} == set(files) - {"publication_inventory.json"}
    for item in inventory["members"]:
        assert item["bytes"] == len(files[item["path"]])
        assert item["sha256"] == _sha(files[item["path"]])


def _files() -> dict[str, bytes]:
    members = _members()
    assert all(name.startswith(ROOT) for name in members)
    return {name.removeprefix(ROOT): body for name, body in members.items()}


def _json(files: dict[str, bytes], name: str):
    assert name in files, f"Missing frozen corpus member: {name}"
    return decisions.decode_json(files[name])


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _instant(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert result.tzinfo is not None
    return result


def _write_files(files: dict[str, bytes], destination: Path) -> None:
    destination.mkdir(mode=0o700)
    for name, body in files.items():
        selected = destination / name
        selected.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with selected.open("xb") as output:
            output.write(body)
        selected.chmod(0o600)


def test_source_freeze_counts_recordings_and_keeps_control_truth_hidden():
    files = _files()
    source = _json(files, "source_manifest.json")
    plan = _json(files, "source_plan.json")
    freeze = _json(files, "source_freeze.json")
    labels = _json(files, "labels.json")
    assert freeze["source_manifest_sha256"] == _sha(files["source_manifest.json"])
    assert freeze["source_plan_sha256"] == _sha(files["source_plan.json"])
    assert freeze["label_rules_sha256"] == _sha(decisions.canonical(labels["rules"]))
    assert _instant(freeze["frozen_at"]) <= _instant(labels["frozen_at"])
    assert len(source["cases"]) == len(plan["cases"]) == 13
    baseline_commit = source["baseline_code_sha"]
    assert re.fullmatch("[0-9a-f]{40}", baseline_commit)
    if baseline_commit != "1af49fd1ef552446ff9856164e0d9811f5d76e85":
        assert source[
            "baseline_changes"
        ], "A baseline repair needs its exact source and rationale recorded before capture"
    assert {item["case_id"] for item in source["cases"]} == {item["case_id"] for item in plan["cases"]}
    assert len({item["incident_id"] for item in plan["cases"]}) == 13
    assert len({item["family_id"] for item in plan["cases"]}) == 5
    assert Counter(item["case_class"] for item in plan["cases"]) == {
        "controlled_application_cache_incident": 1,
        "injected_resource_window": 4,
        "pre_interference_healthy": 4,
        "application_symptom_resource_evidence_absent": 4,
    }
    for case in plan["cases"]:
        if case["case_class"] == "pre_interference_healthy":
            fault = min(item["source_start"] for item in case["scorer_only_ground_truth"]["interference_windows"])
            assert case["source_window"]["end"] < fault
    input_replays = [body for name, body in files.items() if name.startswith("input_replays/")]
    assert sum(map(len, input_replays)) <= 8 * 1024 * 1024


def test_labels_bind_the_capture_and_precede_collection():
    files = _files()
    labels = _json(files, "labels.json")
    collection = _json(files, "collection.json")
    source_cases = _json(files, "source_plan.json")["cases"]
    assert labels["source_freeze_sha256"] == _sha(files["source_freeze.json"])
    assert labels["capture_index_sha256"] == _sha(files["capture_index.json"])
    assert collection["labels_sha256"] == _sha(files["labels.json"])
    assert collection["experiment_manifest_sha256"] == _sha(files["experiment/manifest.json"])
    assert _instant(labels["frozen_at"]) <= _instant(collection["started_at"])
    label_cases = {item["case_id"]: item for item in labels["cases"]}
    captures = {item["case_id"]: item for item in _json(files, "capture_index.json")["cases"]}
    assert len(label_cases) == 13
    for case in source_cases:
        record = label_cases[case["case_id"]]
        assert record["mapping_reason"] and "source_coverage" in record
        if not captures[case["case_id"]]["capture_available"]:
            assert record["labels"] is None
            assert record["source_coverage"]["capture_failed"] is True
            continue
        assert record["labels"] is not None
        if case["case_class"] in {
            "pre_interference_healthy",
            "application_symptom_resource_evidence_absent",
        }:
            assert record["labels"]["acceptable"] == []
            assert record["labels"]["required_abstention"] is True


def _unzipped(body: bytes):
    return gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb")


def _compare_otlp_times(before, after, offset_ns: int, seen: list[int]) -> None:
    assert type(before) is type(after)
    if isinstance(before, dict):
        assert before.keys() == after.keys()
        for key, value in before.items():
            if key.endswith("TimeUnixNano"):
                assert type(value) in (str, int) and type(after[key]) is type(value)
                original, shifted = int(value), int(after[key])
                assert shifted == original + offset_ns if original else shifted == 0
                if original:
                    seen.append(shifted)
            else:
                _compare_otlp_times(value, after[key], offset_ns, seen)
    elif isinstance(before, list):
        assert len(before) == len(after)
        for original, shifted in zip(before, after, strict=True):
            _compare_otlp_times(original, shifted, offset_ns, seen)
    else:
        assert before == after


def test_per_case_admission_clock_preserves_values_and_is_frozen_before_baseline():
    files = _files()
    source = _json(files, "source_manifest.json")
    assert source["schema_version"] == "scenario-source-manifest/2"
    assert source["timing_profile"] == "per-case-admission-clock/1"
    prior = source["invalid_prior_cohort"]
    assert prior["entire_cohort_excluded"] is True and prior["cases"] == 13
    assert prior.get("tev_calls", prior.get("teV_calls")) == 0
    assert prior["all_runtime_roots_terminal_zero"] is True
    assert re.fullmatch("[0-9a-f]{64}", prior["source_manifest_sha256"]) and prior["reason"]
    source_cases = {item["case_id"]: item for item in source["cases"]}
    captures = _json(files, "capture_index.json")["cases"]
    external = _json(files, "external_inputs.json")
    assert external["source_freeze_sha256"] == _sha(files["source_freeze.json"])
    assert external["capture_index_sha256"] == _sha(files["capture_index.json"])
    assert len(external["cases"]) == 1
    omitted = external["cases"][0]
    assert omitted["case_id"] == "clickstack_payment_cache"
    assert {item["case_id"] for item in captures} == set(source_cases)
    admitted_payloads = []
    for entry in captures:
        case_id = entry["case_id"]
        case = source_cases[case_id]
        admission = entry["admitted_replay"]
        record_name = next(
            name
            for name in (
                f"captures/{case_id}/admission_freeze.json",
                f"admitted_replays/{case_id}.admission.json",
            )
            if name in files
        )
        assert admission["admission_freeze_sha256"] == _sha(files[record_name])
        record = _json(files, record_name)
        assert record["case_id"] == case_id and record["timing_profile"] == source["timing_profile"]
        for key, value in admission.items():
            if key != "admission_freeze_sha256":
                assert record[key] == value
        assert admission["base_metric_sha256"] == case["metric_sha256"]
        assert type(admission["offset_ms"]) is int
        assert admission["source_end_ms"] == source["frozen_end_ms"]
        assert admission["end_ms"] == admission["source_end_ms"] + admission["offset_ms"]
        started = _instant(entry["baseline_started_at"])
        frozen = _instant(admission["frozen_at"])
        assert frozen <= started
        freshness_ms = started.timestamp() * 1000 - admission["end_ms"]
        assert 0 <= freshness_ms <= 60_000
        assert abs(entry["replay_freshness_ms"] - freshness_ms) < 2
        assert 0 <= frozen.timestamp() * 1000 - admission["end_ms"] <= 60_000
        if case_id == "clickstack_payment_cache":
            assert case["dataset"] == "clickstack" and case["format"] == "otlp_jsonl"
            assert omitted["omitted_members"] == [
                case["metric_path"],
                admission["metric_path"],
            ]
            assert all(name not in files for name in omitted["omitted_members"])
            for selected, expected in (
                (omitted["base_input"], case),
                (omitted["admitted_input"], admission),
            ):
                assert selected["path"] == expected["metric_path"]
                assert selected["sha256"] == expected["metric_sha256"]
                assert selected["expanded_sha256"] == expected["expanded_sha256"]
                assert selected["expanded_bytes"] == expected["expanded_bytes"]
            assert omitted["admission_record"]["sha256"] == _sha(files[record_name])
            proof_ref = omitted["transform_proof"]
            assert proof_ref["sha256"] == _sha(files[proof_ref["path"]])
            proof = _json(files, proof_ref["path"])
            assert proof["independent_timestamp_reverification"] is False
            assert proof["local_timestamp_transform_reverified"] is True
            assert proof["source_freeze_sha256"] == external["source_freeze_sha256"]
            assert proof["admission_record_sha256"] == omitted["admission_record"]["sha256"]
            assert proof["base_input"] == omitted["base_input"]
            assert proof["admitted_input"] == omitted["admitted_input"]
            assert proof["record_count"] == case["transform"]["record_count"] == 667
            assert proof["timestamp_count"] > 0
            assert proof["offset_ns"] == admission["offset_ms"] * 1_000_000
            assert proof["admitted_max_timestamp_ns"] == admission["end_ms"] * 1_000_000
            assert proof["base_non_time_sha256"] == proof["admitted_non_time_sha256"]
            assert re.fullmatch("[0-9a-f]{64}", proof["base_non_time_sha256"])
            assert all(item["sha256"] == _sha(files[item["path"]]) for item in proof["operator_sources"])
            assert omitted["source_archive"]["url"] == "https://storage.googleapis.com/hyperdx/sample.tar.gz"
            assert omitted["retrieval_recipe"]["independent_timestamp_reverification"] is False
            continue
        assert case["dataset"] == "gamma" and case["format"] == "prometheus"
        base = files[case["metric_path"]]
        admitted = files[admission["metric_path"]]
        admitted_payloads.append(admitted)
        assert _sha(base) == case["metric_sha256"]
        assert _sha(admitted) == admission["metric_sha256"]
        digest = hashlib.sha256()
        base_digest = hashlib.sha256()
        non_time = hashlib.sha256()
        expanded = 0
        base_expanded = 0
        seen = []
        with _unzipped(base) as originals, _unzipped(admitted) as shifted:
            for before, after in zip_longest(originals, shifted):
                assert before is not None and after is not None
                expanded += len(after)
                base_expanded += len(before)
                assert expanded <= 64 * 1024 * 1024
                digest.update(after)
                base_digest.update(before)
                if case["format"] == "otlp_jsonl":
                    _compare_otlp_times(
                        json.loads(before),
                        json.loads(after),
                        admission["offset_ms"] * 1_000_000,
                        seen,
                    )
                else:
                    assert case["format"] == "prometheus"
                    old_values, old_time = before.rstrip(b"\n").rsplit(b" ", 1)
                    new_values, new_time = after.rstrip(b"\n").rsplit(b" ", 1)
                    assert old_values == new_values
                    non_time.update(old_values + b"\n")
                    assert int(new_time) == int(old_time) + admission["offset_ms"]
                    seen.append(int(new_time) * 1_000_000)
        assert seen and max(seen) == admission["end_ms"] * 1_000_000
        assert digest.hexdigest() == admission["expanded_sha256"]
        assert expanded == admission["expanded_bytes"]
        assert base_digest.hexdigest() == case["expanded_sha256"]
        assert base_expanded == case["expanded_bytes"]
        assert non_time.hexdigest() == case["transform"]["non_timestamp_sha256"]
        assert len(seen) == case["transform"]["sample_count"]
    assert sum(map(len, admitted_payloads)) <= 8 * 1024 * 1024


def test_all_source_cases_retain_actual_capture_or_failed_audit(tmp_path, monkeypatch):
    from tacit.investigation_contract import InvestigationContract
    from tacit.investigation_replay import InvestigationReplaySnapshot, rebuild_contract

    files = _files()
    source = _json(files, "source_manifest.json")
    replay_audit = _json(files, "acquisition_replay_audit.json")
    assert replay_audit["baseline_code_sha"] == source["baseline_code_sha"]
    assert replay_audit["network_database_subprocess_denied"] is True
    replay_cases = {item["case_id"]: item for item in replay_audit["cases"]}
    assert len(replay_cases) == 9
    index = _json(files, "capture_index.json")
    assert len(index["cases"]) == 13
    cases = {item["case_id"]: item for item in index["cases"]}
    assert set(cases) == {item["case_id"] for item in source["cases"]}
    destination = tmp_path / "corpus"
    _write_files(files, destination)
    manifest = decisions.load_manifest(destination / "experiment/manifest.json")
    experiments = {item["case_id"]: item for item in manifest["cases"]}
    assert set(experiments) == set(cases)

    def denied(*args, **kwargs):
        raise AssertionError("Captured corpus validation attempted an external capability")

    monkeypatch.setattr(socket, "socket", denied)
    monkeypatch.setattr(sqlite3, "connect", denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    for case_id in cases:
        prefix = f"captures/{case_id}/"
        execution = _json(files, prefix + "execution_manifest.json")
        audit = _json(files, prefix + "run_audit.json")
        response = _json(files, prefix + "response.json")
        assert execution and audit
        assert execution["baseline_code_sha"] == source["baseline_code_sha"]
        _json(files, prefix + "ownership.json")
        path = prefix + "assessment.tar.gz"
        assert cases[case_id]["capture_available"] == (path in files)
        terminal = cases[case_id].get("runtime_terminal")
        if cases[case_id]["measurement_completed"]:
            assert terminal["terminal_zero"] is True and terminal["root_state"] == "closed"
        if path not in files:
            # A failed run is source coverage, not a fabricated ranking or
            # a successfully captured contract with an empty candidate list.
            assert response is None or isinstance(response, dict)
            assert response is None or response.get("investigation_status") != "success"
            assert case_id not in replay_cases
            assert experiments[case_id]["bundle_sha256"] == "0" * 64
            continue
        case = experiments[case_id]
        capture = decisions.load_bundle(destination / path, case, {"bundles": 0, "expanded": 0})
        with tarfile.open(fileobj=io.BytesIO(files[path]), mode="r:gz") as bundle:
            contract = InvestigationContract.model_validate_json(bundle.extractfile("contract.json").read())
        snapshot = InvestigationReplaySnapshot.model_validate(capture)
        recorded_replay = replay_cases[case_id]
        assert recorded_replay["bundle_sha256"] == _sha(files[path]) == cases[case_id]["bundle_sha256"]
        assert recorded_replay["snapshot_sha256"] == _sha(files[prefix + "snapshot.json"])
        assert recorded_replay["input_fingerprint"] == contract.runtime.input_fingerprint
        assert recorded_replay["output_fingerprint"] == contract.runtime.output_fingerprint
        assert recorded_replay["decisions_and_provenance_equal"] is True
        rebuilt = rebuild_contract(snapshot)
        assert rebuilt.runtime.input_fingerprint == contract.runtime.input_fingerprint
        assert rebuilt.runtime.output_fingerprint == contract.runtime.output_fingerprint
        assert rebuilt.decision_log == contract.decision_log
        assert rebuilt.provenance == contract.provenance
        assert snapshot.investigation_id == contract.investigation.id == case["investigation_id"]
        assert snapshot.revision == contract.investigation.revision == case["revision"]
        assert cases[case_id]["investigation_id"] == case["investigation_id"]
        assert cases[case_id]["input_fingerprint"] == contract.runtime.input_fingerprint
        assert cases[case_id]["output_fingerprint"] == cases[case_id]["replay_output_fingerprint"]
        assert cases[case_id]["output_fingerprint"] == contract.runtime.output_fingerprint
        assert snapshot.culprit_ranking.model_dump(mode="json") == capture["culprit_ranking"]
        ranking = capture.get("baseline_culprit_ranking") or capture["culprit_ranking"]
        assert cases[case_id]["baseline_candidates"] == ranking
        original = decisions.captured_projection(capture)
        reordered = deepcopy(capture)
        selected_ranking = reordered.get("baseline_culprit_ranking") or reordered["culprit_ranking"]
        selected_ranking["candidates"].reverse()
        for position, candidate in enumerate(selected_ranking["candidates"], 1):
            candidate["rank"], candidate["score"] = position, position / (len(selected_ranking["candidates"]) + 1)
        permuted = decisions.captured_projection(reordered)
        # Protected identities and approved visible facts survive changes in
        # baseline order/score; labels cannot silently become array positions.
        assert original["preimages"] == permuted["preimages"]
        assert original["state"] == permuted["state"]
        assert set(case["labels"]["acceptable"] + case["labels"]["contradicted"]) <= set(permuted["preimages"])


def test_frozen_requests_hide_labels_and_do_not_inflate_no_call_cases(tmp_path):
    files = _files()
    destination = tmp_path / "corpus"
    _write_files(files, destination)
    manifest_path = destination / "experiment/manifest.json"
    manifest = decisions.load_manifest(manifest_path)
    labels = {item["case_id"]: item for item in _json(files, "labels.json")["cases"]}
    report = _json(files, "scores/report.json")
    forbidden_fields = {
        "ground_truth",
        "source_scenario",
        "fault_types",
        "root_cause_nodes",
        "root_cause_services",
        "interference_intensity",
        "labels",
        "acceptable",
        "contradicted",
        "required_abstention",
    }
    count = Counter()
    for case in manifest["cases"]:
        authoritative_labels = labels[case["case_id"]]["labels"]
        if authoritative_labels is None:
            assert case["labels"] == {
                "acceptable": [],
                "contradicted": [],
                "required_abstention": True,
            }
            assert case["bundle_sha256"] == "0" * 64
            assert case["bundle_path"].endswith("/assessment.tar.gz")
            assert not (manifest_path.parent / case["bundle_path"]).exists()
        else:
            assert case["labels"] == authoritative_labels
        projection, packets, failed = decisions.load_case(
            manifest_path, case, manifest["models"], {"bundles": 0, "expanded": 0}
        )
        count[projection["status"]] += 1
        assert failed == (projection["status"] == "failed")
        if authoritative_labels is None:
            assert failed and packets == {}
        if projection["status"] != "eligible":
            assert packets == {} and all(value is None for value in case["request_hashes"].values())
        for key, body in packets.items():
            assert files[f"prepared/requests/{case['case_id']}.{key}.json"] == body
            packet = decisions.decode_json(body)

            def inspect(value):
                if isinstance(value, dict):
                    assert not forbidden_fields.intersection(value)
                    for item in value.values():
                        inspect(item)
                elif isinstance(value, list):
                    for item in value:
                        inspect(item)

            inspect(packet)
            for candidate in packet["state"]["candidates"]:
                assert not {"rank", "score", "confidence"}.intersection(candidate)
    assert sum(count.values()) == report["counts"]["incidents"] == 13
    assert count["eligible"] == report["counts"]["model_eligible"]
    assert count["policy_abstention"] == report["counts"]["policy_no_call"]
    assert count["deterministic"] == report["counts"]["deterministic_no_call"]
    assert count["resource_ineligible"] == report["counts"]["resource_ineligible"]
    assert count["failed"] == report["counts"]["failed_cases"]


def test_portable_frozen_scenario_scores_replay_without_external_capabilities(tmp_path, monkeypatch):
    files = _files()
    destination = tmp_path / "corpus"
    _write_files(files, destination)

    def denied(*args, **kwargs):
        raise AssertionError("Offline corpus replay attempted an external capability")

    monkeypatch.setattr(socket, "socket", denied)
    monkeypatch.setattr(sqlite3, "connect", denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    args = [
        "--manifest",
        str(destination / "experiment/manifest.json"),
        "--responses",
        str(destination / "responses"),
    ]
    codes = [
        decisions.main(["score", *args, "--out", str(destination / name)]) for name in ("replay_one", "replay_two")
    ]
    assert codes[0] == codes[1]
    before = files["scores/report.json"]
    first = (destination / "replay_one/report.json").read_bytes()
    second = (destination / "replay_two/report.json").read_bytes()
    assert before == first == second
    report = json.loads(first)
    assert report["counts"]["incidents"] == 13
    assert len({row["incident_id"] for row in map(json.loads, files["scores/rows.jsonl"].splitlines())}) == 13
    assert not report["comparisons"]["tev"]["gate_passed"]  # This is a small development corpus.


def test_shadow_abstention_protocol_receipts_and_pure_replay(tmp_path, monkeypatch):
    files = _files()
    destination = tmp_path / "corpus"
    _write_files(files, destination)

    def denied(*args, **kwargs):
        raise AssertionError("Shadow replay attempted an external capability")

    monkeypatch.setattr(socket, "socket", denied)
    monkeypatch.setattr(sqlite3, "connect", denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    module_path = destination / "shadow_operator/shadow_diagnostic.py"
    spec = importlib.util.spec_from_file_location("frozen_scenario_shadow", module_path)
    shadow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shadow)
    manifest_path = destination / "shadow/prepared/manifest.json"
    manifest = shadow.load_manifest(manifest_path)
    assert manifest["schema_version"] == "scenario-abstention-diagnostic/1"
    assert manifest["task"] == "single-candidate-evidence-sufficiency/1"
    assert manifest["protocol_sha256"] == _sha(files["shadow/protocol.json"])
    assert files["shadow/protocol.json"] == files["shadow_operator/protocol.json"]
    protocol = _json(files, "shadow/protocol.json")
    assert protocol["max_inference_calls"] == len(manifest["cases"]) == 9
    assert protocol["automatic_retries"] == protocol["variants"] == 0
    assert protocol["attempts"] == 1
    for field, name in (
        ("source_manifest_sha256", "source_manifest.json"),
        ("labels_sha256", "labels.json"),
        ("label_adjudication_sha256", "label_adjudication.json"),
        ("capture_index_sha256", "capture_index.json"),
    ):
        assert manifest[field] == _sha(files[name])
    collection_start = _json(files, "shadow/collection_start.json")
    assert collection_start["manifest_sha256"] == _sha(files["shadow/prepared/manifest.json"])
    assert collection_start["labels_sha256"] == manifest["labels_sha256"]
    assert _instant(_json(files, "labels.json")["frozen_at"]) <= _instant(collection_start["started_at"])
    collection = _json(files, "shadow/collection.json")
    assert collection["diagnostic_digest"] == manifest["diagnostic_digest"]
    assert collection["actual_inference_calls"] == 9 and collection["automatic_retries"] == 0
    ledger = {item["case_id"]: item for item in collection["receipts"]}
    assert len(ledger) == 9
    accounting = _json(files, "shadow/accounting.json")
    assert accounting["collection_sha256"] == _sha(files["shadow/collection.json"])
    assert accounting["score_report_sha256"] == _sha(files["shadow/scores/report.json"])
    assert accounting["http_submissions"] == len(ledger) == 9
    assert accounting["successful_model_responses"] == 8
    assert accounting["rejected_submissions"] == 1 and accounting["automatic_retries"] == 0
    assert accounting["declared_input_groups"] == 2 and accounting["valid_response_input_groups"] == 1
    accounting_rows = {item["case_id"]: item for item in accounting["rows"]}
    assert set(accounting_rows) == set(ledger)
    labels = {item["case_id"]: item for item in _json(files, "labels.json")["cases"]}
    for case in manifest["cases"]:
        cid = case["case_id"]
        body = shadow.rederive(case, manifest["model"], manifest_path)
        assert body == files[f"shadow/prepared/requests/{cid}.json"]
        packet = decisions.decode_json(body)
        assert set(packet["questions"]["priority"]["criteria"]) == {
            case["candidate_id"],
            "insufficient",
        }
        assert len(packet["state"]["candidates"]) == 1
        assert packet["state"]["candidates"][0]["candidate_id"] == case["candidate_id"]
        assert not {"rank", "score", "confidence", "labels"}.intersection(packet["state"]["candidates"][0])
        assert case["expected_choice"] == "insufficient" and labels[cid]["labels"]["required_abstention"] is True
        receipt_name = f"shadow/responses/{cid}.tev.primary.a1.json"
        raw_name = f"shadow/raw/{cid}.tev.primary.a1.json"
        assert ledger[cid]["request_sha256"] == _sha(body) == case["request_sha256"]
        assert ledger[cid]["receipt_sha256"] == _sha(files[receipt_name])
        assert ledger[cid]["raw_response_sha256"] == _sha(files[raw_name])
        receipt = _json(files, receipt_name)
        raw = _json(files, raw_name)
        accounted = accounting_rows[cid]
        assert accounted["http_status"] == ledger[cid]["http_status"]
        assert accounted["usage"] == raw.get("usage")
        assert accounted["prompt_eval_cached_count"] == raw.get("prompt_eval_cached_count")
        assert accounted["error"] == raw.get("error")
        if ledger[cid]["http_status"] == 200:
            assert receipt["status"] == "ok" and receipt["raw"] == _json(files, raw_name)
        else:
            assert receipt["status"] == "error" and receipt["raw"] is None
            assert _json(files, raw_name)["error"]
        result = decisions.validate_receipt(
            receipt,
            manifest["model"],
            {case["candidate_id"], "insufficient"},
            case["request_sha256"],
            wire_profile=manifest["wire_profile"],
        )
        assert result["valid"] == (ledger[cid]["http_status"] == 200)
    assert len({case["request_sha256"] for case in manifest["cases"]}) == 2
    for name in ("replay_one", "replay_two"):
        shadow.score(manifest_path, destination / "shadow/responses", destination / name)
        assert (destination / name / "report.json").read_bytes() == files["shadow/scores/report.json"]
    report = _json(files, "shadow/scores/report.json")
    assert report["schema_version"] == "scenario-abstention-diagnostic-results/1"
    assert report["paired_priority_evaluation"] is False and report["positive_class_coverage"] == 0
    assert report["adoption_gate"] == "not_applicable_diagnostic_only"
    assert report["counts"]["unique_inputs"] == 2
    assert report["counts"]["case_calls"] == 9
    assert report["counts"]["valid"] == sum(item["http_status"] == 200 for item in ledger.values())
    usage_rows = [item for item in accounting_rows.values() if item["usage"] is not None]
    assert accounting["known_reported_input_tokens"] == sum(item["usage"]["input_tokens"] for item in usage_rows)
    assert accounting["known_reported_output_tokens"] == sum(item["usage"]["output_tokens"] for item in usage_rows)
    assert accounting["responses_with_nonzero_reported_cached_tokens"] == sum(
        (item["prompt_eval_cached_count"] or 0) > 0 for item in accounting_rows.values()
    )
