#!/usr/bin/env python3
"""Build a bounded, leakage-resistant GAMMA metrics pilot.

The converter reads directly from the downloaded Kaggle archive. It deliberately
does not expose scenario filenames, interference labels, VM placement, or GAMMA's
precomputed graph labels as telemetry. Application signals are derived from RPC
latency/start columns; infrastructure signals come from the raw Prometheus files.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
import time
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

DEFAULT_ARCHIVE = Path.home() / "Downloads" / "microservices-bottleneck-detection-dataset.zip"
DEFAULT_SCENARIO = "cpu_aug12_25min_200_0"
OPAQUE_SCENARIO_ID = "gamma-0001"
BUCKET_SECONDS = 15

INFRA_NORMALIZED_NAMES = {
    "container_cpu_usage_seconds_total": "gamma_container_cpu_usage_seconds_total",
    "container_memory_usage_bytes": "gamma_container_memory_usage_bytes",
    "container_fs_reads_bytes_total": "gamma_container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total": "gamma_container_fs_writes_bytes_total",
    "container_network_receive_bytes_total": "gamma_container_network_receive_bytes_total",
    "container_network_transmit_bytes_total": "gamma_container_network_transmit_bytes_total",
}

INFRA_CANONICAL_NAMES = {
    **{key: key for key in INFRA_NORMALIZED_NAMES},
    "container_memory_usage_bytes": "container_memory_working_set_bytes",
}

RESOURCE_KEYS = ("cpu", "mem", "net", "io")


def _prometheus_metric_name(value: str) -> str:
    """Make a source filename legal as a Prometheus metric identifier."""
    return re.sub(r"[^a-zA-Z0-9_:]", "_", value)


def _read_json(archive: zipfile.ZipFile, member: str):
    with archive.open(member) as source:
        return json.load(source)


def _read_metric_points(archive: zipfile.ZipFile, member: str) -> list[list[object]]:
    with archive.open(member) as source:
        payload = source.read()
    if not payload.strip():
        return []
    return json.loads(payload)


def _read_rpc_services(archive: zipfile.ZipFile, endpoint: str) -> list[str]:
    member = f"meta-data/meta-data/{endpoint}_ms_rpc_map.csv"
    services: list[str] = []
    with archive.open(member) as source:
        rows = csv.reader(io.TextIOWrapper(source, encoding="utf-8"))
        for row in rows:
            services.append(row[1].strip().split(" ", 1)[0])
    return services


def _placement(archive: zipfile.ZipFile) -> dict[str, str]:
    result: dict[str, str] = {}
    with archive.open("meta-data/meta-data/placement.csv") as source:
        for service, node in csv.reader(io.TextIOWrapper(source, encoding="utf-8")):
            result[service.strip()] = node.strip()
    return result


def _fault_groups(args: dict[str, object]) -> list[dict[str, object]]:
    """Normalize legacy single-resource and newer multi-resource args schemas."""
    legacy_nodes = args.get("bottlenecked_nodes")
    if legacy_nodes:
        fault_type = str(args.get("bottleneck_type") or "cpu").lower()
        return [
            {
                "fault_type": fault_type,
                "nodes": list(legacy_nodes),
                "intensity": list(args.get("interference_percentage") or []),
            }
        ]

    groups = []
    for resource in RESOURCE_KEYS:
        nodes = args.get(f"{resource}_bottlenecked_nodes")
        if nodes:
            groups.append(
                {
                    "fault_type": "memory" if resource == "mem" else resource,
                    "nodes": list(nodes),
                    "intensity": list(args.get(f"{resource}_interference_percentage") or []),
                }
            )
    if not groups:
        raise ValueError("scenario args contain no bottlenecked nodes")
    return groups


def _labels(values: dict[str, str]) -> str:
    escaped = []
    for key, value in sorted(values.items()):
        safe = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        escaped.append(f'{key}="{safe}"')
    return "{" + ",".join(escaped) + "}"


def _series_line(metric: str, labels: dict[str, str], value: float, timestamp: float) -> str:
    return f"{metric}{_labels(labels)} {value:.12g} {int(timestamp * 1000)}"


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(math.ceil(fraction * len(ordered)) - 1, len(ordered) - 1)
    return ordered[max(index, 0)]


def _application_samples(
    archive: zipfile.ZipFile,
    scenario: str,
    *,
    endpoint: str = "compose",
) -> Iterable[tuple[str, dict[str, str], float, float]]:
    member = f"processed_dataset/{endpoint}/multi-modal-data-separate/{scenario}_graph_1.csv"
    rpc_services = _read_rpc_services(archive, endpoint)
    latency_by_bucket: dict[tuple[int, str], list[float]] = defaultdict(list)
    requests_by_bucket: dict[tuple[int, str], int] = defaultdict(int)

    with archive.open(member) as source:
        rows = csv.DictReader(io.TextIOWrapper(source, encoding="utf-8"))
        for row in rows:
            for rpc_index, service in enumerate(rpc_services):
                latency_raw = row.get(f"{rpc_index}_latency", "")
                start_raw = row.get(f"{rpc_index}_start", "")
                if not latency_raw or not start_raw:
                    continue
                latency_us = float(latency_raw)
                timestamp = float(start_raw) / 1_000_000
                if timestamp <= 0 or latency_us < 0:
                    continue
                bucket = int(timestamp // BUCKET_SECONDS) * BUCKET_SECONDS
                key = (bucket, service)
                latency_by_bucket[key].append(latency_us / 1_000_000)
                requests_by_bucket[key] += 1

    for (bucket, service), latencies in sorted(latency_by_bucket.items()):
        labels = {
            "dataset": "gamma",
            "endpoint": endpoint,
            "scenario_id": OPAQUE_SCENARIO_ID,
            "service": service,
        }
        yield "gamma_request_latency_seconds", labels, _percentile(latencies, 0.95), float(bucket)
        yield "gamma_request_rate", labels, requests_by_bucket[(bucket, service)] / BUCKET_SECONDS, float(bucket)


def _infra_samples(
    archive: zipfile.ZipFile,
    scenario: str,
    naming: str,
) -> Iterable[tuple[str, dict[str, str], float, float]]:
    prefix = f"raw_dataset/{scenario}/prom_metrics/"
    for member in archive.namelist():
        if not member.startswith(prefix):
            continue
        filename = member.removeprefix(prefix)
        for source_suffix, normalized_metric in INFRA_NORMALIZED_NAMES.items():
            marker = f"_{source_suffix}"
            if not filename.endswith(marker):
                continue
            service = filename[: -len(marker)]
            labels = {
                "dataset": "gamma",
                "scenario_id": OPAQUE_SCENARIO_ID,
                "service": service,
            }
            if naming == "canonical":
                metric_name = INFRA_CANONICAL_NAMES[source_suffix]
            elif naming == "prefixed":
                metric_name = f"gamma_{INFRA_CANONICAL_NAMES[source_suffix]}"
            elif naming == "raw":
                metric_name = _prometheus_metric_name(filename)
            else:
                metric_name = normalized_metric
            for timestamp, value in _read_metric_points(archive, member):
                yield metric_name, labels, float(value), float(timestamp)
            break


def _interference_windows(
    archive: zipfile.ZipFile,
    scenario: str,
    fault_groups: list[dict[str, object]],
    offset: float,
) -> list[dict[str, object]]:
    pattern = re.compile(r"Bottleneck of type (\w+) with measure ([\d.]+) (starts|ends) at ([\d.]+)")
    windows: list[dict[str, object]] = []
    archive_members = set(archive.namelist())
    for group in fault_groups:
        fault_type = str(group["fault_type"])
        for node in sorted(group["nodes"]):
            prefix = f"raw_dataset/{scenario}/"
            candidates = [
                f"{prefix}{fault_type}_{node}_phases",
                f"{prefix}{node}_phases",
            ]
            member = next((candidate for candidate in candidates if candidate in archive_members), "")
            shared_phase_file = member == f"{prefix}{node}_phases"
            if not member:
                fallbacks = sorted(
                    candidate
                    for candidate in archive_members
                    if candidate.startswith(prefix) and candidate.endswith(f"_{node}_phases")
                )
                if len(fallbacks) != 1:
                    raise ValueError(f"cannot identify phase file for {scenario=} {fault_type=} {node=}: {fallbacks}")
                member = fallbacks[0]
                shared_phase_file = False

            with archive.open(member) as source:
                events = []
                for line in io.TextIOWrapper(source, encoding="utf-8"):
                    match = pattern.search(line)
                    if match:
                        phase_fault_type = match.group(1).lower()
                        if shared_phase_file and phase_fault_type != fault_type:
                            continue
                        events.append(
                            {
                                "phase_fault_type": phase_fault_type,
                                "intensity": float(match.group(2)),
                                "event": match.group(3),
                                "source_time": float(match.group(4)),
                            }
                        )
            for start, end in zip(events[::2], events[1::2], strict=True):
                windows.append(
                    {
                        "node": node,
                        "fault_type": fault_type,
                        "intensity": start["intensity"],
                        "source_start": start["source_time"],
                        "source_end": end["source_time"],
                        "replay_start": start["source_time"] + offset,
                        "replay_end": end["source_time"] + offset,
                    }
                )
    return windows


def _rebase(
    samples: list[tuple[str, dict[str, str], float, float]],
    replay_end: float,
) -> tuple[list[str], dict[str, float]]:
    source_start = min(timestamp for _, _, _, timestamp in samples)
    source_end = max(timestamp for _, _, _, timestamp in samples)
    offset = replay_end - source_end
    lines = [
        _series_line(metric, labels, value, timestamp + offset)
        for metric, labels, value, timestamp in sorted(samples, key=lambda item: item[3])
    ]
    return lines, {
        "source_start": source_start,
        "source_end": source_end,
        "replay_start": source_start + offset,
        "replay_end": replay_end,
        "offset_seconds": offset,
    }


def build(
    archive_path: Path,
    scenario: str,
    mode: str,
    output_dir: Path,
    *,
    naming: str = "normalized",
) -> tuple[Path, Path]:
    with zipfile.ZipFile(archive_path) as archive:
        args = _read_json(archive, f"raw_dataset/{scenario}/args.txt")
        placement = _placement(archive)
        fault_groups = _fault_groups(args)
        bottlenecked_nodes = {node for group in fault_groups for node in group["nodes"]}
        root_cause_services = sorted(service for service, node in placement.items() if node in bottlenecked_nodes)

        samples: list[tuple[str, dict[str, str], float, float]] = []
        if mode in {"application", "combined"}:
            samples.extend(_application_samples(archive, scenario))
        if mode in {"infrastructure", "combined"}:
            samples.extend(_infra_samples(archive, scenario, naming))

        if not samples:
            raise ValueError(f"no samples produced for {mode=}")

        replay_end = time.time() - 60
        lines, timestamp_transform = _rebase(samples, replay_end)
        fault_windows = _interference_windows(
            archive,
            scenario,
            fault_groups,
            timestamp_transform["offset_seconds"],
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_stem = mode if naming == "normalized" else f"{mode}-{naming}"
    metrics_path = output_dir / f"{output_stem}.prom"
    metrics_path.write_text("\n".join(lines) + "\n")

    # This file is scorer-only. Never send its ground_truth block to Tacit.
    manifest = {
        "schema_version": 1,
        "dataset": "gamma",
        "scenario_id": OPAQUE_SCENARIO_ID,
        "evidence_mode": mode,
        "metric_naming": naming,
        "source_archive": archive_path.name,
        "source_scenario": scenario,
        "sample_count": len(samples),
        "timestamp_transform": timestamp_transform,
        "ground_truth": {
            "fault_types": sorted({str(window["fault_type"]) for window in fault_windows}),
            "root_cause_nodes": sorted(bottlenecked_nodes),
            "root_cause_services": root_cause_services,
            "interference_intensity": {str(group["fault_type"]): group["intensity"] for group in fault_groups},
            "interference_windows": fault_windows,
        },
        "prohibited_evidence": [
            "source_scenario",
            "fault_types",
            "root_cause_nodes",
            "root_cause_services",
            "interference_intensity",
            "GAMMA label columns",
        ],
    }
    manifest_path = output_dir / f"{output_stem}.manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return metrics_path, manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO)
    parser.add_argument("--mode", choices=("application", "infrastructure", "combined"), required=True)
    parser.add_argument(
        "--naming",
        choices=("normalized", "canonical", "prefixed", "raw"),
        default="normalized",
        help="Infrastructure metric naming representation for diagnostic arms.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/gamma/pilot"))
    args = parser.parse_args()
    metrics_path, manifest_path = build(
        args.archive,
        args.scenario,
        args.mode,
        args.output_dir,
        naming=args.naming,
    )
    print(metrics_path)
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
