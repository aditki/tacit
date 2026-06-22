import zipfile

from demo.gamma_pilot import (
    INFRA_CANONICAL_NAMES,
    _fault_groups,
    _interference_windows,
    _labels,
    _percentile,
    _prometheus_metric_name,
    _read_metric_points,
    _rebase,
)


def test_percentile_uses_nearest_rank():
    assert _percentile([4.0, 1.0, 3.0, 2.0], 0.95) == 4.0


def test_labels_are_stable_and_escaped():
    assert _labels({"service": 'a"b', "dataset": "gamma"}) == '{dataset="gamma",service="a\\"b"}'


def test_empty_metric_file_is_ignored(tmp_path):
    archive_path = tmp_path / "metrics.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("empty", b"")
    with zipfile.ZipFile(archive_path) as archive:
        assert _read_metric_points(archive, "empty") == []


def test_rebase_preserves_relative_time():
    samples = [
        ("metric", {"service": "a"}, 1.0, 100.0),
        ("metric", {"service": "a"}, 2.0, 130.0),
    ]
    lines, transform = _rebase(samples, replay_end=1_000.0)
    assert transform["replay_start"] == 970.0
    assert transform["replay_end"] == 1_000.0
    assert lines[0].endswith("970000")
    assert lines[1].endswith("1000000")


def test_canonical_memory_name_matches_packaged_archetype():
    assert INFRA_CANONICAL_NAMES["container_memory_usage_bytes"] == "container_memory_working_set_bytes"


def test_raw_metric_filename_is_prometheus_safe_without_hiding_semantics():
    assert (
        _prometheus_metric_name("compose-post-service_container_cpu_usage_seconds_total")
        == "compose_post_service_container_cpu_usage_seconds_total"
    )


def test_fault_groups_support_legacy_single_resource_schema():
    groups = _fault_groups(
        {
            "bottleneck_type": "memory",
            "bottlenecked_nodes": ["node-a"],
            "interference_percentage": [75],
        }
    )

    assert groups == [{"fault_type": "memory", "nodes": ["node-a"], "intensity": [75]}]


def test_fault_groups_support_new_multi_resource_schema():
    groups = _fault_groups(
        {
            "cpu_bottlenecked_nodes": ["node-a"],
            "cpu_interference_percentage": [80],
            "mem_bottlenecked_nodes": ["node-b"],
            "mem_interference_percentage": [90],
        }
    )

    assert groups == [
        {"fault_type": "cpu", "nodes": ["node-a"], "intensity": [80]},
        {"fault_type": "memory", "nodes": ["node-b"], "intensity": [90]},
    ]


def test_interference_windows_use_args_family_with_mislabeled_phase_file(tmp_path):
    archive_path = tmp_path / "gamma.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "raw_dataset/net-run/memory_node-a_phases",
            "Bottleneck of type memory with measure 0.3 starts at 100\n"
            "Bottleneck of type memory with measure 0.3 ends at 120\n",
        )
    with zipfile.ZipFile(archive_path) as archive:
        windows = _interference_windows(
            archive,
            "net-run",
            [{"fault_type": "net", "nodes": ["node-a"], "intensity": [30]}],
            offset=10,
        )

    assert windows == [
        {
            "node": "node-a",
            "fault_type": "net",
            "intensity": 0.3,
            "source_start": 100.0,
            "source_end": 120.0,
            "replay_start": 110.0,
            "replay_end": 130.0,
        }
    ]


def test_interference_windows_filter_shared_phase_events_by_fault_type(tmp_path):
    archive_path = tmp_path / "gamma.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "raw_dataset/mixed-run/node-a_phases",
            "Bottleneck of type cpu with measure 0.8 starts at 100\n"
            "Bottleneck of type cpu with measure 0.8 ends at 120\n"
            "Bottleneck of type memory with measure 0.9 starts at 200\n"
            "Bottleneck of type memory with measure 0.9 ends at 240\n",
        )
    with zipfile.ZipFile(archive_path) as archive:
        windows = _interference_windows(
            archive,
            "mixed-run",
            [
                {"fault_type": "cpu", "nodes": ["node-a"], "intensity": [80]},
                {"fault_type": "memory", "nodes": ["node-a"], "intensity": [90]},
            ],
            offset=10,
        )

    assert windows == [
        {
            "node": "node-a",
            "fault_type": "cpu",
            "intensity": 0.8,
            "source_start": 100.0,
            "source_end": 120.0,
            "replay_start": 110.0,
            "replay_end": 130.0,
        },
        {
            "node": "node-a",
            "fault_type": "memory",
            "intensity": 0.9,
            "source_start": 200.0,
            "source_end": 240.0,
            "replay_start": 210.0,
            "replay_end": 250.0,
        },
    ]
