import zipfile

from demo.gamma_pilot import _labels, _percentile, _read_metric_points, _rebase


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
