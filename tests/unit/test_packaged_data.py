"""Regression tests for packaged runtime data files."""

from __future__ import annotations

import importlib.util
from importlib.resources import files
from pathlib import Path

from tacit.archetypes.templates import _load_packaged_archetypes
from tacit.signals import SignalStore


def test_packaged_archetypes_are_loadable():
    archetypes = _load_packaged_archetypes()
    assert archetypes is not None
    assert len(archetypes) > 0
    assert sum(len(archetype.panels) for archetype in archetypes) > 0


def test_packaged_signals_are_loadable(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    count = store.load_from_yaml()
    assert count > 20
    assert store.stats()["signal_types"] > 10


def test_packaged_yaml_matches_source_compatibility_files():
    repo_root = Path(__file__).resolve().parents[2]
    for name in ("archetypes.yaml", "signals.yaml"):
        source_path = repo_root / name
        if source_path.exists():
            packaged_text = files("tacit.data").joinpath(name).read_text()
            assert packaged_text == source_path.read_text()


def test_local_signals_yaml_takes_precedence_over_packaged_data(tmp_path, monkeypatch):
    local_yaml = tmp_path / "signals.yaml"
    local_yaml.write_text("""
signals:
  local_only_signal:
    description: Local override
    category: test
    unit: count
    metric_patterns:
      - pattern: "local_only_metric_total"
        confidence: 0.9
""")
    monkeypatch.chdir(tmp_path)

    store = SignalStore(db_path=tmp_path / "signals.db")
    count = store.load_from_yaml()

    assert count == 1
    assert store.get_signal_type("local_only_signal") is not None
    assert store.get_signal_type("request_rate") is None


def test_signal_seeder_prefers_editable_archetypes_yaml(tmp_path, monkeypatch):
    local_yaml = tmp_path / "archetypes.yaml"
    local_yaml.write_text("""
archetypes:
  - id: local_seed
    name: Local Seed
    panels:
      - title: Custom Panel
        queries:
          - expr: "sum(rate(custom_seed_metric_total[5m]))"
""")
    monkeypatch.chdir(tmp_path)

    script_path = Path(__file__).resolve().parents[1] / "seed_signalfx_metrics.py"
    spec = importlib.util.spec_from_file_location("seed_signalfx_metrics", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.extract_metrics_from_archetypes() == {"custom_seed_metric_total"}
