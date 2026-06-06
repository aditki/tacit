"""Regression tests for packaged runtime data files."""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path

from dashforge.archetypes.templates import _load_packaged_archetypes
from dashforge.signals import SignalStore


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
            packaged_text = files("dashforge.data").joinpath(name).read_text()
            assert packaged_text == source_path.read_text()
