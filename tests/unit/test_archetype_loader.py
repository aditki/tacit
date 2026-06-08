from pathlib import Path

import pytest

import dashforge.archetypes.templates as templates


def test_blank_archetype_yaml_is_invalid(tmp_path: Path):
    path = tmp_path / "archetypes.yaml"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="archetype YAML"):
        templates._load_archetypes_from_yaml(path)


def test_empty_archetype_list_is_invalid(tmp_path: Path):
    path = tmp_path / "archetypes.yaml"
    path.write_text("archetypes: []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="at least one archetype"):
        templates._load_archetypes_from_yaml(path)


def test_blank_override_falls_back_to_packaged_or_builtin_archetypes(tmp_path: Path, monkeypatch):
    path = tmp_path / "archetypes.yaml"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("DASHFORGE_ARCHETYPES_PATH", str(path))

    try:
        templates.reload_archetypes()

        assert templates.ALL_ARCHETYPES
        assert templates.get_archetype("resource_saturation") is not None
    finally:
        monkeypatch.delenv("DASHFORGE_ARCHETYPES_PATH", raising=False)
        templates.reload_archetypes()
