from pathlib import Path

from click.testing import CliRunner

from tacit.cli import cli


def test_pypi_quickstart_uses_registered_init_command():
    readme = (Path(__file__).parents[2] / "README-PYPI.md").read_text(encoding="utf-8")

    assert "tacit setup" not in readme
    assert "tacit init" in readme
    result = CliRunner().invoke(cli, ["init", "--help"])
    assert result.exit_code == 0, result.output


def test_public_learning_docs_preserve_generated_archetype_containment():
    root = Path(__file__).parents[2]
    readme = (root / "README.md").read_text(encoding="utf-8")
    test_guide = (root / "tests" / "README.md").read_text(encoding="utf-8")

    assert "There is no auto-approval path for generated archetypes" in readme
    assert "registers the generated learned archetype" not in test_guide
    assert "generated archetype output remains quarantined" in test_guide

    result = CliRunner().invoke(cli, ["learn", "dashboard", "--help"])
    assert result.exit_code == 0, result.output
    assert "generated archetypes remain quarantined" in result.output


def test_generated_archetype_shadow_decisions_are_linked():
    root = Path(__file__).parents[2]
    readme = (root / "README.md").read_text(encoding="utf-8")
    adr_index = (root / "docs" / "adr" / "README.md").read_text(encoding="utf-8")
    roadmap = (root / "docs" / "generated-archetype-evaluation-roadmap.md").read_text(encoding="utf-8")

    assert "docs/adr/020-generated-archetypes-shadow-before-lifecycle.md" in readme
    assert "docs/generated-archetype-evaluation-roadmap.md" in readme
    assert "020-generated-archetypes-shadow-before-lifecycle.md" in adr_index
    assert "021-generated-archetype-scope-context.md" in adr_index
    assert "Option 1: Build A Governed Runtime Lifecycle" in roadmap
    assert "Option 2: Compile Discoveries Into Operational Knowledge" in roadmap
    assert "Option 3: Retire The Runtime Abstraction" in roadmap
    assert "no runtime promotion code is merged before that decision" in roadmap
