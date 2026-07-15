import importlib

from tests.contracts.vendor_specs.validate import _generated_modules, _has_pydantic_model


def test_generated_modules_include_single_file_and_package_outputs(tmp_path):
    (tmp_path / "grafana_models.py").write_text("class Grafana: ...\n")
    opensearch_package = tmp_path / "opensearch_models"
    opensearch_package.mkdir()
    (opensearch_package / "__init__.py").write_text("class OpenSearch: ...\n")

    ignored_package = tmp_path / "signalflow_models"
    ignored_package.mkdir()

    assert _generated_modules(tmp_path) == {"grafana", "opensearch"}


def test_has_pydantic_model_recurses_into_generated_packages(tmp_path, monkeypatch):
    package = tmp_path / "generated_vendor_models"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "child.py").write_text(
        "from pydantic import BaseModel\n\n" "class ChildModel(BaseModel):\n" "    value: str\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    assert _has_pydantic_model("generated_vendor_models")
