from tests.contracts.vendor_specs.validate import _generated_modules


def test_generated_modules_include_single_file_and_package_outputs(tmp_path):
    (tmp_path / "grafana_models.py").write_text("class Grafana: ...\n")
    opensearch_package = tmp_path / "opensearch_models"
    opensearch_package.mkdir()
    (opensearch_package / "__init__.py").write_text("class OpenSearch: ...\n")

    ignored_package = tmp_path / "signalflow_models"
    ignored_package.mkdir()

    assert _generated_modules(tmp_path) == {"grafana", "opensearch"}
