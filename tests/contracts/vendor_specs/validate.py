"""Validate generated vendor OpenAPI contract modules.

This module is used in two places:
* normal CI with ``--allow-empty`` so branches without generated specs do not fail
* the scheduled vendor refresh workflow without ``--allow-empty`` so refresh PRs
  must contain importable generated Pydantic models for every supported vendor
"""

from __future__ import annotations

import argparse
import importlib
import inspect
from pathlib import Path

from pydantic import BaseModel, RootModel

ACTIVE_CONTRACT_MODULES = {
    "grafana": "tests.contracts.grafana_models",
    "opensearch": "tests.contracts.elasticsearch_models",
    "signalflow": "tests.contracts.signalfx_models",
}


def _has_pydantic_model(module_name: str) -> bool:
    module = importlib.import_module(module_name)
    for _, value in inspect.getmembers(module, inspect.isclass):
        if value.__module__ != module_name:
            continue
        if issubclass(value, (BaseModel, RootModel)):
            return True
    return False


def _generated_modules(root: Path) -> set[str]:
    generated_files = {path.stem.removesuffix("_models") for path in root.glob("*_models.py")}
    generated_packages = {
        path.name.removesuffix("_models")
        for path in root.glob("*_models")
        if path.is_dir() and (path / "__init__.py").is_file()
    }
    return generated_files | generated_packages


def validate(vendors: list[str], *, allow_empty: bool) -> None:
    root = Path(__file__).parent
    generated = _generated_modules(root)
    if not generated and allow_empty:
        print("No generated vendor spec modules found; skipping generated contract validation.")
        return

    missing = [vendor for vendor in vendors if vendor not in generated]
    if missing:
        raise SystemExit(f"Missing generated vendor spec modules: {', '.join(sorted(missing))}")

    for vendor in vendors:
        generated_module = f"tests.contracts.vendor_specs.{vendor}_models"
        if not _has_pydantic_model(generated_module):
            raise SystemExit(f"{generated_module} does not define any Pydantic models")

        active_module = ACTIVE_CONTRACT_MODULES[vendor]
        if not _has_pydantic_model(active_module):
            raise SystemExit(f"{active_module} does not define any active contract models")

    print(f"Validated generated vendor contracts: {', '.join(vendors)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vendors", nargs="+", default=sorted(ACTIVE_CONTRACT_MODULES))
    parser.add_argument("--allow-empty", action="store_true")
    args = parser.parse_args()
    validate(args.vendors, allow_empty=args.allow_empty)


if __name__ == "__main__":
    main()
