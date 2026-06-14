from __future__ import annotations

from pathlib import Path

import pytest


def pytest_collection_modifyitems(config, items):
    e2e_root = Path(__file__).parent
    for item in items:
        if Path(str(item.fspath)).is_relative_to(e2e_root):
            item.add_marker(pytest.mark.e2e)
