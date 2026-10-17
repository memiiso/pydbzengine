from __future__ import annotations

from pathlib import Path

import pytest

TESTS_ROOT = Path(__file__).resolve().parent
TEST_CONFIG_DIR = TESTS_ROOT / "config"


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests requiring Docker containers and external services",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Automatically apply the integration marker to tests in tests/integration/."""
    integration_marker = pytest.mark.integration
    for item in items:
        # Check if item path is within tests/integration
        try:
            rel_path = Path(item.fspath).relative_to(TESTS_ROOT)
            if rel_path.parts and rel_path.parts[0] == "integration":
                item.add_marker(integration_marker)
        except ValueError:
            pass
