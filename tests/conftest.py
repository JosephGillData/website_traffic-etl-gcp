"""Pytest configuration and fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def sample_traffic_data():
    """Sample traffic data for testing."""
    import pandas as pd

    return pd.DataFrame({
        "time": pd.to_datetime([
            "2021-05-23 00:00",
            "2021-05-23 00:05",
            "2021-05-23 00:10",
        ]),
        "traffic": [6.13, 6.29, 6.42],
    })
