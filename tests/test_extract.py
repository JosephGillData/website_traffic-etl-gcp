"""Unit tests for extraction module."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.etl.extract import ExtractionError, extract_from_xls


class TestExtractFromXls:
    """Tests for XLS extraction."""

    def test_raises_on_missing_file(self, tmp_path: Path):
        """Should raise ExtractionError if file doesn't exist."""
        with pytest.raises(ExtractionError, match="not found"):
            extract_from_xls(tmp_path / "nonexistent.xls")

    def test_raises_on_missing_columns(self, tmp_path: Path):
        """Should raise if required columns are missing."""
        # Create a valid Excel file but with wrong columns
        xls_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({"wrong_column": [1, 2, 3]})
        df.to_excel(xls_path, index=False)

        with pytest.raises(ExtractionError, match="Missing required columns"):
            extract_from_xls(xls_path)

    def test_extracts_valid_data(self, tmp_path: Path):
        """Should extract data from valid XLS file."""
        xls_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "time": pd.to_datetime(["2021-05-23 14:30"]),
            "traffic": [6.5],
        })
        df.to_excel(xls_path, index=False)

        result = extract_from_xls(xls_path)

        assert len(result) == 1
        assert "time" in result.columns
        assert "traffic" in result.columns

    def test_raises_on_empty_file(self, tmp_path: Path):
        """Should raise if file has no data rows."""
        xls_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({"time": pd.Series([], dtype="datetime64[ns]"), "traffic": pd.Series([], dtype=float)})
        df.to_excel(xls_path, index=False)

        with pytest.raises(ExtractionError, match="no data rows"):
            extract_from_xls(xls_path)
