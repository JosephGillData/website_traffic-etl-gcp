"""Unit tests for transformation module."""

from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from src.etl.transform import (
    TransformationError,
    add_created_at,
    parse_time_column,
    save_to_csv,
    transform,
    validate_data,
)


class TestParseTimeColumn:
    """Tests for time column parsing."""

    def test_parses_day_first_format(self):
        """Should parse dd/mm/YY HH:MM format correctly."""
        df = pd.DataFrame({
            "time": ["23/05/21 14:30", "01/12/21 09:00"],
            "traffic": [1.0, 2.0],
        })
        result = parse_time_column(df)

        assert result["time"].iloc[0] == "2021-05-23 14:30:00"
        assert result["time"].iloc[1] == "2021-12-01 09:00:00"

    def test_parses_datetime_objects(self):
        """Should handle datetime objects from Excel."""
        df = pd.DataFrame({
            "time": pd.to_datetime(["2021-05-23 14:30", "2021-12-01 09:00"]),
            "traffic": [1.0, 2.0],
        })
        result = parse_time_column(df)

        assert result["time"].iloc[0] == "2021-05-23 14:30:00"
        assert result["time"].iloc[1] == "2021-12-01 09:00:00"

    def test_raises_on_invalid_format(self):
        """Should raise TransformationError on unparseable dates."""
        df = pd.DataFrame({
            "time": ["not-a-date", "also-not-a-date"],
            "traffic": [1.0, 2.0],
        })
        with pytest.raises(TransformationError, match="Failed to parse time"):
            parse_time_column(df)

    def test_does_not_modify_original(self):
        """Should not modify the original DataFrame."""
        original_time = "23/05/21 14:30"
        df = pd.DataFrame({"time": [original_time], "traffic": [1.0]})
        parse_time_column(df)

        # Original should be unchanged
        assert df["time"].iloc[0] == original_time


class TestAddCreatedAt:
    """Tests for created_at column addition."""

    def test_adds_created_at_column(self):
        """Should add created_at column with UTC timestamp."""
        df = pd.DataFrame({"time": ["2021-05-23 14:30:00"], "traffic": [1.0]})
        result = add_created_at(df)

        assert "created_at" in result.columns
        # Should be in YYYY-mm-dd HH:MM:SS format
        assert len(result["created_at"].iloc[0]) == 19

    def test_created_at_is_consistent_across_rows(self):
        """All rows should have the same created_at value."""
        df = pd.DataFrame({
            "time": ["2021-05-23 14:30:00"] * 100,
            "traffic": [1.0] * 100,
        })
        result = add_created_at(df)

        # All values should be identical
        assert result["created_at"].nunique() == 1

    def test_does_not_modify_original(self):
        """Should not modify the original DataFrame."""
        df = pd.DataFrame({"time": ["2021-05-23"], "traffic": [1.0]})
        add_created_at(df)

        assert "created_at" not in df.columns


class TestValidateData:
    """Tests for data validation."""

    def test_passes_valid_data(self):
        """Should pass valid data without raising."""
        df = pd.DataFrame({
            "time": ["2021-05-23 14:30:00"],
            "traffic": [6.5],
            "created_at": ["2024-01-01 00:00:00"],
        })
        result = validate_data(df)
        assert len(result) == 1

    def test_raises_on_missing_columns(self):
        """Should raise if required columns are missing."""
        df = pd.DataFrame({"time": ["2021-05-23"], "traffic": [1.0]})
        with pytest.raises(TransformationError, match="Missing required columns"):
            validate_data(df)

    def test_raises_on_null_time(self):
        """Should raise if time column has nulls."""
        df = pd.DataFrame({
            "time": [None, "2021-05-23"],
            "traffic": [1.0, 2.0],
            "created_at": ["2024-01-01", "2024-01-01"],
        })
        with pytest.raises(TransformationError, match="null values in 'time'"):
            validate_data(df)

    def test_raises_on_null_traffic(self):
        """Should raise if traffic column has nulls."""
        df = pd.DataFrame({
            "time": ["2021-05-23", "2021-05-24"],
            "traffic": [1.0, None],
            "created_at": ["2024-01-01", "2024-01-01"],
        })
        with pytest.raises(TransformationError, match="null values in 'traffic'"):
            validate_data(df)

    def test_raises_on_non_numeric_traffic(self):
        """Should raise if traffic is not numeric."""
        df = pd.DataFrame({
            "time": ["2021-05-23"],
            "traffic": ["not a number"],
            "created_at": ["2024-01-01"],
        })
        with pytest.raises(TransformationError, match="numeric values"):
            validate_data(df)


class TestTransform:
    """Tests for the full transform function."""

    def test_full_transform_pipeline(self):
        """Should run full transformation successfully."""
        df = pd.DataFrame({
            "time": pd.to_datetime(["2021-05-23 14:30"]),
            "traffic": [6.5],
        })
        result = transform(df)

        assert list(result.columns) == ["time", "traffic", "created_at"]
        assert result["time"].iloc[0] == "2021-05-23 14:30:00"
        assert result["traffic"].iloc[0] == 6.5
        assert "created_at" in result.columns

    def test_normalizes_column_names(self):
        """Should normalize column names to lowercase."""
        df = pd.DataFrame({
            "TIME": pd.to_datetime(["2021-05-23"]),
            "TRAFFIC": [6.5],
        })
        result = transform(df)

        assert "time" in result.columns
        assert "traffic" in result.columns


class TestSaveToCsv:
    """Tests for CSV output."""

    def test_saves_csv_with_timestamp(self):
        """Should save CSV with correct filename format."""
        df = pd.DataFrame({
            "time": ["2021-05-23 14:30:00"],
            "traffic": [6.5],
            "created_at": ["2024-01-01 00:00:00"],
        })

        with TemporaryDirectory() as tmpdir:
            output_path = save_to_csv(df, Path(tmpdir), "20240101_120000")

            assert output_path.exists()
            assert output_path.name == "traffic_data_20240101_120000.csv"

    def test_csv_has_header(self):
        """CSV should include header row."""
        df = pd.DataFrame({
            "time": ["2021-05-23 14:30:00"],
            "traffic": [6.5],
            "created_at": ["2024-01-01 00:00:00"],
        })

        with TemporaryDirectory() as tmpdir:
            output_path = save_to_csv(df, Path(tmpdir), "20240101_120000")

            # Read back and verify header
            with open(output_path) as f:
                header = f.readline().strip()
                assert header == "time,traffic,created_at"

    def test_csv_content_matches_dataframe(self):
        """CSV content should match DataFrame."""
        df = pd.DataFrame({
            "time": ["2021-05-23 14:30:00", "2021-05-23 15:00:00"],
            "traffic": [6.5, 7.2],
            "created_at": ["2024-01-01 00:00:00", "2024-01-01 00:00:00"],
        })

        with TemporaryDirectory() as tmpdir:
            output_path = save_to_csv(df, Path(tmpdir), "20240101_120000")

            # Read back and compare
            result = pd.read_csv(output_path)
            pd.testing.assert_frame_equal(result, df)

    def test_creates_output_directory(self):
        """Should create output directory if it doesn't exist."""
        df = pd.DataFrame({
            "time": ["2021-05-23 14:30:00"],
            "traffic": [6.5],
            "created_at": ["2024-01-01 00:00:00"],
        })

        with TemporaryDirectory() as tmpdir:
            nested_dir = Path(tmpdir) / "nested" / "output"
            output_path = save_to_csv(df, nested_dir, "20240101_120000")

            assert output_path.exists()
            assert output_path.parent == nested_dir
