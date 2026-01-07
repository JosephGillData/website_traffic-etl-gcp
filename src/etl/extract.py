"""Extract module - reads source data from XLS file."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .logging_config import get_logger


class ExtractionError(Exception):
    """Raised when data extraction fails."""


def extract_from_xls(file_path: Path) -> pd.DataFrame:
    """Extract data from XLS file.

    Args:
        file_path: Path to the XLS file.

    Returns:
        DataFrame with raw data.

    Raises:
        ExtractionError: If file cannot be read or is invalid.
    """
    logger = get_logger()
    logger.info(f"Extracting data from {file_path}")

    try:
        df = pd.read_excel(file_path, engine="xlrd")
    except FileNotFoundError:
        raise ExtractionError(f"XLS file not found: {file_path}")
    except Exception as e:
        raise ExtractionError(f"Failed to read XLS file: {e}")

    # Validate expected columns
    expected_columns = {"time", "traffic"}
    actual_columns = set(df.columns.str.lower())

    if not expected_columns.issubset(actual_columns):
        missing = expected_columns - actual_columns
        raise ExtractionError(
            f"Missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )

    row_count = len(df)
    logger.info(f"Extracted {row_count} rows from XLS file")

    if row_count == 0:
        raise ExtractionError("XLS file contains no data rows")

    return df
