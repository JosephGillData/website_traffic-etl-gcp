"""Transform module - data cleaning and formatting."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .logging_config import get_logger


class TransformationError(Exception):
    """Raised when data transformation fails."""


def parse_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """Parse time column with day-first format to standard datetime.

    Input format: dd/mm/YY HH:MM (e.g., "23/05/21 14:30")
    Output format: YYYY-mm-dd HH:MM:SS (e.g., "2021-05-23 14:30:00")

    Timezone assumption: Times are assumed to be in UTC.
    The source data does not include timezone info, so we treat all
    times as naive UTC timestamps.

    Args:
        df: DataFrame with 'time' column.

    Returns:
        DataFrame with parsed time column.

    Raises:
        TransformationError: If time parsing fails.
    """
    logger = get_logger()
    df = df.copy()

    try:
        # Parse with day-first format (European style: dd/mm/YY)
        df["time"] = pd.to_datetime(df["time"], dayfirst=True)
    except Exception as e:
        raise TransformationError(f"Failed to parse time column: {e}")

    # Check for parsing failures (NaT values)
    null_count = df["time"].isna().sum()
    if null_count > 0:
        raise TransformationError(
            f"Failed to parse {null_count} time values. "
            "Check that time format is dd/mm/YY HH:MM."
        )

    # Format to ISO standard: YYYY-mm-dd HH:MM:SS
    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Parsed {len(df)} time values to YYYY-mm-dd HH:MM:SS format")
    return df


def add_created_at(df: pd.DataFrame) -> pd.DataFrame:
    """Add created_at column with current UTC timestamp.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with created_at column added.
    """
    df = df.copy()
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["created_at"] = created_at

    logger = get_logger()
    logger.info(f"Added created_at timestamp: {created_at}")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate transformed data.

    Args:
        df: Transformed DataFrame.

    Returns:
        Validated DataFrame.

    Raises:
        TransformationError: If validation fails.
    """
    logger = get_logger()

    # Check for required columns
    required_columns = {"time", "traffic", "created_at"}
    missing = required_columns - set(df.columns)
    if missing:
        raise TransformationError(f"Missing required columns after transform: {missing}")

    # Check for null values in critical columns
    for col in ["time", "traffic"]:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise TransformationError(
                f"Found {null_count} null values in '{col}' column. "
                "Data quality check failed."
            )

    # Validate traffic values are numeric and reasonable
    if not pd.api.types.is_numeric_dtype(df["traffic"]):
        raise TransformationError("Traffic column must contain numeric values")

    if (df["traffic"] < 0).any():
        logger.warning("Found negative traffic values - this may indicate data issues")

    logger.info(f"Data validation passed: {len(df)} rows, no null values")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all transformations to the data.

    Args:
        df: Raw extracted DataFrame.

    Returns:
        Transformed and validated DataFrame.
    """
    logger = get_logger()
    logger.info("Starting data transformation")

    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()

    # Apply transformations in order
    df = parse_time_column(df)
    df = add_created_at(df)
    df = validate_data(df)

    # Ensure column order for CSV output
    df = df[["time", "traffic", "created_at"]]

    logger.info("Transformation complete")
    return df


def save_to_csv(df: pd.DataFrame, output_dir: Path, timestamp: str) -> Path:
    """Save DataFrame to CSV with timestamped filename.

    Args:
        df: DataFrame to save.
        output_dir: Directory to save the file.
        timestamp: Timestamp string for filename (format: YYYYMMDD_HHMMSS).

    Returns:
        Path to the saved CSV file.
    """
    logger = get_logger()

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"traffic_data_{timestamp}.csv"
    output_path = output_dir / filename

    df.to_csv(output_path, index=False, header=True)
    logger.info(f"Saved CSV to {output_path}")

    return output_path
