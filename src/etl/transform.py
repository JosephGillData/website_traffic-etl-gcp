"""Transform: Clean and validate data."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger("etl")


class TransformationError(Exception):
    """Raised when data transformation fails."""


def parse_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """Parse time column from dd/mm/YY to ISO format."""
    df = df.copy()
    try:
        df["time"] = pd.to_datetime(df["time"], dayfirst=True)
    except Exception as e:
        raise TransformationError(f"Failed to parse time column: {e}") from e

    if df["time"].isna().sum() > 0:
        raise TransformationError("Failed to parse some time values")

    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def add_created_at(df: pd.DataFrame) -> pd.DataFrame:
    """Add created_at timestamp (UTC)."""
    df = df.copy()
    df["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate transformed data."""
    required = {"time", "traffic", "created_at"}
    missing = required - set(df.columns)
    if missing:
        raise TransformationError(f"Missing columns: {missing}")

    for col in ["time", "traffic"]:
        if df[col].isna().sum() > 0:
            raise TransformationError(f"Null values in '{col}'")

    if not pd.api.types.is_numeric_dtype(df["traffic"]):
        raise TransformationError("Traffic column must be numeric")

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all transformations: normalize, parse, enrich, validate."""
    df.columns = df.columns.str.lower()
    df = parse_time_column(df)
    df = add_created_at(df)
    df = validate_data(df)
    df = df[["time", "traffic", "created_at"]]
    logger.info(f"Transformed {len(df)} rows")
    return df


def save_to_csv(df: pd.DataFrame, output_dir: Path, timestamp: str) -> Path:
    """Save DataFrame to CSV with timestamped filename."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"traffic_data_{timestamp}.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved CSV: {output_path}")
    return output_path
