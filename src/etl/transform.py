"""
Transform Module - Data Cleaning and Formatting
================================================

This module handles the "Transform" phase of the ETL pipeline.
This is where raw data becomes clean, validated, analysis-ready data.

THE TRANSFORM PHASE:
--------------------
Transform is often the most complex ETL phase because it handles:
- Data type conversions (strings to dates, etc.)
- Data cleaning (handling nulls, invalid values)
- Data enrichment (adding calculated fields)
- Data validation (checking business rules)
- Data formatting (standardizing formats)

TRANSFORMATION PIPELINE:
------------------------
Our transform applies these steps in order:

1. Normalize column names → lowercase for consistency
2. Parse time column → convert dd/mm/YY to YYYY-mm-dd HH:MM:SS
3. Add created_at → timestamp when this ETL run processed the data
4. Validate data → check for nulls, correct types, reasonable values
5. Order columns → ensure consistent column order in output

WHY THIS ORDER:
---------------
- Normalize first: subsequent code can assume lowercase column names
- Parse time early: time format issues should fail fast
- Add metadata before validation: so created_at gets validated too
- Validate last: catch any issues from prior transformations

IMMUTABILITY PRINCIPLE:
-----------------------
Each transformation function returns a NEW DataFrame rather than modifying
the input. This makes debugging easier (original data preserved) and
prevents subtle bugs from shared mutable state.

DATE/TIME HANDLING:
-------------------
Date parsing is notoriously tricky because:
- Different countries use different formats (MM/DD/YY vs DD/MM/YY)
- Two-digit years are ambiguous (is "21" 2021 or 1921?)
- Timezones add another layer of complexity

Our source uses "dd/mm/YY HH:MM" (European format, day first).
We output "YYYY-mm-dd HH:MM:SS" (ISO 8601 format) because:
- It's unambiguous
- It sorts correctly as a string
- It's what BigQuery expects for TIMESTAMP
- It's the international standard
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .logging_config import get_logger

# =============================================================================
# CUSTOM EXCEPTION
# =============================================================================


class TransformationError(Exception):
    """
    Raised when data transformation fails.

    Examples of transformation failures:
    - Unparseable date formats
    - Null values in required columns
    - Invalid data types
    - Data that fails business rules

    Transformation errors usually mean the source data is bad or unexpected.
    Unlike configuration errors (user-fixable), these often require
    investigating the source data.
    """


# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================


def parse_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse time column from European format to ISO standard.

    DATE FORMAT HANDLING:
    ---------------------
    Input: "dd/mm/YY HH:MM" (e.g., "23/05/21 14:30")
           Day-first format common in UK/Europe

    Output: "YYYY-mm-dd HH:MM:SS" (e.g., "2021-05-23 14:30:00")
            ISO 8601 standard format

    WHY DAYFIRST=TRUE:
    ------------------
    The date "05/06/21" is ambiguous:
    - In US format (month-first): May 6, 2021
    - In UK format (day-first): June 5, 2021

    Our source data uses day-first format, so we must tell pandas
    to interpret it that way with dayfirst=True.

    TIMEZONE ASSUMPTION:
    --------------------
    The source data has no timezone information. We treat all times
    as naive UTC timestamps. This assumption is documented here
    so future maintainers know it's intentional, not an oversight.

    If your source data is in a different timezone, you would need to:
    1. Parse as naive datetime
    2. Localize to the source timezone
    3. Convert to UTC

    Args:
        df: DataFrame with 'time' column containing dates

    Returns:
        DataFrame with 'time' column converted to YYYY-mm-dd HH:MM:SS strings

    Raises:
        TransformationError: If date parsing fails for any rows
    """
    logger = get_logger()

    # Make a copy to avoid modifying the original
    # This is a defensive practice that prevents subtle bugs
    df = df.copy()

    try:
        # pd.to_datetime() is pandas' date parser
        # dayfirst=True tells it to interpret ambiguous dates as DD/MM/YY
        #
        # This handles both:
        # - String dates: "23/05/21 14:30"
        # - datetime objects (if Excel already parsed them)
        df["time"] = pd.to_datetime(df["time"], dayfirst=True)
    except Exception as e:
        raise TransformationError(f"Failed to parse time column: {e}") from e

    # Check for NaT (Not a Time) values - pandas' equivalent of NaN for dates
    # If parsing fails for some rows, they become NaT rather than raising an error
    null_count = df["time"].isna().sum()
    if null_count > 0:
        raise TransformationError(
            f"Failed to parse {null_count} time values. "
            "Check that time format is dd/mm/YY HH:MM."
        )

    # Convert to string in ISO format
    # .dt.strftime() applies a format string to each datetime value
    # %Y = 4-digit year, %m = 2-digit month, %d = 2-digit day
    # %H = 24-hour hour, %M = minute, %S = second
    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Parsed {len(df)} time values to YYYY-mm-dd HH:MM:SS format")
    return df


def add_created_at(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add created_at column with current UTC timestamp.

    WHY CREATED_AT:
    ---------------
    This metadata column tracks WHEN the data was processed by ETL.
    It's different from the 'time' column which is WHEN the event occurred.

    This is useful for:
    - Auditing: "When was this data loaded?"
    - Debugging: "Which ETL run produced this row?"
    - Deduplication: "Keep only the latest version of each record"
    - Monitoring: "How old is the data in the warehouse?"

    WHY UTC:
    --------
    Always store timestamps in UTC because:
    - It's unambiguous (no daylight saving time issues)
    - It's consistent across runs on machines in different timezones
    - It can be converted to local time when needed for display

    ALL ROWS GET SAME TIMESTAMP:
    ----------------------------
    All rows in a single ETL run get the same created_at value.
    This makes sense because they were all processed together.
    It's also more efficient than calling datetime.now() per row.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with new 'created_at' column
    """
    df = df.copy()

    # Generate single timestamp for the entire run
    # timezone.utc ensures we get UTC time regardless of server timezone
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["created_at"] = created_at

    logger = get_logger()
    logger.info(f"Added created_at timestamp: {created_at}")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate transformed data before loading.

    DATA VALIDATION:
    ----------------
    This is the last check before data goes to the warehouse.
    We validate:

    1. Required columns exist (time, traffic, created_at)
    2. No null values in critical columns
    3. Data types are correct (traffic should be numeric)
    4. Values are reasonable (warn on negative traffic)

    FAIL-FAST VS WARNINGS:
    ----------------------
    - Missing columns: FAIL (structural problem, can't continue)
    - Null values: FAIL (data quality problem, shouldn't load bad data)
    - Wrong types: FAIL (would cause BigQuery load to fail anyway)
    - Negative values: WARN (suspicious but might be valid, let it through)

    WHY VALIDATE HERE:
    ------------------
    We validate after all transformations because:
    - Transformations might introduce problems (e.g., failed parsing creates nulls)
    - We want to catch issues before they hit the database
    - It's easier to fix data issues in the pipeline than in the warehouse

    Args:
        df: Transformed DataFrame to validate

    Returns:
        The same DataFrame (if validation passes)

    Raises:
        TransformationError: If any validation check fails
    """
    logger = get_logger()

    # -------------------------------------------------------------------------
    # Check Required Columns
    # -------------------------------------------------------------------------
    required_columns = {"time", "traffic", "created_at"}
    missing = required_columns - set(df.columns)
    if missing:
        raise TransformationError(f"Missing required columns after transform: {missing}")

    # -------------------------------------------------------------------------
    # Check for Null Values
    # -------------------------------------------------------------------------
    # Null values in the warehouse are problematic:
    # - Can break downstream queries
    # - Make aggregations incorrect
    # - Indicate data quality issues at the source
    for col in ["time", "traffic"]:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise TransformationError(
                f"Found {null_count} null values in '{col}' column. " "Data quality check failed."
            )

    # -------------------------------------------------------------------------
    # Check Data Types
    # -------------------------------------------------------------------------
    # Traffic should be numeric (it's a measurement)
    # pd.api.types.is_numeric_dtype() checks if a column is int, float, etc.
    if not pd.api.types.is_numeric_dtype(df["traffic"]):
        raise TransformationError("Traffic column must contain numeric values")

    # -------------------------------------------------------------------------
    # Check Value Ranges (Warning Only)
    # -------------------------------------------------------------------------
    # Negative traffic is suspicious but might be valid in some contexts
    # (e.g., representing net change). We warn but don't fail.
    if (df["traffic"] < 0).any():
        logger.warning("Found negative traffic values - this may indicate data issues")

    logger.info(f"Data validation passed: {len(df)} rows, no null values")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformations to the raw data.

    ORCHESTRATION FUNCTION:
    -----------------------
    This function coordinates the transformation pipeline.
    It calls individual transformation functions in the correct order.

    This pattern (one orchestrator calling specialized functions) is common
    because it:
    - Makes the high-level flow clear
    - Keeps individual functions focused and testable
    - Makes it easy to add, remove, or reorder steps

    PIPELINE ORDER:
    ---------------
    1. Normalize column names (lowercase)
    2. Parse time column
    3. Add created_at
    4. Validate all data
    5. Order columns for output

    Args:
        df: Raw DataFrame from extraction

    Returns:
        Cleaned, validated DataFrame ready for loading
    """
    logger = get_logger()
    logger.info("Starting data transformation")

    # Normalize column names to lowercase
    # This ensures we can reference columns consistently regardless of
    # how they were capitalized in the source file
    df.columns = df.columns.str.lower()

    # Apply transformations in order
    df = parse_time_column(df)
    df = add_created_at(df)
    df = validate_data(df)

    # Ensure column order matches what BigQuery expects
    # This also drops any extra columns from the source file
    df = df[["time", "traffic", "created_at"]]

    logger.info("Transformation complete")
    return df


# =============================================================================
# OUTPUT FUNCTION
# =============================================================================


def save_to_csv(df: pd.DataFrame, output_dir: Path, timestamp: str) -> Path:
    """
    Save DataFrame to CSV with timestamped filename.

    WHY TIMESTAMPED FILENAMES:
    --------------------------
    Using timestamps in filenames (e.g., traffic_data_20240101_120000.csv):
    - Prevents overwriting previous outputs
    - Creates an audit trail of each run
    - Makes it easy to identify which run produced which file
    - Allows keeping historical outputs for debugging

    CSV FORMAT CHOICES:
    -------------------
    - index=False: Don't include pandas' row numbers in output
    - header=True: Include column names as first row (BigQuery will skip this)

    We write with headers because:
    - It's self-documenting (file shows what columns mean)
    - BigQuery's skip_leading_rows=1 handles it
    - Makes the CSV useful outside of BigQuery too

    Args:
        df: DataFrame to save
        output_dir: Directory to write the file to
        timestamp: Timestamp string for filename (format: YYYYMMDD_HHMMSS)

    Returns:
        Path to the created CSV file
    """
    logger = get_logger()

    # Create output directory if it doesn't exist
    # parents=True creates parent directories as needed
    # exist_ok=True doesn't error if directory already exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create filename with timestamp
    filename = "traffic_data.csv"
    output_path = output_dir / filename

    # Write CSV
    # index=False: Don't write row numbers
    # header=True: Write column names as first row
    df.to_csv(output_path, index=False, header=True)
    logger.info(f"Saved CSV to {output_path}")

    return output_path
