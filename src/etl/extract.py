"""
Extract Module - Read Source Data from XLS File
================================================

This module handles the "Extract" phase of the ETL pipeline.
Its job is simple: read data from the source file and return it as-is.

THE EXTRACT PHASE:
------------------
- Read raw data from source (in this case, an Excel XLS file)
- Perform minimal validation (does file exist? does it have expected columns?)
- Return data in a format other modules can work with (pandas DataFrame)

WHAT EXTRACT DOES NOT DO:
-------------------------
- Clean or modify data (that's Transform's job)
- Upload or store data (that's Load's job)
- Apply business logic

This separation of concerns makes the code easier to test and maintain.
Each module has one clear responsibility.

WHY PANDAS DATAFRAME:
---------------------
pandas DataFrame is the standard in-memory data structure for tabular data in Python.
It's like an Excel spreadsheet in code - rows and columns with named headers.
Most data manipulation libraries expect DataFrames, including:
- Data transformation (pandas itself)
- Machine learning (scikit-learn, XGBoost)
- Visualization (matplotlib, seaborn)
- Database loading (SQLAlchemy, BigQuery client)

EXCEL FILE FORMATS:
-------------------
- .xls: Old Excel format (before 2007), requires 'xlrd' library
- .xlsx: Modern Excel format, requires 'openpyxl' library
- pandas.read_excel() handles both with the appropriate engine
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .logging_config import get_logger

# =============================================================================
# CUSTOM EXCEPTION
# =============================================================================


class ExtractionError(Exception):
    """
    Raised when data extraction fails.

    Examples of extraction failures:
    - File doesn't exist
    - File is corrupted or wrong format
    - File is missing required columns
    - File is empty

    Using a custom exception allows callers to handle extraction errors
    differently from other types of errors (like configuration errors).
    """


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================


def extract_from_xls(file_path: Path) -> pd.DataFrame:
    """
    Extract data from an XLS file.

    This function reads an Excel file and returns its contents as a DataFrame.
    It performs basic validation to ensure the file is usable before returning.

    VALIDATION PHILOSOPHY:
    ----------------------
    We validate here (rather than in Transform) because:
    - If the file is wrong, we want to fail immediately
    - It's clearer to report "missing 'time' column" than "can't parse time column"
    - Transform can assume it has the data it needs

    COLUMN VALIDATION:
    ------------------
    We check that 'time' and 'traffic' columns exist (case-insensitive).
    This is a basic structural validation - we're not checking data types
    or values yet. Transform will do more detailed validation.

    Args:
        file_path: Path to the XLS file to read

    Returns:
        DataFrame containing the raw data from the Excel file.
        Column names and data types are preserved as-is from the source.

    Raises:
        ExtractionError: If file cannot be read or is structurally invalid.
                         Error messages are designed to help users fix the issue.
    """
    logger = get_logger()
    logger.info(f"Extracting data from {file_path}")

    # -------------------------------------------------------------------------
    # Read Excel File
    # -------------------------------------------------------------------------
    # pd.read_excel() is pandas' function for reading Excel files.
    #
    # engine="xlrd": Specifies the library to use for reading.
    #   - xlrd: For .xls files (old Excel format)
    #   - openpyxl: For .xlsx files (modern Excel format)
    #
    # The engine choice matters because .xls and .xlsx are completely different
    # file formats under the hood, despite both being "Excel files".
    try:
        df = pd.read_excel(file_path, engine="xlrd")
    except FileNotFoundError as err:
        # Explicit handling for missing file - common user error
        raise ExtractionError(f"XLS file not found: {file_path}") from err
    except Exception as err:
        # Catch-all for other errors (corrupted file, wrong format, etc.)
        raise ExtractionError(f"Failed to read XLS file: {err}") from err

    # -------------------------------------------------------------------------
    # Validate Column Structure
    # -------------------------------------------------------------------------
    # We expect the file to have 'time' and 'traffic' columns.
    # Using case-insensitive comparison because column names might be
    # "Time", "TIME", "time", etc. depending on how the Excel was created.
    expected_columns = {"time", "traffic"}
    actual_columns = set(df.columns.str.lower())

    # issubset() checks if all expected columns are present
    # We use subset (not equality) to allow extra columns - they'll be ignored
    if not expected_columns.issubset(actual_columns):
        missing = expected_columns - actual_columns
        raise ExtractionError(
            f"Missing required columns: {missing}. " f"Found columns: {list(df.columns)}"
        )

    # -------------------------------------------------------------------------
    # Validate Data Exists
    # -------------------------------------------------------------------------
    row_count = len(df)
    logger.info(f"Extracted {row_count} rows from XLS file")

    # An empty file is technically valid Excel, but useless for ETL
    if row_count == 0:
        raise ExtractionError("XLS file contains no data rows")

    # Return the raw DataFrame - Transform will clean it up
    return df
