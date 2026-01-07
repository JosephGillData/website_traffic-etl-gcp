"""
Extract Module - Download and Read Source Data from GCS
========================================================

This module handles the "Extract" phase of the ETL pipeline.
Its job is simple: download data from GCS and return it as-is.

THE EXTRACT PHASE:
------------------
- Download raw data from GCS (the source XLS file lives in the bucket)
- Read the Excel file into memory
- Perform minimal validation (does it have expected columns?)
- Return data in a format other modules can work with (pandas DataFrame)

DATA FLOW:
----------
GCS (raw_data/traffic_spreadsheet.xls) → Download to temp file → Read into DataFrame

WHY DOWNLOAD TO TEMP FILE:
--------------------------
pandas.read_excel() can read from file paths but not directly from GCS.
We download to a temporary file, read it, then clean up.
This is a common pattern when working with cloud storage.

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

import tempfile
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import storage

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
# GCS DOWNLOAD FUNCTION
# =============================================================================


def download_from_gcs(bucket_name: str, source_blob_path: str) -> Path:
    """
    Download a file from Google Cloud Storage to a temporary local file.

    WHY TEMP FILES:
    ---------------
    We can't read Excel files directly from GCS - pandas needs a local file path.
    Using Python's tempfile module ensures:
    - Unique filenames (no collisions)
    - Proper cleanup on system restart
    - Cross-platform compatibility

    The caller is responsible for cleaning up the temp file after use.

    Args:
        bucket_name: Name of the GCS bucket (without gs:// prefix)
        source_blob_path: Path to the file within the bucket
                          (e.g., "raw_data/traffic_spreadsheet.xls")

    Returns:
        Path to the downloaded temporary file

    Raises:
        ExtractionError: If download fails (file not found, permission denied, etc.)
    """
    logger = get_logger()
    gcs_uri = f"gs://{bucket_name}/{source_blob_path}"
    logger.info(f"Downloading source file from {gcs_uri}")

    # -------------------------------------------------------------------------
    # Create GCS Client
    # -------------------------------------------------------------------------
    # Uses GOOGLE_APPLICATION_CREDENTIALS automatically (set in config.py)
    try:
        client = storage.Client()
    except Exception as e:
        raise ExtractionError(
            f"Failed to create GCS client: {e}\n"
            "Check that GOOGLE_APPLICATION_CREDENTIALS is set correctly."
        ) from e

    # -------------------------------------------------------------------------
    # Get Bucket and Blob References
    # -------------------------------------------------------------------------
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_path)

        # Check if the blob exists before trying to download
        if not blob.exists():
            raise ExtractionError(
                f"Source file not found in GCS: {gcs_uri}\n"
                f"Please check that the file exists at: {source_blob_path}\n"
                "You can verify with: gsutil ls gs://{bucket_name}/{source_blob_path}"
            )
    except NotFound as e:
        raise ExtractionError(
            f"GCS bucket not found: {bucket_name}\n" "Please check the GCS_BUCKET configuration."
        ) from e
    except Forbidden as e:
        raise ExtractionError(
            f"Permission denied accessing GCS: {e}\n"
            "Ensure the service account has 'Storage Object Viewer' role."
        ) from e

    # -------------------------------------------------------------------------
    # Download to Temporary File
    # -------------------------------------------------------------------------
    # Get the file extension from the source path to preserve it
    # This is important because pandas uses the extension to determine the file type
    source_extension = Path(source_blob_path).suffix  # e.g., ".xls"

    try:
        # Create a temporary file with the same extension as the source
        # delete=False means we manage deletion ourselves (after reading)
        temp_file = tempfile.NamedTemporaryFile(
            suffix=source_extension,
            delete=False,
        )
        temp_path = Path(temp_file.name)
        temp_file.close()  # Close so blob.download_to_filename can write to it

        # Download the blob to the temp file
        blob.download_to_filename(str(temp_path))
        logger.info(f"Downloaded {gcs_uri} to temporary file")

        return temp_path

    except Forbidden as e:
        raise ExtractionError(
            f"Permission denied downloading from GCS: {e}\n"
            "Ensure the service account has 'Storage Object Viewer' role."
        ) from e
    except Exception as e:
        raise ExtractionError(f"Failed to download file from GCS: {e}") from e


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================


def extract_from_gcs(bucket_name: str, source_blob_path: str) -> pd.DataFrame:
    """
    Extract data from an XLS file stored in GCS.

    This is the main extraction function. It:
    1. Downloads the file from GCS to a temporary location
    2. Reads the Excel file into a DataFrame
    3. Validates the data structure
    4. Cleans up the temporary file
    5. Returns the DataFrame

    Args:
        bucket_name: Name of the GCS bucket
        source_blob_path: Path to the XLS file within the bucket

    Returns:
        DataFrame containing the raw data from the Excel file.

    Raises:
        ExtractionError: If download or parsing fails.
    """
    logger = get_logger()
    logger.info("=== Starting extraction from GCS ===")

    # Download the file from GCS
    temp_path = download_from_gcs(bucket_name, source_blob_path)

    try:
        # Read the Excel file
        df = _read_excel_file(temp_path)
        return df
    finally:
        # Always clean up the temp file, even if reading fails
        try:
            temp_path.unlink()
            logger.info("Cleaned up temporary file")
        except Exception as e:
            logger.warning(f"Could not delete temp file {temp_path}: {e}")


def _read_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Read and validate an Excel file.

    This is a helper function that handles the actual file reading.
    Separated from download logic for clarity and testability.

    Args:
        file_path: Path to the local Excel file

    Returns:
        DataFrame with validated structure

    Raises:
        ExtractionError: If file cannot be read or is structurally invalid
    """
    logger = get_logger()
    logger.info(f"Reading Excel file: {file_path}")

    # Read the Excel file
    try:
        df = pd.read_excel(file_path, engine="xlrd")
    except FileNotFoundError as err:
        raise ExtractionError(f"XLS file not found: {file_path}") from err
    except Exception as err:
        raise ExtractionError(f"Failed to read XLS file: {err}") from err

    # Validate column structure
    expected_columns = {"time", "traffic"}
    actual_columns = set(df.columns.str.lower())

    if not expected_columns.issubset(actual_columns):
        missing = expected_columns - actual_columns
        raise ExtractionError(
            f"Missing required columns: {missing}. Found columns: {list(df.columns)}"
        )

    # Validate data exists
    row_count = len(df)
    logger.info(f"Extracted {row_count} rows from Excel file")

    if row_count == 0:
        raise ExtractionError("Excel file contains no data rows")

    return df


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
