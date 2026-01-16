"""Extract: Download XLS from GCS and read into DataFrame."""

import logging
import tempfile
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import storage

logger = logging.getLogger("etl")


class ExtractionError(Exception):
    """Raised when data extraction fails."""


def download_from_gcs(bucket_name: str, source_blob_path: str) -> Path:
    """Download a file from GCS to a temporary local file."""
    gcs_uri = f"gs://{bucket_name}/{source_blob_path}"
    logger.info(f"Downloading {gcs_uri}")

    try:
        client = storage.Client()
    except Exception as e:
        raise ExtractionError(
            f"Failed to create GCS client: {e}\n"
            "Run 'gcloud auth application-default login' to authenticate."
        ) from e

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_path)

        if not blob.exists():
            raise ExtractionError(f"File not found: {gcs_uri}")
    except NotFound as e:
        raise ExtractionError(f"Bucket not found: {bucket_name}") from e
    except Forbidden as e:
        raise ExtractionError(f"Permission denied: {e}") from e

    source_extension = Path(source_blob_path).suffix
    temp_file = tempfile.NamedTemporaryFile(suffix=source_extension, delete=False)
    temp_path = Path(temp_file.name)
    temp_file.close()

    try:
        blob.download_to_filename(str(temp_path))
        logger.info(f"Downloaded to {temp_path}")
        return temp_path
    except Exception as e:
        raise ExtractionError(f"Download failed: {e}") from e


def extract_from_gcs(bucket_name: str, source_blob_path: str) -> pd.DataFrame:
    """Extract data from XLS file in GCS. Returns DataFrame."""
    temp_path = download_from_gcs(bucket_name, source_blob_path)

    try:
        df = pd.read_excel(temp_path, engine="xlrd")
    except Exception as e:
        raise ExtractionError(f"Failed to read Excel file: {e}") from e
    finally:
        try:
            temp_path.unlink()
        except Exception:
            pass

    # Validate required columns
    expected = {"time", "traffic"}
    actual = set(df.columns.str.lower())
    if not expected.issubset(actual):
        missing = expected - actual
        raise ExtractionError(f"Missing columns: {missing}. Found: {list(df.columns)}")

    if len(df) == 0:
        raise ExtractionError("Excel file contains no data")

    logger.info(f"Extracted {len(df)} rows")
    return df
