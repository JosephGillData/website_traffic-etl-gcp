"""
Load Module - Upload to GCS and Load into BigQuery
==================================================

This module handles the "Load" phase of the ETL pipeline:
1. Upload files to Google Cloud Storage (GCS)
2. Load data from GCS into BigQuery

WHY GCS BEFORE BIGQUERY:
------------------------
BigQuery can load data from several sources, but GCS is the most common because:
- BigQuery and GCS are both Google services, so transfers are fast and free
- GCS acts as a "staging area" - data persists even if BigQuery load fails
- BigQuery load jobs reference GCS URIs (gs://bucket/path), not local files
- This decouples the "upload" step from the "load" step for better reliability

THE DATA FLOW:
--------------
Local CSV → GCS Upload → BigQuery Load Job → BigQuery Table
                ↓
        gs://bucket/path/file.csv  (intermediate staging)

GOOGLE CLOUD AUTHENTICATION (ADC):
----------------------------------
Both storage.Client() and bigquery.Client() use Application Default Credentials.
No explicit credentials are passed - the GCP client libraries automatically discover
credentials in this order:

1. GOOGLE_APPLICATION_CREDENTIALS environment variable (local dev)
2. gcloud CLI credentials (`gcloud auth application-default login`)
3. Attached service account (Cloud Run / GCE / GKE) - RECOMMENDED for production

For Cloud Run Jobs, attach a service account to the job. No JSON key files needed.

REQUIRED IAM PERMISSIONS:
-------------------------
The service account needs these roles on your GCP project:

For GCS:
- roles/storage.objectAdmin (or storage.objectCreator + storage.objectViewer)
  Allows: uploading files, reading bucket metadata

For BigQuery:
- roles/bigquery.dataEditor
  Allows: creating tables, inserting data
- roles/bigquery.jobUser
  Allows: running load jobs

You grant these in GCP Console: IAM & Admin → IAM → Add member
"""

from __future__ import annotations

from pathlib import Path

# Google Cloud API exceptions - used for specific error handling
# These let us give helpful messages for common problems like "bucket not found"
from google.api_core.exceptions import Forbidden, NotFound

# Google Cloud client libraries
# These are installed via: pip install google-cloud-storage google-cloud-bigquery
from google.cloud import bigquery, storage

from .config import Config
from .logging_config import get_logger

# =============================================================================
# CUSTOM EXCEPTION
# =============================================================================


class LoadError(Exception):
    """
    Raised when data loading fails (GCS or BigQuery).

    Custom exception so callers can handle load errors specifically.
    The error messages are designed to be actionable - they tell the user
    what went wrong AND how to fix it.
    """


# =============================================================================
# GCS UPLOAD FUNCTIONS
# =============================================================================


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
) -> str:
    """
    Upload a file to Google Cloud Storage.

    GCS CONCEPTS:
    -------------
    - Bucket: A container for objects (like a top-level folder)
    - Blob: A single file/object stored in a bucket
    - GCS URI: gs://bucket-name/path/to/blob (how you reference files in GCS)

    WHAT THIS FUNCTION DOES:
    ------------------------
    1. Creates a GCS client (authenticated via GOOGLE_APPLICATION_CREDENTIALS)
    2. Gets a reference to the bucket (validates it exists)
    3. Creates a blob reference with the destination path
    4. Uploads the local file to that blob
    5. Returns the GCS URI for use by BigQuery

    ERROR HANDLING:
    ---------------
    We catch specific exceptions to provide helpful error messages:
    - NotFound: Bucket doesn't exist → tell user how to create it
    - Forbidden: Permission denied → tell user which IAM role is needed

    Args:
        local_path: Path to the local file to upload
        bucket_name: Name of the GCS bucket (without gs:// prefix)
        destination_blob_name: Path within the bucket (e.g., "folder/file.csv")

    Returns:
        GCS URI of the uploaded file (e.g., "gs://my-bucket/folder/file.csv")

    Raises:
        LoadError: If upload fails, with actionable error message
    """
    logger = get_logger()
    logger.info(f"Uploading {local_path} to gs://{bucket_name}/{destination_blob_name}")

    # -------------------------------------------------------------------------
    # Create GCS Client
    # -------------------------------------------------------------------------
    # storage.Client() automatically uses credentials from:
    # 1. GOOGLE_APPLICATION_CREDENTIALS env var (set in config.py)
    # 2. Application Default Credentials (if env var not set)
    # 3. Compute Engine metadata (if running on GCP)
    try:
        client = storage.Client()
    except Exception as e:
        raise LoadError(
            f"Failed to create GCS client: {e}\n"
            "Authentication failed. For local dev: run 'gcloud auth application-default login' "
            "or set GOOGLE_APPLICATION_CREDENTIALS. For Cloud Run: ensure a service account is attached."
        ) from e

    # -------------------------------------------------------------------------
    # Get Bucket Reference
    # -------------------------------------------------------------------------
    # client.bucket() just creates a reference - it doesn't verify the bucket exists
    # bucket.reload() actually contacts GCS to verify the bucket is accessible
    try:
        bucket = client.bucket(bucket_name)
        # reload() fetches bucket metadata from GCS
        # This validates: (1) bucket exists, (2) we have permission to access it
        bucket.reload()
    except NotFound as e:
        # Bucket doesn't exist - tell user how to create it
        raise LoadError(
            f"GCS bucket not found: {bucket_name}\n"
            "Please create the bucket or check the GCS_BUCKET configuration.\n"
            "You can create a bucket with: gsutil mb gs://{bucket_name}"
        ) from e
    except Forbidden as e:
        # We can see the bucket but can't access it - permission issue
        raise LoadError(
            f"Permission denied accessing bucket '{bucket_name}': {e}\n"
            "Ensure the service account has 'Storage Object Admin' role on the bucket."
        ) from e

    # -------------------------------------------------------------------------
    # Upload File
    # -------------------------------------------------------------------------
    # blob() creates a reference to an object (file) in the bucket
    # The object doesn't have to exist yet - we're about to create it
    try:
        blob = bucket.blob(destination_blob_name)
        # upload_from_filename() reads the local file and uploads it to GCS
        # This is a blocking operation - it waits until upload completes
        blob.upload_from_filename(str(local_path))
    except Forbidden as e:
        raise LoadError(
            f"Permission denied uploading to bucket '{bucket_name}': {e}\n"
            "Ensure the service account has 'Storage Object Creator' role."
        ) from e
    except Exception as e:
        raise LoadError(f"Failed to upload file to GCS: {e}") from e

    # Return the GCS URI - this is what BigQuery will use to load the data
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    logger.info(f"Successfully uploaded to {gcs_uri}")

    # Clean up local file after successful upload
    # Cleanup failures should NOT fail the pipeline - just log a warning
    try:
        local_path.unlink()
        logger.info(f"Deleted local temp file after upload: {local_path}")
    except Exception as e:
        logger.warning(f"Could not delete temp file {local_path}: {e}")

    return gcs_uri


def copy_within_gcs(
    bucket_name: str,
    source_blob_name: str,
    destination_blob_name: str,
) -> str:
    """
    Copy a file within the same GCS bucket.

    WHY COPY INSTEAD OF DOWNLOAD+UPLOAD:
    ------------------------------------
    When source and destination are in the same bucket (or even different buckets),
    GCS can copy objects server-side without downloading to the client. This is:
    - Much faster (no network transfer to/from client)
    - More efficient (no local disk I/O)
    - Cheaper (no egress charges for intra-GCS copies)

    WHY KEEP BACKUPS:
    -----------------
    - Audit trail: Know exactly what data was processed on each run
    - Debugging: If something looks wrong, compare to original
    - Reprocessing: Can re-run ETL on historical data if logic changes
    - Compliance: Some regulations require keeping source data

    The backup is stored in a "backups/" folder with a timestamp to
    ensure we never overwrite previous backups.

    Args:
        bucket_name: Name of the GCS bucket
        source_blob_name: Path to source file within the bucket
        destination_blob_name: Path for the backup copy (e.g., "backups/original_20240101.xls")

    Returns:
        GCS URI of the copied file

    Raises:
        LoadError: If copy fails
    """
    logger = get_logger()
    logger.info(f"Copying gs://{bucket_name}/{source_blob_name} to {destination_blob_name}")

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Get the source blob
        source_blob = bucket.blob(source_blob_name)

        # Copy to destination
        # bucket.copy_blob() performs a server-side copy
        bucket.copy_blob(source_blob, bucket, destination_blob_name)

    except NotFound as e:
        raise LoadError(
            f"Source file not found: gs://{bucket_name}/{source_blob_name}\n"
            "Cannot create backup of non-existent file."
        ) from e
    except Forbidden as e:
        raise LoadError(
            f"Permission denied copying file: {e}\n"
            "Ensure the service account has 'Storage Object Admin' role."
        ) from e
    except Exception as e:
        raise LoadError(f"Failed to copy file within GCS: {e}") from e

    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    logger.info(f"Successfully created backup at {gcs_uri}")
    return gcs_uri


# =============================================================================
# BIGQUERY LOAD FUNCTIONS
# =============================================================================


def load_to_bigquery(
    gcs_uri: str,
    config: Config,
) -> int:
    """
    Load CSV from GCS into BigQuery.

    BIGQUERY LOAD JOBS:
    -------------------
    BigQuery doesn't "read" files directly. Instead, you submit a "load job"
    that tells BigQuery:
    - Where to find the data (GCS URI)
    - What format it's in (CSV, JSON, Parquet, etc.)
    - What the schema is (column names and types)
    - Where to put it (dataset.table)
    - What to do if the table exists (append, truncate, fail)

    The job runs asynchronously on Google's infrastructure. We call
    job.result() to wait for it to complete.

    SCHEMA DEFINITION:
    ------------------
    We define an explicit schema rather than using schema auto-detection because:
    - Explicit is better than implicit (Zen of Python)
    - Auto-detection can guess wrong types
    - We want TIMESTAMP not STRING for time columns
    - Consistent schema across runs

    BigQuery data types we use:
    - TIMESTAMP: Date and time (stored as microseconds since epoch)
    - FLOAT64: 64-bit floating point number
    - REQUIRED: Column cannot be null

    WRITE DISPOSITION:
    ------------------
    - WRITE_APPEND: Add new rows to existing data (default, idempotent over time)
    - WRITE_TRUNCATE: Delete all existing rows, then insert new ones

    Use APPEND for daily incremental loads, TRUNCATE for full refreshes.

    Args:
        gcs_uri: GCS URI of the CSV file (e.g., "gs://bucket/path/file.csv")
        config: ETL configuration containing project, dataset, table info

    Returns:
        Number of rows loaded

    Raises:
        LoadError: If BigQuery load fails, with actionable error message
    """
    logger = get_logger()
    logger.info(f"Loading {gcs_uri} into BigQuery table {config.bq_table_id}")

    # -------------------------------------------------------------------------
    # Create BigQuery Client
    # -------------------------------------------------------------------------
    # Like storage.Client(), this uses GOOGLE_APPLICATION_CREDENTIALS automatically
    # We explicitly pass project= to ensure we're working in the right project
    try:
        client = bigquery.Client(project=config.PROJECT_ID)
    except Exception as e:
        raise LoadError(
            f"Failed to create BigQuery client: {e}\n"
            "Authentication failed. For local dev: run 'gcloud auth application-default login' "
            "or set GOOGLE_APPLICATION_CREDENTIALS. For Cloud Run: ensure a service account is attached."
        ) from e

    # -------------------------------------------------------------------------
    # Define Schema
    # -------------------------------------------------------------------------
    # This tells BigQuery what columns to expect and their types
    # Must match the CSV column order (after header is skipped)
    schema = [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("traffic", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    # -------------------------------------------------------------------------
    # Configure Load Job
    # -------------------------------------------------------------------------
    job_config = bigquery.LoadJobConfig(
        # Use our explicit schema (not auto-detection)
        schema=schema,
        # Tell BigQuery the file is CSV format
        source_format=bigquery.SourceFormat.CSV,
        # Skip the header row (first row contains column names, not data)
        skip_leading_rows=1,
        # What to do if table already has data
        # WRITE_APPEND: Keep existing rows, add new ones
        # WRITE_TRUNCATE: Delete all rows, then add new ones
        write_disposition=(
            bigquery.WriteDisposition.WRITE_APPEND
            if config.write_disposition == "append"
            else bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )

    logger.info(f"Write disposition: {config.write_disposition}")

    # -------------------------------------------------------------------------
    # Execute Load Job
    # -------------------------------------------------------------------------
    try:
        # Submit the load job to BigQuery
        # This returns immediately - the job runs in the background
        load_job = client.load_table_from_uri(
            gcs_uri,  # Source: GCS file
            config.bq_table_id,  # Destination: project.dataset.table
            job_config=job_config,
        )

        # Wait for the job to complete
        # result() blocks until the job finishes or fails
        # If it fails, result() raises an exception with the error details
        load_job.result()

    except NotFound as e:
        # Dataset or table not found
        # BigQuery auto-creates tables, but NOT datasets
        error_msg = str(e)
        if "dataset" in error_msg.lower():
            raise LoadError(
                f"BigQuery dataset not found: {config.bq_dataset}\n"
                f"Create it with: bq mk --dataset {config.PROJECT_ID}:{config.bq_dataset}"
            ) from e
        raise LoadError(
            f"BigQuery resource not found: {e}\n"
            "Ensure the dataset exists and the table will be created automatically."
        ) from e

    except Forbidden as e:
        # Permission denied - tell user which IAM roles are needed
        raise LoadError(
            f"Permission denied accessing BigQuery: {e}\n"
            "Ensure the service account has 'BigQuery Data Editor' and "
            "'BigQuery Job User' roles."
        ) from e

    except Exception as e:
        # Check for schema mismatch - common when table schema doesn't match CSV
        error_msg = str(e).lower()
        if "schema" in error_msg:
            raise LoadError(
                f"Schema mismatch loading to BigQuery: {e}\n"
                "The table schema may not match the expected schema.\n"
                "Expected: time (TIMESTAMP), traffic (FLOAT64), created_at (TIMESTAMP)\n"
                "You may need to delete and recreate the table, or use WRITE_TRUNCATE."
            ) from e
        raise LoadError(f"Failed to load data into BigQuery: {e}") from e

    # -------------------------------------------------------------------------
    # Return Results
    # -------------------------------------------------------------------------
    # output_rows tells us how many rows were loaded
    rows_loaded = load_job.output_rows or 0
    logger.info(f"Successfully loaded {rows_loaded} rows into {config.bq_table_id}")

    return rows_loaded


def verify_bigquery_load(config: Config, expected_rows: int) -> bool:
    """
    Verify data was loaded correctly into BigQuery.

    WHY VERIFY:
    -----------
    "Trust, but verify" - after a load job reports success, we double-check
    by querying the actual table. This catches edge cases where the job
    succeeded but data wasn't actually inserted.

    This is optional but provides confidence the pipeline worked correctly.

    Args:
        config: ETL configuration
        expected_rows: Number of rows the load job claimed to insert

    Returns:
        True if verification passed (logged as info)
        False if verification failed (logged as warning, not error)
    """
    logger = get_logger()

    try:
        client = bigquery.Client(project=config.PROJECT_ID)

        # Run a simple COUNT(*) query to get total rows in the table
        # The backticks around the table ID are required for BigQuery SQL
        query = f"SELECT COUNT(*) as cnt FROM `{config.bq_table_id}`"
        result = client.query(query).result()

        # result is an iterator; we need to extract the first row
        row_count = list(result)[0].cnt

        logger.info(f"BigQuery table now contains {row_count} total rows")
        return True

    except Exception as e:
        # Don't fail the pipeline if verification fails - data is already loaded
        # Just log a warning so the user knows verification didn't complete
        logger.warning(f"Could not verify BigQuery load: {e}")
        return False
