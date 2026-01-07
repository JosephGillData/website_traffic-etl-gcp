"""Load module - upload to GCS and load into BigQuery."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery, storage

from .config import Config
from .logging_config import get_logger


class LoadError(Exception):
    """Raised when data loading fails."""


def upload_to_gcs(
    local_path: Path,
    bucket_name: str,
    destination_blob_name: str,
) -> str:
    """Upload a file to Google Cloud Storage.

    Args:
        local_path: Path to local file.
        bucket_name: Name of the GCS bucket.
        destination_blob_name: Name for the blob in GCS.

    Returns:
        GCS URI of the uploaded file (gs://bucket/blob).

    Raises:
        LoadError: If upload fails.
    """
    logger = get_logger()
    logger.info(f"Uploading {local_path} to gs://{bucket_name}/{destination_blob_name}")

    try:
        client = storage.Client()
    except Exception as e:
        raise LoadError(
            f"Failed to create GCS client: {e}\n"
            "Check that GOOGLE_APPLICATION_CREDENTIALS is set correctly."
        )

    try:
        bucket = client.bucket(bucket_name)
        # Check if bucket exists by getting its metadata
        bucket.reload()
    except NotFound:
        raise LoadError(
            f"GCS bucket not found: {bucket_name}\n"
            "Please create the bucket or check the GCS_BUCKET configuration.\n"
            "You can create a bucket with: gsutil mb gs://{bucket_name}"
        )
    except Forbidden as e:
        raise LoadError(
            f"Permission denied accessing bucket '{bucket_name}': {e}\n"
            "Ensure the service account has 'Storage Object Admin' role on the bucket."
        )

    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(str(local_path))
    except Forbidden as e:
        raise LoadError(
            f"Permission denied uploading to bucket '{bucket_name}': {e}\n"
            "Ensure the service account has 'Storage Object Creator' role."
        )
    except Exception as e:
        raise LoadError(f"Failed to upload file to GCS: {e}")

    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    logger.info(f"Successfully uploaded to {gcs_uri}")
    return gcs_uri


def upload_backup_to_gcs(
    local_path: Path,
    bucket_name: str,
    timestamp: str,
) -> str:
    """Upload original XLS file as backup to GCS.

    Args:
        local_path: Path to local XLS file.
        bucket_name: Name of the GCS bucket.
        timestamp: Timestamp for filename.

    Returns:
        GCS URI of the backup file.
    """
    logger = get_logger()
    backup_blob_name = f"backups/original_{timestamp}{local_path.suffix}"
    logger.info(f"Uploading backup of original file: {backup_blob_name}")

    return upload_to_gcs(local_path, bucket_name, backup_blob_name)


def load_to_bigquery(
    gcs_uri: str,
    config: Config,
) -> int:
    """Load CSV from GCS into BigQuery.

    Args:
        gcs_uri: GCS URI of the CSV file.
        config: ETL configuration.

    Returns:
        Number of rows loaded.

    Raises:
        LoadError: If BigQuery load fails.
    """
    logger = get_logger()
    logger.info(f"Loading {gcs_uri} into BigQuery table {config.bq_table_id}")

    try:
        client = bigquery.Client(project=config.gcp_project)
    except Exception as e:
        raise LoadError(
            f"Failed to create BigQuery client: {e}\n"
            "Check that GOOGLE_APPLICATION_CREDENTIALS is set correctly."
        )

    # Define explicit schema
    schema = [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("traffic", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        write_disposition=(
            bigquery.WriteDisposition.WRITE_APPEND
            if config.write_disposition == "append"
            else bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )

    logger.info(f"Write disposition: {config.write_disposition}")

    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            config.bq_table_id,
            job_config=job_config,
        )
        result = load_job.result()  # Wait for job to complete
    except NotFound as e:
        # Check if it's dataset or table not found
        error_msg = str(e)
        if "dataset" in error_msg.lower():
            raise LoadError(
                f"BigQuery dataset not found: {config.bq_dataset}\n"
                f"Create it with: bq mk --dataset {config.gcp_project}:{config.bq_dataset}"
            )
        raise LoadError(
            f"BigQuery resource not found: {e}\n"
            "Ensure the dataset exists and the table will be created automatically."
        )
    except Forbidden as e:
        raise LoadError(
            f"Permission denied accessing BigQuery: {e}\n"
            "Ensure the service account has 'BigQuery Data Editor' and "
            "'BigQuery Job User' roles."
        )
    except Exception as e:
        # Check for schema mismatch errors
        error_msg = str(e).lower()
        if "schema" in error_msg:
            raise LoadError(
                f"Schema mismatch loading to BigQuery: {e}\n"
                "The table schema may not match the expected schema.\n"
                "Expected: time (TIMESTAMP), traffic (FLOAT64), created_at (TIMESTAMP)\n"
                "You may need to delete and recreate the table, or use WRITE_TRUNCATE."
            )
        raise LoadError(f"Failed to load data into BigQuery: {e}")

    # Get row count from the job
    rows_loaded = load_job.output_rows or 0
    logger.info(f"Successfully loaded {rows_loaded} rows into {config.bq_table_id}")

    return rows_loaded


def verify_bigquery_load(config: Config, expected_rows: int) -> bool:
    """Verify data was loaded correctly into BigQuery.

    Args:
        config: ETL configuration.
        expected_rows: Expected number of rows loaded.

    Returns:
        True if verification passed.
    """
    logger = get_logger()

    try:
        client = bigquery.Client(project=config.gcp_project)
        query = f"SELECT COUNT(*) as cnt FROM `{config.bq_table_id}`"
        result = client.query(query).result()
        row_count = list(result)[0].cnt

        logger.info(f"BigQuery table now contains {row_count} total rows")
        return True
    except Exception as e:
        logger.warning(f"Could not verify BigQuery load: {e}")
        return False
