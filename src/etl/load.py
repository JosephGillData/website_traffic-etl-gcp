"""Load: Upload to GCS and load into BigQuery."""

import logging
from pathlib import Path

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery, storage

from .config import Config

logger = logging.getLogger("etl")


class LoadError(Exception):
    """Raised when data loading fails."""


def upload_to_gcs(local_path: Path, bucket_name: str, destination_blob: str) -> str:
    """Upload a file to GCS. Returns the GCS URI."""
    logger.info(f"Uploading to gs://{bucket_name}/{destination_blob}")

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        bucket.reload()
    except NotFound as e:
        raise LoadError(f"Bucket not found: {bucket_name}") from e
    except Forbidden as e:
        raise LoadError(f"Permission denied: {e}") from e
    except Exception as e:
        raise LoadError(f"GCS client error: {e}") from e

    try:
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(str(local_path))
    except Exception as e:
        raise LoadError(f"Upload failed: {e}") from e

    # Clean up local file
    try:
        local_path.unlink()
    except Exception:
        pass

    gcs_uri = f"gs://{bucket_name}/{destination_blob}"
    logger.info(f"Uploaded: {gcs_uri}")
    return gcs_uri


def copy_within_gcs(bucket_name: str, source_blob: str, dest_blob: str) -> str:
    """Copy a file within GCS (server-side). Returns the destination URI."""
    logger.info(f"Copying to {dest_blob}")

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        source = bucket.blob(source_blob)
        bucket.copy_blob(source, bucket, dest_blob)
    except NotFound as e:
        raise LoadError(f"Source not found: gs://{bucket_name}/{source_blob}") from e
    except Exception as e:
        raise LoadError(f"Copy failed: {e}") from e

    return f"gs://{bucket_name}/{dest_blob}"


def load_to_bigquery(gcs_uri: str, config: Config) -> int:
    """Load CSV from GCS into BigQuery. Returns row count."""
    logger.info(f"Loading into {config.bq_table_id}")

    try:
        client = bigquery.Client(project=config.PROJECT_ID)
    except Exception as e:
        raise LoadError(f"BigQuery client error: {e}") from e

    schema = [
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("traffic", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_APPEND
            if config.write_disposition == "append"
            else bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, config.bq_table_id, job_config=job_config)
        load_job.result()
    except NotFound as e:
        if "dataset" in str(e).lower():
            raise LoadError(
                f"Dataset not found: {config.bq_dataset}\n"
                f"Create it: bq mk --dataset {config.PROJECT_ID}:{config.bq_dataset}"
            ) from e
        raise LoadError(f"BigQuery error: {e}") from e
    except Forbidden as e:
        raise LoadError(f"Permission denied: {e}") from e
    except Exception as e:
        raise LoadError(f"Load failed: {e}") from e

    rows = load_job.output_rows or 0
    logger.info(f"Loaded {rows} rows into BigQuery")
    return rows
