"""
Configuration Management for ETL Pipeline
==========================================

This module handles loading and validating configuration from environment variables.
It follows the "12-factor app" methodology: configuration should come from the
environment, not be hardcoded in the source code.

WHY USE ENVIRONMENT VARIABLES:
------------------------------
1. Security: Secrets stay out of git
2. Flexibility: Different values for dev/staging/production
3. Simplicity: No need to modify code to change settings
4. Standard: Works with Docker, Kubernetes, Cloud Run, etc.

HOW CONFIGURATION FLOWS:
------------------------
LOCAL DEVELOPMENT:
1. User creates a `.env` file with key=value pairs (see .env.example)
2. python-dotenv reads `.env` and sets them as environment variables
3. This module reads those environment variables via os.getenv()

CLOUD RUN JOBS:
1. Environment variables are set in the job configuration
2. No .env file needed - Cloud Run provides the vars directly
3. This module reads them the same way via os.getenv()

GOOGLE CLOUD AUTHENTICATION (ADC):
----------------------------------
This pipeline uses Application Default Credentials (ADC). The GCP client libraries
(storage.Client(), bigquery.Client()) automatically discover credentials in this order:

1. GOOGLE_APPLICATION_CREDENTIALS environment variable (local dev only)
   - Points to a JSON file containing service account key
   - Set this in .env for local development

2. gcloud CLI credentials
   - Run `gcloud auth application-default login` for interactive local dev

3. Attached service account (Cloud Run / GCE / GKE)
   - Automatic when running on GCP infrastructure
   - No credentials file needed - uses the attached service account identity
   - This is the RECOMMENDED method for production

IMPORTANT - CLOUD RUN JOBS:
---------------------------
- Do NOT bake JSON key files into container images
- Do NOT set GOOGLE_APPLICATION_CREDENTIALS in Cloud Run
- Instead, attach a service account to the Cloud Run Job
- The GCP client libraries will automatically use the attached identity via ADC

LOCAL DEVELOPMENT OPTIONS:
--------------------------
Option A (Recommended): Use gcloud CLI
  gcloud auth application-default login

Option B: Use service account JSON key
  Set GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json in your .env file
  Never commit the key file to git!
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# python-dotenv reads .env files and sets environment variables
# This is the bridge between your .env file and os.getenv()
# In Cloud Run, this does nothing (no .env file), which is fine.
from dotenv import load_dotenv

# =============================================================================
# CONFIGURATION DATA CLASS
# =============================================================================


@dataclass(frozen=True)
class Config:
    """
    Immutable configuration container for the ETL pipeline.

    WHY USE A DATACLASS:
    --------------------
    - Automatic __init__, __repr__, __eq__ methods
    - Type hints for IDE autocompletion
    - frozen=True makes it immutable (can't accidentally modify config)

    WHY IMMUTABLE (frozen=True):
    ----------------------------
    Configuration should be set once at startup and not change during execution.
    This prevents bugs where config is accidentally modified mid-run.
    If you need to change a value, use dataclasses.replace() to create a new
    Config object (see __main__.py for example with --truncate flag).

    ATTRIBUTES:
    -----------
    All attributes correspond to environment variables:
    - gcp_project: GCP_PROJECT - Your Google Cloud project ID
    - gcs_bucket: GCS_BUCKET - Cloud Storage bucket (extracted from INPUT_GCS_URI)
    - gcs_source_path: Path within bucket (extracted from INPUT_GCS_URI)
    - bq_dataset: BQ_DATASET - BigQuery dataset name
    - bq_table: BQ_TABLE - BigQuery table name
    - write_disposition: BQ_WRITE_DISPOSITION - "append" or "truncate"
    """

    gcp_project: str
    gcs_bucket: str
    gcs_source_path: str
    bq_dataset: str
    bq_table: str
    write_disposition: Literal["append", "truncate"]

    @property
    def input_gcs_uri(self) -> str:
        """
        Full GCS URI for the source file.

        Example: "gs://my-bucket/raw_data/traffic_spreadsheet.xls"
        """
        return f"gs://{self.gcs_bucket}/{self.gcs_source_path}"

    @property
    def bq_table_id(self) -> str:
        """
        Full BigQuery table ID in the format: project.dataset.table

        BigQuery references tables using this three-part identifier:
        - project: The GCP project that contains the dataset
        - dataset: A logical grouping of tables (like a database schema)
        - table: The actual table name

        Example: "my-project.analytics_dataset.traffic_data"
        """
        return f"{self.gcp_project}.{self.bq_dataset}.{self.bq_table}"


# =============================================================================
# CUSTOM EXCEPTION
# =============================================================================


class ConfigError(Exception):
    """
    Raised when configuration is invalid or missing.

    This is a custom exception so callers can catch configuration errors
    specifically and handle them differently from other errors.

    For example, in __main__.py:
        try:
            config = load_config()
        except ConfigError:
            # User-fixable problem - show helpful message and exit
            return EXIT_CONFIG_ERROR
    """


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _get_optional_env(key: str, default: str) -> str:
    """
    Get an optional environment variable with a default value.

    Use this for settings that have sensible defaults.
    """
    return os.getenv(key, default)


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    """
    Parse a GCS URI into bucket name and blob path.

    Args:
        uri: Full GCS URI (e.g., "gs://my-bucket/path/to/file.xls")

    Returns:
        Tuple of (bucket_name, blob_path)

    Raises:
        ConfigError: If URI format is invalid
    """
    # Pattern: gs://bucket-name/path/to/object
    pattern = r"^gs://([^/]+)/(.+)$"
    match = re.match(pattern, uri)

    if not match:
        raise ConfigError(
            f"Invalid GCS URI format: {uri}\n"
            "Expected format: gs://bucket-name/path/to/file.xls"
        )

    bucket_name = match.group(1)
    blob_path = match.group(2)

    return bucket_name, blob_path


# =============================================================================
# MAIN CONFIGURATION LOADER
# =============================================================================


def load_config(env_path: Path | None = None) -> Config:
    """
    Load and validate configuration from environment variables.

    This is the main entry point for configuration. It:
    1. Reads the .env file (if it exists) for local development
    2. Validates all required variables are present
    3. Parses INPUT_GCS_URI into bucket and path
    4. Returns an immutable Config object

    NOTE ON AUTHENTICATION:
    -----------------------
    This function does NOT set up Google Cloud authentication.
    Authentication is handled automatically by the GCP client libraries:
    - In Cloud Run: Uses the attached service account (ADC)
    - Locally: Uses GOOGLE_APPLICATION_CREDENTIALS or `gcloud auth application-default login`

    FAIL-FAST PHILOSOPHY:
    ---------------------
    We validate everything upfront and fail with clear error messages.
    It's better to fail immediately with "GCS_BUCKET not set" than to
    fail 10 minutes into a run with a confusing error.

    Args:
        env_path: Optional explicit path to .env file (for testing).
                  If not provided, python-dotenv searches for .env in
                  the current directory and parent directories.

    Returns:
        Validated Config object with all settings.

    Raises:
        ConfigError: If any required config is missing or invalid.
                     The error message explains exactly what's wrong.
    """
    # -------------------------------------------------------------------------
    # STEP 1: Load .env file (for local development only)
    # -------------------------------------------------------------------------
    # load_dotenv() reads the .env file and calls os.environ[key] = value
    # for each line. After this, os.getenv(key) returns those values.
    #
    # In Cloud Run, there's no .env file, so this does nothing - the env vars
    # are already set by the Cloud Run Job configuration.
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()  # Searches for .env automatically

    # -------------------------------------------------------------------------
    # STEP 2: Check all required variables exist
    # -------------------------------------------------------------------------
    # We collect ALL missing variables before raising an error.
    # This gives the user a complete list to fix, rather than fixing one,
    # running again, fixing another, etc.
    errors: list[str] = []

    # Required environment variables
    required_vars = [
        "GCP_PROJECT",  # e.g., "my-gcp-project-123"
        "GCS_BUCKET",  # e.g., "my-data-bucket"
        "INPUT_GCS_URI",  # e.g., "gs://bucket/raw_data/traffic_spreadsheet.xls"
        "BQ_DATASET",  # e.g., "analytics"
        "BQ_TABLE",  # e.g., "traffic_data"
    ]

    values: dict[str, str] = {}
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            errors.append(f"  - {var}")
        else:
            values[var] = value

    # If any variables are missing, fail with a helpful message
    if errors:
        raise ConfigError(
            "Missing required environment variables:\n"
            + "\n".join(errors)
            + "\n\nFor local development: Set these in your .env file.\n"
            "For Cloud Run: Set these in the Job configuration.\n"
            "See .env.example for reference."
        )

    # -------------------------------------------------------------------------
    # STEP 3: Parse INPUT_GCS_URI
    # -------------------------------------------------------------------------
    # Extract bucket name and blob path from the full GCS URI
    bucket_from_uri, blob_path = _parse_gcs_uri(values["INPUT_GCS_URI"])

    # Validate that GCS_BUCKET matches the bucket in INPUT_GCS_URI
    if values["GCS_BUCKET"] != bucket_from_uri:
        raise ConfigError(
            f"GCS_BUCKET ({values['GCS_BUCKET']}) does not match bucket in "
            f"INPUT_GCS_URI ({bucket_from_uri}).\n"
            "These should be the same bucket."
        )

    # -------------------------------------------------------------------------
    # STEP 4: Handle optional configuration
    # -------------------------------------------------------------------------
    # write_disposition controls whether BigQuery appends to or replaces data
    write_disposition = _get_optional_env("BQ_WRITE_DISPOSITION", "append").lower()
    if write_disposition not in ("append", "truncate"):
        raise ConfigError(
            f"Invalid BQ_WRITE_DISPOSITION: {write_disposition}\n"
            "Must be 'append' or 'truncate'."
        )

    # -------------------------------------------------------------------------
    # STEP 5: Return immutable config object
    # -------------------------------------------------------------------------
    return Config(
        gcp_project=values["GCP_PROJECT"],
        gcs_bucket=values["GCS_BUCKET"],
        gcs_source_path=blob_path,
        bq_dataset=values["BQ_DATASET"],
        bq_table=values["BQ_TABLE"],
        write_disposition=write_disposition,  # type: ignore[arg-type]
    )
