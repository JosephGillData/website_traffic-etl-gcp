"""
Configuration Management for ETL Pipeline
==========================================

This module handles loading and validating configuration from environment variables.
It follows the "12-factor app" methodology: configuration should come from the
environment, not be hardcoded in the source code.

WHY USE ENVIRONMENT VARIABLES:
------------------------------
1. Security: Secrets (like credentials paths) stay out of git
2. Flexibility: Different values for dev/staging/production
3. Simplicity: No need to modify code to change settings
4. Standard: Works with Docker, Kubernetes, CI/CD, cron, etc.

HOW CONFIGURATION FLOWS:
------------------------
1. User creates a `.env` file with key=value pairs (see .env.example)
2. python-dotenv reads `.env` and sets them as environment variables
3. This module reads those environment variables via os.getenv()
4. Values are validated and bundled into an immutable Config object
5. Other modules receive Config and use its values

GOOGLE CLOUD AUTHENTICATION:
----------------------------
Google Cloud client libraries (google-cloud-storage, google-cloud-bigquery)
automatically look for credentials in this order:

1. GOOGLE_APPLICATION_CREDENTIALS environment variable (what we use)
   - Points to a JSON file containing service account key
   - Most explicit and portable method

2. Application Default Credentials (ADC)
   - `gcloud auth application-default login` sets these up
   - Good for local development with your personal account

3. Compute Engine / Cloud Run metadata
   - Automatic when running on GCP infrastructure
   - No credentials file needed

We use method #1 (GOOGLE_APPLICATION_CREDENTIALS) because:
- It's explicit: clear what credentials are being used
- It's portable: works the same everywhere (local, CI, production)
- It's auditable: you know which service account is being used

The JSON file contains:
- project_id: Which GCP project the service account belongs to
- private_key: The actual cryptographic key for authentication
- client_email: The service account's email (e.g., name@project.iam.gserviceaccount.com)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# python-dotenv reads .env files and sets environment variables
# This is the bridge between your .env file and os.getenv()
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
    - gcs_bucket: GCS_BUCKET - Cloud Storage bucket for staging files
    - bq_dataset: BQ_DATASET - BigQuery dataset name
    - bq_table: BQ_TABLE - BigQuery table name
    - local_xls_path: LOCAL_XLS_PATH - Path to source Excel file
    - google_credentials_path: GOOGLE_APPLICATION_CREDENTIALS - Service account JSON
    - write_disposition: BQ_WRITE_DISPOSITION - "append" or "truncate"
    """

    gcp_project: str
    gcs_bucket: str
    bq_dataset: str
    bq_table: str
    local_xls_path: Path
    google_credentials_path: Path
    write_disposition: Literal["append", "truncate"]

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


def _get_required_env(key: str) -> str:
    """
    Get a required environment variable or raise ConfigError.

    The underscore prefix (_) is a Python convention meaning "private" -
    this function is only meant to be used within this module.
    """
    value = os.getenv(key)
    if not value:
        raise ConfigError(
            f"Missing required environment variable: {key}\n"
            f"Please set {key} in your .env file or environment."
        )
    return value


def _get_optional_env(key: str, default: str) -> str:
    """
    Get an optional environment variable with a default value.

    Use this for settings that have sensible defaults.
    """
    return os.getenv(key, default)


# =============================================================================
# MAIN CONFIGURATION LOADER
# =============================================================================


def load_config(env_path: Path | None = None) -> Config:
    """
    Load and validate configuration from environment variables.

    This is the main entry point for configuration. It:
    1. Reads the .env file (if it exists) into environment variables
    2. Validates all required variables are present
    3. Validates file paths exist
    4. Sets up Google Cloud authentication
    5. Returns an immutable Config object

    FAIL-FAST PHILOSOPHY:
    ---------------------
    We validate everything upfront and fail with clear error messages.
    It's better to fail immediately with "GCS_BUCKET not set" than to
    fail 10 minutes into a run with a confusing error.

    Args:
        env_path: Optional explicit path to .env file.
                  If not provided, python-dotenv searches for .env in
                  the current directory and parent directories.

    Returns:
        Validated Config object with all settings.

    Raises:
        ConfigError: If any required config is missing or invalid.
                     The error message explains exactly what's wrong.
    """
    # -------------------------------------------------------------------------
    # STEP 1: Load .env file
    # -------------------------------------------------------------------------
    # load_dotenv() reads the .env file and calls os.environ[key] = value
    # for each line. After this, os.getenv(key) returns those values.
    #
    # If .env doesn't exist, this silently does nothing (no error).
    # Environment variables set by other means (shell, Docker, etc.) still work.
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

    required_vars = [
        "GCP_PROJECT",  # e.g., "my-gcp-project-123"
        "GCS_BUCKET",  # e.g., "my-data-bucket"
        "BQ_DATASET",  # e.g., "analytics"
        "BQ_TABLE",  # e.g., "traffic_data"
        "LOCAL_XLS_PATH",  # e.g., "data/traffic_spreadsheet.xls"
        "GOOGLE_APPLICATION_CREDENTIALS",  # e.g., "keys/service-account.json"
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
            + "\n\nPlease set these in your .env file or environment.\n"
            "See .env.example for reference."
        )

    # -------------------------------------------------------------------------
    # STEP 3: Validate file paths exist
    # -------------------------------------------------------------------------
    # It's better to fail here with "file not found" than to fail later
    # with a cryptic pandas or GCS error.

    xls_path = Path(values["LOCAL_XLS_PATH"])
    if not xls_path.exists():
        raise ConfigError(
            f"XLS file not found: {xls_path}\n" "Please check LOCAL_XLS_PATH in your configuration."
        )

    credentials_path = Path(values["GOOGLE_APPLICATION_CREDENTIALS"])
    if not credentials_path.exists():
        raise ConfigError(
            f"Google credentials file not found: {credentials_path}\n"
            "Please check GOOGLE_APPLICATION_CREDENTIALS in your configuration.\n"
            "See README.md for instructions on creating a service account."
        )

    # -------------------------------------------------------------------------
    # STEP 4: Set up Google Cloud authentication
    # -------------------------------------------------------------------------
    # The Google Cloud client libraries (google-cloud-storage, google-cloud-bigquery)
    # automatically read GOOGLE_APPLICATION_CREDENTIALS from the environment.
    #
    # We set it here with the absolute path to ensure it works regardless of
    # what directory the script runs from.
    #
    # IMPORTANT: This is the "magic" that makes GCS and BigQuery work without
    # explicitly passing credentials to each client. When you do:
    #     client = storage.Client()
    # The client automatically uses the credentials from this env var.
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path.absolute())

    # -------------------------------------------------------------------------
    # STEP 5: Handle optional configuration
    # -------------------------------------------------------------------------
    # write_disposition controls whether BigQuery appends to or replaces data
    write_disposition = _get_optional_env("BQ_WRITE_DISPOSITION", "append").lower()
    if write_disposition not in ("append", "truncate"):
        raise ConfigError(
            f"Invalid BQ_WRITE_DISPOSITION: {write_disposition}\n" "Must be 'append' or 'truncate'."
        )

    # -------------------------------------------------------------------------
    # STEP 6: Return immutable config object
    # -------------------------------------------------------------------------
    return Config(
        gcp_project=values["GCP_PROJECT"],
        gcs_bucket=values["GCS_BUCKET"],
        bq_dataset=values["BQ_DATASET"],
        bq_table=values["BQ_TABLE"],
        local_xls_path=xls_path,
        google_credentials_path=credentials_path,
        write_disposition=write_disposition,  # type: ignore[arg-type]
    )
