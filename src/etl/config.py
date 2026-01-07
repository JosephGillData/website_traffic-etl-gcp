"""Configuration management for ETL pipeline.

Loads configuration from environment variables with validation.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    """Immutable configuration for the ETL pipeline."""

    gcp_project: str
    gcs_bucket: str
    bq_dataset: str
    bq_table: str
    local_xls_path: Path
    google_credentials_path: Path
    write_disposition: Literal["append", "truncate"]

    @property
    def bq_table_id(self) -> str:
        """Full BigQuery table ID."""
        return f"{self.gcp_project}.{self.bq_dataset}.{self.bq_table}"


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""


def _get_required_env(key: str) -> str:
    """Get a required environment variable or raise ConfigError."""
    value = os.getenv(key)
    if not value:
        raise ConfigError(
            f"Missing required environment variable: {key}\n"
            f"Please set {key} in your .env file or environment."
        )
    return value


def _get_optional_env(key: str, default: str) -> str:
    """Get an optional environment variable with a default."""
    return os.getenv(key, default)


def load_config(env_path: Path | None = None) -> Config:
    """Load and validate configuration from environment.

    Args:
        env_path: Optional path to .env file. Defaults to .env in current directory.

    Returns:
        Validated Config object.

    Raises:
        ConfigError: If required configuration is missing or invalid.
    """
    # Load .env file
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    # Collect all errors for better UX
    errors: list[str] = []

    # Required variables
    required_vars = [
        "GCP_PROJECT",
        "GCS_BUCKET",
        "BQ_DATASET",
        "BQ_TABLE",
        "LOCAL_XLS_PATH",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]

    values: dict[str, str] = {}
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            errors.append(f"  - {var}")
        else:
            values[var] = value

    if errors:
        raise ConfigError(
            "Missing required environment variables:\n"
            + "\n".join(errors)
            + "\n\nPlease set these in your .env file or environment.\n"
            "See .env.example for reference."
        )

    # Validate paths exist
    xls_path = Path(values["LOCAL_XLS_PATH"])
    if not xls_path.exists():
        raise ConfigError(
            f"XLS file not found: {xls_path}\n"
            "Please check LOCAL_XLS_PATH in your configuration."
        )

    credentials_path = Path(values["GOOGLE_APPLICATION_CREDENTIALS"])
    if not credentials_path.exists():
        raise ConfigError(
            f"Google credentials file not found: {credentials_path}\n"
            "Please check GOOGLE_APPLICATION_CREDENTIALS in your configuration.\n"
            "See README.md for instructions on creating a service account."
        )

    # Set credentials in environment for GCP client libraries
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path.absolute())

    # Optional: write disposition (append or truncate)
    write_disposition = _get_optional_env("BQ_WRITE_DISPOSITION", "append").lower()
    if write_disposition not in ("append", "truncate"):
        raise ConfigError(
            f"Invalid BQ_WRITE_DISPOSITION: {write_disposition}\n"
            "Must be 'append' or 'truncate'."
        )

    return Config(
        gcp_project=values["GCP_PROJECT"],
        gcs_bucket=values["GCS_BUCKET"],
        bq_dataset=values["BQ_DATASET"],
        bq_table=values["BQ_TABLE"],
        local_xls_path=xls_path,
        google_credentials_path=credentials_path,
        write_disposition=write_disposition,  # type: ignore[arg-type]
    )
