"""Configuration from environment variables."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""


@dataclass(frozen=True)
class Config:
    """Immutable configuration for the ETL pipeline."""

    PROJECT_ID: str
    gcs_bucket: str
    bq_dataset: str
    bq_table: str
    write_disposition: Literal["append", "truncate"]

    @property
    def input_gcs_uri(self) -> str:
        return f"gs://{self.gcs_bucket}/raw_data/traffic_spreadsheet.xls"

    @property
    def bq_table_id(self) -> str:
        return f"{self.PROJECT_ID}.{self.bq_dataset}.{self.bq_table}"


def load_config(env_path: Path | None = None) -> Config:
    """Load and validate configuration from environment variables."""
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    required = ["PROJECT_ID", "GCS_BUCKET", "BQ_DATASET", "BQ_TABLE"]
    missing = [var for var in required if not os.getenv(var)]

    if missing:
        raise ConfigError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Set these in your .env file. See .env.example for reference."
        )

    write_disposition = os.getenv("BQ_WRITE_DISPOSITION", "append").lower()
    if write_disposition not in ("append", "truncate"):
        raise ConfigError(
            f"Invalid BQ_WRITE_DISPOSITION: {write_disposition}. Must be 'append' or 'truncate'."
        )

    return Config(
        PROJECT_ID=os.getenv("PROJECT_ID"),  # type: ignore
        gcs_bucket=os.getenv("GCS_BUCKET"),  # type: ignore
        bq_dataset=os.getenv("BQ_DATASET"),  # type: ignore
        bq_table=os.getenv("BQ_TABLE"),  # type: ignore
        write_disposition=write_disposition,  # type: ignore
    )
