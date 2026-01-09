"""Unit tests for configuration module."""

from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pytest
from src.etl.config import ConfigError, load_config


class TestLoadConfig:
    """Tests for configuration loading."""

    def test_loads_valid_config(self, tmp_path: Path):
        """Should load valid configuration from environment."""
        # Create .env file with INPUT_GCS_URI
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
INPUT_GCS_URI=gs://test-bucket/raw_data/traffic_spreadsheet.xls
BQ_DATASET=test_dataset
BQ_TABLE=test_table
"""
        )

        # Clear any existing env vars and load config
        with mock.patch.dict(os.environ, {}, clear=True):
            config = load_config(env_file)

        assert config.gcp_project == "test-project"
        assert config.gcs_bucket == "test-bucket"
        assert config.gcs_source_path == "raw_data/traffic_spreadsheet.xls"
        assert config.bq_dataset == "test_dataset"
        assert config.bq_table == "test_table"
        assert config.write_disposition == "append"  # default

    def test_raises_on_missing_env_vars(self, tmp_path: Path):
        """Should raise ConfigError if required vars are missing."""
        env_file = tmp_path / ".env"
        env_file.write_text("GCP_PROJECT=test-project")

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="Missing required environment"):
                load_config(env_file)

    def test_validates_write_disposition(self, tmp_path: Path):
        """Should raise ConfigError on invalid write disposition."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
INPUT_GCS_URI=gs://test-bucket/raw_data/traffic_spreadsheet.xls
BQ_DATASET=test_dataset
BQ_TABLE=test_table
BQ_WRITE_DISPOSITION=invalid
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="Invalid BQ_WRITE_DISPOSITION"):
                load_config(env_file)

    def test_validates_bucket_mismatch(self, tmp_path: Path):
        """Should raise ConfigError if GCS_BUCKET doesn't match INPUT_GCS_URI bucket."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
GCP_PROJECT=test-project
GCS_BUCKET=different-bucket
INPUT_GCS_URI=gs://test-bucket/raw_data/traffic_spreadsheet.xls
BQ_DATASET=test_dataset
BQ_TABLE=test_table
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="does not match bucket"):
                load_config(env_file)

    def test_validates_invalid_gcs_uri(self, tmp_path: Path):
        """Should raise ConfigError on invalid GCS URI format."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
INPUT_GCS_URI=invalid-uri-format
BQ_DATASET=test_dataset
BQ_TABLE=test_table
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="Invalid GCS URI format"):
                load_config(env_file)

    def test_bq_table_id_property(self, tmp_path: Path):
        """Should generate correct BigQuery table ID."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
GCP_PROJECT=my-project
GCS_BUCKET=my-bucket
INPUT_GCS_URI=gs://my-bucket/raw_data/traffic_spreadsheet.xls
BQ_DATASET=my_dataset
BQ_TABLE=my_table
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            config = load_config(env_file)

        assert config.bq_table_id == "my-project.my_dataset.my_table"

    def test_input_gcs_uri_property(self, tmp_path: Path):
        """Should generate correct GCS source URI."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
GCP_PROJECT=my-project
GCS_BUCKET=my-bucket
INPUT_GCS_URI=gs://my-bucket/raw_data/traffic_spreadsheet.xls
BQ_DATASET=my_dataset
BQ_TABLE=my_table
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            config = load_config(env_file)

        assert config.input_gcs_uri == "gs://my-bucket/raw_data/traffic_spreadsheet.xls"
