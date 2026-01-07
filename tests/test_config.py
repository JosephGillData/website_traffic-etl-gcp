"""Unit tests for configuration module."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest import mock

import pytest

from src.etl.config import ConfigError, load_config


class TestLoadConfig:
    """Tests for configuration loading."""

    def test_loads_valid_config(self, tmp_path: Path):
        """Should load valid configuration from environment."""
        # Create a fake XLS file
        xls_file = tmp_path / "test.xls"
        xls_file.touch()

        # Create a fake credentials file
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        # Create .env file
        env_file = tmp_path / ".env"
        env_file.write_text(
            f"""
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
BQ_DATASET=test_dataset
BQ_TABLE=test_table
LOCAL_XLS_PATH={xls_file}
GOOGLE_APPLICATION_CREDENTIALS={creds_file}
"""
        )

        # Clear any existing env vars and load config
        with mock.patch.dict(os.environ, {}, clear=True):
            config = load_config(env_file)

        assert config.gcp_project == "test-project"
        assert config.gcs_bucket == "test-bucket"
        assert config.bq_dataset == "test_dataset"
        assert config.bq_table == "test_table"
        assert config.local_xls_path == xls_file
        assert config.google_credentials_path == creds_file
        assert config.write_disposition == "append"  # default

    def test_raises_on_missing_env_vars(self, tmp_path: Path):
        """Should raise ConfigError if required vars are missing."""
        env_file = tmp_path / ".env"
        env_file.write_text("GCP_PROJECT=test-project")

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="Missing required environment"):
                load_config(env_file)

    def test_raises_on_missing_xls_file(self, tmp_path: Path):
        """Should raise ConfigError if XLS file doesn't exist."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        env_file = tmp_path / ".env"
        env_file.write_text(
            f"""
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
BQ_DATASET=test_dataset
BQ_TABLE=test_table
LOCAL_XLS_PATH=/nonexistent/file.xls
GOOGLE_APPLICATION_CREDENTIALS={creds_file}
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="XLS file not found"):
                load_config(env_file)

    def test_raises_on_missing_credentials_file(self, tmp_path: Path):
        """Should raise ConfigError if credentials file doesn't exist."""
        xls_file = tmp_path / "test.xls"
        xls_file.touch()

        env_file = tmp_path / ".env"
        env_file.write_text(
            f"""
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
BQ_DATASET=test_dataset
BQ_TABLE=test_table
LOCAL_XLS_PATH={xls_file}
GOOGLE_APPLICATION_CREDENTIALS=/nonexistent/creds.json
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="credentials file not found"):
                load_config(env_file)

    def test_validates_write_disposition(self, tmp_path: Path):
        """Should raise ConfigError on invalid write disposition."""
        xls_file = tmp_path / "test.xls"
        xls_file.touch()
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        env_file = tmp_path / ".env"
        env_file.write_text(
            f"""
GCP_PROJECT=test-project
GCS_BUCKET=test-bucket
BQ_DATASET=test_dataset
BQ_TABLE=test_table
LOCAL_XLS_PATH={xls_file}
GOOGLE_APPLICATION_CREDENTIALS={creds_file}
BQ_WRITE_DISPOSITION=invalid
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="Invalid BQ_WRITE_DISPOSITION"):
                load_config(env_file)

    def test_bq_table_id_property(self, tmp_path: Path):
        """Should generate correct BigQuery table ID."""
        xls_file = tmp_path / "test.xls"
        xls_file.touch()
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        env_file = tmp_path / ".env"
        env_file.write_text(
            f"""
GCP_PROJECT=my-project
GCS_BUCKET=test-bucket
BQ_DATASET=my_dataset
BQ_TABLE=my_table
LOCAL_XLS_PATH={xls_file}
GOOGLE_APPLICATION_CREDENTIALS={creds_file}
"""
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            config = load_config(env_file)

        assert config.bq_table_id == "my-project.my_dataset.my_table"
