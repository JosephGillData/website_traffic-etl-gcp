"""CLI entrypoint for ETL pipeline.

Usage:
    python -m etl run [--verbose] [--truncate]
    python -m etl validate
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from .config import ConfigError, load_config
from .extract import ExtractionError, extract_from_xls
from .load import LoadError, load_to_bigquery, upload_backup_to_gcs, upload_to_gcs, verify_bigquery_load
from .logging_config import get_logger, setup_logging
from .transform import TransformationError, save_to_csv, transform


# Exit codes
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_EXTRACTION_ERROR = 2
EXIT_TRANSFORM_ERROR = 3
EXIT_LOAD_ERROR = 4
EXIT_UNKNOWN_ERROR = 99


def run_etl(verbose: bool = False, truncate: bool = False) -> int:
    """Run the complete ETL pipeline.

    Args:
        verbose: Enable verbose logging.
        truncate: Use WRITE_TRUNCATE instead of WRITE_APPEND.

    Returns:
        Exit code (0 for success).
    """
    logger = setup_logging(verbose)
    logger.info("Starting ETL pipeline")

    # Generate timestamp for this run
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Run timestamp: {run_timestamp}")

    # Load configuration
    try:
        config = load_config()
        # Override write disposition if truncate flag is set
        if truncate:
            # Create new config with truncate disposition
            from dataclasses import replace
            config = replace(config, write_disposition="truncate")
        logger.info(f"Configuration loaded: project={config.gcp_project}, bucket={config.gcs_bucket}")
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return EXIT_CONFIG_ERROR

    # Extract
    try:
        logger.info("=== EXTRACT PHASE ===")
        df = extract_from_xls(config.local_xls_path)
    except ExtractionError as e:
        logger.error(f"Extraction failed: {e}")
        return EXIT_EXTRACTION_ERROR

    # Transform
    try:
        logger.info("=== TRANSFORM PHASE ===")
        df = transform(df)

        # Save to local CSV
        output_dir = config.local_xls_path.parent
        csv_path = save_to_csv(df, output_dir, run_timestamp)
    except TransformationError as e:
        logger.error(f"Transformation failed: {e}")
        return EXIT_TRANSFORM_ERROR

    # Load
    try:
        logger.info("=== LOAD PHASE ===")

        # Upload backup of original XLS file
        upload_backup_to_gcs(
            config.local_xls_path,
            config.gcs_bucket,
            run_timestamp,
        )

        # Upload processed CSV to GCS
        csv_blob_name = f"processed/traffic_data_{run_timestamp}.csv"
        gcs_uri = upload_to_gcs(csv_path, config.gcs_bucket, csv_blob_name)

        # Load into BigQuery
        rows_loaded = load_to_bigquery(gcs_uri, config)

        # Verify load
        verify_bigquery_load(config, rows_loaded)

    except LoadError as e:
        logger.error(f"Load failed: {e}")
        return EXIT_LOAD_ERROR

    logger.info("=== ETL PIPELINE COMPLETE ===")
    logger.info(f"Processed {rows_loaded} rows successfully")
    return EXIT_SUCCESS


def validate_config() -> int:
    """Validate configuration without running ETL.

    Returns:
        Exit code (0 if configuration is valid).
    """
    logger = setup_logging(verbose=True)
    logger.info("Validating configuration...")

    try:
        config = load_config()
        logger.info("Configuration is valid:")
        logger.info(f"  GCP Project: {config.gcp_project}")
        logger.info(f"  GCS Bucket: {config.gcs_bucket}")
        logger.info(f"  BigQuery Dataset: {config.bq_dataset}")
        logger.info(f"  BigQuery Table: {config.bq_table}")
        logger.info(f"  XLS Path: {config.local_xls_path}")
        logger.info(f"  Credentials: {config.google_credentials_path}")
        logger.info(f"  Write Disposition: {config.write_disposition}")
        return EXIT_SUCCESS
    except ConfigError as e:
        logger.error(f"Configuration invalid: {e}")
        return EXIT_CONFIG_ERROR


def main() -> int:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="etl",
        description="Traffic data ETL pipeline - loads XLS data to BigQuery via GCS",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the ETL pipeline")
    run_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging",
    )
    run_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate BigQuery table before loading (default: append)",
    )

    # Validate command
    subparsers.add_parser("validate", help="Validate configuration without running ETL")

    args = parser.parse_args()

    if args.command == "run":
        return run_etl(verbose=args.verbose, truncate=args.truncate)
    elif args.command == "validate":
        return validate_config()
    else:
        parser.print_help()
        return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
