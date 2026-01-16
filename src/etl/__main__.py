"""CLI entrypoint: python -m etl run"""

import argparse
import logging
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .config import ConfigError, load_config
from .extract import ExtractionError, extract_from_gcs
from .load import LoadError, copy_within_gcs, load_to_bigquery, upload_to_gcs
from .transform import TransformationError, save_to_csv, transform

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl")


def run_etl(truncate: bool = False) -> int:
    """Run the ETL pipeline: Extract -> Transform -> Load."""
    logger.info("Starting ETL pipeline")
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Load configuration
    try:
        config = load_config()
        if truncate:
            config = replace(config, write_disposition="truncate")
        logger.info(f"Project: {config.PROJECT_ID}, Bucket: {config.gcs_bucket}")
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    # Extract
    try:
        logger.info("=== EXTRACT ===")
        df = extract_from_gcs(config.gcs_bucket, "raw_data/traffic_spreadsheet.xls")
    except ExtractionError as e:
        logger.error(f"Extraction failed: {e}")
        return 1

    # Transform
    try:
        logger.info("=== TRANSFORM ===")
        df = transform(df)
        temp_dir = Path(tempfile.gettempdir())
        csv_path = save_to_csv(df, temp_dir, run_timestamp)
    except TransformationError as e:
        logger.error(f"Transformation failed: {e}")
        return 1

    # Load
    try:
        logger.info("=== LOAD ===")
        # Backup original file
        backup_blob = f"backups/original_{run_timestamp}.xls"
        copy_within_gcs(config.gcs_bucket, "raw_data/traffic_spreadsheet.xls", backup_blob)

        # Upload processed CSV and load to BigQuery
        csv_blob = f"processed/traffic_data_{run_timestamp}.csv"
        gcs_uri = upload_to_gcs(csv_path, config.gcs_bucket, csv_blob)
        rows_loaded = load_to_bigquery(gcs_uri, config)

        logger.info(f"=== COMPLETE: {rows_loaded} rows loaded ===")
        return 0
    except LoadError as e:
        logger.error(f"Load failed: {e}")
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="etl",
        description="Traffic data ETL: GCS -> BigQuery",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run the ETL pipeline")
    run_parser.add_argument("--truncate", action="store_true", help="Truncate table before loading")

    args = parser.parse_args()

    if args.command == "run":
        return run_etl(truncate=args.truncate)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
