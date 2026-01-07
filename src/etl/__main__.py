"""
CLI Entrypoint for ETL Pipeline
================================

This is the "main" module that runs when you execute `python -m etl`.

HOW `python -m etl` WORKS:
--------------------------
When you run `python -m <package_name>`, Python:

1. Finds the package (requires `pip install -e .` first - see __init__.py)
2. Looks for a `__main__.py` file inside that package
3. Executes this file as the main program

This is different from running `python etl.py` (which runs a single file).
The `-m` flag runs a package as a script, which is cleaner for complex projects.

EXECUTION FLOW:
---------------
1. User runs: `python -m etl run --verbose`
2. Python finds and executes this __main__.py file
3. The `if __name__ == "__main__":` block at the bottom runs main()
4. main() parses CLI arguments using argparse
5. Based on the command ("run" or "validate"), calls the appropriate function
6. The function returns an exit code (0 = success, non-zero = error)
7. sys.exit() terminates the program with that exit code

WHY USE EXIT CODES:
-------------------
Exit codes are how command-line programs communicate success/failure:
- Exit code 0 = success (everything worked)
- Exit code 1+ = different types of errors

This matters for:
- Shell scripts: `python -m etl run && echo "Success!"` only echoes if exit code is 0
- Cron jobs: Can alert on non-zero exit codes
- CI/CD pipelines: Fail the build if ETL fails

Usage:
    python -m etl run [--verbose] [--truncate]
    python -m etl validate
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

# These are "relative imports" - the dot (.) means "from the current package"
# This works because this file is inside the `etl` package
# .config means etl/config.py, .extract means etl/extract.py, etc.
from .config import ConfigError, load_config
from .extract import ExtractionError, extract_from_xls
from .load import (
    LoadError,
    load_to_bigquery,
    upload_backup_to_gcs,
    upload_to_gcs,
    verify_bigquery_load,
)
from .logging_config import setup_logging
from .transform import TransformationError, save_to_csv, transform

# =============================================================================
# EXIT CODES
# =============================================================================
# Define specific exit codes for different failure modes.
# This helps operators diagnose issues without reading logs.
# Convention: 0 = success, 1 = general error, 2+ = specific errors

EXIT_SUCCESS = 0  # Everything worked
EXIT_CONFIG_ERROR = 1  # Missing env vars, bad config, missing files
EXIT_EXTRACTION_ERROR = 2  # Couldn't read XLS file
EXIT_TRANSFORM_ERROR = 3  # Data transformation/validation failed
EXIT_LOAD_ERROR = 4  # GCS upload or BigQuery load failed
EXIT_UNKNOWN_ERROR = 99  # Unexpected error (shouldn't happen)


# =============================================================================
# ETL PIPELINE EXECUTION
# =============================================================================


def run_etl(verbose: bool = False, truncate: bool = False) -> int:
    """
    Run the complete ETL pipeline: Extract -> Transform -> Load.

    This is the main orchestration function. It coordinates the three ETL phases
    and handles errors at each stage. The pipeline is designed to "fail fast" -
    if any stage fails, we stop immediately rather than continuing with bad data.

    THE ETL PATTERN:
    ----------------
    ETL (Extract, Transform, Load) is a common data pipeline pattern:

    1. EXTRACT: Get raw data from source (XLS file)
       - Read the file, validate it has expected structure
       - Output: Raw pandas DataFrame

    2. TRANSFORM: Clean and reshape the data
       - Parse dates, add metadata, validate data quality
       - Output: Clean DataFrame ready for loading

    3. LOAD: Put data into destination (BigQuery via GCS)
       - Upload to GCS (staging area)
       - Load into BigQuery (final destination)
       - Verify the load succeeded

    WHY THIS ORDER:
    ---------------
    - Extract first: Can't transform data we don't have
    - Transform second: Clean data before storing it (garbage in = garbage out)
    - Load last: Only store validated, clean data

    Args:
        verbose: Enable DEBUG-level logging for troubleshooting
        truncate: If True, replace all data in BigQuery table (WRITE_TRUNCATE)
                  If False (default), append to existing data (WRITE_APPEND)

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Initialize logging first - we need it for all subsequent operations
    logger = setup_logging(verbose)
    logger.info("Starting ETL pipeline")

    # Generate a unique timestamp for this run
    # Used for: output filenames, GCS paths, tracking which run produced which data
    # Using UTC ensures consistency regardless of where the pipeline runs
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Run timestamp: {run_timestamp}")

    # -------------------------------------------------------------------------
    # CONFIGURATION PHASE
    # -------------------------------------------------------------------------
    # Load and validate all configuration before doing any work.
    # This "fail fast" approach prevents partial runs where we do work
    # but can't complete (e.g., extracted data but can't upload to GCS).
    try:
        config = load_config()

        # Handle the --truncate CLI flag by modifying config
        # We use dataclasses.replace() to create a new immutable config object
        # (Config is frozen=True, so we can't modify it directly)
        if truncate:
            from dataclasses import replace

            config = replace(config, write_disposition="truncate")

        logger.info(
            f"Configuration loaded: project={config.gcp_project}, bucket={config.gcs_bucket}"
        )
    except ConfigError as e:
        # Configuration errors are usually user-fixable (missing env vars, wrong paths)
        # The error message should tell them exactly what to fix
        logger.error(f"Configuration error: {e}")
        return EXIT_CONFIG_ERROR

    # -------------------------------------------------------------------------
    # EXTRACT PHASE
    # -------------------------------------------------------------------------
    # Read raw data from the source XLS file.
    # At this stage, we just want the data as-is, with minimal processing.
    try:
        logger.info("=== EXTRACT PHASE ===")
        df = extract_from_xls(config.local_xls_path)
    except ExtractionError as e:
        logger.error(f"Extraction failed: {e}")
        return EXIT_EXTRACTION_ERROR

    # -------------------------------------------------------------------------
    # TRANSFORM PHASE
    # -------------------------------------------------------------------------
    # Clean, reshape, and validate the data.
    # This is where we apply business logic and data quality rules.
    try:
        logger.info("=== TRANSFORM PHASE ===")
        df = transform(df)

        # Save transformed data to local CSV
        # This serves two purposes:
        # 1. Local backup/audit trail of what was processed
        # 2. Source file for GCS upload (BigQuery loads from GCS, not local)
        output_dir = config.local_xls_path.parent
        csv_path = save_to_csv(df, output_dir, run_timestamp)
    except TransformationError as e:
        logger.error(f"Transformation failed: {e}")
        return EXIT_TRANSFORM_ERROR

    # -------------------------------------------------------------------------
    # LOAD PHASE
    # -------------------------------------------------------------------------
    # Upload to GCS and load into BigQuery.
    # We upload both the original (backup) and processed (for loading) files.
    try:
        logger.info("=== LOAD PHASE ===")

        # Upload backup of original XLS file to GCS
        # This preserves the source data as it was when we processed it
        # Useful for auditing, debugging, or reprocessing later
        upload_backup_to_gcs(
            config.local_xls_path,
            config.gcs_bucket,
            run_timestamp,
        )

        # Upload the processed CSV to GCS
        # BigQuery can load directly from GCS (not from local files)
        # The returned gcs_uri is used in the next step
        csv_blob_name = f"processed/traffic_data_{run_timestamp}.csv"
        gcs_uri = upload_to_gcs(csv_path, config.gcs_bucket, csv_blob_name)

        # Load the CSV from GCS into BigQuery
        # This creates the table if it doesn't exist, or appends/truncates based on config
        rows_loaded = load_to_bigquery(gcs_uri, config)

        # Verify the load by querying BigQuery
        # This is a sanity check - belt and suspenders approach
        verify_bigquery_load(config, rows_loaded)

    except LoadError as e:
        logger.error(f"Load failed: {e}")
        return EXIT_LOAD_ERROR

    # If we get here, everything succeeded!
    logger.info("=== ETL PIPELINE COMPLETE ===")
    logger.info(f"Processed {rows_loaded} rows successfully")
    return EXIT_SUCCESS


def validate_config() -> int:
    """
    Validate configuration without running the ETL pipeline.

    This is useful for:
    - Testing your .env file is correct before running ETL
    - CI/CD to verify config in a new environment
    - Debugging "why won't my pipeline run?" issues

    Unlike run_etl(), this doesn't:
    - Read any data files (beyond checking they exist)
    - Connect to GCS or BigQuery
    - Make any changes

    Returns:
        Exit code (0 if configuration is valid)
    """
    logger = setup_logging(verbose=True)
    logger.info("Validating configuration...")

    try:
        config = load_config()

        # Print all config values so the user can verify they're correct
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


# =============================================================================
# CLI ARGUMENT PARSING
# =============================================================================


def main() -> int:
    """
    Main CLI entrypoint - parse arguments and dispatch to the right function.

    This function uses argparse to:
    1. Define what commands and options are available
    2. Parse the command-line arguments the user provided
    3. Call the appropriate function based on the command

    CLI STRUCTURE:
    --------------
    python -m etl <command> [options]

    Commands:
        run       - Run the full ETL pipeline
        validate  - Just check configuration is valid

    Options for 'run':
        -v, --verbose  - Show debug-level logs
        --truncate     - Replace table instead of appending

    ARGPARSE SUBPARSERS:
    --------------------
    We use "subparsers" to create sub-commands (like `git commit`, `git push`).
    Each subcommand can have its own options. This is cleaner than having
    all options at the top level.

    Returns:
        Exit code to pass to sys.exit()
    """
    # Create the top-level parser
    parser = argparse.ArgumentParser(
        prog="etl",  # Program name shown in help text
        description="Traffic data ETL pipeline - loads XLS data to BigQuery via GCS",
    )

    # Create subparsers for sub-commands
    # dest="command" means args.command will contain which subcommand was used
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # -------------------------------------------------------------------------
    # "run" subcommand
    # -------------------------------------------------------------------------
    run_parser = subparsers.add_parser("run", help="Run the ETL pipeline")

    # -v/--verbose flag: action="store_true" means it's a boolean flag
    # If present, args.verbose = True; if absent, args.verbose = False
    run_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging",
    )

    # --truncate flag: determines write disposition for BigQuery
    run_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate BigQuery table before loading (default: append)",
    )

    # -------------------------------------------------------------------------
    # "validate" subcommand
    # -------------------------------------------------------------------------
    # No additional arguments needed - it just validates and exits
    subparsers.add_parser("validate", help="Validate configuration without running ETL")

    # -------------------------------------------------------------------------
    # Parse arguments and dispatch
    # -------------------------------------------------------------------------
    args = parser.parse_args()

    # Dispatch to the appropriate function based on the command
    if args.command == "run":
        return run_etl(verbose=args.verbose, truncate=args.truncate)
    elif args.command == "validate":
        return validate_config()
    else:
        # No command specified - show help
        parser.print_help()
        return EXIT_SUCCESS


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

# This block runs when the module is executed directly (python -m etl)
# but NOT when it's imported by another module.
#
# __name__ is a special variable:
# - When run directly: __name__ == "__main__"
# - When imported: __name__ == "etl.__main__"
#
# sys.exit() terminates the program and returns the exit code to the OS.
# This is how shell scripts, cron, and CI/CD know if the command succeeded.

if __name__ == "__main__":
    sys.exit(main())
