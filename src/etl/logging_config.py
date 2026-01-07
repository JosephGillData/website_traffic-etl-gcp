"""
Logging Configuration for ETL Pipeline
=======================================

This module sets up structured logging for the ETL pipeline.

WHY LOGGING MATTERS:
--------------------
Logging is essential for production ETL pipelines because:
- You need to know what happened when things go wrong
- You need audit trails for compliance
- You can't attach a debugger to a cron job at 3 AM
- Good logs help you diagnose issues without re-running the pipeline

LOGGING VS PRINT:
-----------------
Why use logging instead of print()?
- Log levels: INFO, WARNING, ERROR let you filter by severity
- Timestamps: Automatic timestamps show when things happened
- Configurability: Change verbosity without changing code
- Production-ready: Can send logs to files, monitoring systems, etc.

LOG LEVELS:
-----------
Python's logging module has these levels (lowest to highest severity):
- DEBUG: Detailed info for diagnosing problems
- INFO: Confirmation that things are working as expected
- WARNING: Something unexpected happened, but the program continues
- ERROR: Something failed, the program may not be able to continue
- CRITICAL: A very serious error, the program may crash

We use INFO as the default level. With --verbose, we show DEBUG too.

STRUCTURED LOGGING:
-------------------
Our logs follow a consistent format:
    2024-01-01T12:00:00.000Z [INFO    ] Starting ETL pipeline

This format is:
- Parseable: Tools can extract fields from logs
- Readable: Humans can understand it at a glance
- Sortable: Timestamps at the start enable chronological sorting
- Consistent: Every log line has the same structure
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any

# =============================================================================
# CUSTOM FORMATTER
# =============================================================================


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured log messages.

    WHY A CUSTOM FORMATTER:
    -----------------------
    Python's default log format is fine, but we want:
    - ISO 8601 timestamps in UTC
    - Consistent field widths for readability
    - Easy parsing by log aggregation tools

    HOW FORMATTERS WORK:
    --------------------
    logging.Formatter is a base class that turns LogRecord objects into strings.
    We override format() to control exactly how logs look.

    Every log call (logger.info(), logger.error(), etc.) creates a LogRecord
    with info like:
    - levelname: "INFO", "ERROR", etc.
    - message: The actual log message
    - exc_info: Exception info if logging an exception
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record into a structured string.

        OUTPUT FORMAT:
        --------------
        2024-01-01T12:00:00.000Z [INFO    ] Message here

        - Timestamp: ISO 8601 format with milliseconds, UTC (Z suffix)
        - Level: Padded to 8 chars for alignment
        - Message: The actual log content

        Args:
            record: LogRecord created by a log call

        Returns:
            Formatted log string
        """
        # Generate ISO 8601 timestamp in UTC
        # The [:-3] removes the last 3 digits of microseconds, leaving milliseconds
        # The "Z" suffix indicates UTC timezone
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        level = record.levelname.upper()
        message = record.getMessage()

        # If this log includes an exception, append the traceback
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            message = f"{message}\n{exc_text}"

        # Support for extra structured fields (not commonly used, but available)
        # You could log: logger.info("message", extra={"extra_data": {"key": "value"}})
        extra_fields = ""
        if hasattr(record, "extra_data"):
            extra_data: dict[str, Any] = record.extra_data  # type: ignore[attr-defined]
            extra_fields = " ".join(f"{k}={v}" for k, v in extra_data.items())
            if extra_fields:
                extra_fields = f" [{extra_fields}]"

        # Format: timestamp [LEVEL   ] message [extra_fields]
        # The :8 in {level:8} pads the level to 8 characters for alignment
        return f"{timestamp} [{level:8}] {message}{extra_fields}"


# =============================================================================
# LOGGING SETUP
# =============================================================================


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure logging for the ETL pipeline.

    HOW PYTHON LOGGING WORKS:
    -------------------------
    Python's logging module has these components:
    - Logger: The object you call .info(), .error(), etc. on
    - Handler: Where logs go (console, file, network, etc.)
    - Formatter: How logs are formatted (our StructuredFormatter)
    - Level: Minimum severity to output

    Flow: logger.info("msg") → logger → handler → formatter → output

    NAMED LOGGERS:
    --------------
    We use logging.getLogger("etl") to get a named logger.
    Named loggers form a hierarchy: "etl.config" is a child of "etl".
    This lets you configure logging for specific modules if needed.

    Using a named logger (not the root logger) avoids conflicts with
    logging from libraries like pandas, google-cloud, etc.

    Args:
        verbose: If True, show DEBUG level logs. Otherwise only INFO and above.

    Returns:
        Configured Logger instance ready to use.
    """
    # Get or create the "etl" logger
    # If this logger already exists, we get the same instance
    logger = logging.getLogger("etl")

    # Set the minimum level this logger will process
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Remove any existing handlers to avoid duplicate logs
    # This is important if setup_logging() is called multiple times
    logger.handlers.clear()

    # Create a console handler that writes to stdout
    # StreamHandler(sys.stdout) sends logs to standard output
    # (StreamHandler() with no args would use sys.stderr)
    console_handler = logging.StreamHandler(sys.stdout)

    # Attach our custom formatter to the handler
    console_handler.setFormatter(StructuredFormatter())

    # Add the handler to the logger
    logger.addHandler(console_handler)

    # Don't propagate to the root logger
    # This prevents double-logging if root logger also has handlers
    logger.propagate = False

    return logger


def get_logger() -> logging.Logger:
    """
    Get the ETL logger instance.

    WHY A SEPARATE FUNCTION:
    ------------------------
    This function lets other modules get the logger without needing to
    know its name or call setup_logging() themselves.

    Usage in other modules:
        from .logging_config import get_logger
        logger = get_logger()
        logger.info("Something happened")

    The logger must be set up (via setup_logging()) before use,
    but typically __main__.py does that at startup.

    Returns:
        The "etl" logger instance
    """
    return logging.getLogger("etl")
