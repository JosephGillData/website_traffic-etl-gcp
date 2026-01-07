"""Structured logging configuration for ETL pipeline."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any


class StructuredFormatter(logging.Formatter):
    """Formatter that outputs structured log messages."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with timestamp, level, and message."""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        level = record.levelname.upper()
        message = record.getMessage()

        # Include exception info if present
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            message = f"{message}\n{exc_text}"

        # Add extra fields if present
        extra_fields = ""
        if hasattr(record, "extra_data"):
            extra_data: dict[str, Any] = record.extra_data  # type: ignore[attr-defined]
            extra_fields = " ".join(f"{k}={v}" for k, v in extra_data.items())
            if extra_fields:
                extra_fields = f" [{extra_fields}]"

        return f"{timestamp} [{level:8}] {message}{extra_fields}"


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the ETL pipeline.

    Args:
        verbose: If True, set log level to DEBUG. Otherwise INFO.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler with structured formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(StructuredFormatter())
    logger.addHandler(console_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def get_logger() -> logging.Logger:
    """Get the ETL logger instance."""
    return logging.getLogger("etl")
