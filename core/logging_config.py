"""
core.logging_config
===================

Centralized logging configuration for router, consumers, and workers.

This module configures application-wide logging with support for
structured fields via the `extra` argument. Logs are written to stdout
to support containerized deployments and log aggregation systems.
"""

import logging
import sys
from typing import Dict


class SafeExtraFormatter(logging.Formatter):
    """
    Logging formatter that safely renders `extra` fields.

    Any custom attributes passed via the `extra` argument will be
    appended to the log output as key=value pairs.
    """

    _STANDARD_ATTRS = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "asctime",
    }

    def format(self, record: logging.LogRecord) -> str:
        base_message = super().format(record)

        extras: Dict[str, object] = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._STANDARD_ATTRS
        }

        if not extras:
            return base_message

        extra_str = " ".join(f"{k}={v}" for k, v in extras.items())
        return f"{base_message} | {extra_str}"


def setup_logging(level: str = "INFO") -> None:
    """
    Configure global logging.

    This function must be called exactly once at application startup.

    Parameters
    ----------
    level : str, optional
        Logging level (DEBUG, INFO, WARNING, ERROR).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(log_level)

    # Important for reloads, Celery forks, and test runs
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    handler.setFormatter(
        SafeExtraFormatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root.addHandler(handler)

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("stomp").setLevel(logging.WARNING)
    logging.getLogger("celery").setLevel(logging.INFO)

    root.info("Logging initialized", extra={"level": level.upper()})
