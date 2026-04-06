from __future__ import annotations

import logging
from typing import Any

from rich.logging import RichHandler

LOGGER_NAME = "alpaca_lab"


def configure_logging(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level.upper())
    logger.propagate = False

    if not logger.handlers:
        handler = RichHandler(rich_tracebacks=True, show_path=False, markup=False)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    if not name:
        return logging.getLogger(LOGGER_NAME)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def redact_value(value: Any, keep: int = 4) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    if len(value) <= keep:
        return "*" * len(value)
    return f"{'*' * max(len(value) - keep, 4)}{value[-keep:]}"
