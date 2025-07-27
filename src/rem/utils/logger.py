import logging
import sys
from pathlib import Path
from typing import Optional, Union

_LOGGER = None


def get_logger(
    name: str = "rem",
    log_file: Optional[Path] = None,
    level: Union[str, int] = logging.INFO,
) -> logging.Logger:
    """
    Returns a configured logger instance with optional file logging.

    Args:
        name (str): Name of the logger (default "rem").
        log_file (Path): Optional path to a log file. If provided, logs will be written to this file.
        level (int): Logging level (default is logging.INFO).
    """
    global _LOGGER
    if _LOGGER:
        return _LOGGER

    logger = logging.getLogger(name)
    level = _resolve_level(level)
    logger.setLevel(level)
    logger.propagate = True

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(_get_console_formatter())
        logger.addHandler(console_handler)

        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(_get_file_formatter())
            logger.addHandler(file_handler)

    _LOGGER = logger
    return logger


def _resolve_level(level: Union[str, int]) -> int:
    if isinstance(level, int):
        return level
    level_str = str(level).upper()
    return {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }.get(level_str, logging.INFO)


def _get_console_formatter() -> logging.Formatter:
    return logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )


def _get_file_formatter() -> logging.Formatter:
    return logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
