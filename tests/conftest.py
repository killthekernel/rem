import logging

import pytest

import rem.utils.logger as logger_mod


@pytest.fixture(autouse=True)  # type: ignore[misc]
def reset_logger_singleton() -> None:
    """Reset global logger before each test."""
    logger_mod._LOGGER = None
    logging.getLogger("rem").handlers.clear()
