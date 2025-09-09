import logging
import sys
from pathlib import Path
from typing import Generator

import pytest

import rem.utils.logger as logger_mod


@pytest.fixture(autouse=True)  # type: ignore[misc]
def reset_logger_singleton() -> None:
    """Reset global logger before each test."""
    logger_mod._LOGGER = None
    logging.getLogger("rem").handlers.clear()


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _isolate_rem_root_and_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[None, None, None]:
    monkeypatch.setenv("REM_ROOT", str(tmp_path))

    data_dir = Path(__file__).parent.joinpath("data")
    if str(data_dir) not in sys.path:
        sys.path.insert(0, str(data_dir))
    yield
