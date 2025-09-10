import logging
import shutil
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


@pytest.fixture(scope="session")  # type: ignore[misc]
def data_dir() -> Path:
    return Path(__file__).parent.joinpath("data")


@pytest.fixture  # type: ignore[misc]
def sandbox(tmp_path: Path, data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Ensure experiment imports work: tests.data.experiments...
    tests_root = Path(__file__).parent
    monkeypatch.syspath_prepend(str(tests_root))
    return tmp_path


def copy_subtree(src: Path, dst: Path) -> None:
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
    else:
        shutil.copy2(src, dst)
