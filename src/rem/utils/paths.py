import os
from datetime import date, datetime
from pathlib import Path
from typing import Union

from rem.constants import EVENTS_FILENAME, MANIFEST_FILENAME


# Set the root directory for REM
# This can be set via the REM_ROOT environment variable or defaults to the current working directory
def get_rem_root() -> Path:
    return Path(os.environ.get("REM_ROOT", Path.cwd()))


# Global paths
def get_results_dir() -> Path:
    return get_rem_root().joinpath("results")


def get_default_events_path() -> Path:
    return get_results_dir().joinpath(EVENTS_FILENAME)


def get_default_test_events_path() -> Path:
    return get_results_dir().joinpath("test", EVENTS_FILENAME)


# Group-level paths
def get_group_dir(group_id: str, group_date: Union[date, datetime]) -> Path:
    """Return path to group directory: results/YYYY/MM/DD/GGG/"""
    return get_results_dir().joinpath(
        str(group_date.year),
        f"{group_date.month:02d}",
        f"{group_date.day:02d}",
        group_id,
    )


def get_group_manifest_path(group_id: str, group_date: Union[date, datetime]) -> Path:
    return get_group_dir(group_id, group_date).joinpath(MANIFEST_FILENAME)


# Sweep-level paths
def get_sweep_dir(
    group_id: str, group_date: Union[date, datetime], sweep_id: str
) -> Path:
    return get_group_dir(group_id, group_date).joinpath(sweep_id)


def get_sweep_manifest_path(
    group_id: str, group_date: Union[date, datetime], sweep_id: str
) -> Path:
    return get_sweep_dir(group_id, group_date, sweep_id).joinpath(MANIFEST_FILENAME)


# Rep-level paths
def get_rep_dir(
    group_id: str, group_date: Union[date, datetime], sweep_id: str, rep_id: str
) -> Path:
    return get_sweep_dir(group_id, group_date, sweep_id).joinpath(rep_id)


def get_rep_manifest_path(
    group_id: str, group_date: Union[date, datetime], sweep_id: str, rep_id: str
) -> Path:
    return get_rep_dir(group_id, group_date, sweep_id, rep_id).joinpath(
        MANIFEST_FILENAME
    )
