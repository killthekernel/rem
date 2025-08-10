import os
from datetime import date, datetime
from pathlib import Path
from typing import Union

from rem.constants import EVENTS_FILENAME, MANIFEST_FILENAME


# TODO: Ensure that REM_ROOT environment variable is set on first runner call
def get_rem_root() -> Path:
    """
    Get the root directory for the project.
    This can be set via the `REM_ROOT` environment variable,
    and defaults to the current working directory otherwise.

    """
    return Path(os.environ.get("REM_ROOT", Path.cwd()))


# Global paths
def get_results_dir(test: bool = False) -> Path:
    """
    Return the default results directory.

    """
    if test:
        return get_rem_root().joinpath("results", "test")
    return get_rem_root().joinpath("results")


def get_default_events_path(test: bool = False) -> Path:
    """
    Return the location of the default events.jsonl.

    """
    return get_results_dir(test=test).joinpath(EVENTS_FILENAME)


# Group-level paths
def get_group_dir(
    group_id: str, group_date: Union[date, datetime], test: bool = False
) -> Path:
    """Return path to group directory: results/YYYY/MM/DD/GGG/"""
    return get_results_dir(test=test).joinpath(
        str(group_date.year),
        f"{group_date.month:02d}",
        f"{group_date.day:02d}",
        group_id,
    )


def get_group_manifest_path(
    group_id: str, group_date: Union[date, datetime], test: bool = False
) -> Path:
    return get_group_dir(group_id, group_date, test=test).joinpath(MANIFEST_FILENAME)


# Sweep-level paths
def get_sweep_dir(
    group_id: str,
    group_date: Union[date, datetime],
    sweep_id: str,
    test: bool = False,
) -> Path:
    return get_group_dir(group_id, group_date, test=test).joinpath(sweep_id)


def get_sweep_manifest_path(
    group_id: str,
    group_date: Union[date, datetime],
    sweep_id: str,
    test: bool = False,
) -> Path:
    return get_sweep_dir(group_id, group_date, sweep_id, test=test).joinpath(
        MANIFEST_FILENAME
    )


# Rep-level paths
def get_rep_dir(
    group_id: str,
    group_date: Union[date, datetime],
    sweep_id: str,
    rep_id: str,
    test: bool = False,
) -> Path:
    return get_sweep_dir(group_id, group_date, sweep_id, test=test).joinpath(rep_id)


def get_rep_manifest_path(
    group_id: str,
    group_date: Union[date, datetime],
    sweep_id: str,
    rep_id: str,
    test: bool = False,
) -> Path:
    return get_rep_dir(group_id, group_date, sweep_id, rep_id, test=test).joinpath(
        MANIFEST_FILENAME
    )
