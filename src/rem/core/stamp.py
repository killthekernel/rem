from datetime import datetime
from pathlib import Path
from typing import Tuple

from rem.constants import GROUP_PREFIX, REP_PAD, REP_PREFIX, SWEEP_PAD, SWEEP_PREFIX
from rem.utils.ulid import new_ulid, timestamp_from_ulid


def create_group_stamp() -> Tuple[str, str]:
    """
    Generate a new group ID and group date using a ULID timestamp.
    """
    ulid_str = new_ulid()  # 26-char ULID
    timestamp = timestamp_from_ulid(ulid_str)
    group_date = timestamp.strftime("%Y%m%d")
    group_id = f"{GROUP_PREFIX}{ulid_str[:10]}_{ulid_str[10:]}"
    return group_id, group_date


def format_sweep_id(index: int) -> str:
    return f"{SWEEP_PREFIX}{index:0{SWEEP_PAD}d}"


def format_rep_id(index: int) -> str:
    return f"{REP_PREFIX}{index:0{REP_PAD}d}"


def parse_group_id(group_id: str) -> str:
    if not group_id.startswith(GROUP_PREFIX):
        raise ValueError(f"Invalid group ID: {group_id}")
    raw = group_id[len(GROUP_PREFIX) :]
    parts = raw.split("_")
    if len(parts) != 2 or len(parts[0]) != 10 or len(parts[1]) != 16:
        raise ValueError(f"Malformed group ID: {group_id}")
    return parts[0] + parts[1]  # Reconstruct canonical ULID


def parse_sweep_id(sweep_id: str) -> int:
    if not sweep_id.startswith(SWEEP_PREFIX):
        raise ValueError(f"Invalid sweep ID: {sweep_id}")
    return int(sweep_id[len(SWEEP_PREFIX) :])


def parse_rep_id(rep_id: str) -> int:
    if not rep_id.startswith(REP_PREFIX):
        raise ValueError(f"Invalid rep ID: {rep_id}")
    return int(rep_id[len(REP_PREFIX) :])


def next_rep_id(rep_ids: list[str]) -> str:
    """
    Given a list of R_xxxx IDs, return the next unused rep ID.
    """
    indices = [parse_rep_id(rid) for rid in rep_ids if rid.startswith(REP_PREFIX)]
    next_index = max(indices, default=-1) + 1
    return format_rep_id(next_index)


def is_valid_group_id(group_id: str) -> bool:
    if not group_id.startswith(GROUP_PREFIX):
        return False
    raw = group_id[len(GROUP_PREFIX) :]
    parts = raw.split("_")
    return len(parts) == 2 and len(parts[0]) == 10 and len(parts[1]) == 16


def is_valid_sweep_id(sweep_id: str) -> bool:
    return sweep_id.startswith(SWEEP_PREFIX) and sweep_id[len(SWEEP_PREFIX) :].isdigit()


def is_valid_rep_id(rep_id: str) -> bool:
    return rep_id.startswith(REP_PREFIX) and rep_id[len(REP_PREFIX) :].isdigit()
