import re
import warnings

import pytest

from rem.constants import GROUP_PREFIX, REP_PAD, REP_PREFIX, SWEEP_PAD, SWEEP_PREFIX
from rem.core.stamp import (
    create_group_stamp,
    format_rep_id,
    format_sweep_id,
    is_valid_group_id,
    is_valid_rep_id,
    is_valid_sweep_id,
    next_rep_id,
    parse_group_id,
    parse_rep_id,
    parse_sweep_id,
)


def test_create_group_stamp_format() -> None:
    group_id, group_date = create_group_stamp()

    assert group_id.startswith(GROUP_PREFIX)
    suffix = group_id[len(GROUP_PREFIX) :]
    parts = suffix.split("_")
    assert len(parts) == 2
    assert len(parts[0]) == 10
    assert len(parts[1]) == 16
    assert re.match(r"\d{4}\d{2}\d{2}", group_date)


def test_parse_group_id_and_round_trip() -> None:
    group_id, _ = create_group_stamp()
    ulid_str = parse_group_id(group_id)
    assert len(ulid_str) == 26


def test_format_and_parse_sweep_id() -> None:
    for i in [0, 1, 42, 1234]:
        sid = format_sweep_id(i)
        assert sid.startswith(SWEEP_PREFIX)
        assert parse_sweep_id(sid) == i


def test_format_and_parse_rep_id() -> None:
    for i in [0, 1, 42, 9999]:
        rid = format_rep_id(i)
        assert rid.startswith(REP_PREFIX)
        assert parse_rep_id(rid) == i


def test_next_rep_id() -> None:
    rep_ids = [format_rep_id(i) for i in [1, 3, 4]]
    assert next_rep_id(rep_ids) == format_rep_id(5)

    assert next_rep_id([]) == format_rep_id(0)


def test_is_valid_group_id() -> None:
    group_id, _ = create_group_stamp()
    assert is_valid_group_id(group_id)

    bad_ids = [
        "G_1234567890ABCDEF",
        "G_1234567890_ABC",
        "BAD_01HYZ3W6Y8_1234567890123456",
    ]
    for gid in bad_ids:
        assert not is_valid_group_id(gid)


def test_is_valid_sweep_id() -> None:
    valid = format_sweep_id(42)
    invalids = ["X_0042", "S_abc", "S_"]

    assert is_valid_sweep_id(valid)
    for sid in invalids:
        assert not is_valid_sweep_id(sid)


def test_is_valid_rep_id() -> None:
    valid = format_rep_id(42)
    invalids = ["R_", "REP_0042", "R_abc"]

    assert is_valid_rep_id(valid)
    for rid in invalids:
        assert not is_valid_rep_id(rid)
