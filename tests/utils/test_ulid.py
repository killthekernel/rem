import re
import warnings
from datetime import datetime, timedelta, timezone

import pytest

from rem.utils import ulid as ulid_utils

ULID_REGEX = re.compile(r"^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}$")


def test_new_ulid_format_and_uniqueness() -> None:
    u1 = ulid_utils.new_ulid()
    u2 = ulid_utils.new_ulid()

    assert ULID_REGEX.match(u1)
    assert ULID_REGEX.match(u2)
    assert u1 != u2


def test_ulid_from_timestamp_and_sort_order() -> None:
    dt1 = datetime(2023, 1, 1, 12, 0, 0)
    dt2 = dt1 + timedelta(seconds=1)

    u1 = ulid_utils.ulid_from_timestamp(dt1)
    u2 = ulid_utils.ulid_from_timestamp(dt2)

    # Should be valid ULIDs
    assert ULID_REGEX.match(u1)
    assert ULID_REGEX.match(u2)

    # Lexicographic order should match time order
    assert u1 < u2


def test_timestamp_roundtrip() -> None:
    now = datetime.now(timezone.utc)
    u = ulid_utils.ulid_from_timestamp(now)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        extracted = ulid_utils.timestamp_from_ulid(u)

    delta = abs(extracted - now)
    assert delta < timedelta(milliseconds=1)


@pytest.mark.parametrize(  # type: ignore
    "ulid_str,is_valid",
    [
        ("01HZY6KTQ8A3NZQ0D8BC1TYZVE", True),
        ("not_a_ulid", False),
        ("", False),
        ("01HZY6KTQ8A3NZQ0D8BC1TYZV", False),  # 25 chars
        ("01HZY6KTQ8A3NZQ0D8BC1TYZVE000", False),  # too long
    ],
)
def test_is_valid_ulid(ulid_str: str, is_valid: bool) -> None:
    assert ulid_utils.is_valid_ulid(ulid_str) == is_valid
