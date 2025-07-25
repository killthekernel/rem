from datetime import datetime
from typing import cast

import ulid


def new_ulid() -> str:
    """
    Generate a new ULID string.

    Returns:
        str: A lexicographically sortable ULID.
    """
    return str(ulid.new())


def ulid_from_timestamp(dt: datetime) -> str:
    """
    Generate a ULID from a given datetime.

    Args:
        dt (datetime): The datetime to encode into the ULID.

    Returns:
        str: ULID string corresponding to the given timestamp.
    """
    return str(ulid.from_timestamp(dt))


def timestamp_from_ulid(u: str) -> datetime:
    """
    Extract the datetime from a ULID string.

    Args:
        u (str): ULID string.

    Returns:
        datetime: The datetime encoded in the ULID.
    """
    return cast(datetime, ulid.api.parse(u).timestamp().datetime)


def is_valid_ulid(u: str) -> bool:
    """
    Check whether a given string is a valid ULID.

    Args:
        u (str): String to validate.

    Returns:
        bool: True if valid ULID, False otherwise.
    """
    try:
        ulid.api.parse(u)
        return True
    except Exception:
        return False
