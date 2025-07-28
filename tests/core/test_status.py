import pytest

from rem.core import status


def test_valid_statuses_are_recognized() -> None:
    for s in status.VALID_STATUSES:
        assert isinstance(s, str)


def test_terminal_statuses_are_subset() -> None:
    for s in status.TERMINAL_STATUSES:
        assert s in status.VALID_STATUSES


@pytest.mark.parametrize("s", ["COMPLETED", "CRASHED", "KILLED", "TIMEOUT", "SKIPPED"])  # type: ignore[misc]
def test_is_terminal_true(s: str) -> None:
    assert status.is_terminal(s) is True


@pytest.mark.parametrize("s", ["PENDING", "RUNNING", "UNKNOWN", "", "done"])  # type: ignore[misc]
def test_is_terminal_false(s: str) -> None:
    assert status.is_terminal(s) is False
