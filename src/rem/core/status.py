VALID_STATUSES: set[str] = {
    "PENDING",
    "STAGED",
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "KILLED",
    "TIMEOUT",
    "CRASHED",
    "SKIPPED",
}

TERMINAL_STATUSES: set[str] = {
    "COMPLETED",
    "FAILED",
    "KILLED",
    "TIMEOUT",
    "CRASHED",
    "SKIPPED",
}


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES
