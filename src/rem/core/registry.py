import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Optional, cast

from rem.core.status import VALID_STATUSES, is_terminal
from rem.utils.logger import get_logger
from rem.utils.paths import DEFAULT_EVENTS_PATH

logger = get_logger(__name__)

EventType = Literal[
    "CREATE_GROUP",
    "PATCH_GROUP",
    "SUBMIT_SWEEP",
    "UPDATE_STATUS",
]

VALID_EVENT_TYPES: set[EventType] = {
    "CREATE_GROUP",
    "PATCH_GROUP",
    "SUBMIT_SWEEP",
    "UPDATE_STATUS",
}


class RegistryManager:
    def __init__(self, events_path: Path = DEFAULT_EVENTS_PATH) -> None:
        self.events_path = events_path
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self._events: Optional[list[dict[str, Any]]] = None

    def append_event(self, event: dict[str, Any]) -> None:
        """
        Append a validated event to the events.jsonl file.
        """
        self._validate_event(event)
        with self.events_path.open("a") as f:
            f.write(json.dumps(event) + "\n")
        logger.info(
            f"Appended event: {event['type']} for group {event.get('group_id', '?')}"
        )

    def load_events(self, force_reload: bool = False) -> list[dict[str, Any]]:
        """
        Load all events from the file into memory (with caching).
        """
        if self._events is None or force_reload:
            if not self.events_path.exists():
                logger.warning(f"No events file found at {self.events_path}")
                self._events = []
            else:
                with self.events_path.open("r") as f:
                    self._events = [json.loads(line) for line in f if line.strip()]
                logger.info(
                    f"Loaded {len(self._events)} events from {self.events_path}"
                )
        return self._events

    def get_group_history(self, group_id: str) -> list[dict[str, Any]]:
        """
        Return all events pertaining to a specific group.
        """
        events = [e for e in self.load_events() if e.get("group_id") == group_id]
        logger.debug(f"Found {len(events)} events for group {group_id}")
        return events

    def get_latest_status(self, group_id: str) -> Optional[str]:
        """
        Return the latest known status for the given group.
        """
        for event in reversed(self.load_events()):
            if event.get("group_id") == group_id and event["type"] == "UPDATE_STATUS":
                logger.debug(f"Latest status for {group_id} is {event['status']}")
                return cast(str, event["status"])
        logger.info(f"No status found for group {group_id}")
        return None

    def is_group_terminal(self, group_id: str) -> bool:
        """
        Check if the group's current status is terminal.
        """
        status = self.get_latest_status(group_id)
        if status is None:
            logger.info(f"Group {group_id} has no status yet.")
            return False
        result = is_terminal(status)
        logger.info(
            f"Group {group_id} is {'terminal' if result else 'non-terminal'} ({status})"
        )
        return result

    def _validate_event(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary.")

        if "type" not in event:
            raise ValueError("Event must include 'type' field.")

        if event["type"] not in VALID_EVENT_TYPES:
            raise ValueError(f"Invalid event type: {event['type']}")

        if "group_id" not in event:
            raise ValueError("Event must include 'group_id'.")

        if "timestamp" not in event:
            raise ValueError("Event must include 'timestamp' field.")

        if event["type"] == "UPDATE_STATUS":
            if "status" not in event:
                raise ValueError("UPDATE_STATUS events must include a 'status' field.")
            if event["status"] not in VALID_STATUSES:
                raise ValueError(f"Invalid status: {event['status']}")
