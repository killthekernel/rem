import json
import threading
import time
from pathlib import Path
from typing import Any, Optional

import pytest

from rem.core.registry import VALID_EVENT_TYPES, RegistryManager
from rem.core.status import TERMINAL_STATUSES, VALID_STATUSES


def make_event(
    group_id: str, status: Optional[str] = None, event_type: str = "UPDATE_STATUS"
) -> dict[str, Any]:
    event = {
        "type": event_type,
        "group_id": group_id,
        "timestamp": "2025-07-25T12:00:00Z",
    }
    if status is not None:
        event["status"] = status
    return event


non_status_event_types = [et for et in VALID_EVENT_TYPES if et != "UPDATE_STATUS"]


class TestRegistryManager:
    @pytest.fixture  # type: ignore[misc]
    def events_path(self, tmp_path: Path) -> Path:
        return tmp_path / "events.jsonl"

    @pytest.fixture  # type: ignore[misc]
    def registry(self, events_path: Path) -> RegistryManager:
        return RegistryManager(events_path=events_path)

    def test_append_and_load_event(self, registry: RegistryManager) -> None:
        event = make_event("ABC", "PENDING")
        registry.append_event(event)
        loaded = registry.load_events()
        assert len(loaded) == 1
        assert loaded[0]["group_id"] == "ABC"
        assert loaded[0]["status"] == "PENDING"

    def test_invalid_event_type(self, registry: RegistryManager) -> None:
        bad_event = {
            "type": "BAD_TYPE",
            "group_id": "XYZ",
            "status": "PENDING",
            "timestamp": "2025-07-25T12:00:00Z",
        }
        with pytest.raises(ValueError, match="Invalid event type"):
            registry.append_event(bad_event)

    def test_missing_status_on_update(self, registry: RegistryManager) -> None:
        event = {
            "type": "UPDATE_STATUS",
            "group_id": "XYZ",
            "timestamp": "2025-07-25T12:00:00Z",
        }
        with pytest.raises(ValueError, match="must include a 'status'"):
            registry.append_event(event)

    def test_get_group_history(self, registry: RegistryManager) -> None:
        registry.append_event(make_event("A", "PENDING"))
        registry.append_event(make_event("B", "RUNNING"))
        registry.append_event(make_event("A", "COMPLETED"))
        history = registry.get_group_history("A")
        assert len(history) == 2
        assert history[-1]["status"] == "COMPLETED"

    def test_get_latest_status(self, registry: RegistryManager) -> None:
        registry.append_event(make_event("A", "PENDING"))
        registry.append_event(make_event("A", "RUNNING"))
        latest = registry.get_latest_status("A")
        assert latest == "RUNNING"

    @pytest.mark.parametrize("status", list(TERMINAL_STATUSES))  # type: ignore[misc]
    def test_is_group_terminal(self, registry: RegistryManager, status: str) -> None:
        registry.append_event(make_event("A", "PENDING"))
        registry.append_event(make_event("A", status))
        assert registry.is_group_terminal("A")

    @pytest.mark.parametrize("bad_status", ["BAD", "", "pending", "TIME OUT"])  # type: ignore[misc]
    def test_invalid_status(self, registry: RegistryManager, bad_status: str) -> None:
        event = make_event("BADGROUP", status=bad_status)
        with pytest.raises(ValueError, match="Invalid status"):
            registry.append_event(event)

    @pytest.mark.parametrize("status", list(VALID_STATUSES))  # type: ignore[misc]
    def test_all_valid_statuses_accepted(
        self, registry: RegistryManager, status: str
    ) -> None:
        event = make_event(group_id=f"GROUP_{status}", status=status)
        registry.append_event(event)
        loaded = registry.load_events()
        assert loaded[-1]["status"] == status

    @pytest.mark.parametrize("event_type", non_status_event_types)  # type: ignore[misc]
    def test_non_status_event_types_valid(
        self, registry: RegistryManager, event_type: str
    ) -> None:
        event = make_event(group_id=f"TEST_{event_type}", event_type=event_type)
        registry.append_event(event)
        loaded = registry.load_events()
        assert loaded[-1]["type"] == event_type

    @pytest.mark.parametrize("event_type", non_status_event_types)  # type: ignore[misc]
    def test_missing_group_id_raises(
        self, registry: RegistryManager, event_type: str
    ) -> None:
        event = {"type": event_type, "timestamp": "2025-07-25T12:00:00Z"}
        with pytest.raises(ValueError, match="must include 'group_id'"):
            registry.append_event(event)

    def test_concurrent_appends_are_serialized(self, tmp_path: Path) -> None:
        events_path = tmp_path.joinpath("events.jsonl")
        rm = RegistryManager(events_path=events_path)

        num_threads = 5

        def writer(i: int) -> None:
            event = {
                "type": "CREATE_GROUP",
                "group_id": f"G_{i:04d}",
                "timestamp": time.time(),
            }
            rm.append_event(event)

        threads = [
            threading.Thread(target=writer, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        lines = events_path.read_text().splitlines()
        assert len(lines) == num_threads
        parsed = [json.loads(line) for line in lines]
        assert all("type" in e and "group_id" in e and "timestamp" in e for e in parsed)
