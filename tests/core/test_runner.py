import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest
from ml_collections import ConfigDict

from rem.constants import EVENTS_FILENAME, MANIFEST_FILENAME
from rem.core.manifest import GroupManifest, RepManifest, SweepManifest
from rem.core.registry import RegistryManager
from rem.core.runner import MainRunner


def write_yaml(path: Path, data: dict[str, Any]) -> None:
    import yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        yaml.safe_dump(data, f)


def make_dummy_experiment_module(
    tmp_path: Path, module_name: str = "dummy_experiment"
) -> str:
    """
    Create a tiny experiment module on disk and add tmp_path to sys.path.
    The module defines a class DummyExp(ExperimentBase) with a run() that just returns a small dict.
    This dummy experiment should NEVER fail.
    """
    module_dir = tmp_path
    module_file = module_dir.joinpath(f"{module_name}.py")

    module_src = (
        "from ml_collections import ConfigDict\n"
        "from rem.core.experiment import ExperimentBase\n\n"
        "class DummyExp(ExperimentBase):\n"
        "   def __init__(self, config: ConfigDict) -> None:\n"
        "       super().__init__(config)\n"
        "   def run(self) -> dict[str, str]:\n"
        "       # Some dummy computation\n"
        "       return {'status': 'ok'}\n"
    )
    module_file.write_text(module_src)
    if str(module_dir) not in sys.path:
        sys.path.insert(0, str(module_dir))
    importlib.invalidate_caches()

    return module_name


@pytest.fixture(autouse=True)  # type: ignore[misc]
def rem_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("REM_ROOT", str(tmp_path))
    return tmp_path


@pytest.fixture  # type: ignore[misc]
def cfg_path(tmp_path: Path) -> Path:
    return tmp_path.joinpath("config.yaml")


@pytest.mark.parametrize("test_flag", [True, False])  # type: ignore[misc]
def test_dryrun_stages(rem_root: Path, cfg_path: Path, test_flag: bool) -> None:
    """
    Dryrun should stage directories and manifest files, but not run experiments.
    Registry should be updated with CREATE_GROUP and SUBMIT_SWEEP events only.
    """
    cfg = {
        "experiment_name": "demo",
        "params": {"epochs": 5, "lr": 0.001},
        "sweep": {"lr": [0.001, 0.01, 0.1]},
    }
    write_yaml(cfg_path, cfg)

    runner = MainRunner(test=test_flag, dryrun=True)
    group_id = runner.start(cfg_path, reps_per_sweep=2)

    # Verify directory structure
    # results/YYYY/MM/DD/G_xxx should exist with 3 S_ dirs, each with 2 R_ dirs
    from rem.core.stamp import format_rep_id, format_sweep_id, parse_group_id
    from rem.utils.paths import (
        get_default_events_path,
        get_group_dir,
        get_group_manifest_path,
        get_rep_dir,
        get_rep_manifest_path,
        get_sweep_dir,
        get_sweep_manifest_path,
    )
    from rem.utils.ulid import timestamp_from_ulid

    ulid_str = parse_group_id(group_id)
    group_dt = timestamp_from_ulid(ulid_str)
    group_dir = get_group_dir(group_id, group_dt, test=test_flag)
    assert group_dir.exists() and group_dir.is_dir()

    sweep_ids = [format_sweep_id(i) for i in range(1, 4)]
    rep_ids = [format_rep_id(i) for i in range(1, 3)]

    # Check all directories exist
    for sweep_id in sweep_ids:
        sweep_dir = get_sweep_dir(group_id, group_dt, sweep_id, test=test_flag)
        assert sweep_dir.exists() and sweep_dir.is_dir()
        for rep_id in rep_ids:
            rep_dir = get_rep_dir(group_id, group_dt, sweep_id, rep_id, test=test_flag)
            assert rep_dir.exists() and rep_dir.is_dir()

    events_path = get_default_events_path(test=test_flag)
    rm = RegistryManager(events_path=events_path)
    events = [
        e for e in rm.load_events(force_reload=True) if e.get("group_id") == group_id
    ]

    # Check entries in registry
    # CREATE_GROUP once, SUBMIT_SWEEP 3 times, no UPDATE_STATUS
    types = [e["type"] for e in events]
    assert types.count("CREATE_GROUP") == 1
    assert types.count("SUBMIT_SWEEP") == 3
    assert "UPDATE_STATUS" not in types

    from rem.core.manifest import GroupManifest, RepManifest, SweepManifest

    group_manifest = GroupManifest.load(
        get_group_manifest_path(group_id, group_dt, test=test_flag)
    )
    assert group_manifest.status == "PENDING"
    for sweep_id in sweep_ids:
        sweep_manifest = SweepManifest.load(
            get_sweep_manifest_path(group_id, group_dt, sweep_id, test=test_flag)
        )
        assert sweep_manifest.status == "PENDING"
        for rep_id in rep_ids:
            rep_manifest = RepManifest.load(
                get_rep_manifest_path(
                    group_id, group_dt, sweep_id, rep_id, test=test_flag
                )
            )
            assert rep_manifest.status == "PENDING"


@pytest.mark.parametrize("test_flag", [True, False])  # type: ignore[misc]
def test_full_run(cfg_path: Path, tmp_path: Path, test_flag: bool) -> None:
    """
    Full run should stage directories and manifest files, run experiments, and update statuses.
    Registry should be updated with CREATE_GROUP, SUBMIT_SWEEP, and UPDATE_STATUS events.
    """
    module_name = make_dummy_experiment_module(tmp_path)

    cfg = {
        "experiment_name": "demo",
        "experiment_path": module_name,
        "experiment_class": "DummyExp",
        "params": {"epochs": 5, "lr": 0.001},
        "sweep": {"lr": [0.001, 0.01, 0.1]},
    }
    write_yaml(cfg_path, cfg)

    runner = MainRunner(test=test_flag, dryrun=False)
    group_id = runner.start(cfg_path, reps_per_sweep=2)

    from rem.core.manifest import GroupManifest, RepManifest, SweepManifest
    from rem.core.stamp import format_rep_id, format_sweep_id, parse_group_id
    from rem.utils.paths import (
        get_default_events_path,
        get_group_manifest_path,
        get_rep_manifest_path,
        get_sweep_manifest_path,
    )
    from rem.utils.ulid import timestamp_from_ulid

    ulid_str = parse_group_id(group_id)
    group_dt = timestamp_from_ulid(ulid_str)

    # Registry checks
    events_path = get_default_events_path(test=test_flag)
    rm = RegistryManager(events_path=events_path)
    events = [
        e for e in rm.load_events(force_reload=True) if e.get("group_id") == group_id
    ]
    types = [e["type"] for e in events]

    # Expect:
    # 1 CREATE_GROUP
    # 3 SUBMIT_SWEEP
    # 3 element UPDATE_STATUS (one per sweep element)
    # 1 final UPDATE_STATUS for group completion
    assert types.count("CREATE_GROUP") == 1
    assert types.count("SUBMIT_SWEEP") == 3

    updates = [e for e in events if e["type"] == "UPDATE_STATUS"]
    assert any(e.get("status") == "RUNNING" and "sweep_id" not in e for e in updates)

    sweep_ids = [format_sweep_id(i) for i in range(1, 4)]
    for sweep_id in sweep_ids:
        # expect one completion event per element
        assert sum(1 for e in updates if e.get("sweep_id") == sweep_id) == 1

    final_group_updates = [e for e in updates if "sweep_id" not in e]
    assert any(
        e.get("status") in {"COMPLETED", "PARTIAL_COMPLETION"}
        for e in final_group_updates
    )

    # Manifest checks
    group_manifest = GroupManifest.load(
        get_group_manifest_path(group_id, group_dt, test=test_flag)
    )
    assert group_manifest.status in {"COMPLETED", "PARTIAL_COMPLETION"}

    for sweep_id in sweep_ids:
        sweep_manifest = SweepManifest.load(
            get_sweep_manifest_path(group_id, group_dt, sweep_id, test=test_flag)
        )
        assert sweep_manifest.status in {"COMPLETED", "PARTIAL_COMPLETION"}

        for rep_id in [format_rep_id(i) for i in range(1, 3)]:
            rep_manifest = RepManifest.load(
                get_rep_manifest_path(
                    group_id, group_dt, sweep_id, rep_id, test=test_flag
                )
            )
            assert rep_manifest.status in {"COMPLETED", "CRASHED"}


@pytest.mark.parametrize("test_flag", [True, False])  # type: ignore[misc]
def test_resume_from_dryrun_reuses_group(
    cfg_path: Path, tmp_path: Path, test_flag: bool
) -> None:
    """
    A dryrun followed by a full run should reuse the same group_id and not duplicate the CREATE_GROUP/SUBMIT_SWEEP events.
    The manifests and directories should be reused from the dryrun.
    """
    module_name = make_dummy_experiment_module(tmp_path)

    cfg = {
        "experiment_name": "demo",
        "experiment_path": module_name,
        "experiment_class": "DummyExp",
        "params": {"epochs": 5, "lr": 0.001},
        "sweep": {"lr": [0.001, 0.01, 0.1]},
    }
    write_yaml(cfg_path, cfg)

    # First do a dryrun
    runner_dry = MainRunner(test=test_flag, dryrun=True)
    group_id_dry = runner_dry.start(cfg_path, reps_per_sweep=2)

    from rem.utils.paths import get_default_events_path

    events_path = get_default_events_path(test=test_flag)
    rm = RegistryManager(events_path=events_path)
    pre_events = [
        e
        for e in rm.load_events(force_reload=True)
        if e.get("group_id") == group_id_dry
    ]

    # Now do a full run
    runner_full = MainRunner(test=test_flag, dryrun=False)
    group_id_full = runner_full.start(cfg_path, reps_per_sweep=2, group_id=group_id_dry)

    assert group_id_dry == group_id_full

    post_events = [
        e
        for e in rm.load_events(force_reload=True)
        if e.get("group_id") == group_id_full
    ]

    def count_types(events: list[dict[str, Any]], event_type: str) -> int:
        return sum(1 for e in events if e["type"] == event_type)

    # Check for duplicates of CREATE_GROUP and SUBMIT_SWEEP
    assert count_types(pre_events, "CREATE_GROUP") == 1
    assert count_types(post_events, "CREATE_GROUP") == 1  # No duplicate
    assert count_types(pre_events, "SUBMIT_SWEEP") == 3
    assert count_types(post_events, "SUBMIT_SWEEP") == 3

    # There should be new UPDATE_STATUS events in the full run
    assert count_types(post_events, "UPDATE_STATUS") >= 4
    assert count_types(pre_events, "UPDATE_STATUS") == 0
