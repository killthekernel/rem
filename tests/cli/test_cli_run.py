import importlib
import re
from pathlib import Path
from typing import Any, Generator

import pytest
import typer
from typer.testing import CliRunner

from rem.core.registry import RegistryManager
from rem.core.stamp import format_rep_id, format_sweep_id, parse_group_id
from rem.utils.paths import (
    get_default_events_path,
    get_group_dir,
    get_group_manifest_path,
    get_rep_dir,
    get_rep_manifest_path,
    get_results_dir,
    get_sweep_dir,
    get_sweep_manifest_path,
)
from rem.utils.ulid import timestamp_from_ulid

runner = CliRunner()


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    import yaml

    with path.open("w") as f:
        yaml.safe_dump(data, f)


def _make_dummy_experiment_module(
    tmp_path: Path, module_name: str = "dummy_cli_exp"
) -> str:
    module_file = tmp_path.joinpath(f"{module_name}.py")
    module_src = (
        "from ml_collections import ConfigDict\n"
        "from rem.core.experiment import ExperimentBase\n\n"
        "class DummyExperiment(ExperimentBase):\n"
        "    def __init__(self, config: ConfigDict) -> None:\n"
        "        super().__init__(config)\n"
        "    def run(self) -> dict[str, str]:\n"
        "        p = self.config.get('params', {})\n"
        "        return {\n"
        "            'status': 'ok',\n"
        "            'lr': float(p.get('lr', 0)),\n"
        "            'epochs': int(p.get('epochs', 0)),\n"
        "            'use_gpu': bool(p.get('use_gpu', False)),\n"
        "        }\n"
    )
    module_file.write_text(module_src)

    import sys

    if str(tmp_path) not in sys.path:
        sys.path.insert(0, str(tmp_path))
    importlib.invalidate_caches()
    return module_name


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _isolate_rem_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[None, None, None]:
    monkeypatch.setenv("REM_ROOT", str(tmp_path))
    yield


def _load_root_app() -> typer.Typer:
    mod = importlib.import_module("rem.cli.main")
    return getattr(mod, "app")


def _extract_group_id(output: str) -> str:
    """
    Lazy helper to parse "Experiment group ID: <id>" from CLI output.
    """
    match = re.search(r"Experiment group ID:\s+(G_\w{10}_\w{16})", output)
    assert match, f"Failed to parse group ID from output: {output}"
    return match.group(1)


@pytest.mark.parametrize("test_flag", [False, True])  # type: ignore[misc]
def test_cli_run_dryrun_stages(tmp_path: Path, test_flag: bool) -> None:
    app = _load_root_app()

    cfg = {
        "experiment_name": "demo_cli_exp",
        "params": {"lr": 0.01, "epochs": 5},
        "sweep": {"lr": [0.1, 0.01, 0.001]},
    }
    cfg_path = tmp_path.joinpath("config.yaml")
    _write_yaml(cfg_path, cfg)

    args = ["run", str(cfg_path), "--reps", "2", "--dryrun"]
    if test_flag:
        args.append("--test")

    res = runner.invoke(app, args)
    assert res.exit_code == 0

    group_id = _extract_group_id(res.stdout)
    ulid_str = parse_group_id(group_id)
    group_dt = timestamp_from_ulid(ulid_str)

    # Check directory structure
    group_dir = get_group_dir(group_id, group_dt, test=test_flag)
    assert group_dir.exists() and group_dir.is_dir()

    sweep_ids = [format_sweep_id(i) for i in range(1, 4)]
    rep_ids = [format_rep_id(i) for i in range(1, 3)]

    for sweep_id in sweep_ids:
        sweep_dir = get_sweep_dir(group_id, group_dt, sweep_id, test=test_flag)
        assert sweep_dir.exists() and sweep_dir.is_dir()

        for rep_id in rep_ids:
            rep_dir = get_rep_dir(group_id, group_dt, sweep_id, rep_id, test=test_flag)
            assert rep_dir.exists() and rep_dir.is_dir()

    # Check registry
    # Should only contain CREATE_GROUP and SUBMIT_SWEEP
    events_path = get_default_events_path(test=test_flag)
    rm = RegistryManager(events_path)
    events = [
        e for e in rm.load_events(force_reload=True) if e.get("group_id") == group_id
    ]
    types = [e["type"] for e in events]
    assert types.count("CREATE_GROUP") == 1
    assert types.count("SUBMIT_SWEEP") == 3
    assert "UPDATE_STATUS" not in types

    # Check that manifests are PENDING
    from rem.core.manifest import GroupManifest, RepManifest, SweepManifest

    gm = GroupManifest.load(get_group_manifest_path(group_id, group_dt, test=test_flag))
    assert gm.status == "PENDING"

    for sweep_id in sweep_ids:
        sm = SweepManifest.load(
            get_sweep_manifest_path(group_id, group_dt, sweep_id, test=test_flag)
        )
        assert sm.status == "PENDING"
        for rep_id in rep_ids:
            rmf = RepManifest.load(
                get_rep_manifest_path(
                    group_id, group_dt, sweep_id, rep_id, test=test_flag
                )
            )
            assert rmf.status == "PENDING"


@pytest.mark.parametrize("test_flag", [False, True])  # type: ignore[misc]
def test_cli_run_full(tmp_path: Path, test_flag: bool) -> None:
    app = _load_root_app()
    module_name = _make_dummy_experiment_module(tmp_path)

    cfg = {
        "experiment_name": "demo_cli_full",
        "experiment_path": module_name,
        "experiment_class": "DummyExperiment",
        "params": {"lr": 0.01, "epochs": 5},
        "sweep": {"lr": [0.1, 0.01, 0.001]},
    }
    cfg_path = tmp_path.joinpath("config.yaml")
    _write_yaml(cfg_path, cfg)

    args = ["run", str(cfg_path), "--reps", "2"]
    if test_flag:
        args.append("--test")

    res = runner.invoke(app, args)
    assert res.exit_code == 0

    group_id = _extract_group_id(res.stdout)
    ulid_str = parse_group_id(group_id)
    group_dt = timestamp_from_ulid(ulid_str)

    events_path = get_default_events_path(test=test_flag)
    rm = RegistryManager(events_path)
    events = [
        e for e in rm.load_events(force_reload=True) if e.get("group_id") == group_id
    ]
    types = [e["type"] for e in events]

    assert types.count("CREATE_GROUP") == 1
    assert types.count("SUBMIT_SWEEP") == 3

    updates = [e for e in events if e["type"] == "UPDATE_STATUS"]
    # There should be one group RUNNING, three sweep completions, and one final group status
    assert any(e.get("status") == "RUNNING" and "sweep_id" not in e for e in updates)
    assert sum(1 for e in updates if "sweep_id" in e) == 3

    from rem.core.manifest import GroupManifest

    gm = GroupManifest.load(get_group_manifest_path(group_id, group_dt, test=test_flag))
    assert gm.status in {"COMPLETED", "PARTIAL_COMPLETION"}


@pytest.mark.parametrize("test_flag", [False, True])  # type: ignore[misc]
def test_cli_run_resume(tmp_path: Path, test_flag: bool) -> None:
    app = _load_root_app()
    module_name = _make_dummy_experiment_module(tmp_path)

    cfg = {
        "experiment_name": "demo_cli_resume",
        "experiment_path": module_name,
        "experiment_class": "DummyExperiment",
        "params": {"lr": 0.01, "epochs": 5},
        "sweep": {"lr": [0.1, 0.01, 0.001]},
    }
    cfg_path = tmp_path.joinpath("config.yaml")
    _write_yaml(cfg_path, cfg)

    # First do a dryrun to stage the group
    args1 = ["run", str(cfg_path), "--reps", "2", "--dryrun"]
    if test_flag:
        args1.append("--test")

    res1 = runner.invoke(app, args1)
    assert res1.exit_code == 0
    group_id = _extract_group_id(res1.stdout)

    args2 = ["run", str(cfg_path), "--reps", "2", "--group", group_id]
    if test_flag:
        args2.append("--test")
    res2 = runner.invoke(app, args2)
    assert res2.exit_code == 0
    group_id2 = _extract_group_id(res2.stdout)
    assert group_id2 == group_id

    events_path = get_default_events_path(test=test_flag)
    rm = RegistryManager(events_path)
    events = [
        e for e in rm.load_events(force_reload=True) if e.get("group_id") == group_id
    ]

    def count(events: list[dict[str, Any]], kind: str) -> int:
        return sum(1 for e in events if e["type"] == kind)

    assert count(events, "CREATE_GROUP") == 1
    assert count(events, "SUBMIT_SWEEP") == 3
    assert (
        count(events, "UPDATE_STATUS") >= 4
    )  # At least one group RUNNING and three sweep completions


@pytest.mark.parametrize("test_flag", [False, True])  # type: ignore[misc]
def test_cli_local_no_staging(tmp_path: Path, test_flag: bool) -> None:
    """
    local runs should not stage group/sweep/rep dirs but should still apply overrides.
    """
    app = _load_root_app()
    module_name = _make_dummy_experiment_module(tmp_path)

    cfg = {
        "experiment_name": "demo_cli_local",
        "experiment_path": module_name,
        "experiment_class": "DummyExperiment",
        "params": {"lr": 0.01, "epochs": 10},
    }
    cfg_path = tmp_path.joinpath("config.yaml")
    _write_yaml(cfg_path, cfg)

    assert not get_results_dir(test=test_flag).exists()
    assert not get_default_events_path(test=test_flag).exists()

    res1 = runner.invoke(app, ["local", str(cfg_path)])
    assert res1.exit_code == 0
    assert (
        '"epochs": 10' in res1.stdout or "'epochs': 10" in res1.stdout
    )  # Check original

    res2 = runner.invoke(app, ["local", str(cfg_path), "--override", "params.epochs=5"])
    assert res2.exit_code == 0
    assert (
        '"epochs": 5' in res2.stdout or "'epochs': 5" in res2.stdout
    )  # Check that override worked

    # Check that no staging dirs were created
    assert not get_results_dir(test=test_flag).exists()
    assert not get_default_events_path(test=test_flag).exists()
