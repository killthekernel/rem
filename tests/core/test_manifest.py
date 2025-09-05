import json
import os
import threading
import warnings
from datetime import datetime
from pathlib import Path

import pytest

from rem.core.manifest import (
    GroupManifest,
    RepManifest,
    SweepManifest,
    init_group_manifest,
    init_rep_manifest,
    init_sweep_manifest,
    summarize_group_patches,
    summarize_group_status,
    summarize_sweep_status,
    update_group_manifest,
    update_rep_manifest,
    update_sweep_manifest,
)
from rem.core.stamp import (
    create_group_stamp,
    format_rep_id,
    format_sweep_id,
    is_valid_group_id,
)
from rem.utils.paths import (
    get_group_manifest_path,
    get_rep_manifest_path,
    get_sweep_manifest_path,
)


@pytest.fixture  # type: ignore[misc]
def temp_manifest_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Path, str, datetime, str, str]:
    monkeypatch.setenv("REM_ROOT", str(tmp_path))

    group_id, group_date_str = create_group_stamp()
    group_date = datetime.strptime(group_date_str, "%Y%m%d")
    assert is_valid_group_id(group_id)

    sweep_id = format_sweep_id(1)
    rep_id = format_rep_id(1)

    group_path = get_group_manifest_path(group_id, group_date)
    sweep_path = get_sweep_manifest_path(group_id, group_date, sweep_id)
    rep_path = get_rep_manifest_path(group_id, group_date, sweep_id, rep_id)

    return group_path, sweep_path, rep_path, group_id, group_date, sweep_id, rep_id


def test_manifest_lifecycle(
    temp_manifest_paths: tuple[Path, Path, Path, str, datetime, str, str]
) -> None:
    group_path, sweep_path, rep_path, group_id, _, sweep_id, rep_id = (
        temp_manifest_paths
    )

    group = GroupManifest(
        stamp=group_id,
        group_id=group_id,
        experiment_class="my.Experiment",
        experiment_path="path/to/script.py",
        sweep={"mode": "grid", "vars": {}, "reps": 2},
        slurm={"enabled": False},
    )
    init_group_manifest(group_path, group)
    loaded_group = GroupManifest.load(group_path)
    assert loaded_group.group_id == group_id

    sweep = SweepManifest(
        sweep_id=sweep_id,
        parameter_combination={"lr": 0.01},
        num_reps=2,
        reps=[{"rep_id": rep_id, "status": "PENDING", "version": 1}],
    )
    init_sweep_manifest(sweep_path, sweep)
    loaded_sweep = SweepManifest.load(sweep_path)
    assert loaded_sweep.sweep_id == sweep_id

    rep = RepManifest(
        rep_id=rep_id,
        sweep_id=sweep_id,
        group_id=group_id,
        parameters={"lr": 0.01},
        system_info={"host": "test"},
    )
    init_rep_manifest(rep_path, rep)
    loaded_rep = RepManifest.load(rep_path)
    assert loaded_rep.rep_id == rep_id

    update_rep_manifest(rep_path, {"status": "COMPLETED"})
    rep2 = RepManifest.load(rep_path)
    assert rep2.status == "COMPLETED"

    update_sweep_manifest(sweep_path, {"status": "PARTIAL_COMPLETION"})
    sweep2 = SweepManifest.load(sweep_path)
    assert sweep2.status == "PARTIAL_COMPLETION"

    update_group_manifest(group_path, {"status": "COMPLETED"})
    group2 = GroupManifest.load(group_path)
    assert group2.status == "COMPLETED"


def test_summarize_group_status() -> None:
    sweep1 = SweepManifest(
        sweep_id=format_sweep_id(1),
        parameter_combination={},
        num_reps=1,
        reps=[{"rep_id": format_rep_id(1), "status": "PENDING", "version": 1}],
        status="PENDING",
    )
    sweep2 = SweepManifest(
        sweep_id=format_sweep_id(2),
        parameter_combination={},
        num_reps=1,
        reps=[{"rep_id": format_rep_id(2), "status": "PENDING", "version": 1}],
        status="PENDING",
    )
    assert summarize_group_status([sweep1, sweep2]) == "PENDING"


def test_summarize_sweep_status() -> None:
    reps = [
        {"rep_id": format_rep_id(1), "status": "PENDING", "version": 1},
        {"rep_id": format_rep_id(2), "status": "PENDING", "version": 1},
    ]
    assert summarize_sweep_status(reps) == "PENDING"


def test_group_patch_summary() -> None:
    rep1 = RepManifest(
        rep_id=format_rep_id(1),
        sweep_id=format_sweep_id(1),
        group_id="G_ABC",
        patch_id="patch_001",
        replaces="R_001__v1",
        status="COMPLETED",
        timestamp_end="2025-07-25T09:40:22Z",
    )
    rep2 = RepManifest(
        rep_id=format_rep_id(2),
        sweep_id=format_sweep_id(1),
        group_id="G_ABC",
        patch_id="patch_001",
        replaces="R_002__v1",
        status="COMPLETED",
        timestamp_end="2025-07-25T09:40:22Z",
    )
    patches = summarize_group_patches([rep1, rep2])
    assert len(patches) == 1
    assert patches[0]["patch_id"] == "patch_001"
    assert "R_001__v1" in patches[0]["replaces"]
    assert "R_002__v1" in patches[0]["replaces"]


def test_manifest_atomic_write_and_cleanup(tmp_path: Path) -> None:
    path = tmp_path.joinpath("manifest.json")
    manifest = RepManifest(
        rep_id=format_rep_id(1),
        sweep_id=format_sweep_id(1),
        group_id="G_ABC",
        parameters={"lr": 0.01},
        system_info={"host": "test"},
    )

    manifest.save(path)
    assert path.exists()

    data = json.loads(path.read_text())
    assert data["rep_id"] == format_rep_id(1)
    assert data["sweep_id"] == format_sweep_id(1)
    assert data["group_id"].startswith("G_")

    # Check cleanup is successful
    tmp_files = list(p for p in path.parent.iterdir() if p.name.endswith(".tmp"))
    assert not tmp_files


def test_manifest_concurrent_saves(tmp_path: Path) -> None:
    path = tmp_path.joinpath("manifest.json")
    m1 = RepManifest(
        rep_id=format_rep_id(1), sweep_id=format_sweep_id(1), group_id="G_ABC"
    )
    m2 = RepManifest(
        rep_id=format_rep_id(2), sweep_id=format_sweep_id(1), group_id="G_ABC"
    )

    t1 = threading.Thread(target=lambda: m1.save(path))
    t2 = threading.Thread(target=lambda: m2.save(path))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert path.exists()
    data = json.loads(path.read_text())
    assert data["rep_id"] in (format_rep_id(1), format_rep_id(2))
