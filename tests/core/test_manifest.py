import json
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


def test_rep_manifest_save_load(tmp_path: Path) -> None:
    path = tmp_path / "rep.json"
    rep = RepManifest(rep_id="R_000", sweep_id="S_0001", group_id="G_001")
    rep.save(path)
    loaded = RepManifest.load(path)
    assert loaded.rep_id == "R_000"
    assert loaded.group_id == "G_001"
    assert loaded.timestamp_updated is not None


def test_update_rep_manifest_status(tmp_path: Path) -> None:
    path = tmp_path / "rep.json"
    rep = RepManifest(rep_id="R_001", sweep_id="S_0001", group_id="G_001")
    rep.save(path)
    update_rep_manifest(path, {"status": "COMPLETED"})
    updated = RepManifest.load(path)
    assert updated.status == "COMPLETED"
    assert updated.timestamp_updated is not None


def test_invalid_rep_status_raises(tmp_path: Path) -> None:
    path = tmp_path / "rep.json"
    rep = RepManifest(rep_id="R_002", sweep_id="S_0001", group_id="G_001")
    rep.save(path)
    with pytest.raises(ValueError):
        update_rep_manifest(path, {"status": "INVALID"})


def test_summarize_sweep_status() -> None:
    reps = [
        {"rep_id": "R_1", "status": "COMPLETED"},
        {"rep_id": "R_2", "status": "COMPLETED"},
    ]
    assert summarize_sweep_status(reps) == "COMPLETED"

    reps = [
        {"rep_id": "R_1", "status": "FAILED"},
        {"rep_id": "R_2", "status": "TIMEOUT"},
    ]
    assert summarize_sweep_status(reps) == "PARTIAL_COMPLETION"

    reps = [
        {"rep_id": "R_1", "status": "PENDING"},
        {"rep_id": "R_2", "status": "PENDING"},
    ]
    assert summarize_sweep_status(reps) == "PENDING"

    reps = [
        {"rep_id": "R_1", "status": "PENDING"},
        {"rep_id": "R_2", "status": "RUNNING"},
    ]
    assert summarize_sweep_status(reps) == "IN_PROGRESS"


def test_summarize_group_status() -> None:
    sweep1 = SweepManifest(
        sweep_id="S1", parameter_combination={}, num_reps=1, reps=[], status="COMPLETED"
    )
    sweep2 = SweepManifest(
        sweep_id="S2", parameter_combination={}, num_reps=1, reps=[], status="COMPLETED"
    )
    assert summarize_group_status([sweep1, sweep2]) == "COMPLETED"

    sweep1.status = "COMPLETED"
    sweep2.status = "RUNNING"
    assert summarize_group_status([sweep1, sweep2]) == "IN_PROGRESS"

    sweep1.status = "PENDING"
    sweep2.status = "PENDING"
    assert summarize_group_status([sweep1, sweep2]) == "PENDING"

    sweep1.status = "COMPLETED"
    sweep2.status = "FAILED"
    assert summarize_group_status([sweep1, sweep2]) == "PARTIAL_COMPLETION"


def test_group_patch_summary() -> None:
    reps = [
        RepManifest(
            rep_id="R_1",
            sweep_id="S_1",
            group_id="G_1",
            patch_id="p1",
            replaces="R_0",
            timestamp_end="2025-01-01T00:00:00Z",
        ),
        RepManifest(
            rep_id="R_2",
            sweep_id="S_1",
            group_id="G_1",
            patch_id="p1",
            replaces="R_1",
            timestamp_end="2025-01-01T00:00:01Z",
        ),
        RepManifest(rep_id="R_3", sweep_id="S_1", group_id="G_1"),
    ]
    patches = summarize_group_patches(reps)
    assert any(p["patch_id"] == "p1" and len(p["replaces"]) == 2 for p in patches)


def test_group_manifest_init_and_update(tmp_path: Path) -> None:
    path = tmp_path / "group.json"
    group = GroupManifest(
        stamp="20250730_XXXX",
        group_id="G_123",
        experiment_class="experiments.dummy.MyExp",
        experiment_path="experiments/dummy.py",
        sweep={"mode": "grid", "vars": {}, "reps": 3},
        slurm={"enabled": False},
        status="PENDING",
    )
    init_group_manifest(path, group)
    assert path.exists()
    update_group_manifest(path, {"status": "COMPLETED"})
    loaded = GroupManifest.load(path)
    assert loaded.status == "COMPLETED"
    assert loaded.timestamp_updated is not None


def test_sweep_manifest_init_and_update(tmp_path: Path) -> None:
    path = tmp_path / "sweep.json"
    sweep = SweepManifest(
        sweep_id="S_001",
        parameter_combination={"lr": 0.01},
        num_reps=2,
        reps=[
            {"rep_id": "R_1", "status": "PENDING"},
            {"rep_id": "R_2", "status": "PENDING"},
        ],
    )
    init_sweep_manifest(path, sweep)
    assert path.exists()
    update_sweep_manifest(path, {"status": "COMPLETED"})
    loaded = SweepManifest.load(path)
    assert loaded.status == "COMPLETED"
    assert loaded.timestamp_updated is not None


def test_rep_manifest_init(tmp_path: Path) -> None:
    path = tmp_path / "rep_init.json"
    rep = RepManifest(rep_id="R_123", sweep_id="S_456", group_id="G_789")
    init_rep_manifest(path, rep)
    loaded = RepManifest.load(path)
    assert loaded.rep_id == "R_123"
    assert loaded.timestamp_created is not None
