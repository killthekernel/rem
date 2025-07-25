import importlib
from datetime import date
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from rem.utils import paths
from rem.utils.ulid import new_ulid


def test_rem_root_default(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("REM_ROOT", raising=False)

    assert paths.REM_ROOT == Path.cwd().resolve()


def test_rem_root_env_override(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("REM_ROOT", str(tmp_path))
    # Reload the module to re-evaluate REM_ROOT
    importlib.reload(paths)

    assert paths.REM_ROOT == tmp_path.resolve()


def test_path_construction(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    group_id = new_ulid()
    sweep_id = "S_0001"
    rep_id = "R_001"
    d = date(2025, 7, 25)

    base = tmp_path.joinpath("results")
    monkeypatch.setenv("REM_ROOT", str(tmp_path))
    importlib.reload(paths)

    group_path = paths.get_group_dir(group_id, d)
    assert group_path == base.joinpath("2025", "07", "25", group_id)

    assert paths.get_group_manifest_path(group_id, d) == group_path.joinpath(
        "manifest.json"
    )
    sweep_path = paths.get_sweep_dir(group_id, d, sweep_id)
    assert sweep_path == group_path.joinpath(sweep_id)

    rep_path = paths.get_rep_dir(group_id, d, sweep_id, rep_id)
    assert rep_path == sweep_path.joinpath(rep_id)
    assert paths.get_rep_manifest_path(
        group_id, d, sweep_id, rep_id
    ) == rep_path.joinpath("manifest.json")
