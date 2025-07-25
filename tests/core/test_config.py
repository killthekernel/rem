from pathlib import Path

import pytest
from ml_collections import ConfigDict
from torch import diff

from rem.core.config import (
    diff_configs,
    flatten_config,
    load_config_from_yaml,
    override_config,
    to_dict,
    unflatten_config,
    validate_yaml_structure,
)

TEST_DATA = Path(__file__).parent.parent.joinpath("data")


def test_load_config_from_file() -> None:
    cfg = load_config_from_yaml(TEST_DATA.joinpath("base.yaml"))
    assert isinstance(cfg, ConfigDict)
    assert cfg.training.lr == 0.01  # type: ignore[attr-defined]
    assert cfg.training.epochs == 10  # type: ignore[attr-defined]
    assert cfg.model.type == "mlp"  # type: ignore[attr-defined]


def test_override_from_file() -> None:
    base = load_config_from_yaml(TEST_DATA.joinpath("base.yaml"))
    override = load_config_from_yaml(TEST_DATA.joinpath("override.yaml"))
    overrides_flat = {
        "training.lr": override.training.lr,  # type: ignore[attr-defined]
        "model.type": override.model.type,  # type: ignore[attr-defined]
    }
    updated = override_config(base, overrides_flat)
    assert updated.training.lr == 0.001  # type: ignore[attr-defined]
    assert updated.training.epochs == 10  # type: ignore[attr-defined]
    assert updated.model.type == "cnn"  # type: ignore[attr-defined]


def test_diff_configs_from_file() -> None:
    base = load_config_from_yaml(TEST_DATA.joinpath("base.yaml"))
    override = load_config_from_yaml(TEST_DATA.joinpath("override.yaml"))
    diff = diff_configs(base, override)
    assert diff["training.lr"] == {"config1": 0.01, "config2": 0.001}
    assert diff["model.type"] == {"config1": "mlp", "config2": "cnn"}


def test_validate_yaml_structure() -> None:
    valid_path = TEST_DATA.joinpath("base.yaml")
    invalid_path = TEST_DATA.joinpath("invalid.yaml")
    assert validate_yaml_structure(valid_path) is True
    assert validate_yaml_structure(invalid_path) is False


def test_flatten_and_unflatten_file_based() -> None:
    cfg = load_config_from_yaml(TEST_DATA.joinpath("base.yaml"))
    flat = flatten_config(cfg)
    nested = unflatten_config(flat)
    assert nested == cfg.to_dict()


def test_to_dict() -> None:
    cfg = load_config_from_yaml(TEST_DATA.joinpath("base.yaml"))
    d = to_dict(cfg)
    assert isinstance(d, dict)
    assert d["training"]["lr"] == 0.01
