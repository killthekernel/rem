from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
from ml_collections import ConfigDict

from rem.core import config_validation as cv


def make_base_config(**params: Any) -> ConfigDict:
    return ConfigDict({"experiment_name": "test_exp", "params": params})


def test_missing_required_key_raises() -> None:
    cfg = ConfigDict({"params": {"lr": 0.01}})
    with pytest.raises(
        ValueError, match="Missing required config key: 'experiment_name'"
    ):
        cv.validate_config_structure(cfg)


def test_unknown_top_level_key_logs_warning(caplog: LogCaptureFixture) -> None:
    cfg = make_base_config(lr=0.01)
    cfg["extra"] = "oops"
    with caplog.at_level("WARNING"):
        cv.validate_config_structure(cfg)
    assert any(
        "Unknown top-level config key: 'extra'" in msg for msg in caplog.messages
    )


def test_invalid_sweep_key_raises() -> None:
    cfg = make_base_config(lr=0.01)
    cfg["sweep"] = ["missing_key"]
    with pytest.raises(
        ValueError, match="Sweep key 'missing_key' not found in params."
    ):
        cv.check_sweep_keys(cfg)


def test_reserved_key_in_params_raises() -> None:
    cfg = make_base_config(experiment_name="foo", lr=0.01)
    with pytest.raises(
        ValueError, match="'experiment_name' is a reserved top-level key"
    ):
        cv.check_reserved_keys_in_params(cfg)


def test_param_with_invalid_type_raises() -> None:
    cfg = make_base_config(lr=0.01, model_class=object)
    with pytest.raises(ValueError, match="Param 'model_class' has disallowed type"):
        cv.check_param_types(cfg)


def test_valid_config_passes_all_checks() -> None:
    cfg = ConfigDict(
        {
            "experiment_name": "valid_exp",
            "test": True,
            "sweep": ["lr", "batch_size"],
            "params": {"lr": 0.01, "batch_size": 32, "model": "resnet"},
        }
    )
    # Should not raise
    cv.validate_config(cfg)
