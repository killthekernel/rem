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
    cfg = make_base_config(lr=0.01, batch_size=32, dropout=0.1)

    cfg["sweep"] = {
        "grid": [
            {"lr": [0.01, 0.001]},  # leaf ok
            {"zip": {"batch_size": [32, 64], "dropout": [0.1, 0.2]}},  # zip ok
            {"missing_key": [1, 2]},  # leaf ok but missing in params
        ]
    }

    with pytest.raises(ValueError, match=f"Sweep references undefined params"):
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
            "sweep": [
                {"lr": [0.01, 0.001]},
                {"zip": {"batch_size": [32, 64], "dropout": [0.1, 0.2]}},
            ],
            "params": {
                "lr": 0.01,
                "batch_size": 32,
                "dropout": 0.1,
                "model": "resnet",
            },
        }
    )
    # Should not raise
    cv.validate_config(cfg)


def test_zip_mismatched_lengths_raises() -> None:
    cfg = make_base_config(a=1, b=2)
    cfg["sweep"] = {"zip": {"a": [1, 2], "b": [10]}}  # unequal lengths
    with pytest.raises(ValueError, match=f"equal lengths"):
        cv.check_sweep_keys(cfg)


def test_leaf_value_not_sequence_raises() -> None:
    cfg = make_base_config(lr=0.01)
    cfg["sweep"] = {"lr": 0.1}  # not a list/tuple
    with pytest.raises(
        ValueError, match=f"Expected sweep.lr to be a list or tuple of values"
    ):
        cv.check_sweep_keys(cfg)


def test_top_level_list_sweep_valid() -> None:
    cfg = ConfigDict(
        {
            "experiment_name": "ok",
            "sweep": [
                {"lr": [0.1, 0.01]},
                {"zip": {"bs": [16, 32], "drop": [0.1, 0.2]}},
            ],
            "params": {"lr": 0.1, "bs": 16, "drop": 0.1},
        }
    )
    cv.validate_config(cfg)  # no error


@pytest.mark.parametrize("sweep_value", [None, False, []])  # type: ignore[misc]
def test_empty_or_none_or_false_sweep_is_noop(sweep_value: Any) -> None:
    cfg = ConfigDict(
        {
            "experiment_name": "noop",
            "sweep": sweep_value,
            "params": {"x": 1},
        }
    )
    cv.validate_config(cfg)  # no error


def test_nested_grid_zip_valid() -> None:
    cfg = ConfigDict(
        {
            "experiment_name": "nested_ok",
            "sweep": {
                "grid": [
                    {"zip": {"a": [1, 2], "b": [3, 4]}},
                    {"grid": [{"c": [7, 8]}, {"d": [9, 10]}]},
                ]
            },
            "params": {"a": 1, "b": 3, "c": 7, "d": 9},
        }
    )
    cv.validate_config(cfg)  # no error
