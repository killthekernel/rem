import pytest

from rem.core.sweeps import (
    expand_sweep_node,
    generate_sweep_element_ids,
    get_sweep_elements,
)


def test_flat_grid() -> None:
    node = {"lr": [0.01, 0.1], "batch_size": [32, 64]}
    result = expand_sweep_node(node)
    expected = [
        {"lr": 0.01, "batch_size": 32},
        {"lr": 0.01, "batch_size": 64},
        {"lr": 0.1, "batch_size": 32},
        {"lr": 0.1, "batch_size": 64},
    ]
    assert result == expected


def test_zip() -> None:
    node = {"zip": {"a": [1, 2], "b": [3, 4]}}
    result = expand_sweep_node(node)
    expected = [{"a": 1, "b": 3}, {"a": 2, "b": 4}]
    assert result == expected


def test_nested_grid_zip() -> None:
    node = {
        "grid": [
            {"lr": [0.01, 0.1]},
            {"zip": {"batch_size": [32, 64], "dropout": [0.0, 0.5]}},
        ]
    }
    result = expand_sweep_node(node)
    expected = [
        {"lr": 0.01, "batch_size": 32, "dropout": 0.0},
        {"lr": 0.01, "batch_size": 64, "dropout": 0.5},
        {"lr": 0.1, "batch_size": 32, "dropout": 0.0},
        {"lr": 0.1, "batch_size": 64, "dropout": 0.5},
    ]
    assert result == expected


def test_generate_ids() -> None:
    ids = generate_sweep_element_ids(3)
    assert ids == ["S_0001", "S_0002", "S_0003"]


def test_get_sweep_elements() -> None:
    cfg = {
        "sweep": {
            "grid": [
                {"optimizer": ["adam", "sgd"]},
                {"zip": {"momentum": [0.9, 0.95], "nesterov": [True, False]}},
            ]
        }
    }
    elements = get_sweep_elements(cfg)
    ids = [eid for eid, _ in elements]
    assert ids == ["S_0001", "S_0002", "S_0003", "S_0004"]
    assert all(isinstance(e[1], dict) for e in elements)


def test_missing_sweep_returns_single_element() -> None:
    cfg = {"experiment_name": "baseline"}
    elements = get_sweep_elements(cfg)
    assert elements == [("S_0001", {})]
