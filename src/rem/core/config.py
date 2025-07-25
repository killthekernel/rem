from pathlib import Path
from typing import Any, Union, cast

import yaml
from ml_collections import ConfigDict


def load_config_from_yaml(path: Union[str, Path]) -> ConfigDict:
    """
    Load a config from a YAML file and convert to ConfigDict.
    """
    path = Path(path)
    with path.open("r") as f:
        raw = yaml.safe_load(f)
    return ConfigDict(raw)


def save_config_to_yaml(config: ConfigDict, path: Union[str, Path]) -> None:
    """
    Save ConfigDict to a YAML file.
    """
    path = Path(path)
    with path.open("w") as f:
        yaml.safe_dump(config.to_dict(), f)


def flatten_config(
    config: ConfigDict, parent_key: str = "", sep: str = "."
) -> dict[str, Any]:
    """
    Flatten a nested ConfigDict into a flat dict with dot-separated keys.

    Example:
        ConfigDict({'a': ConfigDict({'b': 1})})
        => {'a.b': 1}
    """
    items: dict[str, Any] = {}
    for k, v in config.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, ConfigDict):
            items.update(flatten_config(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items


def unflatten_config(flat_dict: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """
    Convert a flat dict with dot-separated keys into a nested ConfigDict.

    Example:
        {'a.b': 1} => ConfigDict({'a': ConfigDict({'b': 1})})
    """
    result: dict[str, Any] = {}
    for flat_key, value in flat_dict.items():
        keys = flat_key.split(sep)
        d = result
        for key in keys[:-1]:
            d = d.setdefault(key, {})
        d[keys[-1]] = value
    return result


def override_config(
    config: ConfigDict, overrides: dict[str, Any], sep: str = "."
) -> ConfigDict:
    """
    Apply flat overrides to a config (not in-place).
    """
    overrides_nested = unflatten_config(overrides, sep=sep)
    updated_dict = _deep_update(config.to_dict(), overrides_nested)
    return ConfigDict(updated_dict)


def _deep_update(cfg: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively update a nested dict.
    """
    for k, v in updates.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            _deep_update(cfg[k], v)
        else:
            cfg[k] = v
    return cfg


def diff_configs(
    config1: ConfigDict, config2: ConfigDict, sep: str = "."
) -> dict[str, dict[str, Any]]:
    """
    Return a flat dict of keys that differ between two configs.
    """
    flat1 = flatten_config(config1, sep=sep)
    flat2 = flatten_config(config2, sep=sep)
    diff: dict[str, dict[str, Any]] = {}
    all_keys = set(flat1) | set(flat2)
    for k in all_keys:
        if flat1.get(k) != flat2.get(k):
            diff[k] = {"config1": flat1.get(k), "config2": flat2.get(k)}
    return diff


def validate_yaml_structure(path: Union[str, Path]) -> bool:
    """
    Validate that a YAML file is syntactically correct.
    """
    path = Path(path)
    try:
        with path.open("r") as f:
            yaml.safe_load(f)
        return True
    except yaml.YAMLError:
        return False


def to_dict(config: ConfigDict) -> dict[str, Any]:
    """
    Convert ConfigDict to a regular Python dict.
    """
    return cast(dict[str, Any], config.to_dict())
