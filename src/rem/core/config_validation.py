from collections.abc import Mapping, Sequence
from typing import Any, cast

from ml_collections import ConfigDict

from rem.utils.logger import get_logger

logger = get_logger(__name__)

REQUIRED_TOP_LEVEL_KEYS = ["experiment_name", "params"]
OPTIONAL_TOP_LEVEL_KEYS = ["sweep", "test"]
RESERVED_TOP_LEVEL_KEYS = REQUIRED_TOP_LEVEL_KEYS + OPTIONAL_TOP_LEVEL_KEYS


def validate_config_structure(cfg: ConfigDict) -> None:
    """
    Ensures the config contains the required top-level fields.
    Raises:
        ValueError if required keys are missing.
    """
    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key not in cfg:
            raise ValueError(f"Missing required config key: '{key}'")

    # Warn about unknown top-level keys
    for key in cfg:
        if key not in RESERVED_TOP_LEVEL_KEYS:
            logger.warning(f"Unknown top-level config key: '{key}'")


def _is_nonstring_sequence(x: Any) -> bool:
    return isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray))


# Need this so ConfigDict doesn't get rejected
def _is_mapping(x: Any) -> bool:
    return isinstance(x, Mapping) or isinstance(x, ConfigDict) or hasattr(x, "items")


def _as_items(x: Any) -> Any:
    return x.items() if hasattr(x, "items") else ()


def _collect_sweep_params_keys(node: Any, *, path: str = "sweep") -> set[str]:
    """
    Recursively validate the sweep specification and collect all parameter keys it references.
    This method supports three forms:
        1) Leaf grid dict: {"param_name": [v1, v2, ...]}
        2) Zip dict: {"zip": {param1: [...], param2: [...], ...}} (all lists same length)
        3) Grid list: {"grid": [ {param1: v1, param2: v2}, {...}, ... ]} (cartesian product of children)
        *4) Nested combinations of the above.
    Raises:
        ValueError if the sweep structure is invalid.
    """
    if node is None or node is False:
        return set()

    keys: set[str] = set()

    # list is treated like a grid list (sequence of child nodes)
    if isinstance(node, list):
        for i, child in enumerate(node):
            keys |= _collect_sweep_params_keys(child, path=f"{path}[{i}]")
        return keys

    # Mapping/dict-like nodes (dict, ConfigDict, etc.)
    if _is_mapping(node):
        # grid node
        if "grid" in node:
            grid = node["grid"]
            if not isinstance(grid, list):
                raise ValueError(f"Expected 'grid' to be a list at {path}.")
            for i, child in enumerate(grid):
                keys |= _collect_sweep_params_keys(child, path=f"{path}.grid[{i}]")
            return keys

        # zip node
        if "zip" in node:
            z = node["zip"]
            if not _is_mapping(z):
                raise ValueError(
                    f"Expected {path}.zip to be a mapping of {{param: list}}."
                )
            lengths: set[int] = set()
            for k, v in _as_items(z):
                if not _is_nonstring_sequence(v):
                    raise ValueError(
                        f"Expected {path}.zip.{k} to be a list or tuple of values."
                    )
                lengths.add(len(v))
                keys.add(str(k))
            if len(lengths) > 1:
                raise ValueError(
                    f"All lists in {path}.zip must have equal lengths; got lengths={sorted(lengths)}"
                )
            return keys

        # leaf grid dict (param -> list/tuple of values)
        for k, v in _as_items(node):
            if not _is_nonstring_sequence(v):
                raise ValueError(
                    f"Expected {path}.{k} to be a list or tuple of values."
                )
            keys.add(str(k))
        return keys

    # Anything else is invalid
    raise ValueError(f"Invalid sweep structure at {path}: {node!r}")


def check_sweep_keys(cfg: ConfigDict) -> None:
    """
    Validates that sweep references only parameters present in cfg.params,
    and that the sweep schema is well-formed (recursive grid/zip/leaf).
    """
    if "sweep" not in cfg or cfg["sweep"] in (None, False):
        return

    referenced = _collect_sweep_params_keys(cfg["sweep"], path="sweep")

    params = cast(dict[str, Any], cfg.get("params", {}))
    missing = [k for k in referenced if k not in params]
    if missing:
        # If your test expects a different message, adjust here:
        raise ValueError(f"Sweep references undefined params: {missing}")


def check_reserved_keys_in_params(cfg: ConfigDict) -> None:
    """
    Prevents users from accidentally using reserved top-level keys as param names.
    Raises:
        ValueError if a reserved key is found in cfg.params.
    """
    params = cast(dict[str, Any], cfg.get("params", {}))
    for key in params:
        if key in RESERVED_TOP_LEVEL_KEYS:
            raise ValueError(
                f"'{key}' is a reserved top-level key and cannot appear inside 'params'."
            )


def check_param_types(cfg: ConfigDict) -> None:
    """
    Optionally checks that all param values are of basic serializable types.
    Raises:
        ValueError if an invalid type is encountered.
    """
    ALLOWED_TYPES = (int, float, str, bool, list)

    params = cast(dict[str, Any], cfg.get("params", {}))
    for k, v in params.items():
        if not isinstance(v, ALLOWED_TYPES):
            raise ValueError(f"Param '{k}' has disallowed type: {type(v).__name__}")


def check_experiment_exists(cfg: ConfigDict) -> None:
    """
    Optionally used to detect existing group directory before stamping.
    Can raise FileExistsError if path collision is detected.
    """
    from datetime import date

    from rem.utils.paths import get_group_dir

    group_id = cast(str, cfg.get("group_id"))
    group_date = cast(date, cfg.get("group_date"))

    if not group_id or not group_date:
        logger.debug("Group ID or date missing — skipping experiment existence check.")
        return

    group_path = get_group_dir(group_id, group_date)
    if group_path.exists():
        raise FileExistsError(f"Experiment group already exists at {group_path}")


def validate_config(cfg: ConfigDict) -> None:
    """
    Master validation entry point. Should be called before stamping or running.
    Raises:
        ValueError if the config is structurally invalid or inconsistent.
    """
    validate_config_structure(cfg)
    check_sweep_keys(cfg)
    check_reserved_keys_in_params(cfg)
    check_param_types(cfg)
