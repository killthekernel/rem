# src/rem/core/config_validation.py

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


def check_sweep_keys(cfg: ConfigDict) -> None:
    """
    Validates that sweep keys are defined in the params section.
    Raises:
        ValueError if sweep keys are not found in params.
    """
    sweep_keys = cfg.get("sweep", [])
    if not isinstance(sweep_keys, list):
        raise ValueError("Expected 'sweep' to be a list of parameter keys.")

    params = cast(dict[str, Any], cfg.get("params", {}))
    for key in sweep_keys:
        if key not in params:
            raise ValueError(f"Sweep key '{key}' not found in params.")


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
