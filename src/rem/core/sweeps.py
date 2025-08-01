import itertools
from typing import Any

from rem.constants import SWEEP_PAD, SWEEP_PREFIX


def merge_dicts(*dicts: dict[str, Any]) -> dict[str, Any]:
    """
    Merges multiple dictionaries into one. (Helper function for sweep expansion.)
    Raises ValueError if there are duplicate keys.
    """
    out = {}
    for d in dicts:
        for k, v in d.items():
            if k in out:
                raise ValueError(f"Duplicate key '{k}' in sweep merge")
            out[k] = v
    return out


def expand_sweep_node(node: Any) -> list[dict[str, Any]]:
    """
    Recursively expands a sweep node into a list of parameter combinations.
    Supports 'grid' and 'zip' constructs for nested sweeps.
    """
    if isinstance(node, dict):
        if "grid" in node:
            sub_nodes = node["grid"]
            if not isinstance(sub_nodes, list):
                raise ValueError("'grid' must map to a list of sub-nodes")
            expanded_subs = [expand_sweep_node(n) for n in sub_nodes]
            return [merge_dicts(*combo) for combo in itertools.product(*expanded_subs)]

        elif "zip" in node:
            zip_block = node["zip"]
            if not isinstance(zip_block, dict):
                raise ValueError("'zip' must map to a dict of equal-length lists")

            keys = list(zip_block.keys())
            values = list(zip_block.values())

            if not values or not all(isinstance(v, list) for v in values):
                raise ValueError("All zip values must be lists")

            length = len(values[0])
            if not all(len(v) == length for v in values):
                raise ValueError("All zip lists must be the same length")

            return [dict(zip(keys, zipped_vals)) for zipped_vals in zip(*values)]

        else:
            # Flat grid leaf
            keys = list(node.keys())
            values = list(node.values())
            if not all(isinstance(v, list) for v in values):
                raise ValueError("All values in flat grid leaf must be lists")
            return [dict(zip(keys, combo)) for combo in itertools.product(*values)]

    raise ValueError(f"Invalid sweep node: {node}")


def generate_sweep_element_ids(n: int) -> list[str]:
    return [f"{SWEEP_PREFIX}{i+1:0{SWEEP_PAD}d}" for i in range(n)]


def get_sweep_elements(cfg: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    if "sweep" not in cfg:
        return [(f"{SWEEP_PREFIX}{1:0{SWEEP_PAD}d}", {})]

    sweep_root = cfg["sweep"]
    sweep_space = expand_sweep_node(sweep_root)
    sweep_ids = generate_sweep_element_ids(len(sweep_space))
    return list(zip(sweep_ids, sweep_space))
