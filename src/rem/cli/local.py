from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import typer

from rem.core.runner import run_local
from rem.utils.logger import get_logger

logger = get_logger(__name__)


def _coerce(val: str) -> Any:
    if val.isdigit():
        return int(val)
    try:
        return float(val)
    except ValueError:
        if val.lower() in ("true", "false"):
            return val.lower() == "true"
        return val


def _parse_overrides(items: Optional[list[str]]) -> dict[str, Any]:
    if not items:
        return {}
    out: dict[str, Any] = {}
    for item in items:
        key, val = item.split("=", 1)
        out[key] = _coerce(val)
    return out


def run_local_cmd(
    cfg: Path = typer.Argument(
        ..., help="Path to the experiment configuration YAML file."
    ),
    override: Optional[list[str]] = typer.Option(
        None,
        "--override",
        "-o",
        help="Override configuration parameters (key=value), e.g. -o params.lr=0.01",
    ),
) -> None:
    """
    Run an experiment locally without staging, registry updates or scheduling.
    """
    overrides_dict = _parse_overrides(override)
    result = run_local(config=cfg, overrides=overrides_dict or None)
    typer.echo(result)
