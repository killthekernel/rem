from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import typer

from rem.core.runner import MainRunner, run_local
from rem.utils.logger import get_logger

logger = get_logger(__name__)


def run_cmd(
    cfg: Path = typer.Argument(
        ..., help="Path to the experiment configuration YAML file."
    ),
    reps: int = typer.Option(
        1, "--reps", "-r", help="Number of repetitions for the experiment."
    ),
    dryrun: bool = typer.Option(
        False,
        "--dryrun",
        help="Perform a dry run with staging, without executing the experiment.",
    ),
    test: bool = typer.Option(
        False, "--test", help="Run in test mode, storing in test result directory."
    ),
    group: Optional[str] = typer.Option(
        None, "--group", "-g", help="Resume from an existing experiment group."
    ),
) -> None:
    """
    Run an experiment locally with optional staging and scheduling.
    """
    runner = MainRunner(test=test, dryrun=dryrun)
    group_id = runner.start(config_path=cfg, reps_per_sweep=reps, group_id=group)
    typer.echo(f"Experiment group ID: {group_id}")
