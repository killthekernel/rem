from typing import Optional

import typer

from rem import __version__
from rem.cli import run  # , submit, status, ls, patch, local, events
from rem.utils.logger import get_logger

app = typer.Typer()

app.add_typer(run.app, name="run", help="Run an experiment locally.")


@app.callback()  # type: ignore[misc]
def main(
    context: typer.Context,
    log_level: Optional[str] = typer.Option(
        None, "--log-level", help="Set the logging level (DEBUG, INFO, WARNING, ERROR)."
    ),
    version: bool = typer.Option(
        False, "--version", help="Show the version of the REM package and exit."
    ),
) -> None:
    """
    REM: A framework for managing and running numerical experiments.
    """
    if version:
        typer.echo(f"REM version: {__version__}")
        raise typer.Exit()

    if log_level:
        logger = get_logger("rem", level=log_level)
        logger.info(f"Log level set to {log_level}")


def run_cli() -> None:
    app()
