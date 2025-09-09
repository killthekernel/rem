from __future__ import annotations

from typing import Optional

import typer

from rem.utils.logger import set_global_log_level

try:
    from rem import __version__
except Exception:
    __version__ = "0.0.0.dev0"

app = typer.Typer(
    help="REM: A framework for managing and running numerical experiments."
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback(invoke_without_command=True)  # type: ignore[misc]
def main(
    ctx: typer.Context,
    log_level: Optional[str] = typer.Option(
        None, "--log-level", help="Set global log level (DEBUG/INFO/WARNING/ERROR)"
    ),
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show REM version and exit",
        callback=_version_callback,
        is_eager=True,  # run before requiring a subcommand
    ),
) -> None:
    if log_level:
        set_global_log_level(log_level)  # will configure the cached logger


from rem.cli.local import run_local_cmd
from rem.cli.run import run_cmd

app.command("run")(run_cmd)
app.command("local")(run_local_cmd)


def run_cli() -> None:
    app()
