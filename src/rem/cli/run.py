from pathlib import Path
from typing import Optional

import typer

from rem.core.runner import MainRunner, run_local
from rem.utils.logger import get_logger

logger = get_logger(__name__)
app = typer.Typer(
    help="Run an experiment locally or with staging and scheduled execution."
)
