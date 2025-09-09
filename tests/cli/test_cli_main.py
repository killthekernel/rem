import importlib
import types

import pytest
from typer.testing import CliRunner


@pytest.fixture  # type: ignore[misc]
def runner() -> CliRunner:
    return CliRunner()


def test_help_shows_commands(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod = importlib.import_module("rem.cli.main")
    importlib.reload(mod)  # type: ignore[attr-defined]
    res = runner.invoke(mod.app, ["--help"])
    assert res.exit_code == 0

    # Check some subcommands
    assert "run" in res.stdout
    assert "local" in res.stdout


def test_version_option(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    rem = importlib.import_module("rem")
    monkeypatch.setattr(rem, "__version__", "1.2.3-test", raising=False)
    mod = importlib.import_module("rem.cli.main")
    importlib.reload(mod)  # type: ignore[attr-defined]

    res = runner.invoke(mod.app, ["--version"])
    assert res.exit_code == 0
    assert "1.2.3-test" in res.stdout


def test_log_level_option(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    import logging

    from rem.utils import logger as logger_module

    monkeypatch.setattr(logger_module, "_LOGGER", None, raising=False)

    # Drop all previous existing "rem" loggers to reset state
    for name in list(logging.Logger.manager.loggerDict):
        if name == "rem" or name.startswith("rem."):
            logging.Logger.manager.loggerDict.pop(name, None)

    mod = importlib.import_module("rem.cli.main")
    importlib.reload(mod)  # type: ignore[attr-defined]

    res = runner.invoke(mod.app, ["--log-level", "DEBUG"])
    assert res.exit_code == 0

    # Check that the global logger is set to DEBUG
    base = logger_module.get_logger("rem")
    assert base.getEffectiveLevel() == logging.DEBUG


def test_run_cli_entrypoint(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = importlib.import_module("rem.cli.main")
    importlib.reload(mod)  # type: ignore[attr-defined]

    res = runner.invoke(mod.app, ["--help"])
    assert res.exit_code == 0
