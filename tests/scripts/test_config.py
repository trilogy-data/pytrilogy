import importlib.util
from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

RICH_MODES = [False]

if importlib.util.find_spec("rich") is not None:
    RICH_MODES.append(True)
else:
    RICH_MODES.append(False)


def test_config_bootstrap():
    path = Path(__file__).parent / "config_directory"
    runner = CliRunner()

    for cmd in ["run", "integration"]:
        result = runner.invoke(cli, [cmd, str(path), "duckdb"])
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    for cmd in ["unit"]:
        result = runner.invoke(cli, [cmd, str(path)])
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    # test config
    for cmd in ["run", "integration"]:
        result = runner.invoke(
            cli,
            [
                cmd,
                str(path),
                "duckdb",
                "--config",
                f"{str(path / "trilogy_dev.toml")}",
            ],
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    for cmd in ["unit"]:
        result = runner.invoke(
            cli, [cmd, str(path), "--config", str(path / "trilogy_dev.toml")]
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0


def test_config_bootstrap_dialect():
    path = Path(__file__).parent / "config_directory"
    runner = CliRunner()

    for cmd in ["run", "integration"]:
        result = runner.invoke(
            cli,
            [
                cmd,
                str(path),
            ],
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    # test config
    for cmd in ["run", "integration"]:

        result = runner.invoke(
            cli, [cmd, str(path), "--config", str(path / "trilogy_dev.toml")]
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0
