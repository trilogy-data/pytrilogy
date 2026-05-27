"""Unit tests for ``trilogy.scripts.click_utils`` covering argv hoisting,
the LazyGroup, and the dialect validation hint."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from trilogy.scripts.click_utils import (
    LazyGroup,
    _hoist_group_flags,
    validate_dialect,
)

SUBCOMMANDS = {"run", "refresh"}


def test_hoist_group_flags_empty_argv():
    assert _hoist_group_flags([], SUBCOMMANDS) == []


def test_hoist_group_flags_no_subcommand_returns_unchanged():
    argv = ["--help"]
    assert _hoist_group_flags(argv, SUBCOMMANDS) == ["--help"]


def test_hoist_group_flags_boolean_flag_after_subcommand():
    argv = ["run", "select 1;", "--debug"]
    assert _hoist_group_flags(argv, SUBCOMMANDS) == ["--debug", "run", "select 1;"]


def test_hoist_group_flags_value_flag_with_equals():
    argv = ["run", "select 1;", "--debug-file=log.txt"]
    assert _hoist_group_flags(argv, SUBCOMMANDS) == [
        "--debug-file=log.txt",
        "run",
        "select 1;",
    ]


def test_hoist_group_flags_value_flag_with_space():
    argv = ["run", "select 1;", "--debug-file", "log.txt"]
    assert _hoist_group_flags(argv, SUBCOMMANDS) == [
        "--debug-file",
        "log.txt",
        "run",
        "select 1;",
    ]


def test_hoist_group_flags_value_flag_missing_value_still_hoisted():
    # Don't crash if --debug-file is the last token with no value.
    argv = ["run", "select 1;", "--debug-file"]
    assert _hoist_group_flags(argv, SUBCOMMANDS) == [
        "--debug-file",
        "run",
        "select 1;",
    ]


def test_hoist_group_flags_leaves_unrelated_flags_in_tail():
    argv = ["run", "select 1;", "--other", "duckdb"]
    assert _hoist_group_flags(argv, SUBCOMMANDS) == [
        "run",
        "select 1;",
        "--other",
        "duckdb",
    ]


def test_lazy_group_lists_lazy_commands_alphabetically():
    @click.group(cls=LazyGroup, lazy_subcommands={"foo": ("os", "getcwd", None)})
    def root():
        pass

    @root.command()
    def bar():
        pass

    runner = CliRunner()
    ctx = click.Context(root)
    cmds = root.list_commands(ctx)
    assert "bar" in cmds
    assert "foo" in cmds
    runner.invoke(root, ["--help"])


def test_lazy_group_falls_back_to_eager_commands():
    """A non-lazy registered command must still resolve via super().get_command."""

    @click.group(cls=LazyGroup, lazy_subcommands={})
    def root():
        pass

    @root.command()
    def hello():
        click.echo("hi")

    runner = CliRunner()
    result = runner.invoke(root, ["hello"])
    assert result.exit_code == 0
    assert "hi" in result.output


def test_lazy_group_renames_premade_click_command_and_caches():
    @click.group(
        cls=LazyGroup,
        lazy_subcommands={
            "aliased": ("tests.scripts._click_utils_fixture", "_premade_cmd", None)
        },
    )
    def root():
        pass

    ctx = click.Context(root)
    cmd_first = root.get_command(ctx, "aliased")
    cmd_second = root.get_command(ctx, "aliased")
    assert isinstance(cmd_first, click.Command)
    assert cmd_first.name == "aliased"
    assert cmd_first is cmd_second


def test_lazy_group_wraps_plain_function_in_click_command():
    @click.group(
        cls=LazyGroup,
        lazy_subcommands={
            "noop": ("trilogy.scripts.click_utils", "IGNORE_UNKNOWN", None)
        },
    )
    def root():
        pass

    @click.group(
        cls=LazyGroup,
        lazy_subcommands={
            "say-hi": ("tests.scripts._click_utils_fixture", "_say_hi", None)
        },
    )
    def root2():
        pass

    ctx = click.Context(root2)
    cmd = root2.get_command(ctx, "say-hi")
    assert isinstance(cmd, click.Command)
    runner = CliRunner()
    res = runner.invoke(root2, ["say-hi"])
    assert res.exit_code == 0
    assert "hi" in res.output


def test_validate_dialect_accepts_none_and_real_dialect():
    assert validate_dialect(None, "run") is None
    assert validate_dialect("duckdb", "run") is None


def test_validate_dialect_rejects_flag_in_dialect_slot():
    with pytest.raises(click.UsageError, match="not a valid dialect"):
        validate_dialect("--debug", "run")


def test_validate_dialect_rejects_file_path_lookalikes():
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect("query.preql", "run")
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect("dir/sub", "run")
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect("dir\\sub", "run")
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect(".", "run")
