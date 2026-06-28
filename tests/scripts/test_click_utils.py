"""Unit tests for ``trilogy.scripts.click_utils`` covering argv hoisting,
the LazyGroup, and the dialect validation hint."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from trilogy.scripts.click_utils import (
    LazyGroup,
    _hoist_group_flags,
    derive_hoist_map,
    misplaced_group_value_hint,
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


def test_derive_hoist_map_from_real_group_params():
    """The hoist map is derived from the group's declared options so it can't
    drift: --format/--debug/--debug-file are all present with correct arity,
    and eager value-suppressed --version is excluded."""
    from trilogy.scripts.trilogy import cli

    ctx = click.Context(cli)
    hoist = derive_hoist_map(cli.get_params(ctx))
    assert hoist["--debug"] is False
    assert hoist["--debug-file"] is True
    assert hoist["--format"] is True
    assert "--version" not in hoist


def test_derive_hoist_map_classifies_flags_and_value_options():
    @click.command()
    @click.option("--flag", is_flag=True)
    @click.option("--counter", count=True)
    @click.option("--val")
    def cmd():
        pass

    hoist = derive_hoist_map(cmd.params)
    assert hoist == {"--flag": False, "--counter": False, "--val": True}


def test_hoist_group_flags_with_explicit_map_hoists_format():
    argv = ["run", "select 1;", "--format", "json"]
    hoist = {"--format": True}
    assert _hoist_group_flags(argv, SUBCOMMANDS, hoist) == [
        "--format",
        "json",
        "run",
        "select 1;",
    ]


def test_hoist_group_flags_does_not_hoist_after_double_dash():
    argv = ["run", "--", "--format", "json"]
    hoist = {"--format": True}
    assert _hoist_group_flags(argv, SUBCOMMANDS, hoist) == [
        "run",
        "--",
        "--format",
        "json",
    ]


def test_misplaced_group_value_hint_matches_format_choice():
    from trilogy.scripts.trilogy import cli

    parent = click.Context(cli)
    child = click.Context(cli.get_command(parent, "run"), parent=parent)
    hint = misplaced_group_value_hint("json", child, "run")
    assert hint is not None
    assert "--format" in hint


def test_misplaced_group_value_hint_none_for_unrelated_value():
    from trilogy.scripts.trilogy import cli

    parent = click.Context(cli)
    child = click.Context(cli.get_command(parent, "run"), parent=parent)
    assert misplaced_group_value_hint("not_a_choice", child, "run") is None


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
