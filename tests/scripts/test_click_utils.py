import click
import pytest
from click.testing import CliRunner

from trilogy.scripts.click_utils import (
    LazyGroup,
    _hoist_group_flags,
    validate_dialect,
)


def test_hoist_group_flags_empty():
    assert _hoist_group_flags([], {"run"}) == []


def test_hoist_group_flags_no_subcommand_passthrough():
    argv = ["--debug", "extra"]
    assert _hoist_group_flags(argv, {"run"}) == argv


def test_hoist_group_flags_moves_bool_flag_to_front():
    argv = ["run", "query.preql", "--debug"]
    assert _hoist_group_flags(argv, {"run"}) == ["--debug", "run", "query.preql"]


def test_hoist_group_flags_moves_value_flag_with_arg():
    argv = ["run", "query.preql", "--debug-file", "out.log"]
    assert _hoist_group_flags(argv, {"run"}) == [
        "--debug-file",
        "out.log",
        "run",
        "query.preql",
    ]


def test_hoist_group_flags_value_flag_equals_form():
    argv = ["run", "query.preql", "--debug-file=out.log"]
    assert _hoist_group_flags(argv, {"run"}) == [
        "--debug-file=out.log",
        "run",
        "query.preql",
    ]


def test_hoist_group_flags_value_flag_missing_value_still_moves():
    argv = ["run", "query.preql", "--debug-file"]
    out = _hoist_group_flags(argv, {"run"})
    assert "--debug-file" in out
    assert out.index("--debug-file") < out.index("run")


def test_hoist_group_flags_non_hoisted_flag_stays_after():
    argv = ["run", "query.preql", "--something-else", "val"]
    assert _hoist_group_flags(argv, {"run"}) == argv


def test_lazy_group_lazy_load(monkeypatch):
    @click.command()
    def hello():
        click.echo("hi")

    import types

    fake_mod = types.SimpleNamespace(hello=hello)

    def fake_import(name):
        if name == "fake.module":
            return fake_mod
        raise ImportError(name)

    monkeypatch.setattr("importlib.import_module", fake_import)
    group = LazyGroup(
        name="root",
        lazy_subcommands={"hello": ("fake.module", "hello", None)},
    )
    ctx = click.Context(group)
    assert "hello" in group.list_commands(ctx)
    cmd = group.get_command(ctx, "hello")
    assert cmd is not None
    runner = CliRunner()
    result = runner.invoke(group, ["hello"])
    assert result.exit_code == 0
    assert "hi" in result.output


def test_lazy_group_wraps_plain_function(monkeypatch):
    def plain():
        click.echo("plain-out")

    import types

    fake_mod = types.SimpleNamespace(plain=plain)
    monkeypatch.setattr("importlib.import_module", lambda n: fake_mod)
    group = LazyGroup(
        name="root",
        lazy_subcommands={"plain": ("fake.module", "plain", None)},
    )
    runner = CliRunner()
    result = runner.invoke(group, ["plain"])
    assert result.exit_code == 0
    assert "plain-out" in result.output


def test_lazy_group_unknown_subcommand_falls_through():
    group = LazyGroup(name="root", lazy_subcommands={})
    ctx = click.Context(group)
    assert group.get_command(ctx, "nope") is None


def test_validate_dialect_none_is_ok():
    validate_dialect(None, "run")
    validate_dialect("", "run")


def test_validate_dialect_rejects_flag_in_dialect_slot():
    with pytest.raises(click.UsageError, match="not a valid dialect"):
        validate_dialect("--debug", "run")


def test_validate_dialect_rejects_path_like():
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect("query.preql", "run")
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect("path/to/file", "run")
    with pytest.raises(click.UsageError, match="looks like a file path"):
        validate_dialect(".", "run")


def test_validate_dialect_accepts_legit():
    validate_dialect("duckdb", "run")
    validate_dialect("postgres", "run")
