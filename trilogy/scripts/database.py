"""Database command for Trilogy CLI - inspect the configured warehouse."""

from __future__ import annotations

from pathlib import Path as PathlibPath

import click
from click import argument, option, pass_context

from trilogy.executor import Executor
from trilogy.scripts.common import (
    create_executor,
    get_runtime_config,
    handle_execution_exception,
)
from trilogy.scripts.display import print_error, print_info


def _connect(ctx: click.Context) -> Executor:
    """Build an Executor from the trilogy.toml engine config in the cwd."""
    config = get_runtime_config(PathlibPath.cwd())
    if not config.engine_dialect:
        print_error("No engine configured. Set [engine].dialect in trilogy.toml.")
        raise click.exceptions.Exit(1)
    try:
        return create_executor(
            param=(),
            directory=PathlibPath.cwd(),
            conn_args=(),
            edialect=config.engine_dialect,
            debug=ctx.obj["DEBUG"],
            config=config,
            debug_file=ctx.obj.get("DEBUG_FILE"),
        )
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])
        raise  # unreachable: handle_execution_exception always raises


@click.group()
def database() -> None:
    """Inspect the database configured in trilogy.toml.

    `database list` enumerates tables; `database describe <table>` shows a
    table's columns — the introspection needed before running `ingest` to
    build a Trilogy model.
    """


@database.command("list")
@option("--schema", "-s", type=str, default=None, help="Restrict to one schema.")
@pass_context
def list_cmd(ctx: click.Context, schema: str | None) -> None:
    """List the tables and views in the configured database."""
    executor = _connect(ctx)
    try:
        tables = executor.generator.list_tables(executor, schema)
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])
    if not tables:
        print_info("No tables found.")
        return
    for name, table_type in tables:
        click.echo(f"{name}\t{table_type}")


@database.command("describe")
@argument("table", type=str)
@option("--schema", "-s", type=str, default=None, help="Schema the table is in.")
@pass_context
def describe_cmd(ctx: click.Context, table: str, schema: str | None) -> None:
    """Show the columns and types of TABLE."""
    executor = _connect(ctx)
    try:
        columns = executor.generator.get_table_schema(executor, table, schema)
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])
    if not columns:
        print_error(f"Table '{table}' not found, or it has no columns.")
        raise click.exceptions.Exit(1)
    for col in columns:
        name = str(col.column_name)
        dtype = str(col.data_type)
        nullable = str(col.is_nullable) if col.is_nullable else ""
        click.echo(f"{name}\t{dtype}\t{nullable}")
