"""``trilogy source`` -- inspect and preview script datasources.

The contract a script implements (see ``trilogy/io``) is a command-line one, so
these commands work against any executable that honors it, not just python. That
is the point: ``describe`` is how trilogy learns a source's schema without the
author transcribing it into a datasource block by hand.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import click

from trilogy.dialect.python_source import (
    PythonDatasourceError,
    build_uv_command,
    parse_script_error,
)


def source_command(script: str, args: list[str]) -> list[str]:
    """How to invoke ``script``: uv for python, otherwise run it directly."""
    if Path(script).suffix == ".py":
        return [*build_uv_command(script), *args]
    return [script, *args]


def invoke(
    script: str, args: list[str], capture: bool = True
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        source_command(script, args), capture_output=capture, check=False
    )
    if result.returncode != 0:
        stderr = (result.stderr or b"").decode("utf-8", errors="replace")
        raise PythonDatasourceError(script, result.returncode, stderr)
    return result


def describe_payload(script: str) -> dict:
    """Ask ``script`` what it produces.

    A script that predates the contract ignores ``--describe`` and writes its
    Arrow stream instead, so stdout here is binary as often as it is JSON.
    """
    result = invoke(script, ["--describe"])
    try:
        return json.loads(result.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise click.ClickException(
            f"{script} did not answer --describe with JSON -- it emitted "
            f"{len(result.stdout)} bytes of something else. Scripts that write "
            "Arrow to stdout directly should move to trilogy.io.run() to "
            "support describe."
        ) from e


@click.group()
def source() -> None:
    """Inspect and preview script datasources."""


@source.command()
@click.argument("script", type=click.Path(exists=True, dir_okay=False))
@click.option("--json", "as_json", is_flag=True, help="Print the raw describe payload.")
def describe(script: str, as_json: bool) -> None:
    """Show a script's schema, pushdown support, and datasource block."""
    payload = describe_payload(script)
    if as_json:
        click.echo(json.dumps(payload, indent=2))
        return

    click.echo(f"contract v{payload['contract']}")
    click.echo("\ncolumns:")
    width = max((len(f["name"]) for f in payload["schema"]), default=0)
    for field in payload["schema"]:
        null = "" if field["nullable"] else "  not null"
        click.echo(f"  {field['name']:<{width}}  {field['type']}{null}")
    click.echo(
        "\npushdown: "
        + (
            ", ".join(payload["pushdown"])
            or "none (all filtering applied to the output stream)"
        )
    )
    click.echo("\n" + payload["datasource"])


@source.command()
@click.argument("script", type=click.Path(exists=True, dir_okay=False))
@click.option("--limit", type=int, default=10, show_default=True)
@click.option("--columns", default=None, help="Comma-separated projection.")
@click.option("--filter", "filters", multiple=True, help="Row predicate, repeatable.")
@click.option("--since", default=None, help="Watermark low bound.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["csv", "json"]),
    default="csv",
    show_default=True,
)
def preview(
    script: str,
    limit: int,
    columns: str | None,
    filters: tuple[str, ...],
    since: str | None,
    fmt: str,
) -> None:
    """Run a script through the IO contract and print the rows."""
    args = ["--limit", str(limit), "--format", fmt]
    if columns:
        args += ["--columns", columns]
    for predicate in filters:
        args += ["--filter", predicate]
    if since:
        args += ["--since", since]
    invoke(script, args, capture=False)


@source.command(name="check")
@click.argument("script", type=click.Path(exists=True, dir_okay=False))
def check_(script: str) -> None:
    """Verify a script implements the contract, without reading its rows."""
    try:
        payload = describe_payload(script)
    except PythonDatasourceError as e:
        reported = parse_script_error(e.stderr)
        detail = f"{reported['type']}: {reported['message']}" if reported else e.stderr
        raise click.ClickException(f"{script} failed --describe\n{detail}") from e
    click.echo(
        f"ok: {script} implements contract v{payload['contract']} "
        f"({len(payload['schema'])} columns)"
    )


if __name__ == "__main__":
    sys.exit(source())
