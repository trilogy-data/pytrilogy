"""Explore command - introspect the environment built from a .preql file.

Lists concepts, datasources, and imports so agents and humans can answer
"what can I query from this file?" without reading the raw source.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import click

from trilogy.core.models.environment import Environment
from trilogy.parser import parse_text
from trilogy.scripts.display import print_error, print_info

_CATEGORIES = ("all", "concepts", "datasources", "imports")


def _load_environment(path: Path) -> Environment:
    env = Environment(working_path=str(path.parent))
    _, _ = parse_text(path.read_text(encoding="utf-8"), environment=env, root=path)
    return env


def _concept_row(address: str, concept) -> tuple[str, str, str, str]:
    purpose = getattr(concept.purpose, "value", str(concept.purpose))
    derivation = getattr(concept.derivation, "value", str(concept.derivation))
    datatype = str(getattr(concept, "datatype", "") or "")
    return address, purpose, derivation, datatype


def _emit_table(title: str, headers: tuple[str, ...], rows: Sequence[tuple[str, ...]]):
    click.echo()
    print_info(title)
    if not rows:
        click.echo("  (none)")
        return
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    header_line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    click.echo(header_line)
    click.echo("  ".join("-" * w for w in widths))
    for row in rows:
        click.echo("  ".join(str(c).ljust(w) for c, w in zip(row, widths)))


@click.command("explore")
@click.argument(
    "path",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--show",
    type=click.Choice(_CATEGORIES),
    default="all",
    help="Which section to print (default: all).",
)
@click.option(
    "--purpose",
    type=str,
    default=None,
    help="Filter concepts by purpose (key, property, metric, constant, rowset).",
)
@click.option(
    "--grep",
    type=str,
    default=None,
    help="Case-insensitive substring filter over concept addresses.",
)
@click.option(
    "--include-hidden",
    is_flag=True,
    default=False,
    help="Include concepts that are normally hidden from public view.",
)
@click.option(
    "--include-builtins",
    is_flag=True,
    default=False,
    help="Include internal/builtin concepts (hidden by default).",
)
def explore(
    path: Path,
    show: str,
    purpose: str | None,
    grep: str | None,
    include_hidden: bool,
    include_builtins: bool,
) -> None:
    """Parse PATH and list concepts, datasources, and imports from its environment.

    Use this to discover what's available from a .preql file before writing
    a query. It's equivalent to `trilogy run --import <path> "show concepts;"`
    without requiring a dialect or connection.
    """
    try:
        env = _load_environment(path)
    except Exception as exc:
        print_error(f"Failed to parse {path}: {exc}")
        raise click.exceptions.Exit(1) from exc

    concept_items = (
        list(env.concepts.all_items()) if include_hidden else list(env.concepts.items())
    )
    if not include_builtins:
        concept_items = [
            (k, v)
            for k, v in concept_items
            if not k.startswith("__") and not k.startswith("local._env_")
        ]
    if purpose:
        concept_items = [
            (k, v)
            for k, v in concept_items
            if getattr(v.purpose, "value", str(v.purpose)) == purpose
        ]
    if grep:
        needle = grep.lower()
        concept_items = [(k, v) for k, v in concept_items if needle in k.lower()]

    if show in ("all", "concepts"):
        rows = [_concept_row(k, v) for k, v in sorted(concept_items)]
        _emit_table(
            f"Concepts ({len(rows)})",
            ("address", "purpose", "derivation", "datatype"),
            rows,
        )

    if show in ("all", "datasources"):
        ds_rows = []
        for name, ds in sorted(env.datasources.items()):
            grain = ",".join(sorted(getattr(ds.grain, "components", []) or []))
            ds_rows.append((name, grain or "(no grain)"))
        _emit_table(
            f"Datasources ({len(ds_rows)})",
            ("address", "grain"),
            ds_rows,
        )

    if show in ("all", "imports"):
        import_rows = []
        for alias, stmts in sorted(env.imports.items()):
            for stmt in stmts:
                import_rows.append((alias, str(getattr(stmt, "path", ""))))
        _emit_table(
            f"Imports ({len(import_rows)})",
            ("alias", "path"),
            import_rows,
        )
