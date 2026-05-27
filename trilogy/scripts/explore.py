"""Explore command - introspect the environment built from a .preql file.

The canonical schema-discovery tool. Prefer this over reading the .preql
source — the same content is here as a structured concept listing, smaller and
easier to scan. Token-efficient by default: groups concepts by namespace prefix
so a 378-concept fact like ``store_sales`` collapses to ~25 group lines.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Sequence

import click

from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment
from trilogy.parser import parse_text
from trilogy.scripts.display import print_error, print_info

_CATEGORIES = ("all", "concepts", "datasources", "imports", "groups")


def _load_environment(path: Path) -> Environment:
    env = Environment(working_path=str(path.parent))
    _, _ = parse_text(path.read_text(encoding="utf-8"), environment=env, root=path)
    return env


def _compact_datatype(dt: str) -> str:
    """``Trait<STRING, ['us_state']>`` → ``string::us_state`` to mirror the
    `value::trait` authoring syntax. Keeps the trait name (the semantic hint
    the agent cares about) and drops the redundant base-type wrapping. The
    inner type can itself be a generic (e.g. ``Trait<enum<'TN'>, ['us_state']>``)
    so we split on the trait-list delimiter ``, [`` rather than a flat regex."""
    if dt.startswith("Trait<") and dt.endswith("]>"):
        body = dt[len("Trait<") : -len("]>")]
        sep = ", ["
        idx = body.rfind(sep)
        if idx > 0:
            base = _compact_datatype(body[:idx])
            traits = ", ".join(
                t.strip().strip("'\"")
                for t in body[idx + len(sep) :].split(",")
                if t.strip()
            )
            return f"{base}::{traits}" if traits else base
    if dt.startswith("enum<") and len(dt) > 60:
        # Long enums: keep first 3 + count, e.g. enum<'A','B','C',…+7>
        body = dt[len("enum<") : -1]
        parts = [p.strip() for p in body.split(",")]
        if len(parts) > 4:
            head = ",".join(parts[:3])
            return f"enum<{head},…+{len(parts) - 3}>"
    return dt.lower() if dt.isupper() else dt


_LOCAL_PREFIX = "local."


def _display_address(address: str) -> str:
    """Strip the implicit ``local.`` namespace from a concept address for
    display. The local namespace is the file's own bare declarations; bare
    references work everywhere they're addressable (including from importing
    queries after ``import file as alias``, which rebinds them under the
    alias). Showing ``local.X`` invites the agent to copy that literal into a
    query where it no longer resolves."""
    if address.startswith(_LOCAL_PREFIX):
        return address[len(_LOCAL_PREFIX) :]
    return address


def _concept_row(address: str, concept: Concept) -> tuple[str, str, str, str]:
    purpose = concept.purpose.value
    derivation = concept.derivation.value
    datatype = _compact_datatype(str(concept.datatype))
    return _display_address(address), purpose, derivation, datatype


def _emit_table(
    title: str, headers: tuple[str, ...], rows: Sequence[tuple[str, ...]]
) -> None:
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


def _emit_groups(concept_items: list[tuple[str, Concept]]) -> None:
    """Compact namespace-grouped listing — one line per leaf attribute under
    each namespace prefix, e.g. ``customer.customer_address  →  city, state,
    zip, county, …``. Cuts the size of a typical fact's schema dump by ~5×
    versus the flat table. Concepts in the implicit ``local`` namespace are
    grouped under ``(this file)`` — they're referenced bare from the file's
    own queries, or under the importing query's alias."""
    by_ns: dict[str, list[tuple[str, Concept]]] = defaultdict(list)
    for addr, c in concept_items:
        display = _display_address(addr)
        ns, sep, leaf = display.rpartition(".")
        if not sep:
            # No remaining dot ⇒ this was a `local.X` concept; group under a
            # clear label so the agent can see at a glance which concepts
            # belong to the file itself (vs. its imports).
            by_ns["(this file)"].append((display, c))
        else:
            by_ns[ns].append((leaf, c))
    click.echo()
    print_info(
        f"Concept groups ({len(by_ns)} namespaces, {len(concept_items)} concepts)"
    )
    for ns in sorted(by_ns):
        items = by_ns[ns]
        # ks/ps/ms split so the agent can see grain-defining keys at a glance
        keys = [leaf for leaf, c in items if c.purpose.value == "key"]
        props = [leaf for leaf, c in items if c.purpose.value == "property"]
        metrics = [leaf for leaf, c in items if c.purpose.value == "metric"]
        others = [
            leaf
            for leaf, c in items
            if c.purpose.value not in ("key", "property", "metric")
        ]
        bits = []
        if keys:
            bits.append("keys: " + ", ".join(sorted(keys)))
        if props:
            bits.append("props: " + ", ".join(sorted(props)))
        if metrics:
            bits.append("metrics: " + ", ".join(sorted(metrics)))
        if others:
            bits.append("other: " + ", ".join(sorted(others)))
        click.echo(f"  {ns}")
        for bit in bits:
            click.echo(f"    {bit}")


@click.command("explore")
@click.argument(
    "path",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
)
@click.option(
    "--show",
    type=click.Choice(_CATEGORIES),
    default="groups",
    help=(
        "Which section to print (default: groups — concepts collapsed by "
        "namespace prefix). Use `concepts` for the full flat table, `all` for "
        "everything including datasources and imports."
    ),
)
@click.option(
    "--purpose",
    type=str,
    multiple=True,
    default=(),
    help=(
        "Filter concepts by purpose. Repeatable: "
        "`--purpose key --purpose property`. Allowed: key, property, metric, "
        "constant, rowset."
    ),
)
@click.option(
    "--grep",
    type=str,
    multiple=True,
    default=(),
    help=(
        "Case-insensitive substring filter over concept addresses. Repeatable: "
        "`--grep customer --grep date` matches concepts whose address contains "
        "any of the given needles."
    ),
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
    purpose: tuple[str, ...],
    grep: tuple[str, ...],
    include_hidden: bool,
    include_builtins: bool,
) -> None:
    """Parse PATH and list concepts, datasources, and imports from its environment.

    The canonical "what can I query from this file?" tool. Prefer this over
    reading the raw .preql source — same content, smaller, scannable. The
    default `--show groups` view collapses concepts by namespace so a 378-
    concept fact import becomes ~25 group lines.
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
        allowed = {p.lower() for p in purpose}
        concept_items = [(k, v) for k, v in concept_items if v.purpose.value in allowed]
    if grep:
        needles = [g.lower() for g in grep]
        concept_items = [
            (k, v) for k, v in concept_items if any(n in k.lower() for n in needles)
        ]

    if show in ("all", "groups"):
        _emit_groups(concept_items)

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
            grain = ",".join(sorted(ds.grain.components))
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
                import_rows.append((alias, str(stmt.path)))
        _emit_table(
            f"Imports ({len(import_rows)})",
            ("alias", "path"),
            import_rows,
        )
