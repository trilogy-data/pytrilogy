"""Explore command - introspect the environment built from a .preql file.

The canonical schema-discovery tool. Prefer this over reading the .preql
source — the same content is here as a structured concept listing, smaller and
easier to scan. Token-efficient by default: groups concepts by namespace prefix
so a 378-concept fact like ``store_sales`` collapses to ~25 group lines.
"""

from __future__ import annotations

import textwrap
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


def _concept_description(concept: Concept) -> str:
    """Pull a one-line description off ``concept.metadata`` if set. Whitespace
    is collapsed so multi-line authoring comments still display cleanly in
    the explore output."""
    meta = getattr(concept, "metadata", None)
    raw = getattr(meta, "description", None) if meta is not None else None
    if not raw:
        return ""
    return " ".join(raw.split())


def _concept_row(address: str, concept: Concept) -> tuple[str, str, str, str, str]:
    purpose = concept.purpose.value
    derivation = concept.derivation.value
    datatype = _compact_datatype(str(concept.datatype))
    description = _concept_description(concept)
    return _display_address(address), purpose, derivation, datatype, description


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


_DETAIL_INDENT = " " * 4
_DESC_WIDTH = 100  # Wrap column for descriptions; chosen for comfortable
#                  # human reading. LLMs read by token so the wrap width
#                  # itself doesn't matter to them — what matters is that
#                  # continuation lines preserve the indent so the agent
#                  # can't mis-attribute a wrapped fragment to the next
#                  # concept.


# Map Trilogy concept Purpose values to the short, all-caps tag we render in
# the flat listing's leading column. Tags double as a grep target — `^KEY `
# or `^METRIC ` is enough to slice the output by purpose without needing a
# flag. Unknown purposes fall back to ``purpose.upper()`` — keep these tags
# at or under 6 chars so the tag column never overflows.
_PURPOSE_TAGS = {
    "key": "KEY",
    "property": "PROP",
    "unique_property": "UPROP",
    "metric": "METRIC",
    "constant": "CONST",
    "rowset": "ROWSET",
    "auto": "AUTO",
    "parameter": "PARAM",
    "unknown": "UNK",
}


def _grain_display(addr: str) -> str:
    """Render a key address inside a property's ``@grain`` suffix. Strip the
    implicit ``local.`` prefix so a property bound to a local key reads as
    ``@customer_id`` rather than ``@local.customer_id``; cross-namespace
    addresses keep their full dotted path so the agent can find the key."""
    return addr[len(_LOCAL_PREFIX) :] if addr.startswith(_LOCAL_PREFIX) else addr


def _grain_suffix(key_addrs: tuple[str, ...]) -> str:
    """``@key`` for a single-key grain, ``@<a, b>`` for a compound grain,
    empty string when the concept has no bound grain (e.g. metrics).
    Rendered tight against the role tag so the relationship stays visible:
    ``PROP @customer_id`` rather than the trailing form."""
    if not key_addrs:
        return ""
    if len(key_addrs) == 1:
        return f" @{_grain_display(key_addrs[0])}"
    inner = ", ".join(_grain_display(a) for a in key_addrs)
    return f" @<{inner}>"


def _emit_flat_line(
    purpose: str,
    address: str,
    concept: Concept,
    key_addrs: tuple[str, ...] = (),
) -> None:
    """Render one concept as three logical pieces:

      1. ``address : type``                  ← the concept itself, flush left.
      2. ``    TAG[ @grain]``                ← the role + grain, indented.
      3. ``    description (wrapped)``       ← only when set.

    Putting the role on its own indented line — with the grain tight against
    it — keeps the concept-as-headline reading clean while still making the
    PROP-grain relationship visually obvious. The ``@grain`` suffix sits
    right next to the role tag so the agent never has to scan to the end of
    a type expression to find the binding key."""
    tag = _PURPOSE_TAGS.get(purpose, purpose.upper())
    datatype = _compact_datatype(str(concept.datatype))
    grain = _grain_suffix(key_addrs)
    click.echo(f"{address} : {datatype}")
    click.echo(f"{_DETAIL_INDENT}{tag}{grain}")
    desc = _concept_description(concept)
    if desc:
        click.echo(
            textwrap.fill(
                desc,
                width=_DESC_WIDTH,
                initial_indent=_DETAIL_INDENT,
                subsequent_indent=_DETAIL_INDENT,
            )
        )


def _emit_groups(concept_items: list[tuple[str, Concept]]) -> None:
    """Flat per-concept listing — one (or two, when described) lines per
    concept. Ordering preserves the visual grouping of the prior nested
    layout *without* indentation: keys come first, each immediately followed
    by its single-key properties (the common dim-attr case); compound-grain
    properties group together by grain; metrics and others (constants /
    rowsets / unbound derivations) follow.

    Each line is ``TAG    address : type  [@grain]`` with the optional
    description on the next line at the address column. The agent reads the
    grain relationship off the ``@grain`` suffix without traversing indented
    blocks — which scales better than nesting on long fact files (web_sales
    hits 46 namespaces / 456 concepts; deep indent loses the parent-namespace
    context past one screenful)."""
    by_ns: dict[str, list[tuple[str, Concept]]] = defaultdict(list)
    for addr, c in concept_items:
        display = _display_address(addr)
        ns, sep, leaf = display.rpartition(".")
        if not sep:
            by_ns["(root)"].append((display, c))
        else:
            by_ns[ns].append((leaf, c))
    click.echo()
    print_info(
        f"Available Concepts ({len(by_ns)} namespaces, {len(concept_items)} concepts)"
    )
    for ns in sorted(by_ns):
        # Blank line + `# namespace` header before each section. The blank
        # line is what the eye latches onto when scrolling through a long
        # multi-namespace dump (web_sales has 46 sections); the `#` prefix
        # makes the header obviously not a concept line.
        click.echo()
        click.echo(f"# {ns}")
        _emit_namespace(ns, by_ns[ns])


def _emit_namespace(ns: str, items: list[tuple[str, Concept]]) -> None:
    """Emit one namespace's concepts in the flat form, preserving the
    keys-then-clustered-props-then-compound-then-metrics ordering."""
    keys = [(leaf, c) for leaf, c in items if c.purpose.value == "key"]
    props = [(leaf, c) for leaf, c in items if c.purpose.value == "property"]
    metrics = [(leaf, c) for leaf, c in items if c.purpose.value == "metric"]
    others = [
        (leaf, c)
        for leaf, c in items
        if c.purpose.value not in ("key", "property", "metric")
    ]
    local_key_addrs = {c.address for _, c in keys}
    nested: dict[str, list[tuple[str, Concept]]] = defaultdict(list)
    compound: dict[tuple[str, ...], list[tuple[str, Concept]]] = defaultdict(list)
    for leaf, concept in props:
        key_addrs = tuple(sorted(concept.keys or []))
        if len(key_addrs) == 1 and key_addrs[0] in local_key_addrs:
            nested[key_addrs[0]].append((leaf, concept))
        else:
            compound[key_addrs].append((leaf, concept))

    # Inside (root), properties are addressed by their bare leaf (`name`); in
    # an imported namespace, the agent writes the dotted form
    # (`customer.name`). Pre-format so call sites stay leaf-agnostic.
    def fmt(leaf: str) -> str:
        return leaf if ns == "(root)" else f"{ns}.{leaf}"

    # Keys, each immediately followed by props bound exactly to that key.
    for key_leaf, key_concept in sorted(keys, key=lambda lc: lc[0]):
        _emit_flat_line("key", fmt(key_leaf), key_concept)
        for leaf, concept in sorted(
            nested.get(key_concept.address, []), key=lambda lc: lc[0]
        ):
            _emit_flat_line("property", fmt(leaf), concept, (key_concept.address,))
    for grain_tuple, plist in sorted(compound.items()):
        for leaf, concept in sorted(plist, key=lambda lc: lc[0]):
            _emit_flat_line("property", fmt(leaf), concept, grain_tuple)
    for leaf, concept in sorted(metrics, key=lambda lc: lc[0]):
        _emit_flat_line("metric", fmt(leaf), concept)
    for leaf, concept in sorted(others, key=lambda lc: lc[0]):
        _emit_flat_line(concept.purpose.value, fmt(leaf), concept)


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
            ("address", "purpose", "derivation", "datatype", "description"),
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
