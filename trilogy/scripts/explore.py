"""Explore command - introspect the environment built from a .preql file.

The canonical schema-discovery tool. Prefer this over reading the .preql
source — the same content is here as a structured concept listing, smaller and
easier to scan. Token-efficient by default: groups concepts by namespace prefix
so a 378-concept fact like ``store_sales`` collapses to ~25 group lines.
"""

from __future__ import annotations

import re
import textwrap
from collections import defaultdict
from pathlib import Path
from typing import Sequence

import click

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment
from trilogy.parser import parse_text
from trilogy.scripts.display import emit_event, is_json_mode, print_error, print_info

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


def _emit_groups(
    concept_items: list[tuple[str, Concept]],
    expand_imports: bool = False,
    import_descriptions: dict[str, str] | None = None,
) -> None:
    """Two-tier listing: this file's own concepts (``namespace == 'local'``)
    in full per-concept detail; imported namespaces collapsed to a name-only
    listing with drill-down hints. The collapse is the default because a
    fact file like ``web_sales`` imports 8+ dimensions, each contributing
    30–100 concepts — pages of inherited noise that drown out the local
    declarations the agent is actually inspecting.

    Pass ``expand_imports=True`` (CLI: ``--expand-imports``) to render
    everything in full; ``--regex`` also bypasses the collapse since the
    user is already filtering deliberately.

    Each detailed line is ``TAG    address : type  [@grain]`` with optional
    description on the next line at the address column. The agent reads the
    grain relationship off the ``@grain`` suffix without traversing indented
    blocks — scales better than nesting on long fact files (web_sales hits
    46 namespaces / 456 concepts; deep indent loses parent-namespace context
    past one screenful)."""
    if expand_imports:
        local_items = concept_items
        imported_items: list[tuple[str, Concept]] = []
    else:
        local_items = [
            (addr, c) for addr, c in concept_items if c.namespace == DEFAULT_NAMESPACE
        ]
        imported_items = [
            (addr, c) for addr, c in concept_items if c.namespace != DEFAULT_NAMESPACE
        ]
    _emit_local_groups(local_items, import_descriptions)
    if imported_items:
        _emit_imported_summary(imported_items, import_descriptions)


def _emit_local_groups(
    concept_items: list[tuple[str, Concept]],
    import_descriptions: dict[str, str] | None = None,
) -> None:
    """Render the full per-concept layout for one set of concepts (typically
    the local ones). Pulled out so the import-collapsing path can reuse it
    on the local slice without duplicating the namespace bucketing."""
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
        desc = (import_descriptions or {}).get(ns)
        click.echo(f"# {ns}  — {desc.strip()}" if desc else f"# {ns}")
        _emit_namespace(ns, by_ns[ns])


_IMPORT_LIST_WIDTH = 100


def _emit_imported_summary(
    imported_items: list[tuple[str, Concept]],
    import_descriptions: dict[str, str] | None = None,
) -> None:
    """Compact rendering of imported namespaces: one block per namespace
    with the concept count and a comma-separated list of full addresses,
    wrapped at ``_IMPORT_LIST_WIDTH``. No purpose / datatype / description
    — the agent drills in with ``--regex`` if it wants those.

    Grouped by ``concept.namespace`` rather than by address prefix so the
    bucket matches what the agent will type to reach the concepts (which is
    the namespace, not a regex over the address). For a single-import chain
    like ``import raw.customer as customer``, that's ``customer``; deeper
    chains keep their full dotted namespace (``store_sales.customer``)."""
    by_ns: dict[str, list[str]] = defaultdict(list)
    for addr, c in imported_items:
        by_ns[c.namespace].append(_display_address(addr))
    click.echo()
    print_info(
        f"Imported namespaces ({len(by_ns)} namespaces, "
        f"{len(imported_items)} concepts — collapsed)"
    )
    click.echo(
        "# Pass --expand-imports for full detail, "
        "or --regex '<namespace>\\.' to drill into one."
    )
    for ns in sorted(by_ns):
        # Strip the shared `<ns>.` prefix from each address — the header
        # already announces the prefix, so repeating it on every leaf wastes
        # tokens. Agent reaches a concept by writing `<ns>.<leaf>`; that
        # reconstruction is mechanical from the header + the bare leaf.
        prefix = f"{ns}."
        leaves = sorted(
            leaf
            for addr in by_ns[ns]
            for leaf in [addr[len(prefix) :] if addr.startswith(prefix) else addr]
            if not leaf.startswith("_")  # hide internal/intermediate concepts
        )
        if not leaves:
            continue
        click.echo()
        header = f"# {ns}.* ({len(leaves)} concepts) — reach as {ns}.<leaf>"
        desc = (import_descriptions or {}).get(ns)
        if desc:
            header += f"  — {desc.strip()}"
        click.echo(header)
        click.echo(
            textwrap.fill(
                ", ".join(leaves),
                width=_IMPORT_LIST_WIDTH,
                initial_indent="  ",
                subsequent_indent="  ",
                break_long_words=False,
                break_on_hyphens=False,
            )
        )


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


# Within a namespace, render keys first, then properties, then metrics, then
# everything else — keys anchor the grain the rest hang off, so leading with
# them reads the way the model is authored.
_PURPOSE_ORDER = {"key": 0, "property": 1, "metric": 2}


def _keyset_label(key_addrs: tuple[str, ...]) -> str:
    """Human/agent-facing label for a grain or aggregation key-set: the bare
    key for a single key, ``<a, b>`` for a compound one (the ``local.`` prefix
    stripped throughout so it reads the way it's authored)."""
    if len(key_addrs) == 1:
        return _grain_display(key_addrs[0])
    return "<" + ", ".join(_grain_display(a) for a in key_addrs) + ">"


def _grouped_decls(
    env: Environment, items: list[tuple[str, Concept]]
) -> dict[str, list[dict]]:
    """Group concepts by namespace and, within each, by role + grain:

      * one ``keys`` object listing the namespace's key declarations;
      * one ``properties`` object per grain key-set (``grain`` labels it),
        with the redundant ``<grain>.`` prefix stripped from single-key
        properties since the object already names the grain;
      * one ``metrics`` object per aggregation key-set (``aggregation``
        labels it), with grain-free responsive metrics under ``<responsive>``.

    Anything that isn't a key/property/metric lands in a trailing ``ungrouped``
    object so nothing is dropped. The local namespace surfaces under the
    empty-string key because bare references are what the agent writes."""
    from trilogy.core.statements.author import ConceptDeclarationStatement
    from trilogy.parsing.render import Renderer

    renderer = Renderer(environment=env)

    def decl(concept: Concept) -> str:
        return renderer.to_string(ConceptDeclarationStatement(concept=concept))

    by_ns: dict[str, list[tuple[str, Concept]]] = defaultdict(list)
    for addr, concept in items:
        by_ns[concept.namespace].append((addr, concept))

    out: dict[str, list[dict]] = {}
    for ns in sorted(by_ns):
        keys, props, metrics, others = [], [], [], []
        for addr, c in sorted(by_ns[ns], key=lambda ac: ac[0]):
            purpose = c.purpose.value
            if purpose == "key":
                keys.append(c)
            elif purpose == "property":
                props.append(c)
            elif purpose == "metric":
                metrics.append(c)
            else:
                others.append(c)

        groups: list[dict] = []
        if keys:
            groups.append({"keys": [decl(c) for c in keys]})

        # Properties grouped by grain key-set; single-key props shed the
        # ``property <grain>.`` prefix the group label already carries.
        by_grain: dict[tuple[str, ...], list[Concept]] = defaultdict(list)
        for c in props:
            by_grain[tuple(sorted(c.keys or []))].append(c)
        for grain in sorted(by_grain, key=lambda g: (len(g), g)):
            label = _keyset_label(grain)
            prefix = f"property {label}." if len(grain) == 1 else None
            decls = []
            for c in by_grain[grain]:
                d = decl(c)
                decls.append(d[len(prefix) :] if prefix and d.startswith(prefix) else d)
            groups.append({"grain": label, "properties": decls})

        # Metrics grouped by aggregation key-set; grain-free ones are
        # query-responsive and bucket under "<responsive>".
        by_agg: dict[tuple[str, ...], list[Concept]] = defaultdict(list)
        for c in metrics:
            by_agg[tuple(sorted(c.keys or []))].append(c)
        for agg in sorted(by_agg, key=lambda g: (g == (), len(g), g)):
            label = "<responsive>" if agg == () else _keyset_label(agg)
            groups.append(
                {"aggregation": label, "metrics": [decl(c) for c in by_agg[agg]]}
            )

        if others:
            groups.append({"ungrouped": [decl(c) for c in others]})
        out["" if ns == DEFAULT_NAMESPACE else ns] = groups
    return out


def _emit_explore_json(
    env: Environment,
    concept_items: list[tuple[str, Concept]],
    show: str,
    import_descriptions: dict[str, str],
    expand_imports: bool,
) -> None:
    """Emit the explore results as a stream of pretty-printed JSON events,
    honoring ``--show``. Concepts are grouped by namespace; the local ones are
    rendered in full Trilogy declaration syntax, imported namespaces collapse
    to a name-only list (unless ``--expand-imports``/``--regex``) so a fact
    file's dozens of inherited dimensions don't drown the local declarations."""
    if show in ("all", "groups", "concepts"):
        if expand_imports:
            local_items, imported_items = concept_items, []
        else:
            local_items = [
                (a, c) for a, c in concept_items if c.namespace == DEFAULT_NAMESPACE
            ]
            imported_items = [
                (a, c) for a, c in concept_items if c.namespace != DEFAULT_NAMESPACE
            ]
        # Imported namespaces collapse to ONE comma-joined line of bare leaf
        # names (the `<ns>.` prefix is in the key, so repeating it per leaf —
        # let alone one leaf per pretty-printed line — just burns tokens). The
        # agent reaches a concept as `<ns>.<leaf>`; drill in with --regex.
        imported: dict[str, list[str]] = defaultdict(list)
        for addr, c in sorted(imported_items, key=lambda kc: kc[0]):
            disp = _display_address(addr)
            prefix = f"{c.namespace}."
            leaf = disp[len(prefix) :] if disp.startswith(prefix) else disp
            if leaf.startswith("_"):  # internal/intermediate concept — hide
                continue
            imported[c.namespace].append(leaf)
        imported_joined = {
            ns: ", ".join(imported[ns]) for ns in sorted(imported) if imported[ns]
        }
        emit_event(
            "concepts",
            count=len(concept_items),
            namespaces=_grouped_decls(env, local_items) or None,
            imported=imported_joined or None,
            import_descriptions=import_descriptions or None,
        )
    if show in ("all", "datasources"):
        datasources = [
            {
                "address": name,
                "grain": sorted(ds.grain.components),
            }
            for name, ds in sorted(env.datasources.items())
        ]
        emit_event("datasources", count=len(datasources), datasources=datasources)
    if show in ("all", "imports"):
        imports = [
            {
                "alias": alias,
                "path": str(stmt.path),
                "description": (stmt.description or "").strip() or None,
            }
            for alias, stmts in sorted(env.imports.items())
            for stmt in stmts
        ]
        emit_event("imports", count=len(imports), imports=imports)


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
    "--regex",
    "regex_patterns",
    type=str,
    multiple=True,
    default=(),
    help=(
        "Case-insensitive Python-regex filter over concept addresses. "
        "Repeatable — concepts whose address matches ANY supplied pattern are "
        "kept (OR semantics). Uses ``re.search`` (Python `re` flavor — like "
        "`grep -E` but full Python syntax, e.g. `(?:...)`). A bare word "
        "(`customer`) matches as a substring; metacharacters work too "
        "(`date\\.(year|week_seq)`). A malformed pattern aborts with a "
        "non-zero exit and the underlying ``re.error`` message."
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
@click.option(
    "--expand-imports",
    is_flag=True,
    default=False,
    help=(
        "Render imported concepts in full detail instead of collapsing them "
        "to a name-only listing. The default collapses imports because a fact "
        "file typically pulls in 5-10 dimensions, each contributing dozens of "
        "concepts; the collapse keeps the local declarations readable. Passing "
        "--regex also bypasses the collapse — when you're filtering you want "
        "matching imports in full."
    ),
)
def explore(
    path: Path,
    show: str,
    purpose: tuple[str, ...],
    regex_patterns: tuple[str, ...],
    include_hidden: bool,
    include_builtins: bool,
    expand_imports: bool,
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
    if regex_patterns:
        compiled: list[re.Pattern[str]] = []
        for pat in regex_patterns:
            try:
                compiled.append(re.compile(pat, re.IGNORECASE))
            except re.error as exc:
                print_error(f"Invalid --regex pattern {pat!r}: {exc}")
                raise click.exceptions.Exit(2) from exc
        concept_items = [
            (k, v) for k, v in concept_items if any(p.search(k) for p in compiled)
        ]

    # alias -> trailing-comment description from the `import ... as ...;` lines,
    # surfaced under namespace headers to disambiguate look-alike imports.
    import_descriptions = {
        alias: imp.description
        for alias, imps in env.imports.items()
        for imp in imps
        if imp.description
    }

    # `--regex` is a deliberate filter — show matches in full even if imported,
    # so the agent doesn't have to re-issue with --expand-imports.
    expand = expand_imports or bool(regex_patterns)

    if is_json_mode():
        _emit_explore_json(env, concept_items, show, import_descriptions, expand)
        return

    if show in ("all", "groups"):
        _emit_groups(
            concept_items,
            expand_imports=expand,
            import_descriptions=import_descriptions,
        )

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
                import_rows.append(
                    (alias, str(stmt.path), (stmt.description or "").strip())
                )
        _emit_table(
            f"Imports ({len(import_rows)})",
            ("alias", "path", "description"),
            import_rows,
        )
