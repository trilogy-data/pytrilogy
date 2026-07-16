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
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, Sequence

import click

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment, Import
from trilogy.parser import parse_text
from trilogy.scripts.display import emit_event, is_json_mode, print_error, print_info

_CATEGORIES = ("all", "concepts", "datasources", "imports", "groups")

# --- explore output schema versioning -------------------------------------
# The explore payload shape is versioned per render type so it can evolve
# without breaking consumers that pin or migrate. INTERNAL only — there is no
# CLI flag; flip the default here, or override in tests / pinned callers via
# ``render_version_override``. The emitted payload carries its ``version`` so a
# reader can branch on the shape it got.
#
#   json v1: every namespace rendered in full (role-played conformed
#            dimensions repeat their schema once per role).
#   json v2: conformed dimensions collapse into one combined-key entry
#            (``"date, return_date, …": [schema]``) — the default. Imported
#            namespaces render in the same full grouped-declaration form
#            under ``namespaced`` (each entry pairs its schema with the
#            import-line description(s)); they are never collapsed to the
#            old name-only leaf list.
#   rich v1: the only rich shape so far (no conformed dedup yet).
RENDER_TYPE_JSON = "json"
RENDER_TYPE_RICH = "rich"
_LATEST_RENDER_VERSION: dict[str, int] = {RENDER_TYPE_JSON: 2, RENDER_TYPE_RICH: 1}
_RENDER_VERSION: dict[str, int] = dict(_LATEST_RENDER_VERSION)


def render_version(render_type: str) -> int:
    """Active payload version for a render type (defaults to 1 for unknowns)."""
    return _RENDER_VERSION.get(render_type, 1)


@contextmanager
def render_version_override(render_type: str, version: int) -> Iterator[None]:
    """Temporarily pin a render type to a specific version (tests / callers
    that must emit or assert a fixed shape)."""
    prev = _RENDER_VERSION.get(render_type)
    _RENDER_VERSION[render_type] = version
    try:
        yield
    finally:
        if prev is None:
            _RENDER_VERSION.pop(render_type, None)
        else:
            _RENDER_VERSION[render_type] = prev


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
    meta = concept.metadata
    raw = meta.description if meta is not None else None
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
        if c.namespace.startswith("__"):  # internal model — never expose
            continue
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
        header = f"# {ns}.* ({len(leaves)} - replace * with <leaf> to access)"
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


def _strip_prefix(decl: str, prefix: str) -> str:
    return decl[len(prefix) :] if decl.startswith(prefix) else decl


def _grain_grouped(
    concepts: list[Concept],
    decl: Callable[[Concept], str],
    key_prefix: Callable[[Concept], str],
    keyword: str,
    field: str,
) -> list[dict]:
    """Bucket property-like concepts by grain key-set, one group dict per grain
    (``grain`` labels it) under ``field``, with the redundant
    ``<keyword> <grain>.`` declaration prefix stripped. The prefix to strip is
    taken from the renderer itself (``key_prefix``) rather than reconstructed
    from the group label — the renderer is the authority on key ordering and
    bracket form, so re-deriving it here would silently drift out of sync."""
    by_grain: dict[tuple[str, ...], list[Concept]] = defaultdict(list)
    for c in concepts:
        by_grain[tuple(sorted(c.keys or []))].append(c)
    groups: list[dict] = []
    for grain in sorted(by_grain, key=lambda g: (len(g), g)):
        label = _keyset_label(grain)
        decls = [
            _strip_prefix(decl(c), f"{keyword} {key_prefix(c)}")
            for c in by_grain[grain]
        ]
        groups.append({"grain": label, field: decls})
    return groups


def _grouped_decls(
    env: Environment, items: list[tuple[str, Concept]]
) -> dict[str, list[dict]]:
    """Group concepts by namespace and, within each, by role + grain:

      * one ``keys`` object listing the namespace's key declarations, with
        the redundant leading ``key `` keyword stripped;
      * one ``properties`` (and one ``unique_properties``) object per grain
        key-set (``grain`` labels it), with the redundant
        ``[unique ]property <grain>.`` prefix stripped since the object
        already names the grain;
      * one ``metrics`` object per aggregation key-set — the same ``grain``
        label, since a metric's aggregation keys are conceptually its grain —
        with grain-free responsive metrics under ``<responsive>``.

    Anything that isn't a key/(unique-)property/metric lands in a trailing
    ``ungrouped`` object so nothing is dropped. The local namespace surfaces
    under the empty-string key because bare references are what the agent
    writes."""
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
        keys, props, uniques, metrics, others = [], [], [], [], []
        for addr, c in sorted(by_ns[ns], key=lambda ac: ac[0]):
            purpose = c.purpose.value
            if purpose == "key":
                keys.append(c)
            elif purpose == "property":
                props.append(c)
            elif purpose == "unique_property":
                uniques.append(c)
            elif purpose == "metric":
                metrics.append(c)
            else:
                others.append(c)

        groups: list[dict] = []
        if keys:
            # Shed the leading ``key `` keyword the group label implies.
            groups.append({"keys": [_strip_prefix(decl(c), "key ") for c in keys]})

        # (Unique) properties grouped by grain key-set; each sheds the
        # ``[unique ]property <grain>.`` prefix the group label already carries.
        groups += _grain_grouped(
            props, decl, renderer.property_key_prefix, "property", "properties"
        )
        groups += _grain_grouped(
            uniques,
            decl,
            renderer.property_key_prefix,
            "unique property",
            "unique_properties",
        )

        # Metrics grouped by aggregation key-set; grain-free ones are
        # query-responsive and bucket under "<responsive>".
        by_agg: dict[tuple[str, ...], list[Concept]] = defaultdict(list)
        for c in metrics:
            by_agg[tuple(sorted(c.keys or []))].append(c)
        for agg in sorted(by_agg, key=lambda g: (g == (), len(g), g)):
            label = "<responsive>" if agg == () else _keyset_label(agg)
            groups.append({"grain": label, "metrics": [decl(c) for c in by_agg[agg]]})

        if others:
            groups.append({"ungrouped": [decl(c) for c in others]})
        out["" if ns == DEFAULT_NAMESPACE else ns] = groups
    return out


def _import_entry(alias: str, stmt: Import) -> dict[str, str] | None:
    """One structured import-listing entry, or ``None`` to drop it. Internal
    ``__``-prefixed models are never exposed. The default-namespace alias
    (``local`` — a no-alias ``import std.date;``) is omitted rather than shown,
    and an empty trailing-comment description is dropped entirely."""
    path = str(stmt.path)
    if path.startswith("__") or alias.startswith("__"):
        return None
    entry: dict[str, str] = {}
    if alias and alias != DEFAULT_NAMESPACE:
        entry["alias"] = alias
    entry["path"] = path
    desc = (stmt.description or "").strip()
    if desc:
        entry["description"] = desc
    return entry


def _conformed_signature(concepts: list[Concept]) -> tuple:
    """Leaf-only structural signature of one namespace: the sorted tuple of
    ``(relative leaf, purpose, datatype, description)`` over its concepts.

    Built from concept objects, never from rendered strings, so it is immune
    to a namespace word appearing inside comment prose (``date`` inside "a
    calendar date."). It deliberately omits grain/keys: a role-played
    dimension attaches at a different parent grain
    (``billing_customer.first_sales_date.id`` keyed to ``billing_customer.id``)
    while being the same dimension — and the source-file gate (see
    ``_dedup_conformed``) already guarantees the internal structure is
    identical, so the keys add nothing but spurious splits."""
    sig = []
    for c in concepts:
        prefix = f"{c.namespace}."
        leaf = c.address[len(prefix) :] if c.address.startswith(prefix) else c.address
        sig.append(
            (
                leaf,
                c.purpose.value,
                _compact_datatype(str(c.datatype)),
                _concept_description(c),
            )
        )
    return tuple(sorted(sig))


def _pick_canonical(names: list[str], source: Path) -> str:
    """Choose the namespace to render in full for a conformed group. Prefer a
    role whose leaf segment matches the source file stem (``date`` for
    ``date.preql``); otherwise the shallowest / shortest / lexically-first
    address. Deterministic so an agent can cache "X is date" across the
    re-reads of one explore output."""
    stem = source.stem
    preferred = [n for n in names if n.rsplit(".", 1)[-1] == stem]
    return sorted(preferred or names, key=lambda n: (n.count("."), len(n), n))[0]


_ROLE_DELIM = ", "


def _dedup_conformed(
    env: Environment,
    namespaces: dict[str, list[dict]],
    concept_items: list[tuple[str, Concept]],
) -> dict[str, list[dict]]:
    """Collapse role-played conformed dimensions into a single entry whose
    **key lists every namespace that shares the schema** (canonical first),
    rendered once. ``"date, return_date, billing_customer.first_sales_date, …"``
    maps to one date schema instead of 8 identical copies — the grouping is the
    key itself, so there is no ``same_as`` pointer to chase and no separate map.

    Two namespaces are the same dimension when they were parsed from the **same
    source file** (``env.namespace_source``) and carry an identical leaf-only
    signature. Source file — not physical table — is the identity on purpose:
    the same table can be modelled in two files with different labels (must NOT
    merge), and a multi-datasource namespace has no single physical address.
    Lossless: a role that adds/retypes/re-comments a property gets a different
    signature and stays its own entry.

    The body keeps the canonical namespace's prefix (a derived concept embeds
    its namespace in its lineage expression, so the body can't be made fully
    prefix-free); listing the canonical first makes that prefix match the
    leading key, and every ``<role>.<leaf>`` is reachable by substituting any
    listed namespace for the canonical."""
    by_ns: dict[str, list[Concept]] = defaultdict(list)
    for _, c in concept_items:
        by_ns[c.namespace].append(c)

    groups: dict[tuple, list[str]] = defaultdict(list)
    for key in namespaces:
        ns = key or DEFAULT_NAMESPACE
        source = env.namespace_source.get(ns)
        if source is None:  # the file's own concepts — never a role-play
            groups[("\0", key)].append(key)
        else:
            groups[(str(source), _conformed_signature(by_ns.get(ns, [])))].append(key)

    # canonical display-key -> combined "canon, role, role, …" key
    combined_of: dict[str, str] = {}
    absorbed: set[str] = set()
    for gkey, members in groups.items():
        if len(members) < 2 or gkey[0] == "\0":
            continue
        canon = _pick_canonical(members, Path(gkey[0]))
        others = sorted(m for m in members if m != canon)
        combined_of[canon] = _ROLE_DELIM.join([canon, *others])
        absorbed.update(others)

    deduped: dict[str, list[dict]] = {}
    for key, body in namespaces.items():
        if key in absorbed:
            continue
        deduped[combined_of.get(key, key)] = body
    return deduped


def _imported_payload(
    env: Environment,
    imported_items: list[tuple[str, Concept]],
    descriptions: dict[str, str],
    expand_roles: bool,
    version: int,
) -> dict[str, dict]:
    """Full-detail rendering of imported namespaces: the same grouped
    declaration layout as the local ``namespaces`` section, with role-played
    conformed dimensions deduped into one combined-key entry (v2+, same
    machinery as locals). Each entry pairs its schema (``concepts``) with the
    import-line description — a per-role ``roles`` map on a combined entry,
    because those descriptions are what distinguish look-alike roles
    (sale-time vs current-snapshot demographics). A name-only collapse here
    hid key semantics (surrogate ``sk`` vs business ``id``), enum value sets,
    and role notes — the exact facts an agent mis-guesses most expensively.

    Internal namespaces (``__``) and ``_``-prefixed leaves stay hidden,
    matching the collapse this replaced."""
    visible: list[tuple[str, Concept]] = []
    for addr, c in imported_items:
        if c.namespace.startswith("__"):  # internal model — never expose
            continue
        disp = _display_address(addr)
        prefix = f"{c.namespace}."
        leaf = disp[len(prefix) :] if disp.startswith(prefix) else disp
        if leaf.startswith("_"):  # internal/intermediate concept — hide
            continue
        visible.append((addr, c))
    if not visible:
        return {}
    grouped = _grouped_decls(env, visible)
    if version >= 2 and not expand_roles:
        grouped = _dedup_conformed(env, grouped, visible)
    out: dict[str, dict] = {}
    for key, groups in grouped.items():
        members = key.split(_ROLE_DELIM)
        entry: dict = {}
        descs = {
            m: (descriptions.get(m) or "").strip()
            for m in members
            if (descriptions.get(m) or "").strip()
        }
        if len(members) > 1:
            if descs:
                entry["roles"] = descs
        elif descs:
            entry["description"] = descs[members[0]]
        entry["concepts"] = groups
        out[key] = entry
    return out


def build_concepts_payload(
    env: Environment,
    concept_items: list[tuple[str, Concept]],
    import_descriptions: dict[str, str] | None = None,
    expand_roles: bool = False,
    version: int | None = None,
) -> dict:
    """Build the JSON-serializable concept dump: local namespaces rendered in
    full Trilogy declaration syntax under ``namespaces``; imported namespaces
    rendered in the same full form under ``namespaced``, with role-played
    conformed dimensions deduped into one combined-key entry carrying the
    per-role import descriptions. ``None``-valued keys are dropped so the
    payload stays compact whether emitted as a JSON event or embedded in an
    agent prompt.

    ``version`` selects the payload shape (defaults to the active JSON render
    version): v2 collapses role-played conformed dimensions into one
    combined-key entry; v1 renders every role in full. ``expand_roles`` forces
    the full per-role dump regardless of version."""
    if version is None:
        version = render_version(RENDER_TYPE_JSON)
    local_items = [(a, c) for a, c in concept_items if c.namespace == DEFAULT_NAMESPACE]
    imported_items = [
        (a, c) for a, c in concept_items if c.namespace != DEFAULT_NAMESPACE
    ]
    namespaces = _grouped_decls(env, local_items)
    if namespaces and version >= 2 and not expand_roles:
        namespaces = _dedup_conformed(env, namespaces, local_items)
    imported = _imported_payload(
        env, imported_items, import_descriptions or {}, expand_roles, version
    )
    payload = {
        "version": version,
        "count": len(concept_items),
        "namespaces": namespaces or None,
        "namespaced": imported or None,
    }
    return {k: v for k, v in payload.items() if v is not None}


def _emit_explore_json(
    env: Environment,
    concept_items: list[tuple[str, Concept]],
    show: str,
    import_descriptions: dict[str, str],
    expand_roles: bool,
) -> None:
    """Emit the explore results as a stream of pretty-printed JSON events,
    honoring ``--show``. Concepts are grouped by namespace and rendered in
    full Trilogy declaration syntax — local under ``namespaces``, imported
    under ``namespaced`` with conformed role-players deduped (``--expand-imports``
    only affects the rich renderer; JSON is always full detail)."""
    if show in ("all", "groups", "concepts"):
        emit_event(
            "concepts",
            discriminator="type",
            **build_concepts_payload(
                env, concept_items, import_descriptions, expand_roles
            ),
        )
    if show in ("all", "datasources"):
        datasources = [
            {
                "address": name,
                "grain": sorted(ds.grain.components),
            }
            for name, ds in sorted(env.datasources.items())
        ]
        emit_event(
            "datasources",
            discriminator="type",
            count=len(datasources),
            datasources=datasources,
        )
    if show in ("all", "imports"):
        imports = []
        for alias, stmts in sorted(env.imports.items()):
            for stmt in stmts:
                entry = _import_entry(alias, stmt)
                if entry is not None:
                    imports.append(entry)
        emit_event("imports", discriminator="type", count=len(imports), imports=imports)


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
        "Rich output only: render imported concepts in full detail instead of "
        "collapsing them to a name-only listing. Passing --regex also bypasses "
        "the collapse — when you're filtering you want matching imports in "
        "full. JSON output always renders imports in full (with role-played "
        "conformed dimensions deduped), so this flag is a no-op there."
    ),
)
@click.option(
    "--expand-roles",
    is_flag=True,
    default=False,
    help=(
        "Render every role-played conformed dimension in full. By default, "
        "namespaces parsed from the same source file with an identical shape "
        "(e.g. the 8 date roles a fact plays — sold/returned/first-sales/...) "
        "collapse to one canonical schema plus `same_as` references and a "
        "`conformed` map, which is the bulk of the token savings on fact "
        "files. Pass this for the literal per-role dump."
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
    expand_roles: bool,
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
        _emit_explore_json(env, concept_items, show, import_descriptions, expand_roles)
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
                entry = _import_entry(alias, stmt)
                if entry is None:
                    continue
                import_rows.append(
                    (
                        entry.get("alias", ""),
                        entry["path"],
                        entry.get("description", ""),
                    )
                )
        _emit_table(
            f"Imports ({len(import_rows)})",
            ("alias", "path", "description"),
            import_rows,
        )
