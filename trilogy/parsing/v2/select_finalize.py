"""v2-native select finalization.

Replaces direct ``SelectStatement.finalize(environment)`` calls from v2 rule
modules. The goal is to make mirror removal (SemanticState's compatibility
write-through into Environment.concepts) a safe follow-up: rule code must not
depend on side-effecting environment mutations that happen through helpers.

Audit — ``context.environment`` usage in v2 (Phase 1):

1. Read-only config/catalog (safe to keep):
   - ``namespace``, ``parameters``, ``working_path``
   - ``datasources``, ``functions``, ``data_types``

2. Concept reads (must go through ``context.concepts``):
   - Direct environment concept lookups in rule modules — migrated.
   - ``concepts_to_grain_concepts(..., environment=...)`` — served via a
     merged ``local_concepts`` map in this module so v1 grain logic sees
     pending concepts without consulting the env concept store.
   - ``SelectStatement.validate_syntax(environment)`` inlined here, using
     ``context.concepts``.

3. Parse-time concept mutations (must be eliminated):
   - ``SelectStatement.finalize`` used to call ``environment.add_concept``
     under ``CONFIG.parsing.select_as_definition``. We now go through
     ``context.add_top_level_concept``/``SemanticState`` instead.
   - ``rowset_to_concepts`` writes to ``environment.concepts`` and
     ``environment.alias_origin_lookup``. Handled in ``rowset_semantics``.

4. Final commit/materialization (stays explicit, at statement plan boundary):
   - ``ImportStatementPlan.load_imports``: ``environment.add_import``.
   - ``DatasourceStatementPlan.commit``: ``environment.add_datasource``.
   - ``FunctionDefinitionPlan.commit``: ``environment.functions[...] = ...``.
   - ``MergeStatementPlan.commit``: ``environment.merge_concept``.
   - ``RowsetStatementPlan.commit``: ``environment.add_rowset``.
"""

from __future__ import annotations

from dataclasses import replace as dc_replace
from typing import Any, Callable, Iterable, Iterator, Mapping, NamedTuple, cast

from trilogy.constants import CONFIG
from trilogy.core.enums import (
    AggregateGroupingMode,
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    Modifier,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    NavigationWindowItem,
    NumberingWindowItem,
    OrderBy,
    OrderItem,
    Parenthetical,
    ReferenceReplacements,
    RowsetItem,
    SubselectComparison,
    SubselectItem,
    UndefinedConcept,
    UndefinedConceptFull,
    WhereClause,
)
from trilogy.core.models.core import ListWrapper, MapWrapper, TupleWrapper
from trilogy.core.models.environment import UndefinedConceptException
from trilogy.core.statements.author import (
    ConceptTransform,
    MultiSelectStatement,
    PersistStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectLineage,
    SelectStatement,
)
from trilogy.parsing.common import arbitrary_to_concept, row_tuple_function
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.semantic_state import ConceptLookup, ConceptUpdateKind


def _merged_local_concepts(
    select: SelectStatement, context: RuleContext
) -> dict[str, Concept]:
    """Return the select's own local_concepts, augmented with pending
    concepts the v1 grain helper would otherwise miss during a mirror-off
    run. The grain helper falls through to ``environment.concepts`` only when
    a target ConceptRef is not present in ``local_concepts``; seeding pending
    concepts here keeps the shallow resolution on the parser-owned lookup.
    """
    merged: dict[str, Concept] = {}
    for address, concept in context.semantic_state.pending_concepts():
        merged[address] = concept
    merged.update(select.local_concepts)
    return merged


def _staged_addresses(context: RuleContext) -> list[str]:
    """Addresses of all concepts visible to the parse, INCLUDING ones staged this
    parse but not yet committed to ``environment.concepts`` (e.g. a `rowset`
    output referenced before commit). Lets the undefined-suggestion point at them
    — `context.concepts.values()` already merges pending + committed."""
    try:
        return [c.address for c in context.concepts.values()]
    except Exception:
        return []


def _raise_undefined(
    context: RuleContext, address: str, line_no: int | None = None
) -> None:
    existing = context.concepts.get(address)
    if existing is not None and not isinstance(
        existing, (UndefinedConcept, UndefinedConceptFull)
    ):
        return
    matches: list[str] = []
    try:
        matches = context.environment.concepts._find_similar_concepts(
            address, extra_keys=_staged_addresses(context)
        )
    except Exception:
        matches = []
    message = f"Undefined concept: {address}."
    if matches:
        message += f" Suggestions: {matches}"
    if line_no:
        raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
    raise UndefinedConceptException(message, matches)


class _UndefinedRef(NamedTuple):
    address: str
    clause: str
    line: int | None
    column: int | None


def _is_unresolved(context: RuleContext, address: str) -> bool:
    resolved = context.concepts.get(address)
    return resolved is None or isinstance(
        resolved, (UndefinedConcept, UndefinedConceptFull)
    )


def _undefined_ref(
    ref: ConceptRef | Concept, clause: str, fallback_line: int | None
) -> _UndefinedRef:
    meta = ref.metadata
    line = meta.line_number if meta and meta.line_number else fallback_line
    column = meta.column if meta else None
    return _UndefinedRef(ref.address, clause, line, column)


def collect_clause_undefined(
    context: RuleContext,
    clause: str,
    refs: Iterable[ConceptRef],
    fallback_line: int | None,
) -> list[_UndefinedRef]:
    out: list[_UndefinedRef] = []
    for ref in refs:
        if _is_unresolved(context, ref.address):
            out.append(_undefined_ref(ref, clause, fallback_line))
    return out


def _format_location(rec: _UndefinedRef) -> str:
    parts: list[str] = []
    if rec.line is not None:
        loc = f"line {rec.line}"
        if rec.column is not None:
            loc += f", col {rec.column}"
        parts.append(loc)
    parts.append(f"in {rec.clause}")
    return f" ({', '.join(parts)})"


def raise_collected_undefined(
    context: RuleContext, records: list[_UndefinedRef]
) -> None:
    """Raise a single ``UndefinedConceptException`` describing every undefined
    reference in a statement, each with its position and suggestions. Reporting
    them all at once collapses the fix-rerun-hit-the-next-one loop."""
    seen: set[tuple[str, str, int | None]] = set()
    deduped: list[_UndefinedRef] = []
    for rec in records:
        key = (rec.address, rec.clause, rec.line)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rec)

    staged = _staged_addresses(context)

    def suggest(address: str) -> list[str]:
        try:
            return context.environment.concepts._find_similar_concepts(
                address, extra_keys=staged
            )
        except Exception:
            return []

    if len(deduped) == 1:
        rec = deduped[0]
        matches = suggest(rec.address)
        message = f"Undefined concept: {rec.address}{_format_location(rec)}."
        if matches:
            message += f" Suggestions: {matches}"
        raise UndefinedConceptException(message, matches)

    all_matches: list[str] = []
    lines: list[str] = []
    for rec in deduped:
        matches = suggest(rec.address)
        all_matches.extend(matches)
        hint = f"; did you mean: {', '.join(matches)}?" if matches else ""
        lines.append(f"  - {rec.address}{_format_location(rec)}{hint}")
    message = (
        f"{len(deduped)} undefined concept references; fix all before "
        f"re-running:\n" + "\n".join(lines)
    )
    raise UndefinedConceptException(message, all_matches)


def _calculate_grain(
    select: SelectStatement,
    context: RuleContext,
    local_concepts: Mapping[str, Concept],
) -> Grain:
    targets = [item.concept for item in select.selection]
    result = Grain.from_concepts(
        targets,
        where_clause=select.where_clause,
        environment=context.environment,
        local_concepts=local_concepts,
    )
    if select.join_clauses:
        result = select._collapse_join_keys_in_grain(result)
    return result


def _concept_address(c: Any, rename: Mapping[str, str] | None = None) -> str:
    if isinstance(c, (AggregateWrapper, Function)):
        nested = _render_aggregate(c)
        if nested:
            return nested
        # A composite (non-aggregate) argument like `a || b` embeds concept refs
        # that `rename` must reach: after `_rewrite_aliased_source_refs` rewrites a
        # HAVING copy's inner ref to the SELECT alias while the SELECT copy keeps the
        # source name, the two aggregate signatures only compare equal once the
        # rename is pushed through the whole expression. Both compared sides run
        # this, so the rendered key need only be self-consistent.
        if isinstance(c, Function) and rename:
            replacements: ReferenceReplacements = [
                (src, ConceptRef(address=tgt)) for src, tgt in rename.items()
            ]
            return str(c.with_reference_replacement(replacements))
    addr = c.address if isinstance(c, (ConceptRef, Concept)) else str(c)
    return rename.get(addr, addr) if rename is not None else addr


def _macro_inner_aggregate(node: Any) -> Any | None:
    """If ``node`` is a ``def``-macro invocation whose body is a single aggregate
    (``@rollup_sales()`` -> ``sum(...) by rollup ...``), return that inner
    aggregate node, else ``None``. Lets HAVING/SELECT aggregate matching see
    through the ``FunctionCallWrapper`` produced by macro expansion."""
    if isinstance(node, FunctionCallWrapper):
        inner = node.content
        if isinstance(inner, AggregateWrapper) or (
            isinstance(inner, Function)
            and inner.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return inner
    return None


def _aggregate_full_signature(
    node: Any,
    rename: Mapping[str, str] | None = None,
) -> tuple[Any, tuple[str, ...], tuple[str, ...]] | None:
    """Signature (operator, args, by) for matching aggregates across SELECT/WHERE.

    ``rename`` maps pure-rename SELECT sources to their alias addresses so a
    SELECT ``by rollup sales.channel`` and a HAVING ``by ... local.channel``
    (which finalize rewrites to the output grain) compare equal.

    Returns ``None`` for non-aggregate nodes.
    """
    inner = _macro_inner_aggregate(node)
    if inner is not None:
        return _aggregate_full_signature(inner, rename)
    if isinstance(node, AggregateWrapper):
        return (
            node.function.operator,
            tuple(_concept_address(a, rename) for a in node.function.arguments),
            tuple(sorted(_concept_address(c, rename) for c in node.by)),
        )
    if isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        return (
            node.operator,
            tuple(_concept_address(a, rename) for a in node.arguments),
            (),
        )
    return None


def _render_aggregate(node: Any) -> str:
    sig = _aggregate_full_signature(node)
    if sig is None:
        return ""
    op, args, by = sig
    op_name = (op.value if hasattr(op, "value") else str(op)).lower()
    rendered = f"{op_name}({', '.join(args)})"
    if by:
        rendered += f" by {', '.join(by)}"
    return rendered


def _strip_local_namespace(address: str) -> str:
    return address[len("local.") :] if address.startswith("local.") else address


def _select_aggregate_outputs(
    select: SelectStatement,
    rename: Mapping[str, str] | None = None,
) -> list[tuple[tuple[Any, tuple[str, ...], tuple[str, ...]], str]]:
    """Return ``(signature, output_address)`` for aggregate-producing select items."""
    results: list[tuple[tuple[Any, tuple[str, ...], tuple[str, ...]], str]] = []
    for item in select.selection:
        if isinstance(item.content, ConceptTransform):
            sig = _aggregate_full_signature(item.content.function, rename)
            if sig is not None:
                results.append((sig, item.content.output.address))
    return results


def _norm_window_address(node: Any, rename: Mapping[str, str]) -> Any:
    """Normalize a window operand to a comparable key. Pure-rename SELECT
    aliases (``sales.store as store``) leave the SELECT-side window referencing
    the source name while the HAVING side is rewritten to the alias; map source
    names through ``rename`` so both sides compare equal."""
    addr = node.address if isinstance(node, ConceptRef) else None
    if addr is None:
        return ("expr", str(node))
    return rename.get(addr, addr)


def _window_signature(node: Any, rename: Mapping[str, str]) -> tuple[Any, ...] | None:
    """Signature for matching a window expression across SELECT/HAVING. Returns
    ``None`` for non-window nodes."""
    if isinstance(node, NavigationWindowItem):
        return (
            "nav",
            node.type,
            _norm_window_address(node.content, rename),
            tuple(_norm_window_address(o, rename) for o in node.over),
            tuple(
                (_norm_window_address(o.expr, rename), o.order) for o in node.order_by
            ),
            node.offset,
        )
    if isinstance(node, NumberingWindowItem):
        return (
            "num",
            node.type,
            tuple(_norm_window_address(a, rename) for a in node.arguments),
            tuple(_norm_window_address(o, rename) for o in node.over),
            tuple(
                (_norm_window_address(o.expr, rename), o.order) for o in node.order_by
            ),
        )
    return None


def _select_window_outputs(
    select: SelectStatement, rename: Mapping[str, str]
) -> dict[tuple[Any, ...], ConceptRef]:
    """Map each window-producing select item's signature to its output ref."""
    results: dict[tuple[Any, ...], ConceptRef] = {}
    for item in select.selection:
        if isinstance(item.content, ConceptTransform):
            sig = _window_signature(item.content.function, rename)
            if sig is not None:
                results[sig] = item.content.output.reference
    return results


def _substitute_having_windows(
    select: SelectStatement,
) -> None:
    """Rewrite HAVING window expressions to reference the matching SELECT window
    alias. Without this the renderer re-derives the window at the outer scope,
    where the window's inner aggregate is no longer a resolvable column and
    emits an ``INVALID_REFERENCE_BUG`` sentinel."""
    if not select.having_clause:
        return
    rename = {src: ref.address for src, ref in _alias_rename_map(select).items()}
    sig_to_ref = _select_window_outputs(select, rename)
    if not sig_to_ref:
        return

    def match(node: Any) -> ConceptRef | None:
        sig = _window_signature(node, rename)
        return sig_to_ref.get(sig) if sig is not None else None

    new_conditional = _substitute_condition_tree(
        select.having_clause.conditional, match
    )
    if new_conditional is not select.having_clause.conditional:
        select.having_clause.conditional = new_conditional


def _scalar_expr_signature(
    node: Any, rename: Mapping[str, str]
) -> tuple[Any, ...] | None:
    """Structural signature of a scalar (non-aggregate, non-window) derived
    expression, for matching a HAVING subexpression to its SELECT alias. Returns
    ``None`` if the node contains an aggregate/window (those have dedicated
    passes) or anything else not safely comparable."""
    if isinstance(node, Parenthetical):
        return _scalar_expr_signature(node.content, rename)
    if isinstance(node, ConceptRef):
        return ("ref", rename.get(node.address, node.address))
    if isinstance(node, Function):
        if node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return None
        arg_sigs = []
        for arg in node.arguments:
            sig = _scalar_expr_signature(arg, rename)
            if sig is None:
                return None
            arg_sigs.append(sig)
        return ("fn", node.operator, tuple(arg_sigs))
    if isinstance(node, (AggregateWrapper, NavigationWindowItem, NumberingWindowItem)):
        return None
    return ("lit", type(node).__name__, str(node))


def _select_derived_outputs(
    select: SelectStatement, rename: Mapping[str, str]
) -> dict[tuple[Any, ...], ConceptRef]:
    """Map each scalar-derived (compound function) select item's signature to its
    output ref. Bare renames are handled by ``_alias_rename_map``; aggregates and
    windows by their own passes — only top-level ``fn`` signatures register here."""
    results: dict[tuple[Any, ...], ConceptRef] = {}
    for item in select.selection:
        if not isinstance(item.content, ConceptTransform):
            continue
        if not isinstance(item.content.function, Function):
            continue
        sig = _scalar_expr_signature(item.content.function, rename)
        if sig is not None and sig[0] == "fn":
            results.setdefault(sig, item.content.output.reference)
    return results


def _substitute_having_derived(select: SelectStatement) -> None:
    """Rewrite HAVING scalar-derived expressions (e.g. ``coalesce(x, 0)``) to
    reference the matching SELECT alias. Without this the renderer re-derives the
    expression at the outer scope, where its inner argument is no longer a
    resolvable column, and emits an ``INVALID_REFERENCE_BUG`` sentinel."""
    if not select.having_clause:
        return
    rename = {src: ref.address for src, ref in _alias_rename_map(select).items()}
    sig_to_ref = _select_derived_outputs(select, rename)
    if not sig_to_ref:
        return

    def match(node: Any) -> ConceptRef | None:
        if not isinstance(node, Function):
            return None
        sig = _scalar_expr_signature(node, rename)
        return sig_to_ref.get(sig) if sig is not None else None

    new_conditional = _substitute_condition_tree(
        select.having_clause.conditional, match
    )
    if new_conditional is not select.having_clause.conditional:
        select.having_clause.conditional = new_conditional


def _collect_condition_aggregates(node: Any) -> list[Any]:
    """Walk a WHERE/HAVING conditional tree and return outer aggregate nodes.

    Recognizes both ``AggregateWrapper`` and bare ``Function`` calls whose operator
    is an aggregate function. Does not descend into the inner argument expression
    of an aggregate — nested aggregates are matched by the outer signature.
    """
    found: list[Any] = []
    if isinstance(node, AggregateWrapper):
        found.append(node)
        return found
    if _macro_inner_aggregate(node) is not None:
        # `@rollup_agg()` macro wrapping a single aggregate — match as a whole.
        found.append(node)
        return found
    if isinstance(node, FunctionCallWrapper):
        found.extend(_collect_condition_aggregates(node.content))
        for macro_arg in node.args:
            found.extend(_collect_condition_aggregates(macro_arg))
        return found
    if isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        found.append(node)
        return found
    if isinstance(node, Comparison):
        found.extend(_collect_condition_aggregates(node.left))
        found.extend(_collect_condition_aggregates(node.right))
    elif isinstance(node, Conditional):
        found.extend(_collect_condition_aggregates(node.left))
        found.extend(_collect_condition_aggregates(node.right))
    elif isinstance(node, Parenthetical):
        found.extend(_collect_condition_aggregates(node.content))
    elif isinstance(node, Between):
        found.extend(_collect_condition_aggregates(node.left))
        found.extend(_collect_condition_aggregates(node.low))
        found.extend(_collect_condition_aggregates(node.high))
    elif isinstance(node, Function):
        for arg in node.arguments:
            found.extend(_collect_condition_aggregates(arg))
    return found


def _validate_where_aggregate_matches_select(
    select: SelectStatement, line_no: int | None
) -> None:
    """Reject inline WHERE aggregates that recompute a SELECT aggregate.

    ``where sum(x) > 100 select sum(x) as total`` (with or without matching
    ``by`` clauses) is a HAVING-style filter dressed up as WHERE: the value
    being filtered only exists post-aggregation. Emit a targeted message that
    names the matching select alias instead of falling through to the
    multi-grain error, which is correct but opaque for this common case.
    """
    if not select.where_clause:
        return
    select_aggs = _select_aggregate_outputs(select)
    if not select_aggs:
        return
    sig_to_alias = {sig: addr for sig, addr in select_aggs}
    for node in _collect_condition_aggregates(select.where_clause.conditional):
        sig = _aggregate_full_signature(node)
        if sig is None or sig not in sig_to_alias:
            continue
        alias = _strip_local_namespace(sig_to_alias[sig])
        rendered = _render_aggregate(node)
        raise InvalidSyntaxException(
            f"WHERE clause aggregate `{rendered}` is also computed in the "
            f"SELECT (as `{alias}`); aggregate filters must use the HAVING "
            f"clause - e.g. `having {alias} > ...`"
            + (f"; Line: {line_no}" if line_no else "")
        )


def _aggregate_node_has_nested_aggregate(node: Any) -> bool:
    """True if the aggregate node's argument expression contains another aggregate.

    Nested aggregates (``avg(sum(x) by ...)``) cannot be rendered as inline SQL
    and must be materialized via a SELECT alias.
    """
    if isinstance(node, AggregateWrapper):
        args: list[Any] = list(node.function.arguments)
    elif isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        args = list(node.arguments)
    else:
        return False
    stack: list[Any] = list(args)
    while stack:
        cur = stack.pop()
        if _aggregate_full_signature(cur) is not None:
            return True
        if isinstance(cur, Parenthetical):
            stack.append(cur.content)
        elif isinstance(cur, Function):
            stack.extend(cur.arguments)
    return False


def _substitute_condition_tree(node: Any, match_leaf: Any) -> Any:
    """Walk a HAVING conditional tree, replacing any node for which
    ``match_leaf(node)`` returns a ``ConceptRef`` with that reference. A
    ``None`` return leaves the node to the structural recursion. Matched
    nodes are leaves — we do not descend into their argument expressions."""
    replacement = match_leaf(node)
    if replacement is not None:
        return replacement
    if isinstance(node, Comparison):
        # Preserve the concrete class (e.g. SubselectComparison) — a plain
        # Comparison rebuild would strip a membership's existence semantics,
        # leaving the HAVING set-concept to render as a bare column.
        return type(node)(
            left=_substitute_condition_tree(node.left, match_leaf),
            right=_substitute_condition_tree(node.right, match_leaf),
            operator=node.operator,
        )
    if isinstance(node, Conditional):
        return Conditional(
            left=_substitute_condition_tree(node.left, match_leaf),
            right=_substitute_condition_tree(node.right, match_leaf),
            operator=node.operator,
        )
    if isinstance(node, Parenthetical):
        return Parenthetical(
            content=_substitute_condition_tree(node.content, match_leaf)
        )
    if isinstance(node, Between):
        return Between(
            left=_substitute_condition_tree(node.left, match_leaf),
            low=_substitute_condition_tree(node.low, match_leaf),
            high=_substitute_condition_tree(node.high, match_leaf),
        )
    if isinstance(node, Function):
        new_args = [_substitute_condition_tree(a, match_leaf) for a in node.arguments]
        if all(a is b for a, b in zip(new_args, node.arguments)):
            return node
        replacement_fn = Function.__new__(Function)
        replacement_fn.__dict__.update(node.__dict__)
        replacement_fn.arguments = new_args
        return replacement_fn
    # A window's inner order-by/operand may itself be an aggregate that matches a
    # SELECT alias (`rank(k) over (order by sum(x) desc)` alongside `sum(x) as t`).
    # Descend so it's rewritten to the materialized column — otherwise the renderer
    # re-derives the anonymous inline aggregate against a CTE that only has the
    # projected one, emitting an INVALID_REFERENCE_BUG sentinel.
    if isinstance(node, (NumberingWindowItem, NavigationWindowItem)):
        new_order = [
            OrderItem(
                expr=_substitute_condition_tree(o.expr, match_leaf), order=o.order
            )
            for o in node.order_by
        ]
        new_over = [_substitute_condition_tree(o, match_leaf) for o in node.over]
        order_changed = any(
            a.expr is not b.expr for a, b in zip(new_order, node.order_by)
        )
        over_changed = any(a is not b for a, b in zip(new_over, node.over))
        if isinstance(node, NumberingWindowItem):
            new_args = [
                _substitute_condition_tree(a, match_leaf) for a in node.arguments
            ]
            if not (
                order_changed
                or over_changed
                or any(a is not b for a, b in zip(new_args, node.arguments))
            ):
                return node
            return NumberingWindowItem(
                type=node.type,
                arguments=new_args,
                order_by=new_order,
                over=new_over,
            )
        new_content = _substitute_condition_tree(node.content, match_leaf)
        if not (order_changed or over_changed or new_content is not node.content):
            return node
        return NavigationWindowItem(
            type=node.type,
            content=new_content,
            order_by=new_order,
            over=new_over,
            offset=node.offset,
        )
    return node


def _substitute_having_aggregates(
    node: Any,
    sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef],
    rename: Mapping[str, str] | None = None,
) -> Any:
    """Rewrite a HAVING conditional tree, replacing matched aggregates with
    ``ConceptRef`` to the SELECT alias."""

    def match(n: Any) -> ConceptRef | None:
        sig = _aggregate_full_signature(n, rename)
        return sig_to_ref.get(sig) if sig is not None else None

    return _substitute_condition_tree(node, match)


def _validate_having_aggregates_match_select(
    select: SelectStatement, context: RuleContext, line_no: int | None
) -> None:
    """Reject inline HAVING aggregates whose signature is not in the SELECT.

    HAVING filters rows of the post-aggregation projection, so any aggregate it
    references must already exist in the projection (matching by operator, args,
    and ``by`` grain). An off-grain aggregate like ``avg(sum(x) by store.id)``
    that does not match a SELECT item has no place to be materialized at the
    SELECT grain — the renderer would otherwise emit nested aggregates.

    For aggregates whose signature *does* match a SELECT alias, rewrite the
    HAVING tree to reference the alias directly. Without this the renderer
    re-inlines the AggregateWrapper, which breaks when intermediate CTEs have
    already aggregated away the row-level inputs.
    """
    if not select.having_clause:
        return
    # A SELECT ``by rollup sales.channel`` keeps source names while finalize
    # rewrites the matching HAVING ``by`` to its output aliases (``local.channel``)
    # — normalize both sides through the pure-rename map so they match.
    rename = {src: ref.address for src, ref in _alias_rename_map(select).items()}
    select_aggs = _select_aggregate_outputs(select, rename)
    sig_to_alias_addr = {sig: addr for sig, addr in select_aggs}
    for node in _collect_condition_aggregates(select.having_clause.conditional):
        sig = _aggregate_full_signature(node, rename)
        if sig is None or sig in sig_to_alias_addr:
            continue
        _, _, by = sig
        # Bare aggregates without an explicit ``by`` (and without a nested
        # aggregate input) compute at the SELECT grain; the renderer can
        # inline them safely from the same source columns. Reject only the
        # cases the renderer can't handle inline: off-grain ``by`` clauses
        # and nested aggregates.
        if not by and not _aggregate_node_has_nested_aggregate(node):
            continue
        rendered = _render_aggregate(node)
        raise InvalidSyntaxException(
            f"HAVING clause aggregate `{rendered}` is not in the SELECT "
            f"projection (line {line_no}). HAVING can only filter on "
            f"off-grain or nested aggregates that are also computed in the "
            f"SELECT. Fix one of: (a) add it to SELECT — prefix with `--` to "
            f"keep it out of the output rows, e.g. `select ..., --{rendered}`; "
            f"(b) move the filter to WHERE — for an aggregate condition on a "
            f"non-output grain, write the aggregate inline as `agg(x) by grain` "
            f"directly in WHERE."
        )
    sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef] = {}
    for sig, addr in sig_to_alias_addr.items():
        alias_concept = context.concepts.get(addr)
        if alias_concept is None:
            continue
        sig_to_ref[sig] = alias_concept.reference
    if sig_to_ref:
        new_conditional = _substitute_having_aggregates(
            select.having_clause.conditional, sig_to_ref, rename
        )
        if new_conditional is not select.having_clause.conditional:
            select.having_clause.conditional = new_conditional


def _alias_rename_map(select: SelectStatement) -> dict[str, ConceptRef]:
    """For pure-rename SELECT items (`X as Y` where the source side is just a
    ConceptRef), return a `source_address -> ConceptRef(output)` map.

    Downstream CTEs only carry the alias output address in their `source_map`,
    so any HAVING/ORDER BY reference to the bare source name would miss the
    lookup and fall through to a re-rendering path. Rewriting source refs
    to alias refs here keeps the rendering layer's source_map lookup honest.

    We intentionally only handle pure renames — for compound aliases like
    `f(x, y) as z`, references to `x` in HAVING/ORDER BY are not synonyms
    of `z` and must not be rewritten.
    """
    rename_map: dict[str, ConceptRef] = {}
    for item in select.selection:
        if not isinstance(item.content, ConceptTransform):
            continue
        fn = item.content.function
        if (
            isinstance(fn, Function)
            and fn.operator == FunctionType.ALIAS
            and len(fn.arguments) == 1
            and isinstance(fn.arguments[0], ConceptRef)
        ):
            rename_map[fn.arguments[0].address] = item.content.output.reference
    return rename_map


def _rewrite_aliased_source_refs(select: SelectStatement) -> None:
    """Rewrite HAVING/ORDER BY references to a pure-rename source so they
    point at the SELECT alias. Mirrors `_substitute_having_aggregates`, but
    for plain ConceptRefs and extended to cover ORDER BY too."""
    rename_map = _alias_rename_map(select)
    if not rename_map:
        return
    replacements = [(src, ref) for src, ref in rename_map.items()]
    if select.having_clause:
        select.having_clause = select.having_clause.with_reference_replacement(
            replacements
        )
    if select.order_by:
        select.order_by = OrderBy(
            items=[
                OrderItem(
                    expr=(
                        item.expr.with_reference_replacement(replacements)
                        if hasattr(item.expr, "with_reference_replacement")
                        else item.expr
                    ),
                    order=item.order,
                )
                for item in select.order_by.items
            ]
        )


_GROUPING_FNS = (FunctionType.GROUPING, FunctionType.GROUPING_ID)


def _item_lineage(item: Any, context: RuleContext) -> Any:
    """The lineage expression behind a select item, whether it is an inline
    ``ConceptTransform`` or a by-name reference to an ``auto`` concept."""
    content = item.content
    if isinstance(content, ConceptTransform):
        return content.output.lineage
    addr = getattr(content, "address", None)
    if addr is None:
        return None
    concept = context.concepts.get(addr)
    if concept is None or isinstance(concept, (UndefinedConcept, UndefinedConceptFull)):
        return None
    return concept.lineage


def _child_exprs(node: Any) -> Iterator[Any]:
    """Direct child expressions of a composite ``Expr`` node — the value-bearing
    sub-expressions an aggregate could be nested in. Covers every non-leaf member
    of the ``Expr`` union (author.py); leaves (``ConceptRef``, scalars) yield
    nothing. Filter/window/subselect carry their value in ``content``/``arguments``
    only — their ``where``/``order_by`` are conditions, not part of the projected
    value, so they are intentionally not descended here.

    Keep in sync with the ``Expr`` union; ``test_aggregate_wrapper_collection``'s
    exhaustiveness guard fails if a new composite type is added without coverage.
    """
    if isinstance(node, (Comparison, Conditional)):
        yield node.left
        yield node.right
    elif isinstance(node, Between):
        yield node.left
        yield node.low
        yield node.high
    elif isinstance(node, Parenthetical):
        yield node.content
    elif isinstance(node, CaseWhen):
        yield node.comparison
        yield node.expr
    elif isinstance(node, CaseElse):
        yield node.expr
    elif isinstance(node, AggregateWrapper):
        yield from node.function.arguments
    elif isinstance(node, Function):
        yield from node.arguments
    elif isinstance(node, FunctionCallWrapper):
        # `def` macro expansion (`@rollup_agg(x)`): body in `content`, call args.
        yield node.content
        yield from node.args
    elif isinstance(node, FilterItem):
        yield node.content
    elif isinstance(node, NavigationWindowItem):
        yield node.content
    elif isinstance(node, NumberingWindowItem):
        yield from node.arguments
    elif isinstance(node, SubselectItem):
        yield node.content
    elif isinstance(node, (TupleWrapper, ListWrapper)):
        yield from node
    elif isinstance(node, MapWrapper):
        yield from node.keys()
        yield from node.values()


def _expression_contains_window(node: Any) -> bool:
    if isinstance(node, (NavigationWindowItem, NumberingWindowItem)):
        return True
    return any(_expression_contains_window(child) for child in _child_exprs(node))


def _window_key_addresses(node: Any) -> set[str]:
    if isinstance(node, (NavigationWindowItem, NumberingWindowItem)):
        refs = {
            expr.address
            for expr in [
                *node.over,
                *(item.expr for item in node.order_by),
            ]
            if isinstance(expr, ConceptRef)
        }
        if isinstance(node, NavigationWindowItem):
            refs |= _window_key_addresses(node.content)
        else:
            for argument in node.arguments:
                refs |= _window_key_addresses(argument)
        return refs
    return {
        address
        for child in _child_exprs(node)
        for address in _window_key_addresses(child)
    }


def _collect_aggregate_wrappers(
    node: Any, predicate: Callable[[Any], bool]
) -> list[AggregateWrapper]:
    """Every ``AggregateWrapper`` matching ``predicate`` anywhere in an expression
    tree — so a target agg nested inside ``coalesce(...)``, a ``case``, a ``def``
    macro, a window, a filter, etc. is still found. A matching node is returned
    without descending into it (an aggregate won't nest a same-class aggregate)."""
    if predicate(node):
        return [cast(AggregateWrapper, node)]
    found: list[AggregateWrapper] = []
    for child in _child_exprs(node):
        found += _collect_aggregate_wrappers(child, predicate)
    return found


def _select_rollup_spec(
    select: SelectStatement,
    context: RuleContext,
) -> tuple[AggregateGroupingMode, list[Any], list[list[Any]]] | None:
    """The query's SELECT-level grouping spec (mode, by, grouping_sets) from a
    ``by rollup (…)`` / ``by cube (…)`` / ``by grouping sets (…)`` clause."""
    grouping = select.grouping
    if grouping is None:
        return None
    return (
        grouping.mode,
        list(grouping.by),
        [list(g) for g in grouping.grouping_sets],
    )


def _is_standard_grouping_aggregate(node: Any) -> bool:
    return (
        _is_grouping_aggregate(node) and node.grouping == AggregateGroupingMode.STANDARD
    )


def _is_grouping_aggregate(node: Any) -> bool:
    return (
        isinstance(node, AggregateWrapper) and node.function.operator in _GROUPING_FNS
    )


def _collect_standard_grouping_wrappers(node: Any) -> list[AggregateWrapper]:
    """All STANDARD-mode ``grouping()``/``grouping_id()`` wrappers anywhere in an
    expression tree (e.g. nested inside a ``case`` that derives a rollup level)."""
    return _collect_aggregate_wrappers(node, _is_standard_grouping_aggregate)


def _is_window_item(node: Any) -> bool:
    return isinstance(node, (NavigationWindowItem, NumberingWindowItem))


def _lineage_reaches(
    lineage: Any, context: RuleContext, predicate: Callable[[Any], bool], seen: set[str]
) -> bool:
    """True when a node matching ``predicate`` is reachable from ``lineage``,
    descending through by-name ``ConceptRef``s into their concept lineages so a
    named ``auto`` concept is inspected too. A rowset output is a materialized
    boundary (its aggregates ran inside the rowset's own pass), so descent stops
    at a ``RowsetItem`` lineage — not an ``Expr`` node, so ``_child_exprs`` yields
    nothing. ``seen`` guards against cyclic lineage."""
    if predicate(lineage):
        return True
    if isinstance(lineage, ConceptRef):
        if lineage.address in seen:
            return False
        seen.add(lineage.address)
        concept = context.concepts.get(lineage.address)
        if concept is None or isinstance(
            concept, (UndefinedConcept, UndefinedConceptFull)
        ):
            return False
        return _lineage_reaches(concept.lineage, context, predicate, seen)
    return any(
        _lineage_reaches(child, context, predicate, seen)
        for child in _child_exprs(lineage)
    )


def _is_ungrouped_aggregate(node: Any) -> bool:
    """A projection aggregate with no explicit ``by`` grain — the operand the
    SELECT-level grouping spec applies to. Covers plain aggregates (sum/count/…)
    and the ``grouping()``/``grouping_id()`` level indicators alike."""
    return (
        isinstance(node, AggregateWrapper)
        and node.grouping == AggregateGroupingMode.STANDARD
        and not node.by
    )


def _collect_ungrouped_aggregates_deep(
    lineage: Any, context: RuleContext, seen: set[str]
) -> list[AggregateWrapper]:
    """Un-grouped aggregate wrappers reachable from a lineage, descending through
    by-name ``ConceptRef``s into their concept lineages — so a named
    ``auto t <- sum(x)`` (or a ``grouping()`` buried inside a derived level
    concept) referenced in the SELECT still inherits the grouping spec. Window
    partition and ordering expressions are included because their aggregates
    execute over the enclosing SELECT's grouped rows. The ``seen`` set guards
    against cyclic lineage."""
    found: list[AggregateWrapper] = []
    if isinstance(lineage, ConceptRef):
        if lineage.address in seen:
            return found
        seen.add(lineage.address)
        concept = context.concepts.get(lineage.address)
        if concept is None or isinstance(
            concept, (UndefinedConcept, UndefinedConceptFull)
        ):
            return found
        return _collect_ungrouped_aggregates_deep(concept.lineage, context, seen)
    if _is_ungrouped_aggregate(lineage):
        found.append(cast(AggregateWrapper, lineage))
    for child in _child_exprs(lineage):
        found.extend(_collect_ungrouped_aggregates_deep(child, context, seen))
    if isinstance(lineage, (NavigationWindowItem, NumberingWindowItem)):
        for child in [
            *lineage.over,
            *(item.expr for item in lineage.order_by),
        ]:
            found.extend(_collect_ungrouped_aggregates_deep(child, context, seen))
    return found


def _validate_grouping_args_are_concepts(
    select: SelectStatement, context: RuleContext
) -> None:
    """``grouping(arg)``/``grouping_id(arg)`` must reference a concept, not an
    inline expression. DuckDB (and standard SQL) require the argument to *be* one
    of the GROUP BY keys; the planner materializes a `by rollup (coalesce(a,b))`
    key as a column and groups by position, but the inline ``grouping(coalesce(a,b))``
    re-emits the expression and never matches that column → BinderException
    ("GROUPING child must be a grouping column"). The fix is to name the
    expression as a concept and use it in both places, so reject the inline form
    here with that guidance rather than emitting invalid SQL."""
    for item in select.selection:
        lineage = _item_lineage(item, context)
        if lineage is None:
            continue
        for wrapper in _collect_standard_grouping_wrappers(lineage):
            for arg in wrapper.function.arguments:
                if isinstance(arg, ConceptRef):
                    continue
                raise InvalidSyntaxException(
                    f"{wrapper.function.operator.value}() requires a concept "
                    f"(column) reference as its argument, not an inline expression "
                    f"like '{arg}'. Assign the expression to a named concept and "
                    f"use that concept in both the grouping key and grouping() - "
                    f"e.g. `auto channel <- coalesce(a, b); select ..., "
                    f"grouping(channel) ... by rollup (channel)`."
                )


def _propagate_select_grouping(select: SelectStatement, context: RuleContext) -> None:
    """Apply the SELECT-level ``by rollup (…)`` spec to every aggregate feeding the
    projection that has no explicit ``by`` grain, so all measures — and the
    ``grouping()`` level indicators — compute in ONE grouping pass.

    This is what makes multi-level grouping a select property: there is no
    per-aggregate rollup to distribute inconsistently, so the planner can never
    split one measure's operands across grouping modes (the failure that emitted
    invalid ``GROUP BY ROLLUP`` SQL). Mutates each wrapper in place (dataclasses)
    so by-name ``auto`` concepts are fixed at their canonical address.

    With no spec, a stray ``grouping()`` has no enclosing grouping set to anchor
    it (its grain would be unresolvable and recurse) — reject it here."""
    # WHERE is evaluated before grouping, so `grouping()` there has no grouping
    # set to anchor to; it would otherwise materialize as a standalone groupless
    # GROUPING() CTE -> DuckDB "GROUPING statement cannot be used without groups".
    # (HAVING grouping() is fine: it runs post-aggregation against a real group
    # key, even without an explicit rollup spec.)
    if select.where_clause is not None and _collect_standard_grouping_wrappers(
        select.where_clause.conditional
    ):
        raise InvalidSyntaxException(
            "grouping()/grouping_id() cannot be used in a WHERE clause: WHERE is "
            "evaluated before grouping, so there is no grouping set to anchor to. "
            "It is a post-aggregate level indicator - use it in SELECT / HAVING / "
            "ORDER BY of a query carrying a `by rollup/cube/grouping sets` clause "
            "(e.g. filter subtotal rows in HAVING: `having grouping(state) = 1`)."
        )
    spec = select.grouping
    if spec is None:
        # No grouping to propagate: reject a stray projected grouping(), whether
        # inline or reached through a named `auto g <- grouping(x)` concept — both
        # render a groupless GROUPING() CTE -> DuckDB "GROUPING statement cannot be
        # used without groups". A grouping() sharing a lineage with a window is the
        # valid q70 post-aggregation label (the grain guard keeps it out of the
        # grain and it renders against the window's aggregate GROUP BY), so a window
        # anywhere in the item's reachable lineage exempts it.
        for item in select.selection:
            lineage = _item_lineage(item, context)
            if lineage is None or not _lineage_reaches(
                lineage, context, _is_standard_grouping_aggregate, set()
            ):
                continue
            if _lineage_reaches(lineage, context, _is_window_item, set()):
                continue
            raise InvalidSyntaxException(
                "grouping()/grouping_id() requires a `by rollup (…)`/"
                "`by cube (…)`/`by grouping sets (…)` clause on the enclosing "
                "select; it has no meaning without a grouping set."
            )
        return
    _validate_grouping_args_are_concepts(select, context)
    _normalize_grouping_args_to_rollup_keys(select, context)
    # The spec itself rides SelectLineage.grouping and is applied to un-pinned
    # aggregates by the build factory — select-scoped by construction, so no
    # shared authoring object is ever mutated with it. Here we only detect
    # whether any projection aggregate exists to carry the pass.
    seen: set[str] = set()
    carried = False
    for item in select.selection:
        lineage = _item_lineage(item, context)
        if lineage is None:
            continue
        if _collect_ungrouped_aggregates_deep(lineage, context, seen):
            carried = True
    # The spec travels only through aggregates; a passthrough of a
    # pre-aggregated (rowset) measure has none, so without this the spec
    # would silently drop when nothing carries it (q05) — and a passthrough
    # next to a fresh aggregate would emit as a bare, ungrouped column in the
    # ROLLUP CTE. Re-aggregate the passthroughs, or raise.
    _apply_grouping_to_passthroughs(select, context, spec, carried)


_REAGGREGABLE_OPERATORS = {FunctionType.SUM, FunctionType.COUNT}


def _underlying_aggregate_operators(
    lineage: Any, context: RuleContext, seen: set[str]
) -> set[FunctionType]:
    """The aggregate operators a passthrough projection is materialized from,
    descending through by-name ``ConceptRef``s (``auto`` concepts) and rowset
    outputs (``RowsetItem`` content). Unlike ``_lineage_reaches`` this
    deliberately crosses the rowset boundary: the question is not where the
    aggregate runs but whether the materialized value can be re-aggregated."""
    if isinstance(lineage, AggregateWrapper):
        return {lineage.function.operator}
    if isinstance(lineage, RowsetItem):
        return _underlying_aggregate_operators(lineage.content, context, seen)
    if isinstance(lineage, ConceptRef):
        if lineage.address in seen:
            return set()
        seen.add(lineage.address)
        concept = context.concepts.get(lineage.address)
        if concept is None or isinstance(
            concept, (UndefinedConcept, UndefinedConceptFull)
        ):
            return set()
        return _underlying_aggregate_operators(concept.lineage, context, seen)
    ops: set[FunctionType] = set()
    for child in _child_exprs(lineage):
        ops |= _underlying_aggregate_operators(child, context, seen)
    return ops


def _grouping_spec_key_addresses(spec: Any) -> set[str]:
    keys = {_concept_address(k) for k in spec.by}
    for grouping_set in spec.grouping_sets:
        keys |= {_concept_address(k) for k in grouping_set}
    return keys


def _leaf_concept_addresses(
    lineage: Any, context: RuleContext, key_addresses: set[str], seen: set[str]
) -> set[str]:
    """The leaf concept addresses a projection reads, descending by-name refs
    into their lineages. Descent stops at grouping-key refs (their derivation
    is irrelevant — they ARE the keys) and at rowset boundaries (a rowset
    output is a materialized value, not a function of the outer keys)."""
    if isinstance(lineage, ConceptRef):
        if lineage.address in key_addresses or lineage.address in seen:
            return {lineage.address}
        seen.add(lineage.address)
        concept = context.concepts.get(lineage.address)
        if (
            concept is None
            or isinstance(concept, (UndefinedConcept, UndefinedConceptFull))
            or concept.lineage is None
            or isinstance(concept.lineage, RowsetItem)
        ):
            return {lineage.address}
        return _leaf_concept_addresses(concept.lineage, context, key_addresses, seen)
    addresses: set[str] = set()
    for child in _child_exprs(lineage):
        addresses |= _leaf_concept_addresses(child, context, key_addresses, seen)
    return addresses


def _is_key_or_broadcast_projection(
    lineage: Any, context: RuleContext, key_addresses: set[str]
) -> bool:
    """True when every concept the projection reads is a grouping key or a
    single-row scalar — a key-derived dim (``case when chan = 1 …`` under
    ``rollup (chan, …)``, the q80 fold family), a broadcast scalar, or a
    constant. These need no aggregate carrier: the planner folds key functions
    into the grouped node and cross-joins single-row scalars."""
    for address in _leaf_concept_addresses(lineage, context, key_addresses, set()):
        if address in key_addresses:
            continue
        concept = context.concepts.get(address)
        if concept is not None and concept.granularity == Granularity.SINGLE_ROW:
            continue
        return False
    return True


def _has_aggregate_or_window(lineage: Any, context: RuleContext) -> bool:
    """True when the item already participates in the grouping pass (any
    aggregate wrapper — stamped, explicitly ``by``-grained, or a grouping()
    indicator) or is a window computed over it. ``_lineage_reaches`` stops at
    rowset boundaries, so a rowset passthrough does NOT count."""
    return _lineage_reaches(
        lineage,
        context,
        lambda n: isinstance(n, AggregateWrapper) or _is_window_item(n),
        set(),
    )


def _apply_grouping_to_passthroughs(
    select: SelectStatement, context: RuleContext, spec: Any, stamped: bool
) -> None:
    """Carry a ``by rollup/cube/grouping sets`` spec on projections that are
    passthroughs of pre-aggregated (rowset) measures.

    The spec travels on aggregate wrappers, so a passthrough has no carrier:
    with no fresh aggregate in the SELECT the spec used to vanish silently —
    the query ran as a plain GROUP BY with zero subtotal rows (q05) — and next
    to a fresh aggregate the passthrough emitted as a bare, ungrouped column
    inside the ROLLUP CTE (BinderException). A SUM/COUNT-derived passthrough
    re-aggregates exactly with an implicit ``sum(...)``, matching the explicit
    fresh-aggregate form (leaf rows are single-group identities, subtotal rows
    sum them).

    Left alone: grouping keys and key-derived dims (the planner folds them
    into the grouped node), single-row scalars (broadcast cross join), and
    plain other-grain dims (join-back at leaf grain — NULL on subtotal rows is
    their defined semantics). When nothing was stamped, a measure that cannot
    be re-aggregated soundly (non-additive, or bare and un-aliased) would mean
    silently dropping the clause — raise with the explicit-form fix instead."""
    key_addresses = _grouping_spec_key_addresses(spec)
    clause = f"by {spec.mode.value.replace('_', ' ')}"
    wrapped = False
    for item in select.selection:
        content = item.content
        if not isinstance(content, ConceptTransform):
            address = getattr(content, "address", None)
            if (
                address is None
                or address in key_addresses
                or _has_aggregate_or_window(content, context)
                or _is_key_or_broadcast_projection(content, context, key_addresses)
            ):
                continue
            if not stamped and _underlying_aggregate_operators(content, context, set()):
                raise InvalidSyntaxException(
                    f"`{clause} (…)` cannot re-aggregate the bare measure "
                    f"reference `{address}`. Alias it with an explicit aggregate "
                    f"(e.g. `sum({address}) as {str(address).split('.')[-1]}_total`)."
                )
            continue
        source: Any = content.function
        if (
            isinstance(source, Function)
            and source.operator == FunctionType.ALIAS
            and len(source.arguments) == 1
        ):
            source = source.arguments[0]
        if isinstance(source, ConceptRef) and source.address in key_addresses:
            continue
        if content.output.address in key_addresses:
            continue
        if _has_aggregate_or_window(content.function, context):
            continue
        if _is_key_or_broadcast_projection(content.function, context, key_addresses):
            continue
        operators = _underlying_aggregate_operators(content.function, context, set())
        if not operators:
            # a plain dim at finer/other grain: the planner fetches it through
            # a join-back at the leaf grain (NULL on subtotal rows), which is
            # its defined semantics — leave it alone
            continue
        if not operators.issubset(_REAGGREGABLE_OPERATORS):
            if stamped:
                continue
            raise InvalidSyntaxException(
                f"`{clause} (…)` cannot re-aggregate passthrough projection "
                f"`{content.output.address}`: it is not derived exclusively from "
                f"sum/count measures, so its subtotal rows have no well-defined "
                f"value. Aggregate it explicitly in the select instead (e.g. "
                f"`sum(x) as {content.output.name}`)."
            )
        # bare: the build factory applies the select's grouping spec
        aggregate = AggregateWrapper(
            function=context.function_factory.create_function(
                [source], FunctionType.SUM
            ),
        )
        content.function = aggregate
        # swap, don't mutate: the output object (or its lineage tree) may be the
        # alias's environment-registered definition
        content.output = dc_replace(content.output, lineage=aggregate)
        select.local_concepts[content.output.address] = content.output
        wrapped = True
    if not stamped and not wrapped:
        raise InvalidSyntaxException(
            f"`{clause} (…)` requires at least one aggregate (or re-aggregable "
            f"pre-aggregated measure) in the select to group; found none."
        )


def _normalize_grouping_args_to_rollup_keys(
    select: SelectStatement, context: RuleContext
) -> None:
    """Rewrite a ``grouping(<source>)`` argument to the rollup-key *alias* when the
    key is projected under one (``sales.channel as channel`` + ``by rollup
    (channel, …)``). DuckDB requires GROUPING's child to BE one of the GROUP BY
    ROLLUP columns; the rollup groups by the materialized alias column, not the
    source, so ``grouping(sales.channel)`` renders ``grouping(source_col)`` against
    a column absent from the GROUP BY → BinderException ("GROUPING child must be a
    grouping column"). Pointing the argument at the alias lines it up with the
    GROUP BY. Only renames whose alias is an actual rollup key are rewritten."""
    spec = select.grouping
    if spec is None:
        return
    rollup_key_addrs = {k.address for k in spec.by if hasattr(k, "address")}
    rename = {
        src: ref
        for src, ref in _alias_rename_map(select).items()
        if ref.address in rollup_key_addrs
    }
    if not rename:
        return
    for item in select.selection:
        lineage = _item_lineage(item, context)
        if lineage is None:
            continue
        for wrapper in _collect_standard_grouping_wrappers(lineage):
            wrapper.function.arguments = [
                rename.get(arg.address, arg) if isinstance(arg, ConceptRef) else arg
                for arg in wrapper.function.arguments
            ]


def _grouping_arg_key(wrapper: AggregateWrapper) -> tuple[Any, tuple[str, ...]]:
    """Mode/`by`-insensitive identity of a grouping wrapper (operator + args).
    Two ``grouping(a)`` wrappers share a key regardless of their grouping mode,
    so a HAVING wrapper can reuse an equivalent already projected in the SELECT."""
    return (
        wrapper.function.operator,
        tuple(_concept_address(a) for a in wrapper.function.arguments),
    )


def _existing_grouping_outputs(
    select: SelectStatement,
) -> dict[tuple[Any, tuple[str, ...]], ConceptRef]:
    """SELECT outputs that are themselves a ``grouping()``/``grouping_id()``
    wrapper, keyed mode-insensitively so a HAVING grouping can point at one
    instead of injecting a duplicate output."""
    results: dict[tuple[Any, tuple[str, ...]], ConceptRef] = {}
    for item in select.selection:
        if not isinstance(item.content, ConceptTransform):
            continue
        fn = item.content.function
        if isinstance(fn, AggregateWrapper) and _is_grouping_aggregate(fn):
            results.setdefault(_grouping_arg_key(fn), item.content.output.reference)
    return results


def _substitute_window_grouping_outputs(select: SelectStatement) -> None:
    outputs = _existing_grouping_outputs(select)
    if not outputs:
        return

    def match(node: Any) -> ConceptRef | None:
        if not _is_grouping_aggregate(node):
            return None
        return outputs.get(_grouping_arg_key(node))

    for item in select.selection:
        if not isinstance(item.content, ConceptTransform):
            continue
        function = item.content.function
        if not isinstance(function, (NavigationWindowItem, NumberingWindowItem)):
            continue
        rewritten = _substitute_condition_tree(function, match)
        if rewritten is function:
            continue
        item.content.function = rewritten
        if not isinstance(
            item.content.output, (UndefinedConcept, UndefinedConceptFull)
        ):
            # swap, don't mutate: the output object may be the alias's
            # environment-registered definition
            item.content.output = dc_replace(item.content.output, lineage=rewritten)
            select.local_concepts[item.content.output.address] = item.content.output
        else:
            item.content.output.lineage = rewritten


def _promote_having_grouping_to_outputs(
    select: SelectStatement, context: RuleContext
) -> None:
    """A ``grouping()``/``grouping_id()`` in HAVING (restricting a rollup to
    specific levels) has no independent grain — its grain *is* the rollup's. But
    when a downstream CTE is introduced (e.g. a rowset-membership filter pushes
    the aggregate behind an extra layer), the bare ``grouping(col)`` strands in a
    groupless scope and DuckDB rejects it ("GROUPING function is not supported
    here"). Promote each such wrapper to a hidden SELECT output, which co-locates
    it in the ROLLUP CTE, and point the HAVING reference at that output — exactly
    the manual ``select ..., --grouping(...) as g having ... g ...`` workaround.

    Runs before the SELECT loop so the injected outputs are materialized through
    the normal machinery. Aggregates other than grouping are left to the existing
    HAVING validation: only grouping is unconditionally safe to co-locate.
    """
    if not select.having_clause:
        return
    spec = _select_rollup_spec(select, context)
    if spec is None:
        return
    wrappers = _collect_standard_grouping_wrappers(select.having_clause.conditional)
    if not wrappers:
        return
    reuse = _existing_grouping_outputs(select)
    sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef] = {}
    for wrapper in wrappers:
        # left un-stamped: the build factory applies the select's grouping spec
        # to every un-pinned aggregate in the projection scope
        sig = _aggregate_full_signature(wrapper)
        if sig is None or sig in sig_to_ref:
            continue
        existing = reuse.get(_grouping_arg_key(wrapper))
        if existing is not None:
            sig_to_ref[sig] = existing
            continue
        concept = arbitrary_to_concept(wrapper, context.environment)
        select.selection.append(
            SelectItem(
                content=ConceptTransform(function=wrapper, output=concept),
                modifiers=[Modifier.HIDDEN],
            )
        )
        # Seed local_concepts so grain calc (which runs next, before the SELECT
        # loop materializes the item) can resolve the freshly minted output.
        select.local_concepts[concept.address] = concept
        sig_to_ref[sig] = concept.reference

    def match(node: Any) -> ConceptRef | None:
        sig = _aggregate_full_signature(node)
        return sig_to_ref.get(sig) if sig is not None else None

    new_conditional = _substitute_condition_tree(
        select.having_clause.conditional, match
    )
    if new_conditional is not select.having_clause.conditional:
        select.having_clause.conditional = new_conditional


def _promotion_grows_grain(
    concept: Concept,
    base_targets: list[Any],
    base_components: set[str],
    context: RuleContext,
) -> bool:
    """True if adding ``concept`` as a SELECT output would enlarge the select
    grain. A bare/coarser aggregate broadcasts one value per grain row (grain
    unchanged); a *finer* off-grain aggregate (``agg(x) by k`` where ``k`` is
    finer than the grain) enters the grain as itself, which would fan the output
    out — so it must be semijoined, not promoted."""
    combined = Grain.from_concepts(
        base_targets + [concept],
        where_clause=None,
        environment=context.environment,
        local_concepts={concept.address: concept},
    )
    # Resolve the candidate's own address through the seeded map; every other
    # target is a concrete Concept or resolves via the environment, so the only
    # possible new component beyond ``base_components`` is the candidate itself.
    return bool(combined.components - base_components)


def _is_single_row_rowset_scalar(
    named: Concept,
    concepts: ConceptLookup | Mapping[str, Concept] | None = None,
    _seen: set[str] | None = None,
) -> bool:
    """True for a rowset output whose source select is grainless — it produces
    exactly one row and so broadcasts like a scalar (``rowset r <- select
    sum(x)/sum(y) as v``). Such an output is mis-tagged ``MULTI_ROW`` granularity
    (and ``ROWSET`` derivation, not ``AGGREGATE``), so the plain scalar-promotion
    gate misses it and the HAVING predicate gets misrouted into the finer-dim
    grain-key semijoin — which silently drops rollup subtotal rows (NULL-key
    collision) and doesn't even filter correctly on a plain grouped select.
    Restricted to a single (non-union) ``SelectLineage``: a union of grainless
    arms yields one row *per arm*, not a single broadcast row.

    Also true for a BASIC concept derived only from single-row rowset scalars
    (plus literals) — e.g. ``auto avg <- ov.total / ov.count``. Combining scalars
    stays a single broadcast row, but the wrapper is ``derivation=BASIC`` (not a
    ``RowsetItem``), so without recursing the wrapper the HAVING predicate is
    again misrouted into the semijoin and rollup subtotals vanish. This is the
    natural HAVING form (`having sum(x) > my_scalar`), so it must be covered.
    Requires ``concepts`` to resolve the BASIC args (they arrive as ``ConceptRef``)."""
    lineage = named.lineage
    if isinstance(lineage, RowsetItem):
        select = lineage.rowset.select
        return isinstance(select, SelectLineage) and not select.grain.components
    if concepts is not None and named.derivation == Derivation.BASIC:
        args = named.concept_arguments
        if not args:
            return False
        if _seen is None:
            _seen = set()
        if named.address in _seen:
            return False
        _seen.add(named.address)
        resolved = [concepts.get(a.address) for a in args]
        return all(
            r is not None and _is_single_row_rowset_scalar(r, concepts, _seen)
            for r in resolved
        )
    return False


def _promote_having_aggregates_to_outputs(
    select: SelectStatement, context: RuleContext
) -> None:
    """Mint a hidden SELECT output for each HAVING aggregate that is not already a
    SELECT output, and point the HAVING reference at it.

    A HAVING aggregate filters the post-aggregation rows, so it must be a
    materialized column. Rather than force the user to add a ``--agg(x)`` hidden
    column by hand, we add it for them. Promoting a *bare or coarser* aggregate is
    grain-safe: it broadcasts one value per grain row, so the select grain (and
    therefore the grain at which every projection aggregate computes) is
    unchanged. A *finer* off-grain aggregate (``agg(x) by k`` where ``k`` is finer
    than the select grain) would instead enter the grain as itself and fan the
    output out, so it is skipped here (``_promotion_grows_grain``) and left for the
    semijoin rewrite in ``_rewrite_having_finer_dims_to_membership``.

    Runs before the SELECT loop so the injected outputs go through
    ``set_select_grain`` like any other projection aggregate. ``grouping()`` is
    handled by ``_promote_having_grouping_to_outputs`` (it needs rollup-spec
    colocation) and is skipped here.
    """
    if not select.having_clause:
        return
    rename = {src: ref.address for src, ref in _alias_rename_map(select).items()}
    select_aggs = _select_aggregate_outputs(select, rename)
    existing = {sig for sig, _ in select_aggs}
    # Grain of the explicit (pre-promotion) outputs. Promoting an aggregate is
    # grain-safe only if its value is one-per-grain-row (a bare aggregate, or an
    # off-grain `agg(x) by k` whose `by` is coarser than / equal to this grain).
    # A *finer* off-grain aggregate (e.g. `count(x) by item` under a {brand}
    # grain) would otherwise enter the grain as itself and fan the output out, so
    # it is left for the semijoin rewrite instead of promoted.
    base_targets = [item.concept for item in select.selection]
    base_components = Grain.from_concepts(
        base_targets,
        where_clause=None,
        environment=context.environment,
        local_concepts=select.local_concepts,
    ).components
    sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef] = {}
    for node in _collect_condition_aggregates(select.having_clause.conditional):
        # Macros (`@rollup()`) and grouping indicators are materialized elsewhere;
        # only mint plain/off-grain/nested aggregate expressions here.
        if isinstance(node, FunctionCallWrapper) or _is_standard_grouping_aggregate(
            node
        ):
            continue
        sig = _aggregate_full_signature(node, rename)
        if sig is None or sig in existing or sig in sig_to_ref:
            continue
        concept = arbitrary_to_concept(node, context.environment)
        if _promotion_grows_grain(concept, base_targets, base_components, context):
            continue
        select.selection.append(
            SelectItem(
                content=ConceptTransform(function=node, output=concept),
                modifiers=[Modifier.HIDDEN],
            )
        )
        # Seed local_concepts so the grain calc that runs next can resolve it.
        select.local_concepts[concept.address] = concept
        sig_to_ref[sig] = concept.reference
    if sig_to_ref:
        select.having_clause.conditional = _substitute_having_aggregates(
            select.having_clause.conditional, sig_to_ref, rename
        )
    # A HAVING reference to a *named* aggregate concept (`auto m <- sum(x) by k`,
    # or a `@macro()` aggregate, used directly in HAVING) or to a scalar (a
    # single-row rowset output like a whole-table `avg(...)`) is the same case as
    # an inline aggregate: it must be materialized and broadcast at the select
    # grain. It arrives as a plain ConceptRef (not an aggregate node) so the loop
    # above misses it — add it as a hidden plain output here. Grain-safe: a metric
    # never contributes to grain and a single-row value is constant across every
    # row. A reference *finer* than the grain is left for the semijoin rewrite.
    # ``is_aggregate`` is False for a macro-wrapped aggregate, so key off
    # ``derivation`` too (and SINGLE_ROW granularity for scalars).
    output_addrs = {c.address for c in select.output_components}
    for ref in select.having_clause.row_arguments:
        if ref.address in output_addrs:
            continue
        named = context.concepts.get(ref.address)
        if named is None or isinstance(named, (UndefinedConcept, UndefinedConceptFull)):
            continue
        # Promote computed values (aggregates, macro-aggregates, windows) and
        # scalars; leave a *finer plain dimension* to the semijoin rewrite. A
        # window result, like an aggregate, must be materialized — semijoining it
        # would re-derive the window inside a filter (and can recurse).
        grain_determined = (
            named.is_aggregate
            or named.derivation in (Derivation.AGGREGATE, Derivation.WINDOW)
            or named.granularity == Granularity.SINGLE_ROW
            or _is_single_row_rowset_scalar(named, context.concepts)
        )
        if not grain_determined:
            continue
        select.selection.append(
            SelectItem(content=named.reference, modifiers=[Modifier.HIDDEN])
        )
        select.local_concepts[named.address] = named
        output_addrs.add(named.address)


def _bare_row_refs(node: Any) -> list[ConceptRef]:
    """Concept references of an expression reachable without crossing an
    aggregate or window boundary — the references that must exist as row-level
    columns of the post-aggregation result for the expression to evaluate.

    A ref *inside* an aggregate/window is not one: the computed value is
    materialized (or validated) separately and its operands are collapsed away
    by the grouping. This is the distinction between a raw concept being an
    operand of a selected expression and it being available at the output
    grain — ``count(x ? dim is not null) as c`` consumes ``dim`` but does not
    carry it through the group CTE, so a bare HAVING reference to ``dim`` still
    needs the semijoin rewrite (q72)."""
    if isinstance(node, ConceptRef):
        return [node]
    if isinstance(node, (AggregateWrapper, NavigationWindowItem, NumberingWindowItem)):
        return []
    if isinstance(node, Function):
        if (
            node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
            or node.operator in _GROUPING_FNS
        ):
            return []
        out: list[ConceptRef] = []
        for arg in node.arguments:
            out += _bare_row_refs(arg)
        return out
    if isinstance(node, FunctionCallWrapper):
        return _bare_row_refs(node.content)
    if isinstance(node, Parenthetical):
        return _bare_row_refs(node.content)
    if isinstance(node, SubselectComparison):
        # The existence RHS is sourced as an independent subselect; only the
        # row side must resolve at the output grain.
        return _bare_row_refs(node.left)
    if isinstance(node, (Comparison, Conditional)):
        return _bare_row_refs(node.left) + _bare_row_refs(node.right)
    if isinstance(node, Between):
        return (
            _bare_row_refs(node.left)
            + _bare_row_refs(node.low)
            + _bare_row_refs(node.high)
        )
    if isinstance(node, CaseWhen):
        return _bare_row_refs(node.comparison) + _bare_row_refs(node.expr)
    if isinstance(node, CaseElse):
        return _bare_row_refs(node.expr)
    if isinstance(node, FilterItem):
        return _bare_row_refs(node.content) + _bare_row_refs(node.where.conditional)
    return []


def _row_grain_allowed_addresses(select: SelectStatement) -> set[str]:
    """Addresses available as row-level references of the post-aggregation
    result: the outputs themselves plus operands of *scalar* select derivations
    (which stay at the output grain). Operands that appear only inside a
    selected aggregate or window are excluded — the grouping collapses them.
    Stricter than ``alias_source_addresses``, which intentionally exposes
    aggregate operands so an inline HAVING/ORDER BY aggregate can match its
    projected twin."""
    allowed = {x.address for x in select.output_components}
    for item in select.selection:
        if isinstance(item.content, ConceptTransform):
            for ref in _bare_row_refs(item.content.function):
                allowed.add(ref.address)
    return allowed


def _build_grain_key_membership(
    leaf: Comparison | Between,
    grain_keys: list[str],
    where_clause: WhereClause | None,
    context: RuleContext,
    line_no: int | None,
) -> SubselectComparison:
    """Turn a HAVING predicate on a non-output dimension into a semijoin against
    the select grain key(s): ``key in (filter key where <leaf>)`` for a single-key
    grain, or the row-wise composite ``(k1, k2) in (filter k1 where <leaf>, filter
    k2 where <leaf>)`` for a multi-key grain. The per-key filtered sets share the
    same predicate and are sourced together, so they stay correlated (the surviving
    key *combinations*, not the cross product).

    The select's WHERE clause is AND-ed into the filter so the existence set is
    evaluated within the same pre-aggregation universe the aggregate saw — a
    group only survives if it has a matching row *that also passed WHERE*."""
    if not grain_keys:
        raise InvalidSyntaxException(
            f"HAVING filters on a dimension outside the SELECT projection, but the "
            f"select has no grain key to anchor a post-aggregation semijoin (line "
            f"{line_no}). Move the filter to WHERE to filter before aggregation."
        )
    predicate: Any = leaf
    if where_clause is not None:
        predicate = Conditional(
            left=leaf, right=where_clause.conditional, operator=BooleanOperator.AND
        )
    key_refs: list[ConceptRef] = []
    filtered_refs: list[ConceptRef] = []
    for addr in grain_keys:
        key_concept = context.concepts.get(addr)
        if key_concept is None or isinstance(
            key_concept, (UndefinedConcept, UndefinedConceptFull)
        ):
            raise InvalidSyntaxException(
                f"HAVING filters on a dimension outside the SELECT projection but "
                f"the select grain key '{addr}' could not be resolved to build the "
                f"semijoin (line {line_no}). Move the filter to WHERE instead."
            )
        key_ref = key_concept.reference
        filtered = arbitrary_to_concept(
            FilterItem(content=key_ref, where=WhereClause(conditional=predicate)),
            context.environment,
        )
        context.add_virtual_concept(filtered)
        key_refs.append(key_ref)
        filtered_refs.append(filtered.reference)
    if len(grain_keys) == 1:
        left: Any = key_refs[0]
        right: Any = filtered_refs[0]
    else:
        left = row_tuple_function(list(key_refs))
        right = row_tuple_function(list(filtered_refs))
    return SubselectComparison(left=left, right=right, operator=ComparisonOperator.IN)


def _rewrite_having_finer_dims_to_membership(
    select: SelectStatement,
    context: RuleContext,
    allowed_addresses: set[str],
    line_no: int | None,
) -> None:
    """Rewrite each HAVING predicate on a dimension outside the select grain into a
    post-aggregation semijoin ``key in (filter key where <predicate>)``.

    HAVING filters *after* aggregation, so a finer-grain predicate must never
    change the value of a select-grain aggregate — it may only decide which
    select-grain rows survive (those that have at least one matching finer row).
    Filtering the select grain *key* against the keys that satisfy the predicate
    is exactly that, and — because it filters whole groups rather than rows within
    a group — it is invariant to predicate pushdown. The membership reuses the
    existing existence machinery, which sources the filtered set as an independent
    subquery that never contaminates the aggregate.

    A leaf needs the rewrite under either rule:

    - any of its *bare* row references (outside aggregates/windows) is not
      materialized at the output grain (``_row_grain_allowed_addresses``). The
      broader ``allowed_addresses`` cannot decide this: it includes aggregate
      operands, but ``sum(x ? dim = 1) as c`` consumes ``dim`` without carrying
      it through the group CTE, so a bare HAVING ``dim`` would otherwise slip
      through unrewritten and either get pushed pre-aggregation (silent wrong
      result) or reach rendering with no source (INVALID_REFERENCE_BUG, q72);
    - any reference at all (including inside a finer off-grain aggregate that
      ``_promote_having_aggregates_to_outputs`` deliberately skipped) is outside
      ``allowed_addresses`` — the pre-existing rule.
    """
    if not select.having_clause:
        return
    grain_keys = sorted(select.grain.components)
    stable_grain_keys = []
    window_keys: set[str] = set()
    for address in grain_keys:
        concept = select.local_concepts.get(address) or context.concepts.get(address)
        lineage = getattr(concept, "lineage", None)
        if isinstance(lineage, (NavigationWindowItem, NumberingWindowItem)):
            window_keys |= _window_key_addresses(lineage)
            continue
        if lineage is not None and _expression_contains_window(lineage):
            window_keys |= _window_key_addresses(lineage)
            continue
        stable_grain_keys.append(address)
    stable_grain_keys.extend(sorted(window_keys - set(stable_grain_keys)))
    if stable_grain_keys:
        grain_keys = stable_grain_keys
    row_grain_allowed = _row_grain_allowed_addresses(select)

    def needs_membership(leaf: Any) -> bool:
        # SubselectComparison subclasses Comparison: an existing `x in <set>`
        # whose *row* side x is a non-output dimension is wrapped too (its
        # row_arguments exclude the existence RHS, so only the row side counts).
        if not isinstance(leaf, (Comparison, Between)):
            return False
        extra = [r for r in leaf.row_arguments if r.address not in allowed_addresses]
        extra += [r for r in _bare_row_refs(leaf) if r.address not in row_grain_allowed]
        if not extra:
            return False
        # A genuinely undefined reference is reported by the missing-ref check;
        # don't wrap it.
        return not any(_is_unresolved(context, r.address) for r in extra)

    built_membership = False

    def transform(node: Any) -> Any:
        nonlocal built_membership
        if isinstance(node, Conditional):
            new_left = transform(node.left)
            new_right = transform(node.right)
            if new_left is node.left and new_right is node.right:
                return node
            return Conditional(left=new_left, right=new_right, operator=node.operator)
        if isinstance(node, Parenthetical):
            new_content = transform(node.content)
            if new_content is node.content:
                return node
            return Parenthetical(content=new_content)
        if needs_membership(node):
            built_membership = True
            return _build_grain_key_membership(
                node, grain_keys, select.where_clause, context, line_no
            )
        return node

    select.having_clause.conditional = transform(select.having_clause.conditional)

    # The membership left side is the full select-grain key tuple, but a grain key
    # need not be a projected output (e.g. a window's `order by`/`partition by`
    # key that's pulled into the grain but not selected). Such a key is absent from
    # the final CTE, so the rendered left tuple resolves to INVALID_REFERENCE_BUG.
    # Materialize any unprojected grain key as a hidden output so it survives to the
    # final CTE for the semijoin to reference.
    if built_membership:
        output_addrs = {c.address for c in select.output_components}
        for addr in grain_keys:
            if addr in output_addrs:
                continue
            key_concept = context.concepts.get(addr)
            if key_concept is None or isinstance(
                key_concept, (UndefinedConcept, UndefinedConceptFull)
            ):
                continue
            select.selection.append(
                SelectItem(content=key_concept.reference, modifiers=[Modifier.HIDDEN])
            )
            select.local_concepts[key_concept.address] = key_concept
            output_addrs.add(key_concept.address)


def _order_match_signature(
    node: Any, rename: Mapping[str, str]
) -> tuple[Any, ...] | None:
    """Signature for matching an ORDER BY aggregate to a projected SELECT output,
    normalizing pure-rename args through ``rename``. ``grouping()``/
    ``grouping_id()`` compare mode-insensitively (drop the ``by`` component): the
    rollup-mode alignment ``_fix_projection_grouping_mode`` applies to projections
    but not ORDER BY is irrelevant to the expression's identity."""
    base = _aggregate_full_signature(node)
    if base is None:
        return None
    op, args, by = base
    nargs = tuple(rename.get(a, a) for a in args)
    if op in _GROUPING_FNS:
        return (op, nargs)
    return (op, nargs, tuple(sorted(rename.get(b, b) for b in by)))


def _order_expr_signature(
    node: Any, rename: Mapping[str, str]
) -> tuple[Any, ...] | None:
    """Full structural signature of an ORDER BY expression, composing the
    window/aggregate/scalar signatures so a compound of aggregates (e.g.
    ``grouping(a) + grouping(b)``) can match a projected compound output."""
    if isinstance(node, Parenthetical):
        return _order_expr_signature(node.content, rename)
    wsig = _window_signature(node, rename)
    if wsig is not None:
        return ("win", wsig)
    asig = _order_match_signature(node, rename)
    if asig is not None:
        return ("agg", asig)
    if isinstance(node, ConceptRef):
        return ("ref", rename.get(node.address, node.address))
    if isinstance(node, Function):
        arg_sigs = []
        for arg in node.arguments:
            sig = _order_expr_signature(arg, rename)
            if sig is None:
                return None
            arg_sigs.append(sig)
        return ("fn", node.operator, tuple(arg_sigs))
    if isinstance(node, (AggregateWrapper, NavigationWindowItem, NumberingWindowItem)):
        return None
    return ("lit", type(node).__name__, str(node))


def _select_order_expr_outputs(
    select: SelectStatement, rename: Mapping[str, str]
) -> dict[tuple[Any, ...], ConceptRef]:
    """Map each expression-producing select item's full signature to its output
    ref (first wins). Bare renames are handled by ``_alias_rename_map``; only
    derived shapes (functions, aggregates, windows) register here."""
    results: dict[tuple[Any, ...], ConceptRef] = {}
    for item in select.selection:
        if not isinstance(item.content, ConceptTransform):
            continue
        sig = _order_expr_signature(item.content.function, rename)
        if sig is not None and sig[0] in ("fn", "agg", "win"):
            results.setdefault(sig, item.content.output.reference)
    return results


def _substitute_order_by_outputs(select: SelectStatement) -> None:
    """Rewrite ORDER BY expressions that match a projected SELECT output —
    windows, scalar-derived expressions, aggregates, and compounds thereof — to
    reference that output's alias. The ordering scope operates over the
    projected rows, so an inline expression identical to a (possibly hidden)
    output is just sorting by that materialized column; re-deriving it instead
    re-references source columns a grouped final node no longer exposes
    (ungrouped-column binder errors). Mirrors the ``_substitute_having_*``
    passes."""
    if not select.order_by:
        return
    rename = {src: ref.address for src, ref in _alias_rename_map(select).items()}
    sig_to_ref = _select_order_expr_outputs(select, rename)
    if not sig_to_ref:
        return

    def match(node: Any) -> ConceptRef | None:
        if isinstance(node, ConceptRef):
            return None
        sig = _order_expr_signature(node, rename)
        return sig_to_ref.get(sig) if sig is not None else None

    new_items: list[OrderItem] = []
    changed = False
    for item in select.order_by.items:
        new_expr = _substitute_condition_tree(item.expr, match)
        if new_expr is not item.expr:
            changed = True
            new_items.append(OrderItem(expr=new_expr, order=item.order))
        else:
            new_items.append(item)
    if changed:
        select.order_by = OrderBy(items=new_items)


def _validate_order_by_aggregates(select: SelectStatement, line_no: int | None) -> None:
    """Reject inline aggregates in ORDER BY that are not SELECT outputs.
    ORDER BY may only reference an aggregate via its SELECT
    alias, so an inline one that has no matching projection is an error — add it
    to SELECT (``--`` to keep it out of the output rows) and order by the alias.
    """
    if not select.order_by:
        return
    sig_to_alias = {
        sig: _strip_local_namespace(addr)
        for sig, addr in _select_aggregate_outputs(select)
    }
    for item in select.order_by.items:
        aggregates = _collect_condition_aggregates(item.expr)
        if not aggregates:
            continue
        rendered = _render_aggregate(aggregates[0])
        sig = _aggregate_full_signature(aggregates[0])
        alias = sig_to_alias.get(sig) if sig is not None else None
        if alias is not None:
            hint = (
                f"order by the SELECT alias `{alias}` instead (`order by {alias} desc`)"
            )
        else:
            hint = (
                f"add it to SELECT — prefix with `--` to keep it out of the output "
                f"rows — and order by the alias, e.g. "
                f"`select ..., --{rendered} as g order by g desc`"
            )
        raise InvalidSyntaxException(
            f"ORDER BY contains aggregate `{rendered}` (line {line_no}), which is "
            f"not a SELECT output. Aggregates cannot be computed in the ordering "
            f"scope; {hint}."
        )


def _validate_syntax(select: SelectStatement, context: RuleContext) -> None:
    line_no = select.meta.line_number if select.meta else None
    if select.where_clause:
        replacements: list[tuple[str, ConceptRef]] = []
        for x in select.where_clause.concept_arguments:
            if isinstance(x, UndefinedConcept):
                validate = context.concepts.get(x.address)
                if validate:
                    replacements.append((x.address, validate.reference))
                else:
                    _raise_undefined(
                        context,
                        x.address,
                        x.metadata.line_number if x.metadata else None,
                    )
        if replacements:
            select.where_clause = select.where_clause.with_reference_replacement(
                replacements
            )
    _rewrite_aliased_source_refs(select)
    all_in_output = {x.address for x in select.output_components}
    locally_derived = select.locally_derived
    alias_sources = select.alias_source_addresses
    allowed_addresses = all_in_output | alias_sources
    # A WHERE-clause aggregate that equals a SELECT output is only restricted for a
    # SCALAR select (no grouping key). With a concrete grain the WHERE aggregate
    # computes at the select grain over the WHERE-unfiltered universe (its own CTE),
    # a valid pre-aggregation gate distinct from the post-filter SELECT/HAVING
    # aggregate. A scalar select has no grain to anchor that gate and the planner
    # drops sibling row filters, so keep redirecting it to HAVING.
    if select.where_clause and not select.grain.components:
        for cref in select.where_clause.concept_arguments:
            concept = context.concepts.get(cref.address)
            if concept is None or isinstance(concept, UndefinedConcept):
                continue
            lineage = concept.lineage
            is_aggregate = False
            if isinstance(lineage, Function) and (
                lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
            ):
                is_aggregate = True
            if isinstance(lineage, AggregateWrapper) and (
                lineage.function.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
            ):
                is_aggregate = True
            if is_aggregate and concept.address in locally_derived:
                raise SyntaxError(
                    f"Cannot reference an aggregate derived in the select "
                    f"({concept.address}) in the same statement where clause; "
                    f"move to the HAVING clause instead; Line: {line_no}"
                )
        _validate_where_aggregate_matches_select(select, line_no)
    if select.having_clause:
        # Point HAVING windows at their materialized SELECT alias before
        # validating refs, so the window's inner aggregate input isn't treated
        # as a required (and absent) output column.
        _substitute_having_windows(select)
        # Point HAVING scalar-derived expressions (e.g. coalesce) at their SELECT
        # alias too, so a filter on a derived output resolves to the materialized
        # column instead of re-deriving its (now-absent) inner argument.
        _substitute_having_derived(select)
        # Rewrite a HAVING predicate on a dimension outside the select grain into a
        # post-aggregation semijoin on the grain key, so it filters which groups
        # survive without changing any select-grain aggregate. After this the only
        # row references left are SELECT outputs and the grain key.
        _rewrite_having_finer_dims_to_membership(
            select, context, allowed_addresses, line_no
        )
        # The membership left side is the select grain key, which need not be a
        # projected output — so the surviving non-output references are the grain
        # keys. Anything else is either a typo (undefined) or a dimension we could
        # not turn into a semijoin; report both with actionable guidance. Use
        # row_arguments, not concept_arguments: an `x in <set>` membership's
        # existence RHS is sourced as a subselect at plan time and need not be
        # projected — only the row side must resolve.
        allowed_for_having = (
            allowed_addresses
            | set(select.grain.components)
            | {x.address for x in select.output_components}
        )
        # Bare row references (outside aggregates/windows) are held to the
        # stricter standard the semijoin rewrite used: they must be materialized
        # at the output grain, not merely be an operand of some selected
        # aggregate. Anything the rewrite left bare and unmaterialized would
        # otherwise reach planning with no backing column — pushed
        # pre-aggregation or rendered as a missing-source sentinel (q72).
        row_grain_allowed_for_having = _row_grain_allowed_addresses(select) | set(
            select.grain.components
        )
        flagged = [
            cref.address
            for cref in select.having_clause.row_arguments
            if cref.address not in allowed_for_having
        ]
        flagged += [
            cref.address
            for cref in _bare_row_refs(select.having_clause.conditional)
            if cref.address not in row_grain_allowed_for_having
        ]
        undefined_refs: list[str] = []
        unhandled_refs: list[str] = []
        for address in flagged:
            if _is_unresolved(context, address):
                if address not in undefined_refs:
                    undefined_refs.append(address)
            elif address not in unhandled_refs:
                unhandled_refs.append(address)
        if undefined_refs:
            refs = ", ".join(f"'{a}'" for a in undefined_refs)
            verb = "is" if len(undefined_refs) == 1 else "are"
            raise SyntaxError(
                f"HAVING references {refs}, which {verb} not defined (line "
                f"{line_no}). Check for a typo or import the relevant concept."
            )
        if unhandled_refs:
            refs = ", ".join(f"'{a}'" for a in unhandled_refs)
            snippet = ", ".join(f"--{a}" for a in unhandled_refs)
            raise SyntaxError(
                f"HAVING references {refs} outside the SELECT projection and could "
                f"not be resolved as a post-aggregation filter (line {line_no}). "
                f"Move the filter to WHERE to filter before aggregation, or add it "
                f"to the SELECT (prefix with `--` to keep it out of the output "
                f"rows):\n    select <your existing columns>, {snippet}"
            )
        _validate_having_aggregates_match_select(select, context, line_no)
    if select.order_by:
        # Resolve inline ORDER BY expressions that match a projected output
        # (windows, scalar derivations, aggregates) to that output's alias
        # before validating refs, so an expression identical to a (possibly
        # hidden) projection sorts by the materialized column rather than
        # re-deriving source columns the final node no longer exposes.
        _substitute_order_by_outputs(select)
        for cref in select.order_by.concept_arguments:
            if cref.address not in allowed_addresses:
                raise SyntaxError(
                    f"ORDER BY references '{cref.address}', which is not in the "
                    f"SELECT projection (line {line_no}). Add it to SELECT to "
                    f"sort by it — prefix with `--` to keep it out of the output "
                    f"rows, e.g. `select ..., --{cref.address} "
                    f"order by {cref.address} asc`."
                )
        _validate_order_by_aggregates(select, line_no)


def finalize_select_statement(
    select: SelectStatement,
    context: RuleContext,
) -> None:
    """v2-native equivalent of ``SelectStatement.finalize(environment)``.

    Uses ``context.concepts`` for all concept reads and routes
    ``select_as_definition`` writes through SemanticState so no parse-time
    mutation of ``environment.concepts`` is required.
    """
    # Push the SELECT-level `by rollup (…)` spec onto every un-grouped projection
    # aggregate before grain calc, so the select grain reflects the grouping keys
    # and all measures share one grouping pass.
    _propagate_select_grouping(select, context)
    _substitute_window_grouping_outputs(select)
    # Promote any HAVING grouping() to a hidden output before the SELECT loop so
    # it materializes in the ROLLUP CTE instead of stranding downstream.
    _promote_having_grouping_to_outputs(select, context)
    # Promote any other HAVING aggregate not already projected to a hidden output
    # (grain-safe: metrics don't change the select grain) so HAVING can filter on
    # it without the user adding a `--agg(x)` column by hand.
    _promote_having_aggregates_to_outputs(select, context)
    merged = _merged_local_concepts(select, context)
    select.grain = _calculate_grain(select, context, merged)
    output_addresses: set[str] = set()
    line_no = select.meta.line_number if select.meta else None
    # Accumulate every undefined reference (across SELECT / WHERE / ORDER BY)
    # and raise once at the end, rather than aborting on the first.
    undefined: list[_UndefinedRef] = []
    for x in select.selection:
        if x.is_undefined:
            undefined.append(_undefined_ref(x.concept, "SELECT", line_no))
            continue
        elif isinstance(x.content, ConceptTransform):
            # Validate transform inputs: an undefined concept used only as a
            # function argument (e.g. `sum(missing) as foo`) has a defined
            # output alias, so it would otherwise slip past to SQL generation.
            # Args hydrated through concept resolution carry no token position,
            # so fall back to the select ITEM's line (stamped on the output
            # concept) rather than the statement's first line, which for a
            # `where ... select ...` statement points at the `where` keyword.
            out_meta = x.content.output.metadata
            item_line = (
                out_meta.line_number if out_meta and out_meta.line_number else line_no
            )
            bad_arg = False
            for arg in x.content.function.concept_arguments:
                if isinstance(
                    arg, (UndefinedConcept, UndefinedConceptFull)
                ) and _is_unresolved(context, arg.address):
                    undefined.append(_undefined_ref(arg, "SELECT", item_line))
                    bad_arg = True
            if bad_arg:
                continue
            if isinstance(x.content.output, UndefinedConcept):
                continue
            # A SELECT output whose expression references its own alias address
            # (`select <expr> as foo` where `<expr>` reads `foo`, e.g. an
            # existing `auto foo <- ...`) is a recursive binding: the name `foo`
            # would mean both the original concept (inside the expression) and
            # the new output. The planner keys concepts by address and cannot
            # represent both, so it would silently emit the original. Raise
            # instead — renaming the output is the fix (the alias stays visible
            # to sibling calculations, so renaming the input is not an option).
            out_addr = x.concept.address
            if any(
                arg.address == out_addr for arg in x.content.function.concept_arguments
            ):
                raise InvalidSyntaxException(
                    f"SELECT output '{out_addr}' is defined by an expression that "
                    f"references '{out_addr}' itself (line {line_no or 'unknown'}). "
                    f"This is a recursive self-reference: an alias cannot redefine "
                    f"a name its own calculation reads. Rename the output to a "
                    f"distinct name (e.g. `... as {x.content.output.name}_out`)."
                )
            if CONFIG.parsing.select_as_definition and not context.environment.frozen:
                existing = context.concepts.get(x.concept.address)
                meta: Any = x.content.output.metadata
                if existing is None:
                    context.add_top_level_concept(x.content.output, meta=meta)
                elif (
                    existing.metadata
                    and existing.metadata.concept_source == ConceptSource.SELECT
                ):
                    context.semantic_state.replace_concept(
                        x.concept.address,
                        x.content.output,
                        ConceptUpdateKind.TOP_LEVEL_DECLARATION,
                        meta=meta,
                    )
            adjusted = x.content.output.set_select_grain(
                select.grain,
                context.environment,
                # under a select-level grouping spec, bare aggregates stay
                # un-pinned so the build factory can stamp the spec onto them
                pin_bare_aggregates=select.grouping is None,
            )
            x.content.output = adjusted
            select.local_concepts[adjusted.address] = adjusted
            merged[adjusted.address] = adjusted
            if adjusted.address in output_addresses:
                raise InvalidSyntaxException(
                    f"Duplicate select output for {adjusted.address}; "
                    f"Line: {line_no or 'unknown'}"
                )
            output_addresses.add(adjusted.address)
        else:
            addr = x.concept.address
            # prefer a select-local override (e.g. a grouping-spec-stamped
            # clone) over the shared environment definition
            resolved = select.local_concepts.get(addr) or context.concepts.get(addr)
            if resolved is None or isinstance(
                resolved, (UndefinedConcept, UndefinedConceptFull)
            ):
                undefined.append(_undefined_ref(x.content, "SELECT", line_no))
                continue
            select.local_concepts[addr] = resolved
            if addr in output_addresses:
                raise InvalidSyntaxException(
                    f"Duplicate select output for {addr}; Line: {line_no or 'unknown'}"
                )
            output_addresses.add(addr)
    # WHERE / ORDER BY are checked after the SELECT loop so references to
    # select-defined aliases (added above) resolve before being flagged.
    if select.where_clause:
        undefined += collect_clause_undefined(
            context, "WHERE", select.where_clause.concept_arguments, line_no
        )
    if select.order_by:
        undefined += collect_clause_undefined(
            context, "ORDER BY", select.order_by.concept_arguments, line_no
        )
    if undefined:
        raise_collected_undefined(context, undefined)
    select.grain = _calculate_grain(select, context, merged)
    _validate_syntax(select, context)


def finalize_select_tree(
    output: Any,
    context: RuleContext,
) -> None:
    """v2-native equivalent of the legacy ``finalize_select_tree`` helper.

    Walks a statement output and finalizes any nested SelectStatements
    without mutating the base Environment's concept dictionary.
    """
    if output is None:
        return
    if isinstance(output, SelectStatement):
        finalize_select_statement(output, context)
    elif isinstance(output, MultiSelectStatement):
        for sel in output.selects:
            finalize_select_statement(sel, context)
    elif isinstance(output, PersistStatement):
        finalize_select_tree(output.select, context)
    elif isinstance(output, RowsetDerivationStatement):
        finalize_select_tree(output.select, context)
