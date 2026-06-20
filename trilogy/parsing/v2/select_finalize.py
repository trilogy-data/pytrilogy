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

from typing import Any, Iterable, Mapping, NamedTuple, cast

from trilogy.constants import CONFIG
from trilogy.core.enums import (
    AggregateGroupingMode,
    ConceptSource,
    FunctionClass,
    FunctionType,
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
    Function,
    Grain,
    OrderBy,
    OrderItem,
    Parenthetical,
    UndefinedConcept,
    UndefinedConceptFull,
)
from trilogy.core.models.environment import UndefinedConceptException
from trilogy.core.statements.author import (
    ConceptTransform,
    MultiSelectStatement,
    PersistStatement,
    RowsetDerivationStatement,
    SelectStatement,
)
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.semantic_state import ConceptUpdateKind


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


def _concept_address(c: Any) -> str:
    if isinstance(c, (AggregateWrapper, Function)):
        nested = _render_aggregate(c)
        if nested:
            return nested
    return c.address if hasattr(c, "address") else str(c)


def _aggregate_full_signature(
    node: Any,
) -> tuple[Any, tuple[str, ...], tuple[str, ...]] | None:
    """Signature (operator, args, by) for matching aggregates across SELECT/WHERE.

    Returns ``None`` for non-aggregate nodes.
    """
    if isinstance(node, AggregateWrapper):
        return (
            node.function.operator,
            tuple(_concept_address(a) for a in node.function.arguments),
            tuple(sorted(_concept_address(c) for c in node.by)),
        )
    if isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        return (
            node.operator,
            tuple(_concept_address(a) for a in node.arguments),
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
) -> list[tuple[tuple[Any, tuple[str, ...], tuple[str, ...]], str]]:
    """Return ``(signature, output_address)`` for aggregate-producing select items."""
    results: list[tuple[tuple[Any, tuple[str, ...], tuple[str, ...]], str]] = []
    for item in select.selection:
        if isinstance(item.content, ConceptTransform):
            sig = _aggregate_full_signature(item.content.function)
            if sig is not None:
                results.append((sig, item.content.output.address))
    return results


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


def _substitute_having_aggregates(
    node: Any,
    sig_to_ref: dict[tuple[Any, tuple[str, ...], tuple[str, ...]], ConceptRef],
) -> Any:
    """Rewrite a HAVING conditional tree, replacing matched aggregates with
    ``ConceptRef`` to the SELECT alias. Aggregates are leaves in our walk (we
    do not descend into their argument expressions)."""
    sig = _aggregate_full_signature(node)
    if sig is not None:
        ref = sig_to_ref.get(sig)
        if ref is not None:
            return ref
        return node
    if isinstance(node, Comparison):
        return Comparison(
            left=_substitute_having_aggregates(node.left, sig_to_ref),
            right=_substitute_having_aggregates(node.right, sig_to_ref),
            operator=node.operator,
        )
    if isinstance(node, Conditional):
        return Conditional(
            left=_substitute_having_aggregates(node.left, sig_to_ref),
            right=_substitute_having_aggregates(node.right, sig_to_ref),
            operator=node.operator,
        )
    if isinstance(node, Parenthetical):
        return Parenthetical(
            content=_substitute_having_aggregates(node.content, sig_to_ref)
        )
    if isinstance(node, Between):
        return Between(
            left=_substitute_having_aggregates(node.left, sig_to_ref),
            low=_substitute_having_aggregates(node.low, sig_to_ref),
            high=_substitute_having_aggregates(node.high, sig_to_ref),
        )
    if isinstance(node, Function):
        new_args = [
            _substitute_having_aggregates(a, sig_to_ref) for a in node.arguments
        ]
        if all(a is b for a, b in zip(new_args, node.arguments)):
            return node
        replacement = Function.__new__(Function)
        replacement.__dict__.update(node.__dict__)
        replacement.arguments = new_args
        return replacement
    return node


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
    select_aggs = _select_aggregate_outputs(select)
    sig_to_alias_addr = {sig: addr for sig, addr in select_aggs}
    for node in _collect_condition_aggregates(select.having_clause.conditional):
        sig = _aggregate_full_signature(node)
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
            select.having_clause.conditional, sig_to_ref
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


def _select_rollup_spec(
    select: SelectStatement,
    context: RuleContext,
) -> tuple[AggregateGroupingMode, list[Any], list[list[Any]]] | None:
    """The query's non-STANDARD grouping spec (mode, by, grouping_sets) taken
    from a ROLLUP/CUBE/GROUPING SETS aggregate referenced in the SELECT
    projection (inline or by name)."""
    for item in select.selection:
        lineage = _item_lineage(item, context)
        if (
            isinstance(lineage, AggregateWrapper)
            and lineage.grouping != AggregateGroupingMode.STANDARD
        ):
            return (
                lineage.grouping,
                list(lineage.by),
                [list(g) for g in lineage.grouping_sets],
            )
    return None


def _is_standard_grouping_aggregate(node: Any) -> bool:
    return (
        isinstance(node, AggregateWrapper)
        and node.grouping == AggregateGroupingMode.STANDARD
        and node.function.operator in _GROUPING_FNS
    )


def _collect_standard_grouping_wrappers(node: Any) -> list[AggregateWrapper]:
    """All STANDARD-mode ``grouping()``/``grouping_id()`` wrappers anywhere in an
    expression tree (e.g. nested inside a ``case`` that derives a rollup level)."""
    if _is_standard_grouping_aggregate(node):
        return [cast(AggregateWrapper, node)]
    found: list[AggregateWrapper] = []
    if isinstance(node, Comparison):
        found += _collect_standard_grouping_wrappers(node.left)
        found += _collect_standard_grouping_wrappers(node.right)
    elif isinstance(node, Conditional):
        found += _collect_standard_grouping_wrappers(node.left)
        found += _collect_standard_grouping_wrappers(node.right)
    elif isinstance(node, Parenthetical):
        found += _collect_standard_grouping_wrappers(node.content)
    elif isinstance(node, Between):
        found += _collect_standard_grouping_wrappers(node.left)
        found += _collect_standard_grouping_wrappers(node.low)
        found += _collect_standard_grouping_wrappers(node.high)
    elif isinstance(node, CaseWhen):
        found += _collect_standard_grouping_wrappers(node.comparison)
        found += _collect_standard_grouping_wrappers(node.expr)
    elif isinstance(node, CaseElse):
        found += _collect_standard_grouping_wrappers(node.expr)
    elif isinstance(node, AggregateWrapper):
        for arg in node.function.arguments:
            found += _collect_standard_grouping_wrappers(arg)
    elif isinstance(node, Function):
        for arg in node.arguments:
            found += _collect_standard_grouping_wrappers(arg)
    return found


def _fix_projection_grouping_mode(
    select: SelectStatement, context: RuleContext
) -> None:
    """``grouping()``/``grouping_id()`` are only meaningful inside the scope that
    performs the ROLLUP/CUBE/GROUPING SETS. They parse with the default STANDARD
    grouping mode, which keys them into a separate planner bucket from the rollup
    aggregate; they then strand in a downstream groupless join/filter CTE and
    render as ``grouping(col)`` with no GROUP BY (DuckDB: "GROUPING statement
    cannot be used without groups").

    A ``grouping()`` reached from the SELECT — directly, or nested inside a
    ``case`` deriving the subtotal level, the TPC-DS-native idiom — just needs
    its mode aligned with the query's rollup spec so it co-locates with the
    rollup aggregate. Mutates each wrapper in place (they are dataclasses) so
    by-name ``auto`` concepts are fixed at their canonical address.
    """
    spec = _select_rollup_spec(select, context)
    if spec is None:
        return
    mode, by, grouping_sets = spec
    for sitem in select.selection:
        lineage = _item_lineage(sitem, context)
        if lineage is None:
            continue
        for wrapper in _collect_standard_grouping_wrappers(lineage):
            wrapper.by = list(by)
            wrapper.grouping = mode
            wrapper.grouping_sets = [list(g) for g in grouping_sets]


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
    if select.where_clause:
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
        # Report ALL missing refs at once (deduped, in order). Raising on the
        # first one makes an agent fix it, re-run, hit the next, and loop — one
        # message listing every missing ref collapses those rewrite cycles.
        missing: list[str] = []
        for cref in select.having_clause.concept_arguments:
            if cref.address not in allowed_addresses and cref.address not in missing:
                missing.append(cref.address)
        if missing:
            refs = ", ".join(f"'{a}'" for a in missing)
            verb, obj, subj, stay = (
                ("is", "it", "it", "stays")
                if len(missing) == 1
                else ("are", "them", "they", "stay")
            )
            snippet = ", ".join(f"--{a}" for a in missing)
            raise SyntaxError(
                f"HAVING references {refs}, which {verb} not in the SELECT "
                f"projection (line {line_no}). To filter output rows, add {obj} to "
                f"SELECT — prefix each with `--` so {subj} {stay} out of the output "
                f"rows, keeping your HAVING as-is:\n"
                f"    select <your existing columns>, {snippet}\n"
                f"Or move {obj} to WHERE to filter before aggregation; for an "
                f"aggregate condition on a non-output grain, write `agg(x) by grain` "
                f"inline in WHERE."
            )
        _validate_having_aggregates_match_select(select, context, line_no)
    if select.order_by:
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
            bad_arg = False
            for arg in x.content.function.concept_arguments:
                if isinstance(
                    arg, (UndefinedConcept, UndefinedConceptFull)
                ) and _is_unresolved(context, arg.address):
                    undefined.append(_undefined_ref(arg, "SELECT", line_no))
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
                select.grain, context.environment
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
            resolved = context.concepts.get(addr)
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
    _fix_projection_grouping_mode(select, context)
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
