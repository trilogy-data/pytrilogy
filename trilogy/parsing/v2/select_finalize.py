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

from typing import Any, Mapping

from trilogy.constants import CONFIG
from trilogy.core.enums import ConceptSource, FunctionClass
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AggregateWrapper,
    Between,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    Grain,
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
        matches = context.environment.concepts._find_similar_concepts(address)
    except Exception:
        matches = []
    message = f"Undefined concept: {address}."
    if matches:
        message += f" Suggestions: {matches}"
    if line_no:
        raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
    raise UndefinedConceptException(message, matches)


def _calculate_grain(
    select: SelectStatement,
    context: RuleContext,
    local_concepts: Mapping[str, Concept],
) -> Grain:
    targets = [item.concept for item in select.selection]
    return Grain.from_concepts(
        targets,
        where_clause=select.where_clause,
        environment=context.environment,
        local_concepts=local_concepts,
    )


def _aggregate_grain_signature(agg: AggregateWrapper) -> tuple[str, ...]:
    """A sortable signature of an aggregate's grouping. Empty tuple = abstract."""
    return tuple(sorted({_concept_address(c) for c in agg.by}))


def _concept_address(c: Any) -> str:
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
        return str(node)
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


def _collect_where_aggregates(node: Any) -> list[tuple[Any, tuple[str, ...]]]:
    """Walk a WHERE conditional tree and return (aggregate_node, grain_signature) pairs.

    Recognizes both ``AggregateWrapper`` and bare ``Function`` calls whose operator
    is an aggregate function. Does not descend into the inner argument expression
    of an aggregate (a nested aggregate would be invalid SQL anyway).
    """
    found: list[tuple[Any, tuple[str, ...]]] = []
    if isinstance(node, AggregateWrapper):
        found.append((node, _aggregate_grain_signature(node)))
        return found
    if isinstance(node, Function) and (
        node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
    ):
        found.append((node, ()))
        return found
    if isinstance(node, Comparison):
        found.extend(_collect_where_aggregates(node.left))
        found.extend(_collect_where_aggregates(node.right))
    elif isinstance(node, Conditional):
        found.extend(_collect_where_aggregates(node.left))
        found.extend(_collect_where_aggregates(node.right))
    elif isinstance(node, Parenthetical):
        found.extend(_collect_where_aggregates(node.content))
    elif isinstance(node, Between):
        found.extend(_collect_where_aggregates(node.left))
        found.extend(_collect_where_aggregates(node.low))
        found.extend(_collect_where_aggregates(node.high))
    elif isinstance(node, Function):
        for arg in node.arguments:
            found.extend(_collect_where_aggregates(arg))
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
    for node, _ in _collect_where_aggregates(select.where_clause.conditional):
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


def _validate_where_aggregate_grains(
    select: SelectStatement, line_no: int | None
) -> None:
    """Reject WHERE clauses that mix aggregates at incompatible grains.

    Inline aggregates in WHERE are valid in Trilogy when they share a grain
    (e.g. ``where sum(x) by name > avg(y) by name``). Mixing grains - or
    comparing a naked aggregate against a ``by``-grouped aggregate - is a
    HAVING-style filter on already-aggregated rows and cannot be evaluated as
    a row-level WHERE. We catch it here so the user sees a clean error rather
    than the internal MergeNode discovery dump.
    """
    if not select.where_clause:
        return
    aggregates = _collect_where_aggregates(select.where_clause.conditional)
    if len(aggregates) < 2:
        return
    grain_signatures = {sig for _, sig in aggregates}
    if len(grain_signatures) <= 1:
        return
    expr_str = str(select.where_clause.conditional)
    raise InvalidSyntaxException(
        f"WHERE clause aggregates at multiple grains are not allowed: "
        f"`{expr_str}`. Aggregates filter rows AFTER grouping - use HAVING "
        f"(post-aggregate filter), or align all aggregates to the same `by` "
        f"grain so the filter is a pure row-level pre-aggregate predicate"
        + (f"; Line: {line_no}" if line_no else "")
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
        _validate_where_aggregate_grains(select, line_no)
    if select.having_clause:
        for cref in select.having_clause.concept_arguments:
            if cref.address not in allowed_addresses:
                raise SyntaxError(
                    f"HAVING references '{cref.address}', which is not in the "
                    f"SELECT projection (line {line_no}). Fix one of: "
                    f"(a) add it to SELECT — prefix with `--` to keep it out of "
                    f"the output rows, e.g. `select ..., --{cref.address}`; "
                    f"(b) move the filter to WHERE — for an aggregate condition "
                    f"on a non-output grain, write the aggregate inline as "
                    f"`agg(x) by grain` directly in WHERE."
                )
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
    for x in select.selection:
        if x.is_undefined:
            _raise_undefined(context, x.concept.address, line_no)
        elif isinstance(x.content, ConceptTransform):
            # Validate transform inputs: an undefined concept used only as a
            # function argument (e.g. `sum(missing) as foo`) has a defined
            # output alias, so it would otherwise slip past to SQL generation.
            for arg in x.content.function.concept_arguments:
                if isinstance(arg, (UndefinedConcept, UndefinedConceptFull)):
                    resolved = context.concepts.get(arg.address)
                    if resolved is None or isinstance(
                        resolved, (UndefinedConcept, UndefinedConceptFull)
                    ):
                        _raise_undefined(context, arg.address, line_no)
            if isinstance(x.content.output, UndefinedConcept):
                continue
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
                _raise_undefined(context, addr, line_no)
                continue
            select.local_concepts[addr] = resolved
            if addr in output_addresses:
                raise InvalidSyntaxException(
                    f"Duplicate select output for {addr}; Line: {line_no or 'unknown'}"
                )
            output_addresses.add(addr)
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
