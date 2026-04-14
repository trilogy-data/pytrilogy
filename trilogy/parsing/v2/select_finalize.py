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
    Concept,
    Function,
    Grain,
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


def _validate_syntax(select: SelectStatement, context: RuleContext) -> None:
    line_no = select.meta.line_number if select.meta else None
    if select.where_clause:
        for x in select.where_clause.concept_arguments:
            if isinstance(x, UndefinedConcept):
                validate = context.concepts.get(x.address)
                if validate and select.where_clause:
                    select.where_clause = (
                        select.where_clause.with_reference_replacement(
                            x.address, validate.reference
                        )
                    )
                else:
                    _raise_undefined(
                        context,
                        x.address,
                        x.metadata.line_number if x.metadata else None,
                    )
    all_in_output = set(select.output_components)
    locally_derived = select.locally_derived
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
    if select.having_clause:
        for cref in select.having_clause.concept_arguments:
            if cref.address not in [x for x in select.output_components]:
                raise SyntaxError(
                    f"Cannot reference a column ({cref.address}) that is not in "
                    f"the select projection in the HAVING clause, move to WHERE; "
                    f"Line: {line_no}"
                )
    if select.order_by:
        for cref in select.order_by.concept_arguments:
            if cref.address not in all_in_output:
                raise SyntaxError(
                    f"Cannot order by column {cref.address} that is not in the "
                    f"output projection; line: {line_no}"
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
