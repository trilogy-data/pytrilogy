"""Build-time pin of a NULL-absorbing select output to the select's grain.

``coalesce(x, d)`` and a CASE with an ELSE take a value where their reads are
NULL, so where they are evaluated decides what they say on a row a ``~``
binding pads. Evaluated at their reads' own keys they are absent there (NULL);
evaluated on the select's row the fallback fires. As a select output they are
evaluated on the select's row, the way a bare aggregate groups by the select's
grain: every select key their reads' rows do not cover becomes an input of a
``GRAIN_PIN``. The pin is part of the lineage, so a column persisted at the
reads' own grain is never read for it, while a select whose keys the reads
cover keeps the plain lineage and still reads that column.

Same contract as ``normalize_select_where_scope``: pure, and the pinned
concepts ride the returned copy's ``local_concepts`` under the output's own
address.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace as dc_replace
from typing import Any

from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import Derivation, FunctionType, Purpose
from trilogy.core.having_normalization import _child_exprs
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    Parenthetical,
    SelectLineage,
    UndefinedConcept,
)
from trilogy.core.models.environment import Environment


def _lookup(
    address: str, local: Mapping[str, Concept], environment: Environment
) -> Concept | None:
    concept = local.get(address) or environment.concepts.get(address)
    if concept is None or isinstance(concept, UndefinedConcept):
        return None
    return concept


def _inline(expr: Any, local: Mapping[str, Concept], environment: Environment) -> Any:
    """`expr` with every BASIC concept it reads replaced by its lineage, so the
    whole expression is evaluated on one row."""
    if isinstance(expr, ConceptRef):
        concept = _lookup(expr.address, local, environment)
        if concept is None or concept.derivation != Derivation.BASIC:
            return expr
        if concept.lineage is None:
            return expr
        return _inline(concept.lineage, local, environment)
    if isinstance(expr, Function):
        return dc_replace(
            expr, arguments=[_inline(a, local, environment) for a in expr.arguments]
        )
    if isinstance(expr, Parenthetical):
        return dc_replace(expr, content=_inline(expr.content, local, environment))
    if isinstance(expr, (Comparison, Conditional)):
        return dc_replace(
            expr,
            left=_inline(expr.left, local, environment),
            right=_inline(expr.right, local, environment),
        )
    if isinstance(expr, CaseWhen):
        return dc_replace(
            expr,
            comparison=_inline(expr.comparison, local, environment),
            expr=_inline(expr.expr, local, environment),
        )
    if isinstance(expr, CaseSimpleWhen):
        return dc_replace(
            expr,
            value_expr=_inline(expr.value_expr, local, environment),
            expr=_inline(expr.expr, local, environment),
        )
    if isinstance(expr, CaseElse):
        return dc_replace(expr, expr=_inline(expr.expr, local, environment))
    return expr


def absorbs_null(expr: Any) -> bool:
    """A COALESCE or an ELSE in a row expression, outside any inline aggregate
    (whose argument is read on the aggregate's input rows)."""
    if isinstance(expr, CaseElse):
        return True
    if isinstance(expr, Function) and expr.operator == FunctionType.COALESCE:
        return True
    if isinstance(expr, AggregateWrapper):
        return False
    return any(absorbs_null(child) for child in _child_exprs(expr))


def _row_keys(concept: Concept) -> set[str]:
    if concept.purpose == Purpose.KEY:
        return {concept.address}
    if concept.derivation == Derivation.CONSTANT:
        return set()
    if concept.keys:
        return set(concept.keys)
    return set(concept.grain.components) or {concept.address}


def _own_keys(
    expr: Any,
    local: Mapping[str, Concept],
    environment: Environment,
    anchors: set[str],
) -> set[str]:
    """The keys of the rows `expr` reads. An inline aggregate is read on its
    `by` (a bare one on the select's grain), never on its argument's rows."""
    if isinstance(expr, ConceptRef):
        read = _lookup(expr.address, local, environment)
        return _row_keys(read) if read is not None else set()
    if isinstance(expr, AggregateWrapper):
        return {b.address for b in expr.by} if expr.by else set(anchors)
    out: set[str] = set()
    for child in _child_exprs(expr):
        out |= _own_keys(child, local, environment, anchors)
    return out


def normalize_select_grain_pins(
    base: SelectLineage, environment: Environment, graph: DomainGraph
) -> SelectLineage:
    # every output of a ROLLUP/CUBE select is a grouping key: evaluated on the
    # rows the pass groups, never on its subtotal rows
    if base.grouping is not None:
        return base
    local = base.local_concepts
    outputs = [
        c
        for ref in base.selection
        if (c := _lookup(ref.address, local, environment)) is not None
    ]
    pins: dict[str, Concept] = {}
    for concept in outputs:
        if concept.derivation != Derivation.BASIC or concept.lineage is None:
            continue
        inlined = _inline(concept.lineage, local, environment)
        if not absorbs_null(inlined):
            continue
        anchors: set[str] = set()
        for other in outputs:
            if other.address != concept.address:
                # each output's own row identity: a window's partition key is
                # determined by the row it numbers, not a row of the select
                anchors |= graph.fd_minimal(_row_keys(other))
        # a grouping key of another output is read on that output's input rows
        if concept.address in anchors:
            continue
        own = _own_keys(inlined, local, environment, anchors)
        uncovered = sorted(k for k in anchors - own if not graph.covers(own, k))
        if not uncovered:
            continue
        pins[concept.address] = dc_replace(
            concept,
            lineage=Function(
                operator=FunctionType.GRAIN_PIN,
                output_datatype=concept.datatype,
                output_purpose=concept.purpose,
                arguments=[
                    inlined,
                    *(environment.concepts[k].reference for k in uncovered),
                ],
                arg_count=-1,
            ),
        )
    if not pins:
        return base
    return dc_replace(base, local_concepts={**local, **pins})
