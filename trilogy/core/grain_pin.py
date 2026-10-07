"""A NULL-absorbing expression read by a select is pinned to the select's grain.

``coalesce(x, d)`` and a CASE with an ELSE take a value where their reads are
NULL, so where they are evaluated decides what they say on a row a ``~``
binding pads. Evaluated at their reads' own keys they are absent there (NULL);
evaluated on the select's row the fallback fires. A select evaluates them on
its row, the way a bare aggregate groups by the select's grain: every select
key their reads' rows do not cover becomes an input of a ``GRAIN_PIN``.

The Factory applies it where it resolves a bare aggregate's grain, so every
read of the address in the statement (output, grouping key, aggregate input,
WHERE) carries the one pinned value. The pin is part of the lineage: a column
persisted at the reads' own grain answers only a select its reads cover.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeGuard

from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import Derivation, FunctionType, Purpose
from trilogy.core.having_normalization import _child_exprs
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    Concept,
    ConceptRef,
    Function,
    SelectLineage,
    UndefinedConcept,
)
from trilogy.core.models.environment import Environment

# Functions that take a value where their arguments are NULL; a CASE does so
# through its ELSE.
NULL_ABSORBING_FUNCTIONS = frozenset({FunctionType.COALESCE})


def _lookup(
    address: str, local: Mapping[str, Concept], environment: Environment
) -> Concept | None:
    concept = local.get(address) or environment.concepts.get(address)
    if concept is None or isinstance(concept, UndefinedConcept):
        return None
    return concept


def is_null_absorbing(expr: Any) -> TypeGuard[Function]:
    return isinstance(expr, Function) and (
        expr.operator in NULL_ABSORBING_FUNCTIONS
        or (
            expr.operator == FunctionType.CASE
            and any(isinstance(a, CaseElse) for a in expr.arguments)
        )
    )


def absorbs_null(expr: Any) -> bool:
    """A NULL-absorbing function in a row expression, outside any inline
    aggregate (whose argument is read on the aggregate's input rows)."""
    if is_null_absorbing(expr):
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


def _own_keys(expr: Any, environment: Environment, anchors: set[str]) -> set[str]:
    """The keys of the rows `expr` reads. An inline aggregate is read on its
    `by` (a bare one on the select's grain), never on its argument's rows."""
    if isinstance(expr, ConceptRef):
        read = _lookup(expr.address, {}, environment)
        return _row_keys(read) if read is not None else set()
    if isinstance(expr, AggregateWrapper):
        return {b.address for b in expr.by} if expr.by else set(anchors)
    out: set[str] = set()
    for child in _child_exprs(expr):
        out |= _own_keys(child, environment, anchors)
    return out


def select_anchors(
    base: SelectLineage, environment: Environment, graph: DomainGraph
) -> dict[str, frozenset[str]] | None:
    """Each output's own row identity, or None when nothing is pinned: every
    output of a ROLLUP/CUBE select is a grouping key, evaluated on the rows the
    pass groups and never on its subtotal rows. FD-reduced per output, so a
    window's partition key (determined by the row it numbers) is not a row of
    the select."""
    if base.grouping is not None:
        return None
    out: dict[str, frozenset[str]] = {}
    for ref in base.selection:
        concept = _lookup(ref.address, base.local_concepts, environment)
        if concept is not None:
            out[concept.address] = graph.fd_minimal(_row_keys(concept))
    return out


def grain_pin(
    expr: Any,
    owner: str | None,
    anchors: Mapping[str, frozenset[str]],
    environment: Environment,
    graph: DomainGraph,
) -> Function | None:
    """`expr` evaluated on the select's row: the select keys, but `owner`'s
    own, that the rows `expr` reads do not cover become its inputs. None when
    they cover every one, so the plain lineage (and any column persisting it)
    stands."""
    keys: set[str] = set().union(
        *(k for address, k in anchors.items() if address != owner)
    ) - {owner}
    own = _own_keys(expr, environment, keys)
    uncovered = sorted(k for k in keys - own if not graph.covers(own, k))
    if not uncovered:
        return None
    return Function(
        operator=FunctionType.GRAIN_PIN,
        output_datatype=expr.output_datatype,
        output_purpose=expr.output_purpose,
        arguments=[expr, *(environment.concepts[k].reference for k in uncovered)],
        arg_count=-1,
    )
