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

from collections.abc import Callable, Mapping
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


def _row_keys(concept: Concept) -> set[str]:
    if concept.purpose == Purpose.KEY:
        return {concept.address}
    if concept.derivation == Derivation.CONSTANT:
        return set()
    if concept.keys:
        return set(concept.keys)
    return set(concept.grain.components) or {concept.address}


def _entity_keys(
    concept: Concept,
    local: Mapping[str, Concept],
    environment: Environment,
    seen: frozenset[str] = frozenset(),
) -> set[str]:
    """The entities `concept` is a function of, as the keyspace reads them
    (`keyspace._entity_keys`): a key is its own entity unless derived (`c` of
    `customer_id as c`), and a derived grouping value (`count(..) by status`)
    stands for the rows it is keyed on, never as an input another pinned value
    could be derived beside."""
    if concept.purpose == Purpose.KEY and concept.derivation != Derivation.BASIC:
        return {concept.address}
    if concept.derivation == Derivation.BASIC and concept.lineage is not None:
        reads = {r.address for r in concept.lineage.concept_arguments}
    else:
        reads = _row_keys(concept) - {concept.address}
    out: set[str] = set()
    for address in reads - seen:
        read = _lookup(address, local, environment)
        if read is not None:
            out |= _entity_keys(read, local, environment, seen | {concept.address})
    return out or (
        {concept.address} if concept.derivation != Derivation.CONSTANT else set()
    )


def _own_keys(expr: Any, environment: Environment, anchors: set[str]) -> set[str]:
    """The entities of the rows `expr` reads. An inline aggregate is read on
    its `by` (a bare one on the select's grain), never on its argument's
    rows."""
    if isinstance(expr, ConceptRef):
        read = _lookup(expr.address, {}, environment)
        return _entity_keys(read, {}, environment) if read is not None else set()
    if isinstance(expr, AggregateWrapper):
        if not expr.by:
            return set(anchors)
        out: set[str] = set()
        for b in expr.by:
            out |= _own_keys(b, environment, anchors)
        return out
    out = set()
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
            out[concept.address] = graph.fd_minimal(
                _entity_keys(concept, base.local_concepts, environment)
            )
    return out


def _reads(address: str, target: str, environment: Environment) -> bool:
    """`address` is derived from `target`: a select key computed from the
    pinned value cannot be one of its inputs."""
    seen: set[str] = set()
    stack = [address]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        concept = _lookup(current, {}, environment)
        if concept is None or concept.lineage is None:
            continue
        for ref in concept.lineage.concept_arguments:
            if ref.address == target:
                return True
            stack.append(ref.address)
    return False


def _always_beside(own: set[str], key: str, graph: DomainGraph) -> bool:
    """No select row holds `key` without the rows `expr` reads: those rows
    hold every `key` (`covers`), or every `key` row carries them (an order
    carries its customer)."""
    return graph.covers(own, key) or all(graph.determines({key}, o) for o in own)


def pin_keys(
    expr: Any,
    owner: str | None,
    anchors: Mapping[str, frozenset[str]],
    environment: Environment,
    graph: DomainGraph,
    named: Callable[[str], frozenset[str]],
) -> frozenset[str]:
    """The select keys `expr` must take as inputs: for each NULL-absorbing
    expression in it, the keys (but `owner`'s own) the rows it reads do not
    cover, and whatever a derivation it reads by name is pinned to (`named`):
    a value computed from a pinned one is pinned with it. An inline aggregate
    pins its own argument when it is built."""
    if isinstance(expr, ConceptRef):
        read = _lookup(expr.address, {}, environment)
        if read is None or read.derivation != Derivation.BASIC:
            return frozenset()
        return named(read.address)
    if isinstance(expr, AggregateWrapper):
        return frozenset()
    out: set[str] = set()
    for child in _child_exprs(expr):
        out |= pin_keys(child, owner, anchors, environment, graph, named)
    if is_null_absorbing(expr):
        keys = {
            k
            for address, ks in anchors.items()
            if address != owner
            for k in ks
            if k != owner and not (owner and _reads(k, owner, environment))
        }
        own = _own_keys(expr, environment, keys)
        out |= {k for k in keys - own if not _always_beside(own, k, graph)}
    return frozenset(out)


def grain_pin(expr: Any, keys: frozenset[str], environment: Environment) -> Function:
    return Function(
        operator=FunctionType.GRAIN_PIN,
        output_datatype=expr.output_datatype,
        output_purpose=expr.output_purpose,
        arguments=[expr, *(environment.concepts[k].reference for k in sorted(keys))],
        arg_count=-1,
    )
