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

from trilogy.core.constants import GRAIN_NULL_SENTINEL
from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import ComparisonOperator, Derivation, FunctionType, Purpose
from trilogy.core.having_normalization import _child_exprs
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    Comparison,
    Concept,
    ConceptRef,
    Function,
    SelectLineage,
    UndefinedConcept,
)
from trilogy.core.models.environment import Environment

# What takes a value where its arguments are NULL; a CASE does so through
# its ELSE.
NULL_ABSORBING_FUNCTIONS = frozenset({FunctionType.COALESCE})
NULL_TESTS = (ComparisonOperator.IS, ComparisonOperator.IS_NOT)


def _lookup(
    address: str, local: Mapping[str, Concept], environment: Environment
) -> Concept | None:
    concept = local.get(address) or environment.concepts.get(address)
    if concept is None or isinstance(concept, UndefinedConcept):
        return None
    return concept


def is_null_absorbing(expr: Any) -> TypeGuard[Function | Comparison]:
    """A `grain()` hash's sentinel coalesce is not a fallback: it makes the
    hash total over a NULL member, and a padded row is still no combination."""
    if isinstance(expr, Comparison):
        return expr.operator in NULL_TESTS
    if not isinstance(expr, Function):
        return False
    if expr.operator == FunctionType.CASE:
        return any(isinstance(a, CaseElse) for a in expr.arguments)
    return (
        expr.operator in NULL_ABSORBING_FUNCTIONS
        and GRAIN_NULL_SENTINEL not in expr.arguments
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
    seen = seen | {concept.address}
    if concept.derivation == Derivation.BASIC and concept.lineage is not None:
        out = _expr_entities(concept.lineage, local, environment, seen, set())
    else:
        out = set()
        for address in _row_keys(concept) - seen:
            read = _lookup(address, local, environment)
            if read is not None:
                out |= _entity_keys(read, local, environment, seen)
    # a stored or rowset column with no key is its own row identity; a keyless
    # aggregate, metric or derived value has none to anchor on (an abstract
    # count resolves its grain through the very select it would anchor)
    if (
        out
        or concept.purpose == Purpose.METRIC
        or concept.derivation not in (Derivation.ROOT, Derivation.ROWSET)
    ):
        return out
    return {concept.address}


def _expr_entities(
    expr: Any,
    local: Mapping[str, Concept],
    environment: Environment,
    seen: frozenset[str],
    bare: set[str],
) -> set[str]:
    """The entities of the rows `expr` reads. An inline aggregate is read on
    its `by` (a bare one on `bare`, the select's grain), never on its
    argument's rows."""
    if isinstance(expr, ConceptRef):
        if expr.address in seen:
            return set()
        read = _lookup(expr.address, local, environment)
        return _entity_keys(read, local, environment, seen) if read else set()
    if isinstance(expr, AggregateWrapper):
        if not expr.by:
            return set(bare)
        out: set[str] = set()
        for b in expr.by:
            out |= _expr_entities(b, local, environment, seen, bare)
        return out
    out = set()
    for child in _child_exprs(expr):
        out |= _expr_entities(child, local, environment, seen, bare)
    return out


def _own_keys(expr: Any, environment: Environment, anchors: set[str]) -> set[str]:
    return _expr_entities(expr, {}, environment, frozenset(), anchors)


def select_anchors(
    base: SelectLineage,
    environment: Environment,
    graph: DomainGraph,
    merged: Mapping[str, str],
) -> dict[str, frozenset[str]] | None:
    """Each output's own row identity, or None when nothing is pinned: every
    output of a ROLLUP/CUBE select is a grouping key, evaluated on the rows the
    pass groups and never on its subtotal rows. FD-reduced per output, so a
    window's partition key (determined by the row it numbers) is not a row of
    the select. `merged` folds a statement join's source key into its target."""
    if base.grouping is not None:
        return None
    out: dict[str, frozenset[str]] = {}
    outputs = {ref.address for ref in base.selection}
    for ref in base.selection:
        concept = _lookup(ref.address, base.local_concepts, environment)
        if concept is None or _restates_outputs(concept, outputs):
            continue
        # a declared join's two keys are one select key
        keys = _entity_keys(concept, base.local_concepts, environment)
        out[concept.address] = graph.fd_minimal(merged.get(k, k) for k in keys)
    return out


def _restates_outputs(concept: Concept, outputs: set[str]) -> bool:
    """An aggregate grouped by the other outputs restates their rows."""
    lineage = concept.lineage
    return (
        isinstance(lineage, AggregateWrapper)
        and {b.address for b in lineage.by} <= outputs
    )


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


def _may_pad(graph: DomainGraph) -> bool:
    """A row can lack what another holds only through a `~` binding or a
    coalescing (union/full) join somewhere in the statement's scope."""
    return any(not b.complete for b in graph.binding_edges) or bool(
        graph.coalescing_relation_members()
    )


def _held_beside(reads: set[str], key: str, graph: DomainGraph) -> bool:
    """A table holding `key`'s whole domain carries `reads` on each of its
    rows: no row of `key` lacks them."""
    bound: dict[str, set[str]] = {}
    complete: set[str] = set()
    for b in graph.binding_edges:
        bound.setdefault(b.datasource, set()).add(b.concept)
        if b.concept == key and b.complete and b.condition is None:
            complete.add(b.datasource)
    return any(reads <= bound[datasource] for datasource in complete)


def _always_beside(own: set[str], key: str, graph: DomainGraph) -> bool:
    """No select row holds `key` without the rows `expr` reads: nothing in
    the statement pads them, those rows hold every `key` (`covers`), every
    `key` row carries them (an order carries its customer), or a table holding
    `key`'s whole domain does."""
    return (
        not _may_pad(graph)
        or graph.covers(own, key)
        or all(graph.determines({key}, o) for o in own)
        or _held_beside(own, key, graph)
    )


def pin_keys(
    expr: Any,
    owner: str | None,
    anchors: Mapping[str, frozenset[str]],
    environment: Environment,
    graph: DomainGraph,
    named: Callable[[str], frozenset[str]],
    merged: Mapping[str, str],
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
        out |= pin_keys(child, owner, anchors, environment, graph, named, merged)
    if is_null_absorbing(expr):
        reads = {r.address for r in expr.concept_arguments}
        keys = {
            k
            for address, ks in anchors.items()
            # a table holding the output column carries the reads beside it
            if address != owner and not _held_beside(reads, address, graph)
            for k in ks
            if k != owner and not (owner and _reads(k, owner, environment))
        }
        own = {merged.get(k, k) for k in _own_keys(expr, environment, keys)}
        out |= {k for k in keys - own if not _always_beside(own, k, graph)}
    return frozenset(out)


def grain_pin(expr: Any, keys: frozenset[str], environment: Environment) -> Function:
    return Function(
        operator=FunctionType.GRAIN_PIN,
        output_datatype=expr.output_datatype,
        output_purpose=(
            expr.output_purpose if isinstance(expr, Function) else Purpose.PROPERTY
        ),
        arguments=[expr, *(environment.concepts[k].reference for k in sorted(keys))],
        arg_count=-1,
    )
