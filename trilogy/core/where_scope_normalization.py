"""Build-time WHERE dual-scope normalization.

A row-scope WHERE reference to a select output that computes across the row
set (an aggregate/window/group-to — bare, expression-wrapped, or via a
reference chain) plays two roles with two scopes: the WHERE gates rows using
the POPULATION value at the computation's own grain (ignoring its peers in
the clause), while the select output recomputes over the WHERE-filtered rows.
One address can only carry one value, so ``normalize_select_where_scope``
rewrites the WHERE reference to a minted twin, converging every spelling on
the inline spelling (which mints its own virtual concept and has always had
dual scope).

Runs at build time on ``SelectLineage`` from ``Factory._build_select_lineage``,
immediately after HAVING normalization and under the same contract: pure,
deterministic (a lineage may be rebuilt per rowset body / multiselect arm),
and never mutates the authored statement or environment — minted twins ride
the returned copy's ``local_concepts`` and are built by the WHERE factory,
whose ``virtual_scope_salt`` scope-splits anonymous cross-row virtuals nested
in twin expressions.
"""

from __future__ import annotations

from dataclasses import replace as dc_replace
from typing import Any, Mapping

from trilogy.core.enums import Derivation, FunctionType, Granularity
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Function,
    FunctionCallWrapper,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    SelectLineage,
    UndefinedConcept,
    UndefinedConceptFull,
)
from trilogy.core.models.environment import Environment

_CROSS_ROW_LINEAGE_TYPES = (
    AggregateWrapper,
    NumberingWindowItem,
    NavigationWindowItem,
)


def _resolve_where_scope_target(
    concept: Concept | None,
    local: Mapping[str, Concept],
    environment: Environment,
    seen: set[str] | None = None,
) -> Concept | None:
    """Resolve a WHERE-referenced select output to the concept whose lineage
    scope-analysis should run on, following pure-rename ALIAS chains. Returns
    None for concepts the dual-scope split must not touch: single-row scalars
    (`sum(x) by *` gating is handled by the scalar-output filter path, which
    keys off the shared address) and lineage-less roots."""
    if concept is None or isinstance(concept, (UndefinedConcept, UndefinedConceptFull)):
        return None
    if concept.granularity == Granularity.SINGLE_ROW:
        return None
    lineage = concept.lineage
    if lineage is None:
        return None
    if (
        isinstance(lineage, Function)
        and lineage.operator == FunctionType.ALIAS
        and len(lineage.arguments) == 1
        and isinstance(lineage.arguments[0], ConceptRef)
    ):
        seen = seen if seen is not None else {concept.address}
        target = lineage.arguments[0].address
        if target in seen:
            return None
        seen.add(target)
        resolved = local.get(target) or environment.concepts.get(target)
        return _resolve_where_scope_target(resolved, local, environment, seen)
    return concept


def _resolve_in(
    address: str, local: Mapping[str, Concept], environment: Environment
) -> Concept | None:
    found = local.get(address) or environment.concepts.get(address)
    if found is None or isinstance(found, (UndefinedConcept, UndefinedConceptFull)):
        return None
    return found


def _covered_by_grain(
    address: str,
    cover: set[str],
    local: Mapping[str, Concept],
    environment: Environment,
    seen: set[str],
) -> bool:
    """The concept is functionally determined by the ``cover`` addresses:
    either a member, or (transitively) a property/metric whose keys are."""
    if address in cover:
        return True
    if address in seen:
        return False
    seen.add(address)
    concept = _resolve_in(address, local, environment)
    if concept is None or not concept.keys:
        return False
    return all(
        _covered_by_grain(k, cover, local, environment, seen) for k in concept.keys
    )


def _where_is_group_atomic(
    target_address: str,
    aggregate: AggregateWrapper,
    base: SelectLineage,
    environment: Environment,
) -> bool:
    """Every WHERE row argument is constant within the aggregate's groups
    (a member of / functionally determined by its ``by`` grain — or the select
    grain for a bare aggregate), so row admission keeps or drops whole groups
    and the population and select-scope values coincide. Used to skip the twin
    and keep the single-instance plan (q30/q81 gate whole customers on a state
    average — a twin would just scan the fact table twice)."""
    assert base.where_clause is not None
    if aggregate.by:
        cover = {c.address for c in aggregate.by}
    else:
        cover = set(base.grain.components)
    cover.add(target_address)
    return all(
        _covered_by_grain(ref.address, cover, base.local_concepts, environment, set())
        for ref in base.where_clause.row_arguments
    )


def _constant_universe(
    lineage: Any,
    local: Mapping[str, Concept],
    environment: Environment,
) -> bool:
    """No concept in the lineage closure is datasource-bound (ROOT): the
    computation ranges over a pure constant/unnest universe. Row identity for
    such universes is regenerated rather than carried through condition nodes,
    so a WHERE-scope twin cannot gate the regenerated rows (pre-existing hole,
    shared with the inline spelling) — callers skip minting."""
    stack = [lineage]
    seen: set[str] = set()
    while stack:
        node = stack.pop()
        for arg in getattr(node, "concept_arguments", []):
            if arg.address in seen:
                continue
            seen.add(arg.address)
            concept = _resolve_in(arg.address, local, environment)
            if concept is None:
                return False
            if concept.derivation == Derivation.ROOT:
                return False
            if concept.lineage is not None:
                stack.append(concept.lineage)
    return True


def _collect_cross_row_parts(
    node: Any,
    local: Mapping[str, Concept],
    environment: Environment,
    seen: set[str],
) -> list[Any]:
    """Cross-row computation nodes (aggregate/window/group-to wrappers)
    reachable in an expression tree — through scalar functions, parentheticals,
    case branches, macro wrappers, and references to derived concepts. A
    FilterItem is a boundary: its per-row value is row-invariant. The returned
    wrappers drive the mint decision and the group-atomicity analysis."""
    if isinstance(node, _CROSS_ROW_LINEAGE_TYPES):
        return [node]
    if isinstance(node, Function) and node.operator == FunctionType.GROUP:
        return [node]
    if isinstance(node, ConceptRef):
        if node.address in seen:
            return []
        seen.add(node.address)
        resolved = _resolve_in(node.address, local, environment)
        if (
            resolved is None
            or resolved.lineage is None
            or resolved.granularity == Granularity.SINGLE_ROW
        ):
            return []
        return _collect_cross_row_parts(resolved.lineage, local, environment, seen)
    children: list[Any] = []
    if isinstance(node, Function):
        children = list(node.arguments)
    elif isinstance(node, FunctionCallWrapper):
        children = [node.content, *node.args]
    elif isinstance(node, Parenthetical):
        children = [node.content]
    elif isinstance(node, CaseWhen):
        children = [node.comparison, node.expr]
    elif isinstance(node, CaseElse):
        children = [node.expr]
    elif isinstance(node, (Comparison, Conditional)):
        children = [node.left, node.right]
    found: list[Any] = []
    for child in children:
        found.extend(_collect_cross_row_parts(child, local, environment, seen))
    return found


def _where_scope_lineage(
    lineage: Any,
    local: Mapping[str, Concept],
    environment: Environment,
    seen: set[str],
) -> Any:
    """WHERE-scope rewrite of a lineage: references to cross-row-bearing
    concepts are INLINED as their (recursively rewritten) lineage nodes, so a
    chain like ``auto v <- sx + 1`` gates on population ``sx`` even when ``sx``
    is also a (select-scoped) output. Inlining rather than minting sub-twin
    concepts keeps the expression self-contained: its cross-row parts become
    anonymous wrappers (which the WHERE factory's ``virtual_scope_salt``
    instantiates at scope-distinct addresses) and every remaining reference
    resolves through the environment, which grain/keys recomputation
    requires."""
    replacements: list[tuple[str, Any]] = []
    for ref in lineage.concept_arguments:
        if ref.address in seen:
            continue
        seen.add(ref.address)
        resolved = _resolve_in(ref.address, local, environment)
        if (
            resolved is None
            or resolved.lineage is None
            or resolved.granularity == Granularity.SINGLE_ROW
        ):
            continue
        if not _collect_cross_row_parts(resolved.lineage, local, environment, set()):
            continue
        replacements.append(
            (
                ref.address,
                _where_scope_lineage(resolved.lineage, local, environment, seen),
            )
        )
    if not replacements:
        return lineage
    return lineage.with_reference_replacement(replacements)  # type: ignore[arg-type]


def normalize_select_where_scope(
    base: SelectLineage, environment: Environment
) -> SelectLineage:
    """A row-scope WHERE reference to a select output that computes across the
    row set (an aggregate/window/group-to — bare, expression-wrapped, or via a
    reference chain) plays two roles with two scopes: it gates rows using the
    population value at its own grain (ignoring its peers in the clause), while
    the select output recomputes over the WHERE-filtered rows. One address can
    only carry one value — the shared address made the population value
    silently leak into the projection — so rewrite the WHERE reference to a
    minted twin, converging every spelling on the inline spelling's dual scope.
    Pure and deterministic: reruns identically per rowset body / multiselect
    arm."""
    from trilogy.parsing.common import arbitrary_to_concept

    if base.where_clause is None:
        return base
    output_addrs = {r.address for r in base.selection}
    replacements: list[tuple[str, ConceptRef]] = []
    minted: dict[str, Concept] = {}
    handled: set[str] = set()
    for ref in base.where_clause.row_arguments:
        if ref.address in handled or ref.address not in output_addrs:
            continue
        handled.add(ref.address)
        concept = base.local_concepts.get(ref.address) or environment.concepts.get(
            ref.address
        )
        target = _resolve_where_scope_target(concept, base.local_concepts, environment)
        if target is None:
            continue
        parts = _collect_cross_row_parts(
            target.lineage, base.local_concepts, environment, set()
        )
        if not parts:
            continue
        # Group-atomic WHEREs keep or drop whole groups of an aggregate, so its
        # two scopes coincide — keep the single shared instance. Windows always
        # split: any row admission reshuffles them.
        if all(isinstance(p, AggregateWrapper) for p in parts) and all(
            _where_is_group_atomic(ref.address, p, base, environment) for p in parts
        ):
            continue
        if _constant_universe(target.lineage, base.local_concepts, environment):
            continue
        twin_lineage = _where_scope_lineage(
            target.lineage, base.local_concepts, environment, {target.address}
        )
        twin = arbitrary_to_concept(twin_lineage, environment)
        minted[twin.address] = twin
        replacements.append((ref.address, twin.reference))
    if not replacements:
        return base
    local_concepts = dict(base.local_concepts)
    local_concepts.update(minted)
    return dc_replace(
        base,
        where_clause=base.where_clause.with_reference_replacement(replacements),
        local_concepts=local_concepts,
    )
