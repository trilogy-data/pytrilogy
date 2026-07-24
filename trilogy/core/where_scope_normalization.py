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

from collections.abc import Iterable, Mapping
from dataclasses import replace as dc_replace
from typing import Any

from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    Derivation,
    FunctionType,
    Granularity,
)
from trilogy.core.having_normalization import _child_exprs
from trilogy.core.models.author import (
    AggregateWrapper,
    Comparison,
    Concept,
    ConceptArgs,
    ConceptRef,
    Conditional,
    FilterItem,
    Function,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    SelectLineage,
    UndefinedConcept,
    UndefinedConceptFull,
)
from trilogy.core.models.environment import Environment

# Suffix for WHERE-scope (population-gate) virtual concept names. Shared with
# the WHERE factory's ``virtual_scope_salt`` (build.py) so a minted twin and an
# anonymous cross-row virtual instantiated during a WHERE build land on the
# same address for the same expression — while never colliding with the
# unsalted select-scope instantiation of that expression.
WHERE_SCOPE_SALT = "wscope"

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


# ComparisonOperator overrides __eq__ without __hash__, so no dict lookup.
_MIRRORED_OPS = (
    (ComparisonOperator.GT, ComparisonOperator.LT),
    (ComparisonOperator.GTE, ComparisonOperator.LTE),
    (ComparisonOperator.LT, ComparisonOperator.GT),
    (ComparisonOperator.LTE, ComparisonOperator.GTE),
)


def _mirror_op(op: ComparisonOperator) -> ComparisonOperator:
    for source, mirrored in _MIRRORED_OPS:
        if op == source:
            return mirrored
    return op


def _and_conjuncts(node: Any) -> list[Any] | None:
    """Leaves of a pure-AND conditional tree, or None for any other boolean
    structure (an OR admits rows outside a prefix, so no prefix-atomicity
    reasoning survives it)."""
    if isinstance(node, Parenthetical):
        return _and_conjuncts(node.content)
    if isinstance(node, Conditional):
        if node.operator != BooleanOperator.AND:
            return None
        left = _and_conjuncts(node.left)
        right = _and_conjuncts(node.right)
        if left is None or right is None:
            return None
        return left + right
    return [node]


def _is_prefix_gate(leaf: Any, target_address: str) -> bool:
    """``leaf`` admits a PREFIX of a numbering window's ordering: bare
    ``target <= k`` / ``target < k`` / ``target = 1`` against a literal (or
    mirrored). Every row the population ranks at or above an admitted row is
    also admitted, so recomputing the window over the admitted rows reproduces
    the population values row-for-row."""
    if type(leaf) is not Comparison:
        return False
    left, right, op = leaf.left, leaf.right, leaf.operator
    if isinstance(right, ConceptRef) and not isinstance(left, ConceptRef):
        left, right, op = right, left, _mirror_op(op)
    if not (isinstance(left, ConceptRef) and left.address == target_address):
        return False
    if not isinstance(right, (int, float)):
        return False
    if op in (ComparisonOperator.LT, ComparisonOperator.LTE):
        return True
    return op == ComparisonOperator.EQ and right == 1


def _target_gates_within_partition(
    ref_address: str,
    window: NumberingWindowItem | NavigationWindowItem,
    base: SelectLineage,
    environment: Environment,
) -> list[Any] | None:
    """If the WHERE is a pure AND tree where every conjunct NOT referencing
    the target is constant within the window's partition (``over``) keys — so
    row admission only drops whole partitions, never reshuffling the window's
    rows inside a surviving partition — return the target-referencing
    conjuncts (each of which must reference ONLY the target). Otherwise
    None."""
    assert base.where_clause is not None
    if not all(isinstance(o, ConceptRef) for o in window.over):
        return None
    conjuncts = _and_conjuncts(base.where_clause.conditional)
    if conjuncts is None:
        return None
    cover = {o.address for o in window.over}
    gates: list[Any] = []
    for leaf in conjuncts:
        if isinstance(leaf, ConceptRef):
            addresses = {leaf.address}
        elif isinstance(leaf, ConceptArgs):
            addresses = {r.address for r in leaf.row_arguments}
        else:
            return None
        if ref_address in addresses:
            if addresses != {ref_address}:
                return None
            gates.append(leaf)
        elif not all(
            _covered_by_grain(a, cover, base.local_concepts, environment, set())
            for a in addresses
        ):
            return None
    return gates


def _where_is_prefix_atomic(
    ref_address: str,
    target: Concept,
    window: NumberingWindowItem,
    base: SelectLineage,
    environment: Environment,
) -> bool:
    """A numbering-window gate (rank/row_number/dense_rank) keeps the single
    shared instance when the admitted rows are a prefix of the window's own
    ordering within each partition: the target IS the bare window, every
    target-referencing conjunct is prefix-selecting (`<= k` / `< k` / `= 1` —
    the top-k and one-per-group idioms), and every other conjunct only drops
    whole partitions. Then the population and admitted-row values coincide
    and a twin would only duplicate the window computation."""
    if target.lineage is not window:
        return False
    gates = _target_gates_within_partition(ref_address, window, base, environment)
    if gates is None:
        return False
    return all(_is_prefix_gate(leaf, ref_address) for leaf in gates)


def _where_is_navigation_atomic(
    ref_address: str,
    window: NavigationWindowItem,
    base: SelectLineage,
    environment: Environment,
) -> bool:
    """A navigation-window (lead/lag) gate keeps the single shared instance
    when the gate cannot change the window's input population: every conjunct
    referencing the target is a bare gate on the computed value — which
    condition placement defers to after the window — and every other conjunct
    only drops whole partitions. Unlike numbering windows the gate shape is
    unrestricted: recomputing lead/lag over rows admitted by its own
    population value is never coherent (`where ratio is not null` would emit
    rows whose displayed ratio is null), so the population value IS the
    output value and a twin would only duplicate the window."""
    return (
        _target_gates_within_partition(ref_address, window, base, environment)
        is not None
    )


def _where_scopes_coincide(
    ref_address: str,
    target: Concept,
    parts: list[Any],
    base: SelectLineage,
    environment: Environment,
) -> bool:
    """The population-gate and select-scope values of every cross-row part are
    provably equal, so the WHERE can keep the single shared instance: aggregate
    parts must be group-atomic, numbering-window parts prefix-atomic,
    navigation-window parts partition-atomic. Group-to splits — row admission
    reshuffles it."""
    for part in parts:
        if isinstance(part, AggregateWrapper):
            if not _where_is_group_atomic(ref_address, part, base, environment):
                return False
        elif isinstance(part, NumberingWindowItem):
            if not _where_is_prefix_atomic(
                ref_address, target, part, base, environment
            ):
                return False
        elif isinstance(part, NavigationWindowItem):
            if not _where_is_navigation_atomic(ref_address, part, base, environment):
                return False
        else:
            return False
    return True


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
    reachable in an expression tree — through scalar composites (via the
    exhaustive ``_child_exprs`` walk shared with HAVING normalization) and
    references to derived concepts. A FilterItem is a boundary: its per-row
    value is row-invariant. The returned wrappers drive the mint decision and
    the group-atomicity analysis."""
    if isinstance(node, _CROSS_ROW_LINEAGE_TYPES):
        return [node]
    if isinstance(node, Function) and node.operator == FunctionType.GROUP:
        return [node]
    if isinstance(node, FilterItem):
        return []
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
    found: list[Any] = []
    for child in _child_exprs(node):
        found.extend(_collect_cross_row_parts(child, local, environment, seen))
    return found


def _reference_closure(
    addresses: Iterable[str],
    local: Mapping[str, Concept],
    environment: Environment,
) -> set[str]:
    """Addresses transitively reachable from ``addresses`` through concept
    lineages (locals shadow the environment)."""
    stack = list(addresses)
    closure: set[str] = set()
    while stack:
        address = stack.pop()
        if address in closure:
            continue
        closure.add(address)
        concept = _resolve_in(address, local, environment)
        if concept is None or concept.lineage is None:
            continue
        stack.extend(r.address for r in concept.lineage.concept_arguments)
    return closure


def _scope_sensitive(
    address: str,
    local: Mapping[str, Concept],
    environment: Environment,
    cache: dict[str, bool],
) -> bool:
    """The address's value depends on which rows are visible: its lineage
    closure holds a cross-row computation. Single-row scalars are excluded —
    their gating is owned by the scalar-output filter path, which keys off the
    shared address (skip gate 1)."""
    if address in cache:
        return cache[address]
    cache[address] = False
    concept = _resolve_in(address, local, environment)
    sensitive = bool(
        concept is not None
        and concept.lineage is not None
        and concept.granularity != Granularity.SINGLE_ROW
        and _collect_cross_row_parts(concept.lineage, local, environment, set())
    )
    cache[address] = sensitive
    return sensitive


def _where_scope_lineage(
    lineage: Any,
    local: Mapping[str, Concept],
    environment: Environment,
    active: set[str],
    rewritten: dict[str, Any],
) -> Any:
    """WHERE-scope rewrite of a lineage: references to cross-row-bearing
    concepts are INLINED as their (recursively rewritten) lineage nodes, so a
    chain like ``auto v <- sx + 1`` gates on population ``sx`` even when ``sx``
    is also a (select-scoped) output. Inlining rather than minting sub-twin
    concepts keeps the expression self-contained: its cross-row parts become
    anonymous wrappers (which the WHERE factory's ``virtual_scope_salt``
    instantiates at scope-distinct addresses) and every remaining reference
    resolves through the environment, which grain/keys recomputation
    requires.

    ``active`` guards reference cycles along the current path only;
    ``rewritten`` memoizes finished rewrites so a concept reachable along
    several paths (``v <- sx + m`` with ``m <- sx * 2``) inlines on every
    path, not just the first one walked — ``with_reference_replacement`` does
    not descend into replacement values, so a skipped ref would survive as a
    select-scope address inside the twin."""
    replacements: list[tuple[str, Any]] = []
    listed: set[str] = set()
    for ref in lineage.concept_arguments:
        if ref.address in active or ref.address in listed:
            continue
        if ref.address in rewritten:
            listed.add(ref.address)
            replacements.append((ref.address, rewritten[ref.address]))
            continue
        resolved = _resolve_in(ref.address, local, environment)
        if (
            resolved is None
            or resolved.lineage is None
            or resolved.granularity == Granularity.SINGLE_ROW
        ):
            continue
        if not _collect_cross_row_parts(resolved.lineage, local, environment, set()):
            continue
        inlined = _where_scope_lineage(
            resolved.lineage, local, environment, active | {ref.address}, rewritten
        )
        rewritten[ref.address] = inlined
        listed.add(ref.address)
        replacements.append((ref.address, inlined))
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
    from trilogy.parsing.common import arbitrary_to_concept, generate_concept_name

    if base.where_clause is None:
        return base
    select_scope = _reference_closure(
        (r.address for r in base.selection), base.local_concepts, environment
    )
    sensitivity: dict[str, bool] = {}
    replacements: list[tuple[str, ConceptRef]] = []
    minted: dict[str, Concept] = {}
    handled: set[str] = set()
    for ref in base.where_clause.row_arguments:
        if ref.address in handled:
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
        # The two roles only conflict when a scope-sensitive value is consumed
        # on both sides of the WHERE/select boundary. Direct output identity is
        # the common case, but a reference chain crossing the boundary in
        # either direction (`where sx ... select sx + 1 as v` / `where v ...
        # select sx`) shares the same address and leaks identically.
        shared = (
            _reference_closure([ref.address], base.local_concepts, environment)
            & select_scope
        )
        if not any(
            _scope_sensitive(a, base.local_concepts, environment, sensitivity)
            for a in shared
        ):
            continue
        if _where_scopes_coincide(ref.address, target, parts, base, environment):
            continue
        if _constant_universe(target.lineage, base.local_concepts, environment):
            continue
        twin_lineage = _where_scope_lineage(
            target.lineage, base.local_concepts, environment, {target.address}, {}
        )
        twin = arbitrary_to_concept(
            twin_lineage,
            environment,
            # scope-salted like the WHERE factory's anonymous virtuals: the
            # identical expression nested in a select output instantiates at
            # the unsalted hash name, and the two must never share an address
            name=f"{generate_concept_name(twin_lineage)}_{WHERE_SCOPE_SALT}",
        )
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
