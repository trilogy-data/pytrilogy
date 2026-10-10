"""Drop tautological ``X IS NOT NULL`` atoms from a CTE's condition.

Runs on the built query tree, where ``nullable_concepts`` reflects the real join
path: a column non-null at its source and not padded by any outer join feeding the
CTE can never be NULL there. Before join planning only model nullability is known,
which would force a global over-conservative guess.

The CTE's own ``nullable_concepts`` cannot decide it: build-time refinement
removes a concept from that set when the CTE's own WHERE null-rejects it, so
judging that very condition by it is circular and would strip the only thing
keeping the column non-null. The rule reads one level down instead
(``_nullable_before_own_filter``): the parents' sets, which only filters that
really ran have narrowed, plus the CTE's own tables and outer-join padding.

The concept must also be a tracked, non-derived output of the CTE:

- ``Derivation.ROOT``: a derived concept (FILTER, ``CASE``, ...) can be NULL via
  its own expression, which ``nullable_concepts`` does not record.
- present in ``output_columns``: only there is ``nullable_concepts`` authoritative.
  A concept appearing solely inside the condition is not tracked, so absence from
  the nullable set says nothing about whether it can be NULL.

Independently, an atom on a key this CTE's own INNER join pairs by plain equality
is tautological when it reads the key off a side of that pair and no later RIGHT
or FULL join can pad that side back (`ss.customer.sk is not null` beside
``INNER JOIN customer on ss.customer_sk = c.customer_sk``).
"""

from __future__ import annotations

from trilogy.core.enums import BooleanOperator, Derivation, JoinType, Modifier
from trilogy.core.models.build import BuildConcept, BuildConditional, BuildDatasource
from trilogy.core.models.execute import (
    CTE,
    Join,
    UnionCTE,
    pair_matches_nulls,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import equivalent_addresses, null_padded_nodes
from trilogy.core.processing.condition_utility import (
    _not_null_concept,
    combine_condition_atoms,
    is_scalar_condition,
)
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
)


def _node_nullable(node: CTE | UnionCTE) -> list[BuildConcept]:
    """What a parent hands its consumer as possibly NULL. A union's arms are
    not tracked one by one, so every column it emits counts."""
    if isinstance(node, UnionCTE):
        return node.output_columns
    return node.nullable_concepts


def _nullable_before_own_filter(cte: CTE) -> set[str]:
    """Addresses `cte` could hold NULL before its own WHERE runs: what its
    parents hand it, its raw tables' nullable columns, and every column its
    own outer joins pad. Its own ``nullable_concepts`` is narrowed by that
    WHERE, so judging the WHERE by it would be circular; the parents' lists
    are narrowed only by filters that really ran."""
    base = cte.source.base_datasource
    nullable = [c for c in cte.output_columns if c.is_nullable]
    if isinstance(base, BuildDatasource):
        nullable += base.nullable_concepts
    for node in cte.dependency_nodes():
        nullable += _node_nullable(node)
    # an inlined parent's WHERE may now be this CTE's own: read its table
    for inlined in cte.inlined_parents:
        nullable += inlined.datasource.nullable_concepts
    for node in null_padded_nodes(cte):
        nullable += node.output_columns
    return equivalent_addresses(nullable)


def _and_atoms(condition: object) -> list:
    """Every AND-ed atom, a bare boolean concept included, which
    ``decompose_condition`` keeps bundled with its siblings."""
    if isinstance(condition, BuildConditional) and (
        condition.operator == BooleanOperator.AND
    ):
        return _and_atoms(condition.left) + _and_atoms(condition.right)
    return [condition]


def _inner_join_rejected(cte: CTE) -> set[tuple[str, str]]:
    """(source, address) pairs every row of ``cte`` holds non-null: a side of a
    plain-equality INNER join pair no later RIGHT or FULL join can pad."""
    out: set[tuple[str, str]] = set()
    for idx, join in enumerate(cte.joins):
        if (
            not isinstance(join, Join)
            or join.join_type != JoinType.INNER
            or Modifier.NULLABLE in join.modifiers
            or any(
                isinstance(later, Join) and later.join_type in PADS_LEFT_JOIN_TYPES
                for later in cte.joins[idx + 1 :]
            )
        ):
            continue
        for pair in join.pairs or []:
            if pair_matches_nulls(pair):
                continue
            for node, concept in ((pair.node, pair.left), (join.right, pair.right)):
                alias = join.name_for(cte, join.authoritative(cte, node))
                out.update((alias, a) for a in concept.equivalent_addresses)
    return out


def _read_off_rejected_side(
    cte: CTE, address: str, rejected: set[tuple[str, str]]
) -> bool:
    sources = cte.source_map.get(address, [])
    return len(sources) == 1 and (sources[0], address) in rejected


class StripRedundantNotNull(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or cte.condition is None:
            return False, None
        nullable = _nullable_before_own_filter(cte)
        output = equivalent_addresses(cte.output_columns)
        atoms = _and_atoms(cte.condition)
        survivors: list = []
        dropped = False
        rejected = _inner_join_rejected(cte)
        for atom in atoms:
            concept = _not_null_concept(atom)
            if (
                concept is not None
                and is_scalar_condition(atom)
                and _read_off_rejected_side(cte, concept.address, rejected)
            ):
                dropped = True
                self.log(f"{cte.name}: {concept.address} IS NOT NULL is an INNER key")
                continue
            if (
                concept is not None
                and concept.derivation == Derivation.ROOT
                and is_scalar_condition(atom)
                and not concept.equivalent_addresses.isdisjoint(output)
                and concept.equivalent_addresses.isdisjoint(nullable)
            ):
                dropped = True
                self.log(
                    f"{cte.name}: dropping tautological {concept.address} IS NOT NULL"
                )
                continue
            survivors.append(atom)
        if not dropped:
            return False, None
        cte.condition = combine_condition_atoms(survivors)
        return True, None
