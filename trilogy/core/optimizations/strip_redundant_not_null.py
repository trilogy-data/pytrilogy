"""Drop tautological ``X IS NOT NULL`` atoms from a CTE's condition.

Runs on the built query tree, where ``nullable_concepts`` reflects the real join
path: a column non-null at its source and not padded by any outer join feeding the
CTE can never be NULL there. Before join planning only model nullability is known,
which would force a global over-conservative guess.

Absence from ``nullable_concepts`` is not sufficient on its own: build-time
refinement removes a concept from the nullable set when the node's own WHERE
null-rejects it, so judging that very condition by the refined set is circular and
would strip the only thing keeping the column non-null. A drop additionally
requires the concept to be non-nullable at ground truth: never bound nullable at a
base table and never outer-join padded anywhere in the CTE's source tree
(``_unfiltered_nullable_addresses``).

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
from trilogy.core.models.build import BuildConditional, BuildDatasource
from trilogy.core.models.execute import CTE, Join, QueryDatasource, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import equivalent_addresses
from trilogy.core.processing.condition_utility import (
    _not_null_concept,
    combine_condition_atoms,
    is_scalar_condition,
)
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    find_nullable_concepts,
)


def _unfiltered_nullable_addresses(source: QueryDatasource) -> set[str]:
    """Addresses that could be NULL anywhere in ``source``'s tree absent all
    WHERE filtering: base-table nullability plus outer-join padding at every
    level. Intermediate ``nullable_concepts`` lists are condition-refined, so
    the walk goes to the ``BuildDatasource`` leaves. Over-approximate on
    purpose: a false positive only keeps a redundant guard.
    """
    out: set[str] = set()
    stack: list[QueryDatasource] = [source]
    seen: set[int] = set()
    while stack:
        qds = stack.pop()
        if id(qds) in seen:
            continue
        seen.add(id(qds))
        out.update(find_nullable_concepts(qds.source_map, qds.datasources, qds.joins))
        for c in qds.output_concepts:
            if c.is_nullable:
                out.add(c.address)
                out.update(c.pseudonyms)
        for ds in qds.datasources:
            if isinstance(ds, QueryDatasource):
                stack.append(ds)
            elif isinstance(ds, BuildDatasource):
                for c in ds.nullable_concepts:
                    out.add(c.address)
                    out.update(c.pseudonyms)
    return out


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
            or join.jointype != JoinType.INNER
            or Modifier.NULLABLE in join.modifiers
            or any(
                isinstance(later, Join) and later.jointype in PADS_LEFT_JOIN_TYPES
                for later in cte.joins[idx + 1 :]
            )
        ):
            continue
        for pair in join.joinkey_pairs or []:
            if Modifier.NULLABLE in (
                pair.modifiers
                + (pair.left.modifiers or [])
                + (pair.right.modifiers or [])
            ):
                continue
            for node, concept in ((pair.cte, pair.left), (join.right_cte, pair.right)):
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
        nullable = equivalent_addresses(cte.nullable_concepts)
        output = equivalent_addresses(cte.output_columns)
        atoms = _and_atoms(cte.condition)
        survivors: list = []
        dropped = False
        unfiltered_nullable: set[str] | None = None
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
                if unfiltered_nullable is None:
                    unfiltered_nullable = _unfiltered_nullable_addresses(cte.source)
                if concept.equivalent_addresses.isdisjoint(unfiltered_nullable):
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
