"""Drop tautological ``X IS NOT NULL`` atoms from a CTE's condition.

Once join types and CTE nullability have settled, ``nullable_concepts`` reflects
the real join tree: a column that is non-null in its source and not padded by any
outer join feeding the CTE can never be NULL there, so an ``IS NOT NULL`` on it is
a tautology — pure noise that only pins the concept into the CTE.

This deliberately runs on the built query tree rather than pre-resolution. Before
join planning the only signal is model nullability, which forces a global,
over-conservative guess (a column non-null in its own table can still be padded by
an outer join two hops away). Here the join path is known.

Absence from ``nullable_concepts`` is NOT sufficient proof on its own: build-time
refinement (``StrategyNode._refine_nullable_for_conditions`` and the
``proven_non_null`` gates in the scan/group layers) removes a concept from the
nullable set when the node's own WHERE null-rejects it, so downstream join
planning sees post-filter truth. Trusting that refined set to judge the very
condition that did the proving is circular — it strips the only thing keeping the
column non-null (q78: an authored ``customer IS NOT NULL`` on a source-nullable
FK silently vanished). So a drop additionally requires the concept to be
non-nullable at ground truth — never bound nullable at a base table and never
outer-join padded anywhere in the CTE's source tree
(``_unfiltered_nullable_addresses``).

To stay sound the concept must also be a tracked, non-derived output of the CTE:

- ``Derivation.ROOT``: a derived concept (FILTER/BASIC ``CASE`` …) can be NULL via
  its own expression, which ``nullable_concepts`` does not record.
- present in ``output_columns``: only there is ``nullable_concepts`` authoritative.
  A concept that appears solely inside the condition (e.g. a WHERE-only key that an
  aggregate dropped from its outputs) is not tracked, so absence from the nullable
  set says nothing about whether it can be NULL.
"""

from __future__ import annotations

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildDatasource
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    _not_null_concept,
    combine_condition_atoms,
    decompose_condition,
    is_scalar_condition,
)
from trilogy.core.processing.utility import find_nullable_concepts


def _equivalent_addresses(concepts: list) -> set[str]:
    out: set[str] = set()
    for c in concepts:
        out |= c.equivalent_addresses
    return out


def _unfiltered_nullable_addresses(source: QueryDatasource) -> set[str]:
    """Addresses that could be NULL anywhere in ``source``'s tree absent all
    WHERE filtering: intrinsic nullability at base-table bindings plus
    outer-join padding at every level.

    Intermediate ``nullable_concepts`` lists are condition-refined, so they
    cannot distinguish "never nullable" from "nullable but currently filtered";
    walking to the ``BuildDatasource`` leaves recovers the unrefined truth.
    Over-approximate on purpose: a false positive only keeps a redundant guard.
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


class StripRedundantNotNull(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or cte.condition is None:
            return False, None
        nullable = _equivalent_addresses(cte.nullable_concepts)
        output = _equivalent_addresses(cte.output_columns)
        atoms = decompose_condition(cte.condition)
        survivors: list = []
        dropped = False
        unfiltered_nullable: set[str] | None = None
        for atom in atoms:
            concept = _not_null_concept(atom)
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
