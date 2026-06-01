"""Drop tautological ``X IS NOT NULL`` atoms from a CTE's condition.

Once join types and CTE nullability have settled, ``nullable_concepts`` reflects
the real join tree: a column that is non-null in its source and not padded by any
outer join feeding the CTE can never be NULL there, so an ``IS NOT NULL`` on it is
a tautology — pure noise that only pins the concept into the CTE.

This deliberately runs on the built query tree rather than pre-resolution. Before
join planning the only signal is model nullability, which forces a global,
over-conservative guess (a column non-null in its own table can still be padded by
an outer join two hops away). Here the join path is known, so the decision is
exact — never dropping a guard that a real outer join makes meaningful.

To stay sound the concept must be a tracked, non-derived output of the CTE:

- ``Derivation.ROOT``: a derived concept (FILTER/BASIC ``CASE`` …) can be NULL via
  its own expression, which ``nullable_concepts`` does not record.
- present in ``output_columns``: only there is ``nullable_concepts`` authoritative.
  A concept that appears solely inside the condition (e.g. a WHERE-only key that an
  aggregate dropped from its outputs) is not tracked, so absence from the nullable
  set says nothing about whether it can be NULL.
"""

from __future__ import annotations

from trilogy.core.enums import Derivation
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    _not_null_concept,
    combine_condition_atoms,
    decompose_condition,
    is_scalar_condition,
)


def _equivalent_addresses(concepts: list) -> set[str]:
    out: set[str] = set()
    for c in concepts:
        out |= c.equivalent_addresses
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
        for atom in atoms:
            concept = _not_null_concept(atom)
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
