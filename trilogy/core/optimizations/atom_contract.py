"""The WHERE-atom contract the optimizer driver needs to reach a fixpoint
(`formal/Trilogy/Fixpoint.lean`).

A *fact remover* is a phase that drops a WHERE atom because the CTE's plan
facts (its outputs, joins, nullability) make the atom a tautology there. No
other phase may add an atom a fact remover would drop: the two would undo
each other on every pass, and the driver would run to its pass cap
(issue #830, `Fixpoint.ungated_cycles`). An adder gates on `removable`; under
`enforce_fixpoint_contract` the driver checks every phase's additions against
the same predicates (`contract_violations`), so the theorem's premise holds on
every plan the test suite builds, not only for the rules that gate today.
"""

from collections.abc import Callable, Sequence
from typing import Any

from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.strip_redundant_not_null import (
    and_atoms,
    redundant_not_null,
)

# phase name -> whether it would drop the atom from the CTE
FACT_REMOVERS: dict[str, Callable[[CTE, Any], bool]] = {
    "strip_redundant_not_null": redundant_not_null,
}

AtomSnapshot = dict[str, list]


def removable(cte: CTE, atom: Any) -> str | None:
    """The fact remover that would drop `atom` from `cte`'s WHERE, if any."""
    for name, removes in FACT_REMOVERS.items():
        if removes(cte, atom):
            return name
    return None


def atom_snapshot(ctes: Sequence[CTE | UnionCTE]) -> AtomSnapshot:
    return {
        cte.name: and_atoms(cte.condition) if cte.condition is not None else []
        for cte in ctes
        if isinstance(cte, CTE)
    }


def contract_violations(
    phase: str, before: AtomSnapshot, ctes: Sequence[CTE | UnionCTE]
) -> list[str]:
    """Atoms `phase` added to a CTE that a fact remover would drop again."""
    if phase in FACT_REMOVERS:
        return []
    out: list[str] = []
    for cte in ctes:
        if not isinstance(cte, CTE) or cte.name not in before:
            continue
        prior = before[cte.name]
        for atom in atom_snapshot([cte])[cte.name]:
            if atom in prior:
                continue
            remover = removable(cte, atom)
            if remover is not None:
                out.append(f"{phase} added {atom} to {cte.name}; {remover} drops it")
    return out
