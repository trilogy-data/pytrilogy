"""The WHERE-atom contract the optimizer driver needs to reach a fixpoint
(`formal/Trilogy/Fixpoint.lean`).

A *remover* is a phase that drops WHERE atoms; it registers the predicate it
drops by (`register_remover`). An adder that adds an atom a remover would drop
undoes it on every pass, and the driver runs to its pass cap (issue #830,
`Fixpoint.ungated_cycles`), so adders gate on `removable`.

A *fact* remover judges an atom by the CTE's plan facts alone (outputs, joins,
nullability), which no adder changes. For those, under
`enforce_fixpoint_contract`, the driver checks every phase's additions
(`contract_violations`), so the theorem's premise holds on every plan the test
suite builds. A remover that judges an atom by other atoms
(`PredicatePushdownRemove` reads the parents' WHERE) is gated, not checked:
pushdown moving an atom down a chain makes each copy above redundant, and that
is progress, not a cycle; the pass cap guards it.

A merge is exempt: it moves a CTE's atoms into the CTE that absorbs it and
removes a CTE, so it can happen only finitely often and drive no cycle.
"""

from collections.abc import Callable, Collection, Sequence
from typing import Any

from trilogy.core.enums import BooleanOperator
from trilogy.core.models.build import BuildConditional
from trilogy.core.models.execute import CTE, UnionCTE

AtomRemover = Callable[[CTE, Any], bool]

# phase name -> whether that phase would drop the atom from the CTE
REMOVERS: dict[str, AtomRemover] = {}
# phase name -> the part of its remover that reads the CTE's plan facts alone
FACT_REMOVERS: dict[str, AtomRemover] = {}

AtomSnapshot = dict[str, list]


def register_remover(
    *phases: str, facts_only: bool = False, facts: AtomRemover | None = None
) -> Callable[[AtomRemover], AtomRemover]:
    """`facts` is the fact part of a remover that also reads other atoms:
    the driver checks additions against it, while adders gate on the whole."""

    def register(removes: AtomRemover) -> AtomRemover:
        for phase in phases:
            REMOVERS[phase] = removes
            if facts_only or facts is not None:
                FACT_REMOVERS[phase] = facts or removes
        return removes

    return register


def and_atoms(condition: object) -> list:
    """Every AND-ed atom, a bare boolean concept included, which
    ``decompose_condition`` keeps bundled with its siblings."""
    if isinstance(condition, BuildConditional) and (
        condition.operator == BooleanOperator.AND
    ):
        return and_atoms(condition.left) + and_atoms(condition.right)
    return [condition]


def removable(cte: CTE, atom: Any, phases: Collection[str] | None = None) -> str | None:
    """The remover that would drop `atom` from `cte`'s WHERE, if any."""
    for name, removes in REMOVERS.items():
        if (phases is None or name in phases) and removes(cte, atom):
            return name
    return None


def atom_snapshot(ctes: Sequence[CTE | UnionCTE]) -> AtomSnapshot:
    return {
        cte.name: and_atoms(cte.condition) if cte.condition is not None else []
        for cte in ctes
        if isinstance(cte, CTE)
    }


def contract_violations(
    phase: str,
    before: AtomSnapshot,
    ctes: Sequence[CTE | UnionCTE],
    merged_into: set[str],
) -> list[str]:
    """Atoms `phase` added to a CTE that a fact remover would drop again."""
    if phase in REMOVERS:
        return []
    out: list[str] = []
    for cte in ctes:
        if (
            not isinstance(cte, CTE)
            or cte.name not in before
            or cte.name in merged_into
        ):
            continue
        prior = before[cte.name]
        for atom in atom_snapshot([cte])[cte.name]:
            if atom in prior:
                continue
            for remover, removes in FACT_REMOVERS.items():
                if removes(cte, atom):
                    out.append(
                        f"{phase} added {atom} to {cte.name}; {remover} drops it"
                    )
                    break
    return out
