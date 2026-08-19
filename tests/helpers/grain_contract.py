"""Plan-time check for the join fan-out contract.

The contract a rendered plan must satisfy:

    a joined (right) side emits at most one row per its join keys

Break it and the join silently multiplies the other side's rows — valid SQL,
wrong answer, invisible unless you count rows. ``CTE.grain`` cannot be trusted
for this: it is what the node *claims*, inferred from what it projects, and a
plain SELECT emits one row per PARENT row regardless. ``cte_true_grain``
computes what a CTE can actually emit instead — a CTE only collapses rows when
it groups, otherwise it inherits its driving source's row identity.

Used by ``test_join_grain_contract.py``. Kept as a utility rather than inlined
because it is the cheap way to check a new planner shape for fan-out: point it
at a model + query and it reports violations at plan time instead of leaving you
to notice a row count is wrong.

NOT suitable for gating the whole corpus as-is — it flags legitimate joins whose
key/grain equivalence it cannot see through (aliases, renames, FD chains the
build environment does not expose). Curated models only.
"""

from dataclasses import dataclass

from trilogy.core.enums import Derivation
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.processing.v4_helper.functional_dependency import (
    build_fd_determines,
)


@dataclass(frozen=True)
class FanoutViolation:
    consumer: str
    joined: str
    join_keys: tuple[str, ...]
    claimed_grain: tuple[str, ...]
    true_grain: tuple[str, ...]

    def __str__(self) -> str:
        return (
            f"{self.consumer} joins {self.joined} on {list(self.join_keys)}; "
            f"{self.joined} claims grain {list(self.claimed_grain)} but emits "
            f"one row per {list(self.true_grain)} — the join multiplies "
            f"{self.consumer}'s rows"
        )


def _driving(cte: CTE | UnionCTE) -> CTE | UnionCTE | None:
    """The source whose row identity this CTE inherits (its FROM base)."""
    parents = cte.dependency_nodes()
    if not parents:
        return None
    base = cte.base_name
    return next((p for p in parents if p.name == base), parents[0])


def unique_per(
    addresses: set[str], keys: set[str], environment: BuildEnvironment
) -> bool:
    """Is every component of ``addresses`` fixed once ``keys`` are fixed?"""
    for address in addresses:
        if address in keys:
            continue
        if build_fd_determines(environment, keys, address, include_empty_grain=False):
            continue
        return False
    return True


def cte_true_grain(
    cte: CTE | UnionCTE,
    environment: BuildEnvironment,
    seen: frozenset[str] = frozenset(),
) -> set[str]:
    """Row identity ``cte`` can actually emit, as opposed to ``cte.grain``."""
    if cte.name in seen:
        return set(cte.grain.components)
    seen = seen | {cte.name}
    # Grouping is the only thing that collapses rows to a declared grain.
    if cte.group_to_grain:
        return set(cte.grain.components)
    # A UNION ALL emits every row of every arm.
    if isinstance(cte, UnionCTE):
        arms = list(cte.internal_ctes) or cte.dependency_nodes()
        emitted: set[str] = set()
        for arm in arms:
            emitted |= cte_true_grain(arm, environment, seen)
        return emitted or set(cte.grain.components)
    base = _driving(cte)
    if base is None:
        return set(cte.grain.components)
    emitted = cte_true_grain(base, environment, seen)
    # A joined side that is already unique per its keys contributes no
    # multiplicity, so it does not widen what this CTE emits.
    for join in cte.joins or []:
        if not isinstance(join, Join) or not join.joinkey_pairs:
            continue
        right = join.right_cte
        if not isinstance(right, (CTE, UnionCTE)) or right.name == base.name:
            continue
        keys = {pair.right.address for pair in join.joinkey_pairs}
        right_grain = cte_true_grain(right, environment, seen)
        if not unique_per(right_grain, keys, environment):
            emitted |= right_grain
    return emitted


def _collapses_duplicates(cte: CTE) -> bool:
    """Does ``cte`` undo multiplicity a join introduced?

    A GROUP BY over plain projections collapses duplicate rows, which is why a
    one-to-many join feeding a grouping consumer is legitimate rather than a
    fan-out — expanding obs rows to one row per tree and then grouping is the
    intended relation, not a bug. It stops being a rescue the moment an
    aggregate is involved: grouping does not un-sum a doubled ``sum(dbh)``.
    """
    if not cte.group_to_grain:
        return False
    return not any(c.derivation == Derivation.AGGREGATE for c in cte.output_columns)


def find_fanout_joins(
    ctes: list[CTE | UnionCTE], environment: BuildEnvironment
) -> list[FanoutViolation]:
    """Every join in ``ctes`` whose right side can multiply the left, where
    nothing downstream in that same CTE collapses the duplicates again."""
    violations: list[FanoutViolation] = []
    for cte in ctes:
        if not isinstance(cte, CTE):
            continue
        if _collapses_duplicates(cte):
            continue
        base = _driving(cte)
        for join in cte.joins or []:
            if not isinstance(join, Join) or not join.joinkey_pairs:
                continue
            right = join.right_cte
            if not isinstance(right, (CTE, UnionCTE)):
                continue
            if base is not None and right.name == base.name:
                continue
            keys = {pair.right.address for pair in join.joinkey_pairs}
            true_grain = cte_true_grain(right, environment)
            if true_grain and not unique_per(true_grain, keys, environment):
                violations.append(
                    FanoutViolation(
                        consumer=cte.name,
                        joined=right.name,
                        join_keys=tuple(sorted(keys)),
                        claimed_grain=tuple(sorted(right.grain.components)),
                        true_grain=tuple(sorted(true_grain)),
                    )
                )
    return violations
