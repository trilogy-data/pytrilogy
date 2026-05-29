from typing import cast

from trilogy.core.enums import BooleanOperator
from trilogy.core.models.build import (
    BoolExpr,
    BuildConditional,
    BuildDatasource,
)
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup


def render_cte_used_map(cte: CTE | UnionCTE) -> dict[str, set[str]]:
    """Render ``cte`` against a throwaway dialect and return the per-parent map
    of addresses it actually consumed. The renderer follows alias/lineage
    chains, so it captures concepts reached via ``output_column.lineage`` that
    a shallow ``output_columns`` scan would miss."""
    from trilogy.dialect.base import BaseDialect

    renderer = BaseDialect()
    renderer.SUPPORTS_AGGREGATE_GROUPING_MODES = True
    renderer.render_cte(cte)
    return dict(renderer.used_map)


def replace_parent(old: CTE, new: CTE, target: CTE | UnionCTE) -> None:
    """Replace old parent with new parent in target CTE's source map."""
    target.replace_dependency(old, new)


def condition_contains_atom(atom: object, condition: object | None) -> bool:
    if condition is None:
        return False
    if condition == atom:
        return True
    if (
        isinstance(condition, BuildConditional)
        and condition.operator == BooleanOperator.AND
    ):
        return condition_contains_atom(atom, condition.left) or condition_contains_atom(
            atom, condition.right
        )
    return False


def strip_condition_atom(
    condition: BoolExpr | None,
    atom: object,
) -> BoolExpr | None:
    if condition is None or condition == atom:
        return None
    if not (
        isinstance(condition, BuildConditional)
        and condition.operator == BooleanOperator.AND
    ):
        return condition
    left = strip_condition_atom(cast(BoolExpr | None, condition.left), atom)
    right = strip_condition_atom(cast(BoolExpr | None, condition.right), atom)
    if left is None:
        return right
    if right is None:
        return left
    return BuildConditional(left=left, operator=BooleanOperator.AND, right=right)


def append_condition(
    condition: BoolExpr | None,
    atom: BoolExpr,
) -> BoolExpr:
    if condition is None:
        return atom
    # Dedup on AND-atoms so re-appending a predicate the condition already
    # carries is a no-op (returns `condition` unchanged) rather than growing
    # an `X AND X` chain across optimizer re-fires.
    return merge_conditions_and_dedup(atom, condition)


def rebuild_and_condition(
    atoms: list[BoolExpr],
) -> BoolExpr | None:
    if not atoms:
        return None
    condition = atoms[0]
    for atom in atoms[1:]:
        condition = BuildConditional(
            left=condition,
            operator=BooleanOperator.AND,
            right=atom,
        )
    return condition


def add_datasource_sorted(
    cte: CTE, datasource: BuildDatasource | QueryDatasource
) -> None:
    if datasource in cte.source.datasources:
        return
    cte.source.datasources = sorted(
        cte.source.datasources + [datasource],
        key=lambda x: x.identifier,
    )


def add_parent_cte(cte: CTE | UnionCTE, parent: CTE | UnionCTE) -> None:
    cte.add_dependency(parent)


def is_sole_consumer(
    cte: CTE,
    parent: CTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> bool:
    """Return True if cte is the only consumer of parent in the inverse map."""
    children = {c.name for c in inverse_map.get(parent.name, [])}
    return len(children) == 1 and cte.name in children


def repoint_consumers(
    old: CTE,
    new: CTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> None:
    """Redirect all consumers of old to new and update the inverse map."""
    consumers = inverse_map.get(old.name, [])
    for child in consumers:
        replace_parent(old, new, child)
    if consumers:
        inverse_map[new.name] = inverse_map.get(new.name, []) + consumers
