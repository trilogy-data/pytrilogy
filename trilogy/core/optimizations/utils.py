from typing import cast

from trilogy.core.enums import BooleanOperator
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
)
from trilogy.core.models.execute import CTE, Join, QueryDatasource, UnionCTE
from trilogy.utility import unique

ConditionExpression = BuildComparison | BuildConditional | BuildParenthetical


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
    target.parent_ctes = [
        x for x in target.parent_ctes if x.safe_identifier != old.safe_identifier
    ] + [new]
    for k, v in target.source_map.items():
        if isinstance(v, list):
            new_sources = []
            for x in v:
                if x == old.safe_identifier:
                    new_sources.append(new.safe_identifier)
                else:
                    new_sources.append(x)
            target.source_map[k] = new_sources
    if not isinstance(target, CTE):
        return
    if target.base_alias_override == old.safe_identifier:
        target.base_alias_override = new.safe_identifier
    if target.base_name_override == old.safe_identifier:
        target.base_name_override = new.safe_identifier

    for join in target.joins:
        if not isinstance(join, Join):
            continue
        if join.left_cte and join.left_cte.safe_identifier == old.safe_identifier:
            join.left_cte = new
        if join.joinkey_pairs:
            for pair in join.joinkey_pairs:
                if pair.cte and pair.cte.safe_identifier == old.safe_identifier:
                    pair.cte = new
        if join.right_cte.safe_identifier == old.safe_identifier:
            join.right_cte = new


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
    condition: ConditionExpression | None,
    atom: object,
) -> ConditionExpression | None:
    if condition is None or condition == atom:
        return None
    if not (
        isinstance(condition, BuildConditional)
        and condition.operator == BooleanOperator.AND
    ):
        return condition
    left = strip_condition_atom(cast(ConditionExpression | None, condition.left), atom)
    right = strip_condition_atom(
        cast(ConditionExpression | None, condition.right), atom
    )
    if left is None:
        return right
    if right is None:
        return left
    return BuildConditional(left=left, operator=BooleanOperator.AND, right=right)


def append_condition(
    condition: ConditionExpression | None,
    atom: ConditionExpression,
) -> ConditionExpression:
    if condition is None:
        return atom
    return BuildConditional(
        left=condition,
        operator=BooleanOperator.AND,
        right=atom,
    )


def rebuild_and_condition(
    atoms: list[ConditionExpression],
) -> ConditionExpression | None:
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
    cte.parent_ctes = unique(cte.parent_ctes + [parent], "name")


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
