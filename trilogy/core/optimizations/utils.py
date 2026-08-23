import dataclasses
from typing import cast

from trilogy.core.enums import BooleanOperator, Derivation, FunctionType, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildRowsetItem,
)
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup

# Derivations whose rows cannot be re-scoped: a window, unnest or recursive
# output changes meaning when its CTE is folded into, or filtered by, another.
SENSITIVE_DERIVATIONS = frozenset(
    {Derivation.WINDOW, Derivation.UNNEST, Derivation.RECURSIVE}
)


def is_grouped_cte(cte: CTE) -> bool:
    return cte.group_to_grain or cte.source.source_type == SourceType.GROUP


def equivalent_addresses(concepts: list[BuildConcept]) -> set[str]:
    out: set[str] = set()
    for c in concepts:
        out |= c.equivalent_addresses
    return out


def base_datasource(
    datasource: BuildDatasource | QueryDatasource,
) -> BuildDatasource | QueryDatasource | None:
    if isinstance(datasource, QueryDatasource):
        return datasource.base_datasource
    return None


def render_cte_used_map(cte: CTE | UnionCTE) -> dict[str, set[str]]:
    """Render ``cte`` against a throwaway dialect and return the per-parent map
    of addresses it actually consumed. The renderer follows alias/lineage
    chains, so it captures concepts reached via ``output_column.lineage`` that
    a shallow ``output_columns`` scan would miss."""
    from trilogy.dialect.base import BaseDialect

    renderer = BaseDialect()
    renderer.SUPPORTS_AGGREGATE_GROUPING_MODES = True
    renderer.SUPPORTS_QUALIFY = True
    renderer.render_cte(cte)
    return dict(renderer.used_map)


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


def add_datasource_sorted(
    cte: CTE, datasource: BuildDatasource | QueryDatasource
) -> None:
    if datasource in cte.source.datasources:
        return
    cte.source.datasources = sorted(
        cte.source.datasources + [datasource],
        key=lambda x: x.identifier,
    )


def rename_reference(column: BuildConcept) -> BuildConcept | None:
    """The single column a pure rename re-labels, else None.

    Two rename shapes exist: a rowset boundary output (`with rs as select x ...`
    exposing `rs.x` over `x`) and a concept alias (`select x as y`). Both render
    as `<content's sql> as <new name>`, so a CTE whose novel outputs are all
    renames of parent columns folds into the parent — the merged CTE renders the
    rename from lineage (no source_map entry) against its own columns."""
    lineage = column.lineage
    if isinstance(lineage, BuildRowsetItem):
        return lineage.content
    if isinstance(lineage, BuildFunction) and lineage.operator == FunctionType.ALIAS:
        args = lineage.concept_arguments
        if len(args) == 1:
            return args[0]
    return None


def consumed_parent_column(
    column: BuildConcept, cte: CTE, parent: CTE
) -> BuildConcept | None:
    """The parent output column `cte` actually renders `column` from, when that
    render is a bare parent-column reference (`"parent"."T"`); None otherwise.

    Pseudonym recovery may have picked ANY exposed twin T — including a
    scoped-join canonical whose expression is a coalesce the lineage chain
    never mentions — so T must be recovered from the actual render, not
    predicted from lineage."""
    from trilogy.dialect.base import BaseDialect

    renderer = BaseDialect()
    try:
        rendered = renderer.render_concept_sql(column, cte, alias=False)
    except Exception:  # unrenderable: not a bare reference
        return None
    q = renderer.QUOTE_CHARACTER
    prefix = f"{q}{parent.safe_identifier}{q}.{q}"
    if not (rendered.startswith(prefix) and rendered.count(q) == 4):
        return None
    consumed_name = rendered[len(prefix) : -1]
    return next(
        (c for c in parent.output_columns if c.safe_address == consumed_name), None
    )


def rebind_rename_to_consumed(
    column: BuildConcept, consumed: BuildConcept
) -> BuildConcept:
    """Rebind a rename column's lineage to the exact parent column OBJECT it
    consumed, so the merged CTE renders it through that object forever.

    A rename's lineage re-derivation FLOATS: it resolves against whatever
    bindings the CTE has when rendered, and later phases (datasource inlining,
    further collapses) add bindings that flip it to a same-address side-variant
    (raw column where the child read the coalescing canonical — a FULL
    union-join axis then NULLs one-sided keys). Pinning the consumed object
    makes the rename and the parent's own output render identically in every
    future context, because they are the same object."""
    lineage = column.lineage
    if isinstance(lineage, BuildRowsetItem) and lineage.content is not consumed:
        return dataclasses.replace(
            column, lineage=dataclasses.replace(lineage, content=consumed)
        )
    if (
        isinstance(lineage, BuildFunction)
        and lineage.operator == FunctionType.ALIAS
        and len(lineage.arguments) == 1
        and lineage.arguments[0] is not consumed
    ):
        return dataclasses.replace(
            column, lineage=dataclasses.replace(lineage, arguments=[consumed])
        )
    return column


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
        child.replace_dependency(old, new)
    if consumers:
        inverse_map[new.name] = inverse_map.get(new.name, []) + consumers
