from enum import Enum

from trilogy.core.enums import Derivation, SourceType
from trilogy.core.models.build import (
    BuildWindowItem,
)
from trilogy.core.models.execute import (
    CTE,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import is_sole_consumer, repoint_consumers

UNSAFE_DERIVATIONS = {
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
}


class MergeMode(Enum):
    AGGREGATE = "aggregate"
    WINDOW = "window"
    BASIC = "basic"


def has_unsafe_derivations(cte: CTE) -> bool:
    """Check if a CTE derives any concepts that can't be merged into an aggregate."""
    for concept in cte.output_columns:
        if concept.derivation in UNSAFE_DERIVATIONS:
            return True
        if isinstance(concept.lineage, BuildWindowItem):
            return True
    return False


def get_merge_mode(cte: CTE) -> MergeMode | None:
    if cte.group_to_grain or cte.source.source_type == SourceType.GROUP:
        return MergeMode.AGGREGATE
    if cte.source.source_type == SourceType.WINDOW:
        return MergeMode.WINDOW
    if cte.source.source_type == SourceType.BASIC:
        return MergeMode.BASIC
    return None


def parent_is_ineligible(parent: CTE, merge_mode: MergeMode) -> bool:
    if merge_mode == MergeMode.AGGREGATE:
        return parent.group_to_grain or parent.source.source_type in (
            SourceType.GROUP,
            SourceType.WINDOW,
            SourceType.SUBSELECT,
        )
    if merge_mode == MergeMode.WINDOW:
        return (
            parent.group_to_grain
            or parent.condition is not None
            or parent.source.source_type
            in (
                SourceType.GROUP,
                SourceType.FILTER,
                SourceType.SUBSELECT,
                SourceType.WINDOW,
            )
        )
    # BASIC
    return parent.group_to_grain or parent.source.source_type in (
        SourceType.GROUP,
        SourceType.WINDOW,
        SourceType.SUBSELECT,
        SourceType.UNNEST,
    )


def child_has_merge_blockers(cte: CTE, merge_mode: MergeMode) -> bool:
    if merge_mode == MergeMode.WINDOW and cte.condition is not None:
        return True
    if merge_mode == MergeMode.BASIC and cte.condition is not None:
        return True
    return False


def apply_child_merge(parent: CTE, cte: CTE, merge_mode: MergeMode) -> None:
    for column in cte.output_columns:
        if column not in parent.output_columns:
            parent.output_columns.append(column)

    if merge_mode == MergeMode.AGGREGATE:
        # Aggregate merge: keep only columns the child exposes (grouping keys +
        # aggregate outputs). Everything else is rolled up.
        parent.output_columns = [
            column
            for column in parent.output_columns
            if column.address in cte.output_lcl
        ]
        parent.group_to_grain = True
    elif merge_mode == MergeMode.WINDOW:
        # Window merge: the parent's intermediate columns may still be referenced
        # by window expressions (e.g. inlined CASE branches or unmaterialized
        # aggregates). Don't prune — just extend.
        parent.source.source_type = SourceType.WINDOW
    # BASIC merge: keep parent's source_type; child's basic projections render
    # alongside parent's outputs. HideUnusedConcepts handles pruning later.


class CollapseSingleParent(OptimizationRule):
    """Collapse a child CTE into its single parent by folding the parent's
    datasources and conditions into the child's shape.

    Handles three merge modes (AGGREGATE, WINDOW, BASIC). When the child CTE
    has a single parent that:
    1. Is only used by this child (no other children)
    2. Doesn't derive window functions or other unsafe derivations
    3. Has compatible datasources

    We can merge the parent's datasources and conditions directly into the
    child, eliminating an unnecessary subquery.
    """

    def __init__(self) -> None:
        super().__init__()
        self.completed: set[str] = set()

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, (UnionCTE, RecursiveCTE)):
            return False, None

        if cte.name in self.completed:
            return False, None

        if cte.joins:
            return False, None

        merge_mode = get_merge_mode(cte)
        if merge_mode is None:
            return False, None

        if child_has_merge_blockers(cte, merge_mode):
            self.debug(f"CTE {cte.name} has child-specific merge blockers, skipping")
            return False, None

        if not cte.parent_ctes:
            return False, None

        # Only merge single-parent scenarios for simplicity
        if len(cte.parent_ctes) != 1:
            self.debug(f"CTE {cte.name} has multiple parents, skipping")
            return False, None

        parent = cte.parent_ctes[0]
        if cte.base_alias != parent.safe_identifier:
            self.debug(
                f"CTE {cte.name} base alias {cte.base_alias} does not match parent {parent.safe_identifier}, skipping"
            )
            return False, None
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            self.debug(f"Parent {parent.name} is union/recursive, skipping")
            return False, None
        if parent_is_ineligible(parent, merge_mode):
            self.debug(
                f"Parent {parent.name} is ineligible type {parent.source.source_type}, skipping"
            )
            return False, None

        # Parent must only be used by this CTE
        if not is_sole_consumer(cte, parent, inverse_map):
            self.debug(f"Parent {parent.name} has multiple children, skipping")
            return False, None

        if has_unsafe_derivations(parent):
            self.log(f"Parent {parent.name} has unsafe derivations, skipping")
            return False, None
        if merge_mode == MergeMode.AGGREGATE:
            for x in parent.output_columns:
                if x.derivation == Derivation.AGGREGATE and not parent.source_map.get(
                    x.address
                ):
                    self.log(
                        f"Parent {parent.name} has aggregate derivations without source map, skipping"
                    )
                    return False, None

        self.log(
            f"Collapsing {merge_mode.value} CTE {cte.name} into parent {parent.name} ({parent.source.source_type})."
        )

        apply_child_merge(parent, cte, merge_mode)
        repoint_consumers(cte, parent, inverse_map)

        # Return merged map: old CTE name -> replacement CTE name
        return True, {cte.name: parent.name}
