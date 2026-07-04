from enum import Enum

from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    SourceType,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
    BuildRowsetItem,
    BuildWindowItem,
)
from trilogy.core.models.execute import (
    CTE,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import is_sole_consumer, repoint_consumers
from trilogy.core.processing.condition_utility import merge_conditions_and_dedup

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


def has_basic_derivation(cte: CTE) -> bool:
    return any(concept.derivation == Derivation.BASIC for concept in cte.output_columns)


def get_merge_mode(cte: CTE) -> MergeMode | None:
    if cte.group_to_grain or cte.source.source_type == SourceType.GROUP:
        return MergeMode.AGGREGATE
    if cte.source.source_type == SourceType.WINDOW:
        return MergeMode.WINDOW
    if has_basic_derivation(cte):
        return MergeMode.BASIC
    return None


def lineage_contains_aggregate(concept: BuildConcept, seen: set[str]) -> bool:
    """True if `concept`'s lineage tree contains an aggregate anywhere — directly
    (`sum(x)`), or wrapped in a filter/function/rowset (`sum(x) ? cond`,
    `coalesce(sum(x), 0)`). Used to detect a parent column that renders an
    aggregate inline; folding it into an AGGREGATE child's `sum(...)` would
    nest aggregates ("aggregate function calls cannot be nested")."""
    if concept.address in seen:
        return False
    seen.add(concept.address)
    lineage = concept.lineage
    if isinstance(lineage, BuildAggregateWrapper):
        return True
    if isinstance(lineage, BuildConceptArgs):
        return any(
            lineage_contains_aggregate(arg, seen) for arg in lineage.concept_arguments
        )
    return False


def has_nonstandard_aggregate_grouping(concept: BuildConcept) -> bool:
    lineage = concept.lineage
    if isinstance(lineage, BuildAggregateWrapper):
        return lineage.grouping != AggregateGroupingMode.STANDARD
    if isinstance(lineage, BuildRowsetItem):
        return has_nonstandard_aggregate_grouping(lineage.content)
    return False


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
    # BASIC: a scalar projection over a GROUP parent folds into the GROUP's
    # SELECT list (`sum(a)/sum(b)` rendered alongside the GROUP BY is valid SQL,
    # and matches v3's single-CTE shape). This is only sound for the safe subset
    # gated by `basic_fold_into_group_is_safe` (checked separately in `optimize`),
    # never blanket. WINDOW/SUBSELECT/UNNEST parents still can't absorb a
    # downstream row projection without changing row shape.
    return parent.source.source_type in (
        SourceType.WINDOW,
        SourceType.SUBSELECT,
        SourceType.UNNEST,
    )


def parent_is_group(parent: CTE) -> bool:
    return parent.group_to_grain or parent.source.source_type == SourceType.GROUP


def basic_fold_into_group_is_safe(parent: CTE, cte: CTE) -> bool:
    """Gate the BASIC-into-GROUP fold to the provably row-preserving subset.

    Folding a row projection into a GROUP parent is only sound when it cannot
    alter the parent's grouping. GROUP BY is rendered solely from
    ``parent.group_concepts`` (which `apply_child_merge`'s BASIC path leaves
    untouched), and a BASIC-merge child is row-preserving by construction (it is
    not ``group_to_grain`` and its source is not GROUP, so it never regroups).
    The remaining requirement is that every output the child *derives locally*
    is a scalar row projection over the parent's ``{grain keys, aggregates}``:

    - aggregate / window / unnest / recursive columns the child computes *anew*
      can't ride in the parent's GROUP BY select, so they disqualify the fold;
    - the same column kinds merely *passed through* from the GROUP parent are
      fine (already computed there) -- e.g. a dimension projection that joins a
      label onto a grouped fact carries the parent's aggregates straight through.

    The caller has already established the GROUP is the child's sole parent, so
    every child input necessarily resolves to a parent output."""
    parent_outputs = parent.output_lcl
    for column in cte.output_columns:
        if column.address in parent_outputs:
            continue  # passthrough of a parent grain key / aggregate -- safe
        if (
            column.derivation in UNSAFE_DERIVATIONS
            or column.derivation == Derivation.AGGREGATE
        ):
            return False
        if isinstance(column.lineage, (BuildAggregateWrapper, BuildWindowItem)):
            return False
    return True


def child_has_merge_blockers(cte: CTE, merge_mode: MergeMode) -> bool:
    if merge_mode == MergeMode.WINDOW and cte.condition is not None:
        return True
    if merge_mode == MergeMode.BASIC and cte.condition is not None:
        return True
    if merge_mode == MergeMode.AGGREGATE:
        return any(
            has_nonstandard_aggregate_grouping(concept)
            for concept in cte.output_columns
        )
    return False


def apply_child_merge(parent: CTE, cte: CTE, merge_mode: MergeMode) -> None:
    for column in cte.output_columns:
        if column not in parent.output_columns:
            parent.output_columns.append(column)

    # AND-combine the child's WHERE into the parent — for AGGREGATE merges the
    # child sits BELOW the aggregate (its condition is the pre-aggregation
    # WHERE), and the parent becomes the aggregate after the merge, so its
    # WHERE has to carry the predicate forward. (WINDOW/BASIC modes are blocked
    # upstream by `child_has_merge_blockers` when the child has a condition.)
    # Dedup on AND-atoms so a chain of merges (or a predicate already carried
    # by the parent) can't re-stamp it into `H AND H AND H` — q31's HAVING.
    if cte.condition is not None:
        parent.condition = (
            merge_conditions_and_dedup(cte.condition, parent.condition)
            if parent.condition is not None
            else cte.condition
        )

    # The child's row limit (and the ORDER BY it selects under) applies to the
    # merged CTE's output — LIMIT is the last logical operation of a SELECT,
    # so absorbing the parent's shape below it is row-identical. (The caller
    # rejects limited PARENTS, the direction that would cross the boundary.)
    if cte.limit is not None:
        parent.limit = cte.limit
        parent.order_by = cte.order_by

    if merge_mode == MergeMode.AGGREGATE:
        # Aggregate merge: keep only columns the child exposes (grouping keys +
        # aggregate outputs). Everything else is rolled up.
        parent.output_columns = [
            column
            for column in parent.output_columns
            if column.address in cte.output_lcl
        ]
        parent.group_to_grain = True
        parent.grain = cte.grain
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

        parents = cte.dependency_nodes()
        if not parents:
            return False, None

        # Only merge single-parent scenarios for simplicity
        if len(parents) != 1:
            self.debug(f"CTE {cte.name} has multiple parents, skipping")
            return False, None

        parent = parents[0]
        if cte.base_alias != parent.safe_identifier:
            self.debug(
                f"CTE {cte.name} base alias {cte.base_alias} does not match parent {parent.safe_identifier}, skipping"
            )
            return False, None
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            self.debug(f"Parent {parent.name} is union/recursive, skipping")
            return False, None
        # A row LIMIT on the PARENT is an opaque boundary: folding the child's
        # shape into it moves work below the limit (pre-limit rows change).
        # A limited CHILD is fine — LIMIT evaluates last in the merged SELECT
        # and `apply_child_merge` carries it (with its ORDER BY) across.
        if parent.limit is not None:
            self.debug(f"Parent {parent.name} carries a row limit, skipping")
            return False, None
        if parent_is_ineligible(parent, merge_mode):
            self.debug(
                f"Parent {parent.name} is ineligible type {parent.source.source_type}, skipping"
            )
            return False, None
        if (
            merge_mode == MergeMode.BASIC
            and parent_is_group(parent)
            and not basic_fold_into_group_is_safe(parent, cte)
        ):
            self.debug(
                f"BASIC fold of {cte.name} into GROUP parent {parent.name} is "
                "not row-preserving (grain change or non-scalar output), skipping"
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
            # An AGGREGATE merge wraps the parent's exposed columns in the child's
            # aggregates. A parent column that is rendered inline (no source_map
            # entry — not materialized by a grouping below) and whose lineage
            # contains an aggregate would be folded *inside* the child's `sum(...)`,
            # producing illegal nested aggregates. This covers a direct aggregate
            # column and one wrapped in a filter/function (`sum(x) ? cond`).
            for x in parent.output_columns:
                if not parent.source_map.get(x.address) and lineage_contains_aggregate(
                    x, set()
                ):
                    self.log(
                        f"Parent {parent.name} renders inline aggregate {x.address}, skipping"
                    )
                    return False, None

        self.log(
            f"Collapsing {merge_mode.value} CTE {cte.name} into parent {parent.name} ({parent.source.source_type})."
        )

        apply_child_merge(parent, cte, merge_mode)
        repoint_consumers(cte, parent, inverse_map)

        # Return merged map: old CTE name -> replacement CTE name
        return True, {cte.name: parent.name}
