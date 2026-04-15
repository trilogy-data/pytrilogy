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
    # Derivation.AGGREGATE,
}


def has_unsafe_derivations(cte: CTE) -> bool:
    """Check if a CTE derives any concepts that can't be merged into an aggregate."""
    for concept in cte.output_columns:
        if concept.derivation in UNSAFE_DERIVATIONS:
            return True
        if isinstance(concept.lineage, BuildWindowItem):
            return True
    return False


class MergeAggregate(OptimizationRule):
    """Merge a parent CTE into an aggregate child CTE.

    When an aggregate CTE has a single parent that:
    1. Is only used by this aggregate (no other children)
    2. Doesn't derive window functions or other unsafe derivations
    3. Has compatible datasources

    We can merge the parent's datasources and conditions directly into the
    aggregate, eliminating an unnecessary subquery.
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

        # Only optimize aggregate CTEs
        if not cte.group_to_grain and cte.source.source_type != SourceType.GROUP:
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
        if parent.group_to_grain or parent.source.source_type in (
            SourceType.GROUP,
            SourceType.WINDOW,
            SourceType.SUBSELECT,
        ):
            self.debug(
                f"Parent {parent.name} is ineligible type {parent.source.source_type}, skipping"
            )
            return False, None

        # Parent must only be used by this CTE
        if not is_sole_consumer(cte, parent, inverse_map):
            self.debug(f"Parent {parent.name} has multiple children, skipping")
            return False, None

        # Parent must not have unsafe derivations
        if has_unsafe_derivations(parent):
            self.log(f"Parent {parent.name} has unsafe derivations, skipping")
            return False, None
        for x in parent.output_columns:
            if x.derivation == Derivation.AGGREGATE and not parent.source_map.get(
                x.address
            ):
                self.log(
                    f"Parent {parent.name} has aggregate derivations without source map, skipping"
                )
                return False, None
        self.log(
            f"Merging aggregate {cte.name} into parent {parent.name} ({parent.source.source_type})."
        )

        for x in cte.output_columns:
            if x not in parent.output_columns:
                parent.output_columns.append(x)

        parent.output_columns = [
            x for x in parent.output_columns if x.address in cte.output_columns
        ]

        parent.group_to_grain = True
        repoint_consumers(cte, parent, inverse_map)

        # Return merged map: old CTE name -> replacement CTE name
        return True, {cte.name: parent.name}
