from trilogy.core.enums import Derivation, SourceType
from trilogy.core.models.execute import (
    CTE,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.merge_aggregate import replace_parent

# Child must have no aggregates or other unsafe derivations — it must be
# pure scalar transforms so its GROUP BY is truly vacuous relative to parent.
CHILD_INELIGIBLE_DERIVATIONS = {
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
    Derivation.AGGREGATE,
}


def _is_group_by_cte(cte: CTE) -> bool:
    return cte.group_to_grain or cte.source.source_type == SourceType.GROUP


def _grain_determined_by(
    address: str, parent_grain: set[str], child: CTE, visited: set[str]
) -> bool:
    """Check if a concept address is functionally determined by parent's grain."""
    if address in parent_grain:
        return True
    if address in visited:
        return False
    visited.add(address)
    concept = child.get_concept(address)
    if concept is None or concept.lineage is None:
        return False
    return all(
        _grain_determined_by(arg.address, parent_grain, child, visited)
        for arg in concept.concept_arguments
    )


def _all_grain_determined_by_parent(child: CTE, parent: CTE) -> bool:
    """Return True if child has grain and every component is determined by parent's grain."""
    parent_grain = parent.grain.components
    return bool(child.grain.components) and all(
        _grain_determined_by(addr, parent_grain, child, set())
        for addr in child.grain.components
    )


class MergeVacuousGroupBy(OptimizationRule):
    """Merge a GROUP BY CTE into its parent GROUP BY CTE when the grouping is redundant.

    When a CTE groups by keys that are all functionally determined by its parent's
    grain, the GROUP BY adds no new deduplication. We fold the child's computed columns
    into the parent, replace the parent's grain/output with the child's (coarser) grain,
    and eliminate the child CTE.
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
        if cte.condition:
            return False, None
        if not _is_group_by_cte(cte):
            return False, None
        if len(cte.parent_ctes) != 1:
            return False, None

        parent = cte.parent_ctes[0]
        if cte.base_alias != parent.safe_identifier:
            self.debug(
                f"CTE {cte.name} base alias {cte.base_alias} != parent {parent.safe_identifier}, skipping"
            )
            return False, None
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            return False, None
        if not _is_group_by_cte(parent):
            return False, None

        # Parent must only be used by this CTE
        parent_children = inverse_map.get(parent.name, [])
        unique_child_names = {c.name for c in parent_children}
        if len(unique_child_names) != 1 or cte.name not in unique_child_names:
            self.debug(
                f"Parent {parent.name} has multiple children: {list(unique_child_names)}"
            )
            return False, None

        # Child must be pure scalar transforms — no aggregates, windows, etc.
        for concept in cte.output_columns:
            if concept.derivation in CHILD_INELIGIBLE_DERIVATIONS:
                return False, None

        if not _all_grain_determined_by_parent(cte, parent):
            self.debug(
                f"CTE {cte.name} grain is not determined by parent {parent.name}, skipping"
            )
            return False, None

        self.log(f"Merging vacuous group-by {cte.name} into parent {parent.name}")

        # Ensure any new derived columns from child exist in parent's source_map
        # (empty list → renderer uses concept lineage to compute the expression).
        parent_output_addresses = {x.address for x in parent.output_columns}
        for x in cte.output_columns:
            if x.address not in parent_output_addresses:
                parent.output_columns.append(x)
            if x.address not in parent.source_map:
                parent.source_map[x.address] = []

        # Replace parent's output with child's (coarser) output and grain.
        # Child's output_columns already contains the hidden group-by keys
        # (e.g. customer_id, item_id) so GROUP BY is preserved correctly.
        cte_output_addresses = {x.address for x in cte.output_columns}
        parent.output_columns = [
            x for x in parent.output_columns if x.address in cte_output_addresses
        ]
        parent.grain = cte.grain
        parent.hidden_concepts = parent.hidden_concepts.union(cte.hidden_concepts)

        # Repoint child's consumers at parent; child becomes childless and is
        # dropped by filter_irrelevant_ctes.
        for k, v in inverse_map.items():
            if cte.name == k:
                for child in v:
                    replace_parent(cte, parent, child)
                inverse_map[parent.name] = inverse_map.get(parent.name, []) + v

        self.completed.add(parent.name)
        return True, {cte.name: parent.name}
