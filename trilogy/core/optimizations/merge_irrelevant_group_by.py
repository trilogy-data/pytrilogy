from trilogy.core.enums import Derivation, SourceType
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import (
    CTE,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    is_sole_consumer,
    render_cte_used_map,
    repoint_consumers,
)

# Child must have no aggregates or other unsafe derivations - it must be
# pure scalar transforms so its GROUP BY is truly vacuous relative to parent.
CHILD_INELIGIBLE_DERIVATIONS = {
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
    Derivation.AGGREGATE,
}
PARENT_INELIGIBLE_DERIVATIONS = {
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
}


def _is_group_by_cte(cte: CTE) -> bool:
    return cte.group_to_grain or cte.source.source_type == SourceType.GROUP


def _is_child_ineligible(concept: BuildConcept, cte: CTE, parent: CTE) -> bool:
    if concept.derivation not in CHILD_INELIGIBLE_DERIVATIONS:
        return False
    return cte.source_map.get(concept.address) != [parent.name]


def _active_parent_ctes(cte: CTE) -> list[CTE | UnionCTE]:
    used_map = render_cte_used_map(cte)
    referenced = set(used_map)
    active = [parent for parent in cte.parent_ctes if parent.name in referenced]
    if active:
        return active
    referenced = {
        source
        for source_list in [
            *cte.source_map.values(),
            *cte.existence_source_map.values(),
        ]
        for source in source_list
    }
    active = [parent for parent in cte.parent_ctes if parent.name in referenced]
    return active or list(cte.parent_ctes)


class MergeIrrelevantGroupBy(OptimizationRule):
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
        active_parents = _active_parent_ctes(cte)
        if len(active_parents) != 1:
            return False, None

        cte.parent_ctes = active_parents
        parent = active_parents[0]
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
        if not is_sole_consumer(cte, parent, inverse_map):
            self.debug(f"Parent {parent.name} has multiple children, skipping")
            return False, None

        # Child must be pure scalar transforms - no aggregates, windows, etc.
        for concept in cte.output_columns:
            if _is_child_ineligible(concept, cte, parent):
                return False, None

        parent_has_aggregate = False
        for concept in parent.output_columns:
            if concept.derivation in PARENT_INELIGIBLE_DERIVATIONS:
                return False, None
            if concept.derivation == Derivation.AGGREGATE:
                parent_has_aggregate = True

        # When the parent computes aggregates, its GROUP BY grain matters; only
        # safe to merge when the child preserves (or refines) that grain.
        # Compare via equivalent_addresses so aliased keys are recognized as equal.
        if parent_has_aggregate:
            child_grain_addresses: set[str] = set()
            for column in cte.output_columns:
                if column.address in cte.grain.components:
                    child_grain_addresses.update(column.equivalent_addresses)
            for component in parent.grain.components:
                if component not in child_grain_addresses:
                    return False, None

        self.log(f"Merging  group-by {cte.name} into irrelevant parent {parent.name}")
        # Ensure any new derived columns from child exist in parent's source_map
        # (empty list means renderer uses concept lineage to compute the expression).
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

        repoint_consumers(cte, parent, inverse_map)

        self.completed.add(cte.name)
        self.completed.add(parent.name)
        return True, {cte.name: parent.name}
