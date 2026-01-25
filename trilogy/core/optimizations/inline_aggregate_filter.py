from trilogy.core.enums import BooleanOperator, Derivation, SourceType
from trilogy.core.models.build import BuildConditional, BuildDatasource
from trilogy.core.models.execute import CTE, RecursiveCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import OptimizationRule


class InlineAggregateFilter(OptimizationRule):
    """Inline a parent filter CTE into a child aggregate CTE.

    When aggregating over filtered columns (e.g., sum(x? y=z)) or using a global
    WHERE clause, the filter creates an intermediate CTE. This optimization inlines
    the filter CTE's datasource directly into the aggregate CTE, producing a single
    SELECT.

    Handles two cases:
    1. SourceType.FILTER - filtered aggregates like sum(x? y=z)
    2. SourceType.DIRECT_SELECT with condition - global WHERE clauses
    """

    def __init__(self) -> None:
        super().__init__()
        self.completed: set[str] = set()

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> bool:
        if isinstance(cte, (UnionCTE, RecursiveCTE)):
            return False
        if cte.name in self.completed:
            return False
        if not cte.group_to_grain:
            self.debug(f"{cte.name} is not a group CTE")
            return False
        if len(cte.parent_ctes) != 1:
            self.debug(f"{cte.name} has {len(cte.parent_ctes)} parents, need exactly 1")
            return False

        parent = cte.parent_ctes[0]
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            self.debug(f"Parent {parent.name} is Union/Recursive CTE")
            return False

        # Check for eligible source types
        is_filter_type = parent.source.source_type == SourceType.FILTER
        is_direct_select_with_condition = (
            parent.source.source_type == SourceType.DIRECT_SELECT
            and parent.condition is not None
        )

        if not (is_filter_type or is_direct_select_with_condition):
            self.debug(
                f"Parent {parent.name} is not a filter or direct select with condition"
            )
            return False

        if not parent.is_root_datasource:
            self.debug(f"Parent {parent.name} is not a root datasource")
            return False

        # For FILTER type, parent should not have a condition (it's in the lineage)
        if is_filter_type and parent.condition:
            self.debug(f"Parent {parent.name} has a condition, cannot inline")
            return False

        if parent.parent_ctes:
            self.debug(f"Parent {parent.name} has its own parents")
            return False

        raw_root = parent.source.datasources[0]
        if not isinstance(raw_root, BuildDatasource):
            self.debug(f"Parent {parent.name}'s datasource is not a BuildDatasource")
            return False
        root: BuildDatasource = raw_root
        if not root.can_be_inlined:
            self.debug(f"Datasource {root.name} cannot be inlined")
            return False

        # Check that all concepts sourced from parent are available on the root
        # or are filter-derived (which will be computed on the fly)
        inherited = {x for x, v in cte.source_map.items() if v and parent.name in v}
        root_outputs = {x.address for x in root.output_concepts}
        filter_outputs = {
            x.address
            for x in parent.output_columns
            if x.derivation == Derivation.FILTER
        }
        available = root_outputs | filter_outputs
        if not inherited.issubset(available):
            missing = inherited - available
            self.debug(
                f"Cannot inline: missing concepts {missing} from parent {parent.name}"
            )
            return False

        # Check grain compatibility
        if not root.grain.issubset(parent.grain):
            self.debug(f"Cannot inline: grain mismatch {root.grain} vs {parent.grain}")
            return False

        # Check that only this CTE uses the parent (otherwise inlining would duplicate work)
        parent_children = inverse_map.get(parent.name, [])
        # Use set of unique names since the same CTE can appear multiple times in the list
        unique_children = {c.name for c in parent_children}
        if len(unique_children) > 1:
            self.debug(
                f"Parent {parent.name} has {len(unique_children)} children, skipping"
            )
            return False

        # Store parent's condition before inlining (for DIRECT_SELECT case)
        parent_condition = parent.condition

        # Perform the inline
        result = cte.inline_parent_datasource(parent, force_group=True)
        if result:
            # Clear source_map for filter-derived concepts so rendering uses their lineage
            for output_col in parent.output_columns:
                if output_col.derivation == Derivation.FILTER:
                    if output_col.address in cte.source_map:
                        cte.source_map[output_col.address] = []

            # Transfer parent's condition to child (for DIRECT_SELECT with WHERE)
            if parent_condition:
                if cte.condition:
                    cte.condition = BuildConditional(
                        left=parent_condition,
                        operator=BooleanOperator.AND,
                        right=cte.condition,
                    )
                else:
                    cte.condition = parent_condition

            self.log(
                f"Inlined filter parent {parent.name} into aggregate CTE {cte.name}"
            )
            self.completed.add(cte.name)
            return True
        self.debug(f"Failed to inline {parent.name} into {cte.name}")
        return False
