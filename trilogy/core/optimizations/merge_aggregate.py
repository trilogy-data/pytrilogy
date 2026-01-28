from trilogy.core.enums import BooleanOperator, Derivation, SourceType
from trilogy.core.models.build import (
    BuildConditional,
    BuildDatasource,
    BuildWindowItem,
)
from trilogy.core.models.execute import (
    CTE,
    InlinedCTE,
    InstantiatedUnnestJoin,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import OptimizationRule

UNSAFE_DERIVATIONS = {
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
}


def has_unsafe_derivations(cte: CTE) -> bool:
    """Check if a CTE derives any concepts that can't be merged into an aggregate."""
    for concept in cte.output_columns:
        if concept.derivation in UNSAFE_DERIVATIONS:
            return True
        if isinstance(concept.lineage, BuildWindowItem):
            return True
    return False


def replace_parent(old:CTE, new:CTE, target:CTE|UnionCTE, logger):
    """Replace old parent with new parent in target CTE's source map."""
    logger(target)
    target.parent_ctes =  [x for x in target.parent_ctes if x.safe_identifier != old.safe_identifier] + [new] 
    if target.base_alias_override == old.safe_identifier:
        target.base_alias_override = new.safe_identifier
    if target.base_name_override == old.safe_identifier:
        target.base_name_override = new.safe_identifier
    for k, v in target.source_map.items():
        if isinstance(v, list):
            new_sources = []
            for x in v:
                if x == old.safe_identifier:
                    new_sources.append(new.safe_identifier)
                else:
                    new_sources.append(x)
            target.source_map[k] = new_sources
    for join in target.joins:
        if join.left_cte and join.left_cte.safe_identifier == old.safe_identifier:
            join.left_cte = new
        if join.joinkey_pairs:
            for pair in join.joinkey_pairs:
                if pair.cte and pair.cte.safe_identifier == old.safe_identifier:
                    pair.cte = new
        if join.right_cte.safe_identifier == old.safe_identifier:
            join.right_cte = new
    logger(target)
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
    ) -> bool:
        if isinstance(cte, (UnionCTE, RecursiveCTE)):
            return False

        if cte.name in self.completed:
            return False
        
        if cte.joins:
            return False

        # Only optimize aggregate CTEs
        if not cte.group_to_grain and cte.source.source_type != SourceType.GROUP:
            return False

        if not cte.parent_ctes:
            return False

        # Only merge single-parent scenarios for simplicity
        if len(cte.parent_ctes) != 1:
            self.debug(f"CTE {cte.name} has multiple parents, skipping")
            return False

        parent = cte.parent_ctes[0]
        if isinstance(parent, (UnionCTE, RecursiveCTE)):
            self.debug(f"Parent {parent.name} is union/recursive, skipping")
            return False
        if parent.group_to_grain:
            self.debug(f"Parent {parent.name} is aggregate, skipping")
            return False

        # Parent must only be used by this CTE
        parent_children = inverse_map.get(parent.name, [])
        unique_child_names = set(c.name for c in parent_children)
        if len(unique_child_names) != 1 or cte.name not in unique_child_names:
            self.debug(
                f"Parent {parent.name} has multiple children: {list(unique_child_names)}"
            )
            return False

        # Parent must not have unsafe derivations
        if has_unsafe_derivations(parent):
            self.debug(f"Parent {parent.name} has unsafe derivations, skipping")
            return False


        self.log(f"Merging aggregate {cte.name} into parent {parent.name}.")

        for x in cte.output_columns:
            if x not in parent.output_columns:
                parent.output_columns.append(x)

        parent.output_columns = [x for x in parent.output_columns if x.address in cte.output_columns]
        parent.group_to_grain = True
        for k, v in inverse_map.items():
            if cte.name == k:
                for child in v:
                    replace_parent(cte, parent, child, self.log)
                inverse_map[parent.name] = inverse_map.get(parent.name, []) + v
        return True
