from trilogy.core.enums import (
    BooleanOperator,
    SourceType,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConceptArgs,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
    BuildWindowItem,
    BuildConcept
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import OptimizationRule
from trilogy.core.processing.utility import is_scalar_condition
from trilogy.utility import unique


def is_child_of(a, comparison):
    base = comparison == a
    if base:
        return True
    if isinstance(comparison, BuildConditional):
        return (
            is_child_of(a, comparison.left) or is_child_of(a, comparison.right)
        ) and comparison.operator == BooleanOperator.AND
    return base


class HideUnusedConcepts(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


   

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> bool:
        used = set()
        for parent_concept in cte.output_columns:
            for v in inverse_map.get(cte.name, []):
                for child_concept, sources in v.source_map.items():
                    if child_concept == parent_concept.address and cte.name in sources:
                        if parent_concept.address in [c.address for c in v.output_columns]:
                            used.add(parent_concept.address)
                    if not sources:
                        full = [x for x in v.output_columns if x.address == child_concept][0]
                        if parent_concept.address in full.concept_arguments:
                            used.add(parent_concept.address)
                if isinstance(v, CTE) and v.joins:
                    for join in v.joins:
                        # if isinstance(join, InstantiatedUnnestJoin):
                        #     continue
                        for pair in (join.joinkey_pairs or []):
                            if (pair.cte.name == cte.name or cte.name == join.right_cte.name) and pair.right.address == parent_concept.address:
                                used.add(parent_concept.address)
                if v.condition and v.condition and parent_concept.address in v.condition.concept_arguments:
                    used.add(parent_concept.address)


        add_to_hidden:list[BuildConcept] = []
        for concept in cte.output_columns:
            if concept.address not in used:
                add_to_hidden.append(concept)
        newly_hidden = [x.address for x in add_to_hidden if x.address not in cte.hidden_concepts]
        if not newly_hidden:
            return False
        self.log(
            f"Hiding unused concepts {[x.address for x in add_to_hidden]} from {cte.name} (used: {used}, all: {[x.address for x in cte.output_columns]})"
        )
        cte.hidden_concepts = cte.hidden_concepts.union(set([x.address for x in add_to_hidden if x.address not in cte.hidden_concepts]))
        return True

