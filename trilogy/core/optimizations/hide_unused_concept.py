from trilogy.core.enums import (
    BooleanOperator,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import OptimizationRule


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
        from trilogy.dialect.base import BaseDialect

        renderer = BaseDialect()
        # for parent_concept in cte.output_columns:
        children = inverse_map.get(cte.name, [])
        if not children:
            return False
        return False
        for v in children:
            self.log(f"Analyzing usage of {cte.name} in {v.name}")
            renderer.render_cte(v)
            # if parent_concept.address in used:
            #     break
            # for child_concept, sources in v.source_map.items():
            #     if parent_concept.address in used:
            #         break
            #     if child_concept == parent_concept.address and cte.name in sources:
            #         if parent_concept.address in [
            #             c.address for c in v.output_columns
            #         ]:
            #             used.add(parent_concept.address)
            #             break
            #     if not sources:
            #         full = [
            #             x for x in v.output_columns if x.address == child_concept
            #         ][0]
            #         self.log(
            #             f"Checking {parent_concept.address} against {full.address} in {v.name} with {full.concept_arguments}"
            #         )
            #         if parent_concept.address in full.concept_arguments:
            #             used.add(parent_concept.address)
            # if isinstance(v, CTE) and v.joins:
            #     for join in v.joins:
            #         # if isinstance(join, InstantiatedUnnestJoin):
            #         #     continue
            #         for pair in join.joinkey_pairs or []:
            #             if (
            #                 pair.cte.name == cte.name
            #                 or cte.name == join.right_cte.name
            #             ) and pair.right.address == parent_concept.address:
            #                 used.add(parent_concept.address)
            # self.log('v.condition')
            # self.log(v.condition)
            # self.log(v.condition.row_arguments if v.condition else None)
            # if (
            #     v.condition
            #     and parent_concept.address in [x.address for x in v.condition.row_arguments]
            # ):
            #     used.add(parent_concept.address)
        used = renderer.used_map.get(cte.name, set())
        self.log(f"Used concepts for {cte.name}: {used}")
        self.log(renderer.used_map)
        add_to_hidden: list[BuildConcept] = []
        for concept in cte.output_columns:
            if concept.address not in used:
                add_to_hidden.append(concept)
        newly_hidden = [
            x.address for x in add_to_hidden if x.address not in cte.hidden_concepts
        ]
        if not newly_hidden:
            return False
        self.log(
            f"Hiding unused concepts {[x.address for x in add_to_hidden]} from {cte.name} (used: {used}, all: {[x.address for x in cte.output_columns]})"
        )
        cte.hidden_concepts = cte.hidden_concepts.union(
            set(
                [
                    x.address
                    for x in add_to_hidden
                    if x.address not in cte.hidden_concepts
                ]
            )
        )
        return True
