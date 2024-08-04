from trilogy.core.models import (
    CTE,
    Conditional,
    BooleanOperator,
)
from trilogy.constants import logger
from trilogy.core.optimizations.base_optimization import OptimizationRule


def decompose_condition(conditional: Conditional):
    chunks = []
    if conditional.operator == BooleanOperator.AND:
        for val in [conditional.left, conditional.right]:
            if isinstance(val, Conditional):
                chunks.extend(decompose_condition(val))
            else:
                chunks.append(val)
    else:
        chunks.append(conditional)
    return chunks


def is_child_of(a, comparison):
    if isinstance(comparison, Conditional):
        return (
            is_child_of(a, comparison.left) or is_child_of(a, comparison.right)
        ) and comparison.operator == BooleanOperator.AND
    return comparison == a


class PredicatePushdown(OptimizationRule):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:

        if not cte.parent_ctes:
            self.debug(f"No parent CTEs for {cte.name}")

            return False

        optimized = False
        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False
        self.log(
            f"Checking {cte.name} for predicate pushdown with {len(cte.parent_ctes)} parents"
        )
        if isinstance(cte.condition, Conditional):
            candidates = cte.condition.decompose()
        else:
            candidates = [cte.condition]
        logger.info(f"Have {len(candidates)} candidates to try to push down")
        for candidate in candidates:
            conditions = {x.address for x in candidate.concept_arguments}
            for parent_cte in cte.parent_ctes:
                materialized = {k for k, v in parent_cte.source_map.items() if v != []}
                if conditions.issubset(materialized):
                    if all(
                        [
                            is_child_of(candidate, child.condition)
                            for child in inverse_map[parent_cte.name]
                        ]
                    ):
                        self.log(
                            f"All concepts are found on {parent_cte.name} and all it's children include same filter; pushing up filter"
                        )
                        if parent_cte.condition:
                            parent_cte.condition = Conditional(
                                left=parent_cte.condition,
                                operator=BooleanOperator.AND,
                                right=candidate,
                            )
                        else:
                            parent_cte.condition = candidate
                        optimized = True
                else:
                    logger.info("conditions not subset of parent materialized")

        if all(
            [
                is_child_of(cte.condition, parent_cte.condition)
                for parent_cte in cte.parent_ctes
            ]
        ):
            self.log("All parents have same filter, removing filter")
            cte.condition = None
            optimized = True

        return optimized
