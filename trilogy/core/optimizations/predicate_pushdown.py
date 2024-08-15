from trilogy.core.models import (
    CTE,
    Conditional,
    BooleanOperator,
    Datasource,
)
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
    base = comparison == a
    if base:
        return True
    if isinstance(comparison, Conditional):
        return (
            is_child_of(a, comparison.left) or is_child_of(a, comparison.right)
        ) and comparison.operator == BooleanOperator.AND
    return base

COUNT = 0

class PredicatePushdown(OptimizationRule):

    def _check_parent(
        self,
        parent_cte: CTE,
        candidate: Conditional,
        inverse_map: dict[str, list[CTE]],
    ):
        conditions = {x.address for x in candidate.concept_arguments}
        if is_child_of(candidate, parent_cte.condition):
            return False

        materialized = {k for k, v in parent_cte.source_map.items() if v != []}
        if not conditions or not materialized:
            return False
        # if it's a root datasource, we can filter on _any_ of the output concepts
        if parent_cte.is_root_datasource:
            extra_check = {
                x.address for x in parent_cte.source.datasources[0].output_concepts
            }
            if conditions.issubset(extra_check):
                for x in conditions:
                    if x not in materialized:
                        materialized.add(x)
                        parent_cte.source_map[x] = [
                            parent_cte.source.datasources[0].name
                        ]
        if conditions.issubset(materialized):
            children = inverse_map.get(parent_cte.name, [])
            if all(
                [
                    is_child_of(candidate, child.condition)
                    for child in children
                ]
            ):
                self.log(
                    f"All concepts are found on {parent_cte.name} with existing {parent_cte.condition} and all it's {len(children)} children include same filter; pushing up {candidate}"
                )
                if parent_cte.condition:
                    parent_cte.condition = Conditional(
                        left=parent_cte.condition,
                        operator=BooleanOperator.AND,
                        right=candidate,
                    )
                else:
                    parent_cte.condition = candidate
                return True
        self.debug(
            f"conditions {conditions} not subset of parent {parent_cte.name} parent has {materialized} "
        )
        return False

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:

        if not cte.parent_ctes:
            self.debug(f"No parent CTEs for {cte.name}")

            return False

        optimized = False
        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False
        self.debug(
            f"Checking {cte.name} for predicate pushdown with {len(cte.parent_ctes)} parents"
        )
        if isinstance(cte.condition, Conditional):
            candidates = cte.condition.decompose()
        else:
            candidates = [cte.condition]
        self.debug(f"Have {len(candidates)} candidates to try to push down")
        for candidate in candidates:
            for parent_cte in cte.parent_ctes:
                optimized = self._check_parent(parent_cte=parent_cte, candidate=candidate, inverse_map=inverse_map)
                if optimized:
                    if all(
                        [
                            is_child_of(cte.condition, parent_cte.condition)
                            for parent_cte in cte.parent_ctes
                        ]
                    ) and not any([isinstance(x, Datasource) for x in cte.source.datasources]):
                        self.log("All parents have same filter, removing filter")
                        cte.condition = None
                    return True
        return optimized
