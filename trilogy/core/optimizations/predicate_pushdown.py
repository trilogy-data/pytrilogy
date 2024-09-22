from trilogy.core.models import (
    CTE,
    Conditional,
    BooleanOperator,
    Datasource,
    ConceptArgs,
    Comparison,
    Parenthetical,
)
from trilogy.core.optimizations.base_optimization import OptimizationRule
from trilogy.core.processing.utility import is_scalar_condition
from trilogy.utility import unique


def is_child_of(a, comparison):
    base = comparison == a
    if base:
        return True
    if isinstance(comparison, Conditional):
        return (
            is_child_of(a, comparison.left) or is_child_of(a, comparison.right)
        ) and comparison.operator == BooleanOperator.AND
    return base


class PredicatePushdown(OptimizationRule):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def _check_parent(
        self,
        cte: CTE,
        parent_cte: CTE,
        candidate: Conditional | Comparison | Parenthetical | None,
        inverse_map: dict[str, list[CTE]],
    ):
        if not isinstance(candidate, ConceptArgs):
            return False
        row_conditions = {x.address for x in candidate.row_arguments}
        existence_conditions = {
            y.address for x in candidate.existence_arguments for y in x
        }
        all_inputs = {x.address for x in candidate.concept_arguments}
        if is_child_of(candidate, parent_cte.condition):
            return False

        materialized = {k for k, v in parent_cte.source_map.items() if v != []}
        if not row_conditions or not materialized:
            return False
        output_addresses = {x.address for x in parent_cte.output_columns}
        # if any of the existence conditions are created on the asset, we can't push up to it
        if existence_conditions and existence_conditions.intersection(output_addresses):
            return False
        # if it's a root datasource, we can filter on _any_ of the output concepts
        if parent_cte.is_root_datasource:
            extra_check = {
                x.address for x in parent_cte.source.datasources[0].output_concepts
            }
            if row_conditions.issubset(extra_check):
                for x in row_conditions:
                    if x not in materialized:
                        materialized.add(x)
                        parent_cte.source_map[x] = [
                            parent_cte.source.datasources[0].name
                        ]
        if row_conditions.issubset(materialized):
            children = inverse_map.get(parent_cte.name, [])
            if all([is_child_of(candidate, child.condition) for child in children]):
                self.log(
                    f"All concepts are found on {parent_cte.name} with existing {parent_cte.condition} and all it's {len(children)} children include same filter; pushing up {candidate}"
                )
                if parent_cte.condition and not is_scalar_condition(
                    parent_cte.condition
                ):
                    self.log("Parent condition is not scalar, not safe to push up")
                    return False
                if parent_cte.condition:
                    parent_cte.condition = Conditional(
                        left=parent_cte.condition,
                        operator=BooleanOperator.AND,
                        right=candidate,
                    )
                else:
                    parent_cte.condition = candidate
                # promote up existence sources
                if all_inputs.difference(row_conditions):
                    for x in all_inputs.difference(row_conditions):
                        if x not in parent_cte.source_map and x in cte.source_map:
                            sources = [
                                parent
                                for parent in cte.parent_ctes
                                if parent.name in cte.source_map[x]
                            ]
                            parent_cte.source_map[x] = cte.source_map[x]
                            parent_cte.parent_ctes = unique(
                                parent_cte.parent_ctes + sources, "name"
                            )
                return True
        self.debug(
            f"conditions {row_conditions} not subset of parent {parent_cte.name} parent has {materialized} "
        )
        return False

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        optimized = False

        if not cte.parent_ctes:
            self.debug(f"No parent CTEs for {cte.name}")

            return False

        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False
        if self.complete.get(cte.name):
            self.debug("Have done this CTE before")
            return False

        self.debug(
            f"Checking {cte.name} for predicate pushdown with {len(cte.parent_ctes)} parents"
        )
        if isinstance(cte.condition, Conditional):
            candidates = cte.condition.decompose()
        else:
            candidates = [cte.condition]
        self.debug(
            f"Have {len(candidates)} candidates to try to push down from parent {type(cte.condition)}"
        )
        optimized = False
        for candidate in candidates:
            if not is_scalar_condition(candidate):
                self.debug(
                    f"Skipping {candidate} as not a basic [no aggregate, etc] condition"
                )
                continue
            self.debug(
                f"Checking candidate {candidate}, {type(candidate)}, scalar: {is_scalar_condition(candidate)}"
            )
            for parent_cte in cte.parent_ctes:
                local_pushdown = self._check_parent(
                    cte=cte,
                    parent_cte=parent_cte,
                    candidate=candidate,
                    inverse_map=inverse_map,
                )
                optimized = optimized or local_pushdown
                if local_pushdown:
                    # taint a CTE again when something is pushed up to it.
                    self.complete[parent_cte.name] = False
                self.debug(
                    f"Pushed down {candidate} from {cte.name} to {parent_cte.name}"
                )

        self.complete[cte.name] = True
        return optimized


class PredicatePushdownRemove(OptimizationRule):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        optimized = False

        if not cte.parent_ctes:
            self.debug(f"No parent CTEs for {cte.name}")

            return False

        if not cte.condition:
            self.debug(f"No CTE condition for {cte.name}")
            return False

        parent_filter_status = {
            parent.name: is_child_of(cte.condition, parent.condition)
            for parent in cte.parent_ctes
        }
        # flatten existnce argument tuples to a list

        flattened_existence = [
            x.address for y in cte.condition.existence_arguments for x in y
        ]

        existence_only = [
            parent.name
            for parent in cte.parent_ctes
            if all([x.address in flattened_existence for x in parent.output_columns])
            and len(flattened_existence) > 0
        ]
        if all(
            [
                value
                for key, value in parent_filter_status.items()
                if key not in existence_only
            ]
        ) and not any([isinstance(x, Datasource) for x in cte.source.datasources]):
            self.log(
                f"All parents of {cte.name} have same filter or are existence only inputs, removing filter from {cte.name}"
            )
            cte.condition = None
            # remove any "parent" CTEs that provided only existence inputs
            if existence_only:
                original = [y.name for y in cte.parent_ctes]
                cte.parent_ctes = [
                    x for x in cte.parent_ctes if x.name not in existence_only
                ]
                self.log(
                    f"new parents for {cte.name} are {[x.name for x in cte.parent_ctes]}, vs {original}"
                )
            return True

        self.complete[cte.name] = True
        return optimized
