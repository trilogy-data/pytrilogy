from trilogy.core.models import (
    CTE,
    SelectStatement,
    PersistStatement,
    Datasource,
    MultiSelectStatement,
    Conditional,
    BooleanOperator,
)
from trilogy.core.enums import PurposeLineage
from trilogy.constants import logger, CONFIG
from abc import ABC


class OptimizationRule(ABC):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        raise NotImplementedError

    def log(self, message: str):
        logger.info(f"[Optimization][{self.__class__.__name__}] {message}")

    def debug(self, message: str):
        logger.debug(f"[Optimization][{self.__class__.__name__}] {message}")


class InlineDatasource(OptimizationRule):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        if not cte.parent_ctes:
            return False

        optimized = False
        self.log(
            f"Checking {cte.name} for consolidating inline tables with {len(cte.parent_ctes)} parents"
        )
        to_inline: list[CTE] = []
        force_group = False
        for parent_cte in cte.parent_ctes:
            if not parent_cte.is_root_datasource:
                self.log(f"parent {parent_cte.name} is not root")
                continue
            if parent_cte.parent_ctes:
                self.log(f"parent {parent_cte.name} has parents")
                continue
            raw_root = parent_cte.source.datasources[0]
            if not isinstance(raw_root, Datasource):
                self.log(f"parent {parent_cte.name} is not datasource")
                continue
            root: Datasource = raw_root
            if not root.can_be_inlined:
                self.log(f"parent {parent_cte.name} datasource is not inlineable")
                continue
            root_outputs = {x.address for x in root.output_concepts}
            cte_outputs = {x.address for x in parent_cte.output_columns}
            grain_components = {x.address for x in root.grain.components}
            if not cte_outputs.issubset(root_outputs):
                self.log(f"Not all {parent_cte.name} outputs are found on datasource")
                continue
            if not grain_components.issubset(cte_outputs):
                self.log("Not all datasource components in cte outputs, forcing group")
                force_group = True
            to_inline.append(parent_cte)

        for replaceable in to_inline:

            result = cte.inline_parent_datasource(replaceable, force_group=force_group)
            if result:
                self.log(f"Inlined parent {replaceable.name}")
            else:
                self.log(f"Failed to inline {replaceable.name}")
        return optimized


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


def filter_irrelevant_ctes(
    input: list[CTE],
    root_cte: CTE,
):
    relevant_ctes = set()

    def recurse(cte: CTE):
        relevant_ctes.add(cte.name)
        for cte in cte.parent_ctes:
            recurse(cte)

    recurse(root_cte)
    return [cte for cte in input if cte.name in relevant_ctes]


def gen_inverse_map(input: list[CTE]) -> dict[str, list[CTE]]:
    inverse_map: dict[str, list[CTE]] = {}
    for cte in input:
        for parent in cte.parent_ctes:
            if parent.name not in inverse_map:
                inverse_map[parent.name] = []
            inverse_map[parent.name].append(cte)
    return inverse_map


def is_direct_return_eligible(
    cte: CTE, select: SelectStatement | PersistStatement | MultiSelectStatement
) -> bool:
    if isinstance(select, (PersistStatement, MultiSelectStatement)):
        return False
    derived_concepts = [
        c for c in cte.source.output_concepts if c not in cte.source.input_concepts
    ]
    eligible = True
    conditions = (
        set(x.address for x in select.where_clause.concept_arguments)
        if select.where_clause
        else set()
    )
    if conditions and select.limit:
        return False
    for x in derived_concepts:
        if x.derivation == PurposeLineage.WINDOW:
            return False
        if x.derivation == PurposeLineage.UNNEST:
            return False
        if x.derivation == PurposeLineage.AGGREGATE:
            if x.address in conditions:
                return False
    logger.info(
        f"Upleveling output select to final CTE with derived_concepts {[x.address for x in derived_concepts]}"
    )
    return eligible


def sort_select_output(cte: CTE, query: SelectStatement | MultiSelectStatement):
    hidden_addresses = [c.address for c in query.hidden_components]
    output_addresses = [
        c.address for c in query.output_components if c.address not in hidden_addresses
    ]

    mapping = {x.address: x for x in cte.output_columns}

    new_output = []
    for x in output_addresses:
        new_output.append(mapping[x])
    cte.output_columns = new_output


def optimize_ctes(
    input: list[CTE], root_cte: CTE, select: SelectStatement | MultiSelectStatement
):
    complete = False
    REGISTERED_RULES: list["OptimizationRule"] = []

    if CONFIG.optimizations.datasource_inlining:
        REGISTERED_RULES.append(InlineDatasource())
    if CONFIG.optimizations.predicate_pushdown:
        REGISTERED_RULES.append(PredicatePushdown())

    while not complete:
        actions_taken = False
        for rule in REGISTERED_RULES:
            for cte in input:
                inverse_map = gen_inverse_map(input)
                actions_taken = rule.optimize(cte, inverse_map)
        complete = not actions_taken

    if CONFIG.optimizations.direct_return and is_direct_return_eligible(
        root_cte, select
    ):
        root_cte.order_by = select.order_by
        root_cte.limit = select.limit
        if select.where_clause:

            if root_cte.condition:
                root_cte.condition = Conditional(
                    left=root_cte.condition,
                    operator=BooleanOperator.AND,
                    right=select.where_clause.conditional,
                )
            else:
                root_cte.condition = select.where_clause.conditional
        root_cte.requires_nesting = False
        sort_select_output(root_cte, select)

    return filter_irrelevant_ctes(input, root_cte)
