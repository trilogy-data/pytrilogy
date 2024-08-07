from trilogy.core.models import (
    CTE,
    SelectStatement,
    PersistStatement,
    MultiSelectStatement,
    Conditional,
    BooleanOperator,
)
from trilogy.core.enums import PurposeLineage
from trilogy.constants import logger, CONFIG
from trilogy.core.optimizations import (
    OptimizationRule,
    InlineConstant,
    PredicatePushdown,
    InlineDatasource,
)


MAX_OPTIMIZATION_LOOPS = 100


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
        c
        for c in cte.source.output_concepts + cte.source.hidden_concepts
        if c not in cte.source.input_concepts
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
) -> list[CTE]:
    complete = False
    REGISTERED_RULES: list["OptimizationRule"] = []
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
    if CONFIG.optimizations.datasource_inlining:
        REGISTERED_RULES.append(InlineDatasource())
    if CONFIG.optimizations.predicate_pushdown:
        REGISTERED_RULES.append(PredicatePushdown())
    if CONFIG.optimizations.constant_inlining:
        REGISTERED_RULES.append(InlineConstant())
    loops = 0
    while not complete and (loops <= MAX_OPTIMIZATION_LOOPS):
        actions_taken = False
        for rule in REGISTERED_RULES:
            for cte in input:
                inverse_map = gen_inverse_map(input)
                actions_taken = actions_taken or rule.optimize(cte, inverse_map)
        complete = not actions_taken
        loops += 1

    return filter_irrelevant_ctes(input, root_cte)
