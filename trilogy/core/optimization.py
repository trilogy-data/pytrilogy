from trilogy.core.models import (
    CTE,
    SelectStatement,
    MultiSelectStatement,
    Conditional,
)
from trilogy.core.enums import PurposeLineage, BooleanOperator
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


def is_direct_return_eligible(cte: CTE) -> CTE | None:
    # if isinstance(select, (PersistStatement, MultiSelectStatement)):
    #     return False
    if len(cte.parent_ctes) != 1:
        return None
    direct_parent = cte.parent_ctes[0]

    output_addresses = set([x.address for x in cte.output_columns])
    parent_output_addresses = set([x.address for x in direct_parent.output_columns])
    if not output_addresses.issubset(parent_output_addresses):
        return None
    if not direct_parent.grain == cte.grain:
        return None
    derived_concepts = [
        c
        for c in cte.source.output_concepts + cte.source.hidden_concepts
        if c not in cte.source.input_concepts
    ]
    conditions = (
        set(x.address for x in direct_parent.condition.concept_arguments)
        if direct_parent.condition
        else set()
    )
    for x in derived_concepts:
        if x.derivation == PurposeLineage.WINDOW:
            return None
        if x.derivation == PurposeLineage.UNNEST:
            return None
        if x.derivation == PurposeLineage.AGGREGATE:
            if x.address in conditions:
                return None
    # handling top level nodes that require unpacking
    for x in cte.output_columns:
        if x.derivation == PurposeLineage.UNNEST:
            return None
    logger.info(
        f"[Optimization][EarlyReturn] Removing redundant output CTE with derived_concepts {[x.address for x in derived_concepts]}"
    )
    return direct_parent


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

    direct_parent: CTE | None = root_cte
    while CONFIG.optimizations.direct_return and (
        direct_parent := is_direct_return_eligible(root_cte)
    ):
        direct_parent.order_by = root_cte.order_by
        direct_parent.limit = root_cte.limit
        direct_parent.hidden_concepts = (
            root_cte.hidden_concepts + direct_parent.hidden_concepts
        )
        if root_cte.condition:
            if direct_parent.condition:
                direct_parent.condition = Conditional(
                    left=direct_parent.condition,
                    operator=BooleanOperator.AND,
                    right=root_cte.condition,
                )
            else:
                direct_parent.condition = root_cte.condition
        root_cte = direct_parent

    sort_select_output(root_cte, select)

    REGISTERED_RULES: list["OptimizationRule"] = []
    if CONFIG.optimizations.constant_inlining:
        REGISTERED_RULES.append(InlineConstant())
    if CONFIG.optimizations.datasource_inlining:
        REGISTERED_RULES.append(InlineDatasource())
    if CONFIG.optimizations.predicate_pushdown:
        REGISTERED_RULES.append(PredicatePushdown())

    for rule in REGISTERED_RULES:
        loops = 0
        complete = False
        while not complete and (loops <= MAX_OPTIMIZATION_LOOPS):
            actions_taken = False
            # assume we go through all CTEs once
            look_at = [root_cte, *input]
            inverse_map = gen_inverse_map(look_at)
            for cte in look_at:
                opt = rule.optimize(cte, inverse_map)
                actions_taken = actions_taken or opt
            complete = not actions_taken
            loops += 1
        logger.info(f"finished checking for {type(rule).__name__} in {loops} loops")

    return filter_irrelevant_ctes(input, root_cte)
