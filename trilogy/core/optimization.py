from trilogy.constants import CONFIG, logger
from trilogy.core.enums import BooleanOperator, Derivation
from trilogy.core.models.build import (
    BuildConditional,
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations import (
    InlineConstant,
    InlineDatasource,
    OptimizationRule,
    PredicatePushdown,
    PredicatePushdownRemove,
)
from trilogy.core.processing.utility import sort_select_output
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement

MAX_OPTIMIZATION_LOOPS = 100


# other optimizations may make a CTE a pure passthrough
# remove those
# def is_locally_irrelevant(cte: CTE) -> CTE | bool:
#     if not len(cte.parent_ctes) == 1:
#         return False
#     parent = cte.parent_ctes[0]
#     if not parent.output_columns == cte.output_columns:
#         return False
#     if cte.condition is not None:
#         return False
#     if cte.group_to_grain:
#         return False
#     if len(cte.joins)>1:
#         return False
#     return parent


def reorder_ctes(
    input: list[CTE],
):
    import networkx as nx

    # Create a directed graph
    G = nx.DiGraph()
    mapping: dict[str, CTE] = {}
    for cte in input:
        mapping[cte.name] = cte
        for parent in cte.parent_ctes:
            G.add_edge(parent.name, cte.name)
    # Perform topological sort (only works for DAGs)
    try:
        topological_order = list(nx.topological_sort(G))
        if not topological_order:
            return input
        return [mapping[x] for x in topological_order]
    except nx.NetworkXUnfeasible as e:
        print(
            "The graph is not a DAG (contains cycles) and cannot be topologically sorted."
        )
        raise e


def filter_irrelevant_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
):
    relevant_ctes = set()

    def recurse(cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]):
        # TODO: revisit this
        # if parent := is_locally_irrelevant(cte):
        #     logger.info(
        #         f"[Optimization][Irrelevent CTE filtering] Removing redundant CTE {cte.name} and replacing with {parent.name}"
        #     )
        #     for child in inverse_map.get(cte.name, []):
        #         child.parent_ctes = [
        #             x for x in child.parent_ctes if x.name != cte.name
        #         ] + [parent]
        #         for x in child.source_map:
        #             if cte.name in child.source_map[x]:
        #                 child.source_map[x].remove(cte.name)
        #                 child.source_map[x].append(parent.name)
        #         for x2 in child.existence_source_map:
        #             if cte.name in child.existence_source_map[x2]:
        #                 child.existence_source_map[x2].remove(cte.name)
        #                 child.existence_source_map[x2].append(parent.name)
        # else:
        relevant_ctes.add(cte.name)
        for cte in cte.parent_ctes:
            recurse(cte, inverse_map)
        if isinstance(cte, UnionCTE):
            for cte in cte.internal_ctes:
                recurse(cte, inverse_map)

    inverse_map = gen_inverse_map(input)
    recurse(root_cte, inverse_map)
    final = [cte for cte in input if cte.name in relevant_ctes]
    if len(final) == len(input):
        return input
    return filter_irrelevant_ctes(final, root_cte)


def gen_inverse_map(input: list[CTE | UnionCTE]) -> dict[str, list[CTE | UnionCTE]]:
    inverse_map: dict[str, list[CTE | UnionCTE]] = {}
    for cte in input:
        if isinstance(cte, UnionCTE):
            for internal in cte.internal_ctes:
                if internal.name not in inverse_map:
                    inverse_map[internal.name] = []
                inverse_map[internal.name].append(cte)
        else:
            for parent in cte.parent_ctes:
                if parent.name not in inverse_map:
                    inverse_map[parent.name] = []
                inverse_map[parent.name].append(cte)

    return inverse_map


def is_direct_return_eligible(cte: CTE | UnionCTE) -> CTE | UnionCTE | None:
    # if isinstance(select, (PersistStatement, MultiSelectStatement)):
    #     return False
    if len(cte.parent_ctes) != 1:
        return None
    direct_parent = cte.parent_ctes[0]
    if isinstance(direct_parent, UnionCTE):
        return None

    output_addresses = set([x.address for x in cte.output_columns])
    parent_output_addresses = set([x.address for x in direct_parent.output_columns])
    if not output_addresses.issubset(parent_output_addresses):
        return None
    if not direct_parent.grain == cte.grain:
        return None

    assert isinstance(cte, CTE)
    derived_concepts = [
        c for c in cte.source.output_concepts if c not in cte.source.input_concepts
    ]

    parent_derived_concepts = [
        c
        for c in direct_parent.source.output_concepts
        if c not in direct_parent.source.input_concepts
    ]
    condition_arguments = cte.condition.row_arguments if cte.condition else []
    for x in derived_concepts:
        if x.derivation == Derivation.WINDOW:
            return None
        if x.derivation == Derivation.UNNEST:
            return None
        if x.derivation == Derivation.AGGREGATE:
            return None
    for x in parent_derived_concepts:
        if x.address not in condition_arguments:
            continue
        if x.derivation == Derivation.UNNEST:
            return None
        if x.derivation == Derivation.WINDOW:
            return None
    logger.info(
        f"[Optimization][EarlyReturn] Removing redundant output CTE with derived_concepts {[x.address for x in derived_concepts]}"
    )
    return direct_parent


def optimize_ctes(
    input: list[CTE | UnionCTE],
    root_cte: CTE | UnionCTE,
    select: SelectStatement | MultiSelectStatement,
) -> list[CTE | UnionCTE]:
    direct_parent: CTE | UnionCTE | None = root_cte
    while CONFIG.optimizations.direct_return and (
        direct_parent := is_direct_return_eligible(root_cte)
    ):
        direct_parent.order_by = root_cte.order_by
        direct_parent.limit = root_cte.limit
        direct_parent.hidden_concepts = root_cte.hidden_concepts.union(
            direct_parent.hidden_concepts
        )
        if root_cte.condition:
            if direct_parent.condition:
                direct_parent.condition = BuildConditional(
                    left=direct_parent.condition,
                    operator=BooleanOperator.AND,
                    right=root_cte.condition,
                )
            else:
                direct_parent.condition = root_cte.condition
        root_cte = direct_parent

        sort_select_output(root_cte, select)

    REGISTERED_RULES: list["OptimizationRule"] = []

    if CONFIG.optimizations.datasource_inlining:
        REGISTERED_RULES.append(InlineDatasource())
    if CONFIG.optimizations.predicate_pushdown:
        REGISTERED_RULES.append(PredicatePushdown())
    if CONFIG.optimizations.predicate_pushdown:
        REGISTERED_RULES.append(PredicatePushdownRemove())
    if CONFIG.optimizations.constant_inlining:
        REGISTERED_RULES.append(InlineConstant())
    for rule in REGISTERED_RULES:
        loops = 0
        complete = False
        while not complete and (loops <= MAX_OPTIMIZATION_LOOPS):
            actions_taken = False
            # assume we go through all CTEs once
            look_at = [root_cte, *reversed(input)]
            inverse_map = gen_inverse_map(look_at)
            for cte in look_at:
                opt = rule.optimize(cte, inverse_map)
                actions_taken = actions_taken or opt
            complete = not actions_taken
            loops += 1
            input = reorder_ctes(filter_irrelevant_ctes(input, root_cte))
        logger.info(f"finished checking for {type(rule).__name__} in {loops} loops")

    return reorder_ctes(filter_irrelevant_ctes(input, root_cte))
