from collections import defaultdict
from typing import List, Optional, Dict, Tuple, Union, Set

from preql.constants import logger
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph

from preql.core.processing.concept_strategies_v2 import source_query_concepts

from preql.core.models import (
    Environment,
    Select,
    CTE,
    Join,
    JoinKey,
    ProcessedQuery,
    QueryDatasource,
    Datasource,
    Concept,
    JoinType,
    BaseJoin,
    merge_ctes,
)

from preql.utility import string_to_hash, unique
from preql.hooks.base_hook import BaseHook


LOGGER_PREFIX = "[QUERY BUILD]"


def base_join_to_join(base_join: BaseJoin, ctes: List[CTE]) -> Join:
    """This function converts joins at the datasource level
    to joins at the CTE level"""

    left_ctes = [
        cte
        for cte in ctes
        if (
            # cte.source.datasources[0].identifier == base_join.left_datasource.identifier
            # or
            cte.source.full_name
            == base_join.left_datasource.full_name
        )
    ]
    if not left_ctes:
        left_ctes = [
            cte
            for cte in ctes
            if (
                cte.source.datasources[0].full_name
                == base_join.left_datasource.full_name
            )
        ]
    left_cte = left_ctes[0]
    right_ctes = [
        cte
        for cte in ctes
        if (
            cte.source.full_name
            == base_join.right_datasource.full_name
            #     or cte.source.datasources[0].identifier
            # == base_join.right_datasource.identifier
        )
    ]
    if not right_ctes:
        right_ctes = [
            cte
            for cte in ctes
            if (
                cte.source.datasources[0].full_name
                == base_join.right_datasource.full_name
            )
        ]
    right_cte = right_ctes[0]
    return Join(
        left_cte=left_cte,
        right_cte=right_cte,
        joinkeys=[JoinKey(concept=concept) for concept in base_join.concepts],
        jointype=base_join.join_type,
    )


def datasource_to_ctes(query_datasource: QueryDatasource) -> List[CTE]:
    int_id = string_to_hash(query_datasource.identifier)
    output = []
    children = []
    if len(query_datasource.datasources) > 1 or any(
        [isinstance(x, QueryDatasource) for x in query_datasource.datasources]
    ):
        source_map = {}
        for datasource in query_datasource.datasources:
            if isinstance(datasource, QueryDatasource):
                sub_datasource = datasource
            else:
                sub_select: Dict[str, Set[Union[Datasource, QueryDatasource]]] = {
                    key: item
                    for key, item in query_datasource.source_map.items()
                    if datasource in item
                }
                sub_select = {
                    **sub_select,
                    **{c.address: {datasource} for c in datasource.concepts},
                }
                concepts = [
                    c for c in datasource.concepts  # if c.address in sub_select.keys()
                ]
                concepts = unique(concepts, "address")
                sub_datasource = QueryDatasource(
                    output_concepts=concepts,
                    input_concepts=concepts,
                    source_map=sub_select,
                    grain=datasource.grain,
                    datasources=[datasource],
                    joins=[],
                )
            sub_cte = datasource_to_ctes(sub_datasource)
            children += sub_cte
            for cte in sub_cte:
                for value in cte.output_columns:
                    source_map[value.address] = cte.name
    else:
        source = query_datasource.datasources[0]
        source_map = {
            concept.address: source.identifier
            for concept in query_datasource.output_concepts
        }
        source_map = {
            **source_map,
            **{
                concept.address: source.identifier
                for concept in query_datasource.input_concepts
            },
        }
    human_id = (
        query_datasource.identifier.replace("<", "").replace(">", "").replace(",", "_")
    )

    cte = CTE(
        name=f"cte_{human_id}_{int_id}",
        source=query_datasource,
        # output columns are what are selected/grouped by
        output_columns=[
            c.with_grain(query_datasource.grain)
            for c in query_datasource.output_concepts
        ],
        source_map=source_map,
        # related columns include all referenced columns, such as filtering
        # related_columns=datasource.concepts,
        joins=[base_join_to_join(join, children) for join in query_datasource.joins],
        related_columns=query_datasource.input_concepts,
        grain=query_datasource.grain,
        group_to_grain=query_datasource.group_required,
        # we restrict parent_ctes to one level
        # as this set is used as the base for rendering the query
        parent_ctes=children,
        condition=query_datasource.condition,
    )
    if cte.grain != query_datasource.grain:
        raise ValueError("Grain was corrupted in CTE generation")
    output.append(cte)
    return output


def get_query_datasources_v2(
    environment: Environment,
    statement: Select,
    graph: Optional[ReferenceGraph] = None,
    hooks: Optional[List[BaseHook]] = None,
) -> QueryDatasource:
    graph = graph or generate_graph(environment)

    ds = source_query_concepts(
        statement.output_components, [], environment=environment, g=graph
    )
    if hooks:
        for hook in hooks:
            hook.process_root_strategy_node(ds)
    final_qds = ds.resolve()
    return final_qds


get_query_datasources = get_query_datasources_v2


def flatten_ctes(input: CTE) -> list[CTE]:
    output = [input]
    for cte in input.parent_ctes:
        output += flatten_ctes(cte)
    return output


def process_query_v2(
    environment: Environment, statement: Select, hooks: List[BaseHook] | None = None
) -> ProcessedQuery:
    hooks = hooks or []
    graph = generate_graph(environment)
    root_datasource = get_query_datasources_v2(
        environment=environment, graph=graph, statement=statement, hooks=hooks
    )
    for hook in hooks:
        hook.process_root_datasource(root_datasource)
    # this should always return 1 - TODO, refactor
    root_cte = datasource_to_ctes(root_datasource)[0]
    for hook in hooks:
        hook.process_root_cte(root_cte)
    raw_ctes = list(reversed(flatten_ctes(root_cte)))
    seen = dict()
    # we can have duplicate CTEs at this point
    # so merge them together
    for cte in raw_ctes:
        if cte.name not in seen:
            seen[cte.name] = cte
        else:
            # merge them up
            seen[cte.name] = seen[cte.name] + cte

    final_ctes = list(seen.values())

    # for cte in others:
    #     # we do the with_grain here to fix an issue
    #     # where a query with a grain of properties has the components of the grain
    #     # with the default key grain rather than the grain of the select
    #     # TODO - evaluate if we can fix this in select definition
    #     joinkeys = [
    #         JoinKey(c)
    #         for c in statement.grain.components
    #         if c.with_grain(cte.grain) in cte.output_columns
    #         and c.with_grain(base.grain) in base.output_columns
    #         and cte.grain.issubset(statement.grain)
    #     ]
    #     if joinkeys:
    #         joins.append(
    #             Join(
    #                 left_cte=base,
    #                 right_cte=cte,
    #                 joinkeys=joinkeys,
    #                 jointype=JoinType.LEFT_OUTER,
    #             )
    #         )
    return ProcessedQuery(
        order_by=statement.order_by,
        grain=statement.grain,
        limit=statement.limit,
        where_clause=statement.where_clause,
        output_columns=statement.output_components,
        ctes=final_ctes,
        base=root_cte,
        # we no longer do any joins at final level, this should always happen in parent CTEs
        joins=[],
    )


from preql.hooks.base_hook import BaseHook


def process_query(
    environment: Environment, statement: Select, hooks: Optional[List[BaseHook]] = None
) -> ProcessedQuery:
    """Turn the raw query input into an instantiated execution tree."""
    return process_query_v2(environment, statement, hooks)
