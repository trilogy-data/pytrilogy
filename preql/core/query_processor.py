from typing import List, Optional, Set, Union, Dict

from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph
from preql.core.constants import CONSTANT_DATASET
from preql.core.processing.concept_strategies_v2 import source_query_concepts
from preql.constants import CONFIG, DEFAULT_NAMESPACE
from preql.core.models import (
    Environment,
    Persist,
    Select,
    CTE,
    Join,
    UnnestJoin,
    JoinKey,
    MaterializedDataset,
    ProcessedQuery,
    ProcessedQueryPersist,
    QueryDatasource,
    Datasource,
    BaseJoin,
    InstantiatedUnnestJoin,
)

from preql.utility import string_to_hash, unique
from preql.hooks.base_hook import BaseHook
from preql.constants import logger
from random import shuffle
from preql.core.ergonomics import CTE_NAMES
from math import ceil

LOGGER_PREFIX = "[QUERY BUILD]"


def base_join_to_join(
    base_join: BaseJoin | UnnestJoin, ctes: List[CTE]
) -> Join | InstantiatedUnnestJoin:
    """This function converts joins at the datasource level
    to joins at the CTE level"""
    if isinstance(base_join, UnnestJoin):
        return InstantiatedUnnestJoin(concept=base_join.concept, alias=base_join.alias)
    left_ctes = [
        cte
        for cte in ctes
        if (cte.source.full_name == base_join.left_datasource.full_name)
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
        if (cte.source.full_name == base_join.right_datasource.full_name)
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
    int_id = string_to_hash(query_datasource.full_name)
    output = []
    parents = []
    if len(query_datasource.datasources) > 1 or any(
        [isinstance(x, QueryDatasource) for x in query_datasource.datasources]
    ):
        SLABEL = "MULTIPLE"
        source_map = {}
        all_new_ctes: List[CTE] = []
        for datasource in query_datasource.datasources:
            if isinstance(datasource, QueryDatasource):
                sub_datasource = datasource
            else:
                # this is when it's not a query datasource
                # sub_select: Dict[str, Set[Union[Datasource, QueryDatasource]]] = {
                #     key: item
                #     for key, item in query_datasource.source_map.items()
                # }
                sub_select: Dict[
                    str, Set[Union[Datasource, QueryDatasource, UnnestJoin]]
                ] = {
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
                    partial_concepts=[
                        x.concept for x in datasource.columns if not x.is_complete
                    ],
                )
            sub_cte = datasource_to_ctes(sub_datasource)
            parents += sub_cte
            all_new_ctes += sub_cte

        # now populate anything derived in this level
        for qdk, qdv in query_datasource.source_map.items():
            if (
                qdk not in source_map
                and len(qdv) == 1
                and isinstance(list(qdv)[0], UnnestJoin)
            ):
                source_map[qdk] = ""

            else:
                for cte in all_new_ctes:
                    output_address = [
                        x.address
                        for x in cte.output_columns
                        if x.address not in [z.address for z in cte.partial_concepts]
                    ]
                    if qdk in output_address:
                        if qdk not in source_map:
                            source_map[qdk] = cte.name
                            break
                # now do a pass that accepts partials
                # TODO: move this into a second loop by first creationg all sub sourcdes
                # then loop through this
                for cte in all_new_ctes:
                    output_address = [x.address for x in cte.output_columns]
                    if qdk in output_address:
                        if qdk not in source_map:
                            source_map[qdk] = cte.name
            if qdk not in source_map and not qdv:
                # set source to empty, as it must be derived in this element
                source_map[qdk] = ""
            if qdk not in source_map:
                raise ValueError(
                    f"Missing {qdk} in {source_map}, {SLABEL} source map {query_datasource.source_map.keys()} "
                )

    else:
        SLABEL = "SINGULAR"
        # source is the first datasource of the query datasource
        source = query_datasource.datasources[0]
        # this is required to ensure that constant datasets
        # render properly on initial access; since they have
        # no actual source
        if source.full_name == DEFAULT_NAMESPACE + "_" + CONSTANT_DATASET:
            source_map = {k: "" for k in query_datasource.source_map}
        else:
            source_map = {k: source.full_name for k in query_datasource.source_map}
    human_id = (
        query_datasource.full_name.replace("<", "").replace(">", "").replace(",", "_")
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
        joins=[base_join_to_join(join, parents) for join in query_datasource.joins],
        grain=query_datasource.grain,
        group_to_grain=query_datasource.group_required,
        # we restrict parent_ctes to one level
        # as this set is used as the base for rendering the query
        parent_ctes=parents,
        condition=query_datasource.condition,
        partial_concepts=query_datasource.partial_concepts,
        join_derived_concepts=query_datasource.join_derived_concepts,
    )
    if cte.grain != query_datasource.grain:
        raise ValueError("Grain was corrupted in CTE generation")
    for x in cte.output_columns:
        if x.address not in cte.source_map:
            raise ValueError(
                f"Missing {x.address} in {cte.source_map}, {SLABEL} source map {cte.source.source_map.keys()} "
            )
    output.append(cte)
    return output


def get_query_datasources(
    environment: Environment,
    statement: Select,
    graph: Optional[ReferenceGraph] = None,
    hooks: Optional[List[BaseHook]] = None,
) -> QueryDatasource:
    graph = graph or generate_graph(environment)
    logger.info(
        f"{LOGGER_PREFIX} getting source datasource for query with output {[str(c) for c in statement.output_components]}"
    )
    if not statement.output_components:
        raise ValueError(f"Statement has no output components {statement}")
    ds = source_query_concepts(
        statement.output_components, environment=environment, g=graph
    )
    if hooks:
        for hook in hooks:
            hook.process_root_strategy_node(ds)
    final_qds = ds.resolve()
    return final_qds


def flatten_ctes(input: CTE) -> list[CTE]:
    output = [input]
    for cte in input.parent_ctes:
        output += flatten_ctes(cte)
    return output


def process_auto(
    environment: Environment,
    statement: Persist | Select,
    hooks: List[BaseHook] | None = None,
):
    if isinstance(statement, Persist):
        return process_persist(environment, statement, hooks)
    elif isinstance(statement, Select):
        return process_query(environment, statement, hooks)
    raise ValueError(f"Do not know how to process {type(statement)}")


def process_persist(
    environment: Environment, statement: Persist, hooks: List[BaseHook] | None = None
) -> ProcessedQueryPersist:
    select = process_query(
        environment=environment, statement=statement.select, hooks=hooks
    )

    # build our object to return
    arg_dict = {k: v for k, v in select.__dict__.items()}
    return ProcessedQueryPersist(
        **arg_dict,
        output_to=MaterializedDataset(address=statement.address),
        datasource=statement.datasource,
    )


def process_query(
    environment: Environment, statement: Select, hooks: List[BaseHook] | None = None
) -> ProcessedQuery:
    hooks = hooks or []
    graph = generate_graph(environment)
    root_datasource = get_query_datasources(
        environment=environment, graph=graph, statement=statement, hooks=hooks
    )
    for hook in hooks:
        hook.process_root_datasource(root_datasource)
    # this should always return 1 - TODO, refactor
    root_cte = datasource_to_ctes(root_datasource)[0]
    for hook in hooks:
        hook.process_root_cte(root_cte)
    raw_ctes: List[CTE] = list(reversed(flatten_ctes(root_cte)))
    seen = dict()
    # we can have duplicate CTEs at this point
    # so merge them together
    for cte in raw_ctes:
        if cte.name not in seen:
            seen[cte.name] = cte
        else:
            # merge them up
            seen[cte.name] = seen[cte.name] + cte
    for cte in raw_ctes:
        cte.parent_ctes = [seen[x.name] for x in cte.parent_ctes]
    final_ctes: List[CTE] = list(seen.values())

    mapping = {}

    # this makes debugging a lot easier
    if CONFIG.hash_identifiers:
        shuffle(CTE_NAMES)
        for idx, cte in enumerate(final_ctes):
            suffix = ""
            if idx > len(CTE_NAMES):
                int = ceil(len(CTE_NAMES) / idx)
                suffix = f"_{int}"
            # find the remainder from the len
            lookup = idx % len(CTE_NAMES)
            new_name = f"{CTE_NAMES[lookup]}{suffix}"
            mapping[cte.name] = new_name
            cte.name = new_name
        for cte in final_ctes:
            for k, v in cte.source_map.items():
                cte.source_map[k] = mapping.get(v, v)
            for join in cte.joins:
                if isinstance(join, Join):
                    join.left_cte.name = mapping.get(
                        join.left_cte.name, join.left_cte.name
                    )
                    join.right_cte.name = mapping.get(
                        join.right_cte.name, join.right_cte.name
                    )
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
        hidden_columns=[x for x in statement.hidden_components],
    )
