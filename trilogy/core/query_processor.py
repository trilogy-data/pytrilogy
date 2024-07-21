from typing import List, Optional, Set, Union, Dict, Tuple

from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.processing.concept_strategies_v3 import source_query_concepts
from trilogy.constants import CONFIG, DEFAULT_NAMESPACE
from trilogy.core.models import (
    Environment,
    PersistStatement,
    SelectStatement,
    MultiSelectStatement,
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

from trilogy.utility import unique
from collections import defaultdict
from trilogy.hooks.base_hook import BaseHook
from trilogy.constants import logger
from random import shuffle
from trilogy.core.ergonomics import CTE_NAMES
from trilogy.core.optimization import optimize_ctes
from math import ceil

LOGGER_PREFIX = "[QUERY BUILD]"


def base_join_to_join(
    base_join: BaseJoin | UnnestJoin, ctes: List[CTE]
) -> Join | InstantiatedUnnestJoin:
    """This function converts joins at the datasource level
    to joins at the CTE level"""
    if isinstance(base_join, UnnestJoin):
        return InstantiatedUnnestJoin(concept=base_join.concept, alias=base_join.alias)
    if base_join.left_datasource.identifier == base_join.right_datasource.identifier:
        raise ValueError(f"Joining on same datasource {base_join}")
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


def generate_source_map(
    query_datasource: QueryDatasource, all_new_ctes: List[CTE]
) -> Tuple[Dict[str, list[str]], Dict[str, list[str]]]:
    source_map: Dict[str, list[str]] = defaultdict(list)
    # now populate anything derived in this level
    for qdk, qdv in query_datasource.source_map.items():
        unnest = [x for x in qdv if isinstance(x, UnnestJoin)]
        for x in unnest:
            source_map[qdk] = []
        if (
            qdk not in source_map
            and len(qdv) == 1
            and isinstance(list(qdv)[0], UnnestJoin)
        ):
            source_map[qdk] = []
        basic = [x for x in qdv if isinstance(x, Datasource)]
        for base in basic:
            source_map[qdk].append(base.name)

        ctes = [x for x in qdv if isinstance(x, QueryDatasource)]
        if ctes:
            names = set([x.name for x in ctes])
            matches = [cte for cte in all_new_ctes if cte.source.name in names]

            if not matches and names:
                raise SyntaxError(query_datasource.source_map)
            for cte in matches:
                output_address = [
                    x.address
                    for x in cte.output_columns
                    if x.address not in [z.address for z in cte.partial_concepts]
                ]
                if qdk in output_address:
                    source_map[qdk].append(cte.name)
            # now do a pass that accepts partials
            # TODO: move this into a second loop by first creationg all sub sources
            # then loop through this
            for cte in matches:
                if qdk not in source_map:
                    source_map[qdk] = [cte.name]
        if qdk not in source_map:
            if not qdv:
                source_map[qdk] = []
            elif CONFIG.validate_missing:
                raise ValueError(
                    f"Missing {qdk} in {source_map}, source map {query_datasource.source_map} "
                )

    # existence lookups use a separate map
    # as they cannot be referenced in row resolution
    existence_source_map: Dict[str, list[str]] = defaultdict(list)
    for ek, ev in query_datasource.existence_source_map.items():
        names = set([x.name for x in ev])
        ematches = [cte.name for cte in all_new_ctes if cte.source.name in names]
        existence_source_map[ek] = ematches
    return {k: [] if not v else v for k, v in source_map.items()}, existence_source_map


def datasource_to_query_datasource(datasource: Datasource) -> QueryDatasource:
    sub_select: Dict[str, Set[Union[Datasource, QueryDatasource, UnnestJoin]]] = {
        **{c.address: {datasource} for c in datasource.concepts},
    }
    concepts = [c for c in datasource.concepts]
    concepts = unique(concepts, "address")
    return QueryDatasource(
        output_concepts=concepts,
        input_concepts=concepts,
        source_map=sub_select,
        grain=datasource.grain,
        datasources=[datasource],
        joins=[],
        partial_concepts=[x.concept for x in datasource.columns if not x.is_complete],
    )


def generate_cte_name(full_name: str, name_map: dict[str, str]) -> str:
    if CONFIG.human_identifiers:
        if full_name in name_map:
            return name_map[full_name]
        suffix = ""
        idx = len(name_map)
        if idx >= len(CTE_NAMES):
            int = ceil(idx / len(CTE_NAMES))
            suffix = f"_{int}"
        valid = [x for x in CTE_NAMES if x + suffix not in name_map.values()]
        shuffle(valid)
        lookup = valid[0]
        new_name = f"{lookup}{suffix}"
        name_map[full_name] = new_name
        return new_name
    else:
        return full_name.replace("<", "").replace(">", "").replace(",", "_")


def resolve_cte_base_name_and_alias(
    name: str,
    source: QueryDatasource,
    parents: List[CTE],
    joins: List[Join | InstantiatedUnnestJoin],
) -> Tuple[str | None, str | None]:

    valid_joins: List[Join] = [join for join in joins if isinstance(join, Join)]
    relevant_parent_sources = set()
    for k, v in source.source_map.items():
        if v:
            relevant_parent_sources.update(v)
    eligible = [x for x in source.datasources if x in relevant_parent_sources]
    if (
        len(eligible) == 1
        and isinstance(eligible[0], Datasource)
        and not eligible[0].name == CONSTANT_DATASET
    ):
        ds = eligible[0]
        return ds.safe_location, ds.identifier

    # if we have multiple joined CTEs, pick the base
    # as the root
    elif len(eligible) == 1 and len(parents) == 1:
        return parents[0].name, parents[0].name
    elif valid_joins and len(valid_joins) > 0:
        candidates = [x.left_cte.name for x in valid_joins]
        disallowed = [x.right_cte.name for x in valid_joins]
        try:
            cte = [y for y in candidates if y not in disallowed][0]
            return cte, cte
        except IndexError:
            raise SyntaxError(
                f"Invalid join configuration {candidates} {disallowed} with all parents {[x.base_name for x in parents]}"
            )
    elif eligible:
        matched = [x for x in parents if x.source.name == eligible[0].name]
        if matched:
            return matched[0].name, matched[0].name

    logger.info(
        f"Could not determine CTE base name for {name} with relevant sources {relevant_parent_sources}"
    )
    return None, None


def datasource_to_ctes(
    query_datasource: QueryDatasource, name_map: dict[str, str]
) -> List[CTE]:
    output: List[CTE] = []
    parents: list[CTE] = []
    if len(query_datasource.datasources) > 1 or any(
        [isinstance(x, QueryDatasource) for x in query_datasource.datasources]
    ):
        all_new_ctes: List[CTE] = []
        for datasource in query_datasource.datasources:
            if isinstance(datasource, QueryDatasource):
                sub_datasource = datasource
            else:
                sub_datasource = datasource_to_query_datasource(datasource)

            sub_cte = datasource_to_ctes(sub_datasource, name_map)
            parents += sub_cte
            all_new_ctes += sub_cte
        source_map, existence_map = generate_source_map(query_datasource, all_new_ctes)

    else:
        # source is the first datasource of the query datasource
        source = query_datasource.datasources[0]
        # this is required to ensure that constant datasets
        # render properly on initial access; since they have
        # no actual source
        if source.full_name == DEFAULT_NAMESPACE + "_" + CONSTANT_DATASET:
            source_map = {k: [] for k in query_datasource.source_map}
            existence_map = source_map
        else:
            source_map = {
                k: [] if not v else [source.identifier]
                for k, v in query_datasource.source_map.items()
            }
            existence_map = source_map

    human_id = generate_cte_name(query_datasource.full_name, name_map)
    logger.info(
        f"Finished building source map for {human_id} with {len(parents)} parents, have {source_map}, parent had non-empty keys {[k for k, v in query_datasource.source_map.items() if v]} "
    )
    final_joins = [
        x
        for x in [base_join_to_join(join, parents) for join in query_datasource.joins]
        if x
    ]
    base_name, base_alias = resolve_cte_base_name_and_alias(
        human_id, query_datasource, parents, final_joins
    )
    cte = CTE(
        name=human_id,
        source=query_datasource,
        # output columns are what are selected/grouped by
        output_columns=[
            c.with_grain(query_datasource.grain)
            for c in query_datasource.output_concepts
        ],
        source_map=source_map,
        existence_source_map=existence_map,
        # related columns include all referenced columns, such as filtering
        joins=final_joins,
        grain=query_datasource.grain,
        group_to_grain=query_datasource.group_required,
        # we restrict parent_ctes to one level
        # as this set is used as the base for rendering the query
        parent_ctes=parents,
        condition=query_datasource.condition,
        partial_concepts=query_datasource.partial_concepts,
        join_derived_concepts=query_datasource.join_derived_concepts,
        hidden_concepts=query_datasource.hidden_concepts,
        base_name_override=base_name,
        base_alias_override=base_alias,
    )
    if cte.grain != query_datasource.grain:
        raise ValueError("Grain was corrupted in CTE generation")
    for x in cte.output_columns:
        if x.address not in cte.source_map and CONFIG.validate_missing:
            raise ValueError(
                f"Missing {x.address} in {cte.source_map}, source map {cte.source.source_map.keys()} "
            )

    output.append(cte)
    return output


def get_query_datasources(
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
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
        statement.output_components,
        environment=environment,
        g=graph,
    )
    if hooks:
        for hook in hooks:
            hook.process_root_strategy_node(ds)
    final_qds = ds.resolve()

    # we if we have a where clause doing an existence check
    # treat that as separate subquery
    if (where := statement.where_clause) and where.existence_arguments:
        for subselect in where.existence_arguments:
            if not subselect:
                continue
            logger.info(
                f"{LOGGER_PREFIX} fetching existance clause inputs {[str(c) for c in subselect]}"
            )
            eds = source_query_concepts([*subselect], environment=environment, g=graph)

            final_eds = eds.resolve()
            first_parent = final_qds
            first_parent.datasources.append(final_eds)
            for x in final_eds.output_concepts:
                if x.address not in first_parent.existence_source_map:
                    first_parent.existence_source_map[x.address] = {final_eds}
    return final_qds


def flatten_ctes(input: CTE) -> list[CTE]:
    output = [input]
    for cte in input.parent_ctes:
        output += flatten_ctes(cte)
    return output


def process_auto(
    environment: Environment,
    statement: PersistStatement | SelectStatement,
    hooks: List[BaseHook] | None = None,
):
    if isinstance(statement, PersistStatement):
        return process_persist(environment, statement, hooks)
    elif isinstance(statement, SelectStatement):
        return process_query(environment, statement, hooks)
    raise ValueError(f"Do not know how to process {type(statement)}")


def process_persist(
    environment: Environment,
    statement: PersistStatement,
    hooks: List[BaseHook] | None = None,
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
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
    hooks: List[BaseHook] | None = None,
) -> ProcessedQuery:
    hooks = hooks or []
    graph = generate_graph(environment)
    root_datasource = get_query_datasources(
        environment=environment, graph=graph, statement=statement, hooks=hooks
    )
    for hook in hooks:
        hook.process_root_datasource(root_datasource)
    # this should always return 1 - TODO, refactor
    root_cte = datasource_to_ctes(root_datasource, environment.cte_name_map)[0]
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
    deduped_ctes: List[CTE] = list(seen.values())

    final_ctes = optimize_ctes(deduped_ctes, root_cte, statement)

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
