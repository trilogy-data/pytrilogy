from collections import defaultdict
from math import ceil
from typing import Dict, List, Optional, Set, Tuple, Union

from trilogy.constants import CONFIG, logger
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import BooleanOperator, SourceType
from trilogy.core.env_processor import generate_graph
from trilogy.core.ergonomics import generate_cte_names
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildMultiSelectLineage,
    BuildParamaterizedConceptReference,
    BuildSelectLineage,
    Factory,
)
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
    QueryDatasource,
    RecursiveCTE,
    UnionCTE,
    UnnestJoin,
)
from trilogy.core.optimization import optimize_ctes
from trilogy.core.processing.concept_strategies_v3 import source_query_concepts
from trilogy.core.processing.nodes import History, SelectNode, StrategyNode
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    CopyStatement,
    MultiSelectStatement,
    PersistStatement,
    SelectStatement,
)
from trilogy.core.statements.common import MaterializedDataset
from trilogy.core.statements.execute import (
    ProcessedCopyStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
)
from trilogy.hooks.base_hook import BaseHook
from trilogy.utility import unique

LOGGER_PREFIX = "[QUERY BUILD]"


def base_join_to_join(
    base_join: BaseJoin | UnnestJoin, ctes: List[CTE | UnionCTE]
) -> Join | InstantiatedUnnestJoin:
    """This function converts joins at the datasource level
    to joins at the CTE level"""
    if isinstance(base_join, UnnestJoin):
        object_to_unnest = base_join.parent.arguments[0]
        if not isinstance(
            object_to_unnest,
            (BuildConcept | BuildParamaterizedConceptReference | BuildFunction),
        ):
            raise ValueError(f"Unnest join must be a concept; got {object_to_unnest}")
        return InstantiatedUnnestJoin(
            object_to_unnest=object_to_unnest,
            alias=base_join.alias,
        )

    def get_datasource_cte(
        datasource: BuildDatasource | QueryDatasource,
    ) -> CTE | UnionCTE:
        eligible = set()
        for cte in ctes:
            if cte.source.identifier == datasource.identifier:
                return cte
            eligible.add(cte.source.identifier)
        for cte in ctes:
            if cte.source.datasources[0].identifier == datasource.identifier:
                return cte
            eligible.add(cte.source.datasources[0].identifier)
        raise ValueError(
            f"Could not find CTE for datasource {datasource.identifier}; have {eligible}"
        )

    if base_join.left_datasource is not None:
        left_cte = get_datasource_cte(base_join.left_datasource)
    else:
        # multiple left ctes
        left_cte = None
    right_cte = get_datasource_cte(base_join.right_datasource)
    if base_join.concept_pairs:
        final_pairs = [
            CTEConceptPair(
                left=pair.left,
                right=pair.right,
                existing_datasource=pair.existing_datasource,
                modifiers=pair.modifiers,
                cte=get_datasource_cte(pair.existing_datasource),
            )
            for pair in base_join.concept_pairs
        ]
    elif base_join.concepts and base_join.left_datasource:
        final_pairs = [
            CTEConceptPair(
                left=concept,
                right=concept,
                existing_datasource=base_join.left_datasource,
                modifiers=[],
                cte=get_datasource_cte(
                    base_join.left_datasource,
                ),
            )
            for concept in base_join.concepts
        ]
    else:
        final_pairs = []
    return Join(
        left_cte=left_cte,
        right_cte=right_cte,
        jointype=base_join.join_type,
        joinkey_pairs=final_pairs,
    )


def generate_source_map(
    query_datasource: QueryDatasource, all_new_ctes: List[CTE | UnionCTE]
) -> Tuple[Dict[str, list[str]], Dict[str, list[str]]]:
    source_map: Dict[str, list[str]] = defaultdict(list)
    # now populate anything derived in this level
    for qdk, qdv in query_datasource.source_map.items():
        unnest = [x for x in qdv if isinstance(x, UnnestJoin)]
        for _ in unnest:
            source_map[qdk] = []
        if (
            qdk not in source_map
            and len(qdv) == 1
            and isinstance(list(qdv)[0], UnnestJoin)
        ):
            source_map[qdk] = []
        basic = [x for x in qdv if isinstance(x, BuildDatasource)]
        for base in basic:
            source_map[qdk].append(base.safe_identifier)

        ctes = [x for x in qdv if isinstance(x, QueryDatasource)]
        if ctes:
            names = set([x.safe_identifier for x in ctes])
            matches = [
                cte for cte in all_new_ctes if cte.source.safe_identifier in names
            ]

            if not matches and names:
                raise SyntaxError(
                    f"Missing parent CTEs for source map; expecting {names}, have {[cte.source.safe_identifier for cte in all_new_ctes]}"
                )
            for cte in matches:
                output_address = [
                    x.address
                    for x in cte.output_columns
                    if x.address not in [z.address for z in cte.partial_concepts]
                ]
                if qdk in output_address:
                    source_map[qdk].append(cte.safe_identifier)
            # now do a pass that accepts partials
            for cte in matches:
                if qdk not in source_map:
                    source_map[qdk] = [cte.safe_identifier]
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
        ids = set([x.safe_identifier for x in ev])
        ematches = [
            cte.name for cte in all_new_ctes if cte.source.safe_identifier in ids
        ]
        existence_source_map[ek] = ematches
    return {
        k: [] if not v else list(set(v)) for k, v in source_map.items()
    }, existence_source_map


def datasource_to_query_datasource(datasource: BuildDatasource) -> QueryDatasource:
    sub_select: Dict[str, Set[Union[BuildDatasource, QueryDatasource, UnnestJoin]]] = {
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
    cte_names = generate_cte_names()
    if CONFIG.human_identifiers:
        if full_name in name_map:
            return name_map[full_name]
        suffix = ""
        idx = len(name_map)
        if idx >= len(cte_names):
            int = ceil(idx / len(cte_names))
            suffix = f"_{int}"
        valid = [x for x in cte_names if x + suffix not in name_map.values()]
        lookup = valid[0]
        new_name = f"{lookup}{suffix}"
        name_map[full_name] = new_name
        return new_name
    else:
        return full_name.replace("<", "").replace(">", "").replace(",", "_")


def resolve_cte_base_name_and_alias_v2(
    name: str,
    source: QueryDatasource,
    source_map: Dict[str, list[str]],
    raw_joins: List[Join | InstantiatedUnnestJoin],
) -> Tuple[str | None, str | None]:
    if not source.datasources:
        return None, None
    if (
        isinstance(source.datasources[0], BuildDatasource)
        and not source.datasources[0].name == CONSTANT_DATASET
    ):
        ds = source.datasources[0]
        return ds.safe_location, ds.safe_identifier

    joins: List[Join] = [join for join in raw_joins if isinstance(join, Join)]
    if joins and len(joins) > 0:
        candidates = [x.left_cte.name for x in joins if x.left_cte]
        for join in joins:
            if join.joinkey_pairs:
                candidates += [x.cte.name for x in join.joinkey_pairs if x.cte]
        disallowed = [x.right_cte.name for x in joins]
        try:
            cte = [y for y in candidates if y not in disallowed][0]
            return cte, cte
        except IndexError:
            raise SyntaxError(
                f"Invalid join configuration {candidates} {disallowed} for {name}",
            )
    counts: dict[str, int] = defaultdict(lambda: 0)
    output_addresses = [x.address for x in source.output_concepts]
    input_address = [x.address for x in source.input_concepts]
    for k, v in source_map.items():
        for vx in v:
            if k in output_addresses:
                counts[vx] = counts[vx] + 1

            if k in input_address:
                counts[vx] = counts[vx] + 1

            counts[vx] = counts[vx]
    if counts:
        return max(counts, key=counts.get), max(counts, key=counts.get)  # type: ignore
    return None, None


def datasource_to_cte(
    query_datasource: QueryDatasource, name_map: dict[str, str]
) -> CTE | UnionCTE:
    parents: list[CTE | UnionCTE] = []
    if query_datasource.source_type == SourceType.UNION:
        direct_parents: list[CTE | UnionCTE] = []
        for child in query_datasource.datasources:
            assert isinstance(child, QueryDatasource)
            child_cte = datasource_to_cte(child, name_map=name_map)
            direct_parents.append(child_cte)
            parents += child_cte.parent_ctes
        human_id = generate_cte_name(query_datasource.identifier, name_map)
        final = UnionCTE(
            name=human_id,
            source=query_datasource,
            parent_ctes=parents,
            internal_ctes=direct_parents,
            output_columns=[
                c.with_grain(query_datasource.grain)
                for c in query_datasource.output_concepts
            ],
            grain=direct_parents[0].grain,
            order_by=query_datasource.ordering,
        )
        return final

    if len(query_datasource.datasources) > 1 or any(
        [isinstance(x, QueryDatasource) for x in query_datasource.datasources]
    ):
        all_new_ctes: List[CTE | UnionCTE] = []
        for datasource in query_datasource.datasources:
            if isinstance(datasource, QueryDatasource):
                sub_datasource = datasource
            else:
                sub_datasource = datasource_to_query_datasource(datasource)

            sub_cte = datasource_to_cte(sub_datasource, name_map)
            parents.append(sub_cte)
            all_new_ctes.append(sub_cte)
        source_map, existence_map = generate_source_map(query_datasource, all_new_ctes)

    else:
        # source is the first datasource of the query datasource
        if query_datasource.datasources:

            source = query_datasource.datasources[0]
            # this is required to ensure that constant datasets
            # render properly on initial access; since they have
            # no actual source
            if source.name == CONSTANT_DATASET:
                source_map = {k: [] for k in query_datasource.source_map}
                existence_map = source_map
            else:
                source_map = {
                    k: [] if not v else [source.safe_identifier]
                    for k, v in query_datasource.source_map.items()
                }
                existence_map = source_map
        else:
            source_map = {k: [] for k in query_datasource.source_map}
            existence_map = source_map

    human_id = generate_cte_name(query_datasource.identifier, name_map)

    final_joins = [
        base_join_to_join(join, [x for x in parents if isinstance(x, (CTE, UnionCTE))])
        for join in query_datasource.joins
    ]

    base_name, base_alias = resolve_cte_base_name_and_alias_v2(
        human_id, query_datasource, source_map, final_joins
    )
    cte_class = CTE

    if query_datasource.source_type == SourceType.RECURSIVE:
        cte_class = RecursiveCTE
        # extra_kwargs['left_recursive_concept'] = query_datasource.left
    cte = cte_class(
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
        nullable_concepts=query_datasource.nullable_concepts,
        join_derived_concepts=query_datasource.join_derived_concepts,
        hidden_concepts=query_datasource.hidden_concepts,
        base_name_override=base_name,
        base_alias_override=base_alias,
        order_by=query_datasource.ordering,
    )
    if cte.grain != query_datasource.grain:
        raise ValueError("Grain was corrupted in CTE generation")
    for x in cte.output_columns:
        if (
            x.address not in cte.source_map
            and not any(y in cte.source_map for y in x.pseudonyms)
            and CONFIG.validate_missing
        ):
            raise ValueError(
                f"Missing {x.address} in {cte.source_map}, source map {cte.source.source_map.keys()} "
            )

    return cte


def get_query_node(
    environment: Environment,
    statement: SelectLineage | MultiSelectLineage,
    history: History | None = None,
) -> StrategyNode:
    if not statement.output_components:
        raise ValueError(f"Statement has no output components {statement}")
    history = history or History(base_environment=environment)
    logger.info(
        f"{LOGGER_PREFIX} building query node for {statement.output_components} grain {statement.grain}"
    )
    build_statement: BuildSelectLineage | BuildMultiSelectLineage = Factory(
        environment=environment,
    ).build(statement)

    build_environment = environment.materialize_for_select(
        build_statement.local_concepts
    )
    graph = generate_graph(build_environment)

    logger.info(
        f"{LOGGER_PREFIX} getting source datasource for outputs {build_statement.output_components} grain {build_statement.grain}"
    )

    search_concepts: list[BuildConcept] = build_statement.output_components

    ods: StrategyNode = source_query_concepts(
        output_concepts=search_concepts,
        environment=build_environment,
        g=graph,
        conditions=build_statement.where_clause,
        history=history,
    )
    if not ods:
        raise ValueError(
            f"Could not find source query concepts for {[x.address for x in search_concepts]}"
        )
    ds: StrategyNode = ods
    if build_statement.having_clause:
        final = build_statement.having_clause.conditional
        if ds.conditions:
            final = BuildConditional(
                left=ds.conditions,
                right=build_statement.having_clause.conditional,
                operator=BooleanOperator.AND,
            )
        ds = SelectNode(
            output_concepts=build_statement.output_components,
            input_concepts=ds.usable_outputs,
            parents=[ds],
            environment=ds.environment,
            partial_concepts=ds.partial_concepts,
            conditions=final,
        )
    ds.hidden_concepts = build_statement.hidden_components
    ds.ordering = build_statement.order_by
    # TODO: avoid this
    ds.rebuild_cache()
    return ds


def get_query_datasources(
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
    hooks: Optional[List[BaseHook]] = None,
) -> QueryDatasource:
    ds = get_query_node(environment, statement.as_lineage(environment))

    final_qds = ds.resolve()

    if hooks:
        for hook in hooks:
            hook.process_root_strategy_node(ds)

    return final_qds


def flatten_ctes(input: CTE | UnionCTE) -> list[CTE | UnionCTE]:
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
    elif isinstance(statement, ConceptDeclarationStatement):
        return None
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


def process_copy(
    environment: Environment,
    statement: CopyStatement,
    hooks: List[BaseHook] | None = None,
) -> ProcessedCopyStatement:
    select = process_query(
        environment=environment, statement=statement.select, hooks=hooks
    )

    # build our object to return
    arg_dict = {k: v for k, v in select.__dict__.items()}
    return ProcessedCopyStatement(
        **arg_dict,
        target=statement.target,
        target_type=statement.target_type,
    )


def process_query(
    environment: Environment,
    statement: SelectStatement | MultiSelectStatement,
    hooks: List[BaseHook] | None = None,
) -> ProcessedQuery:
    hooks = hooks or []

    root_datasource = get_query_datasources(
        environment=environment, statement=statement, hooks=hooks
    )
    for hook in hooks:
        hook.process_root_datasource(root_datasource)
    # this should always return 1 - TODO, refactor
    root_cte = datasource_to_cte(root_datasource, environment.cte_name_map)

    for hook in hooks:
        hook.process_root_cte(root_cte)
    raw_ctes: List[CTE | UnionCTE] = list(reversed(flatten_ctes(root_cte)))
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
    deduped_ctes: List[CTE | UnionCTE] = list(seen.values())

    root_cte.limit = statement.limit
    root_cte.hidden_concepts = statement.hidden_components

    final_ctes = optimize_ctes(deduped_ctes, root_cte, statement)

    return ProcessedQuery(
        order_by=root_cte.order_by,
        limit=statement.limit,
        output_columns=statement.output_components,
        ctes=final_ctes,
        base=root_cte,
        hidden_columns=set([x for x in statement.hidden_components]),
        local_concepts=statement.local_concepts,
    )
