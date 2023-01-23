from collections import defaultdict
from typing import List, Optional, Union, Tuple, Set, Dict

import networkx as nx
from typing import TypedDict
from preql.constants import logger
from preql.core.enums import Purpose, PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.hooks import BaseProcessingHook
from preql.core.models import (
    Concept,
    Environment,
    Select,
    Datasource,
    CTE,
    Join,
    JoinKey,
    ProcessedQuery,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin,
    Function,
    WindowItem,
)
from preql.utility import string_to_hash
from preql.utility import unique


def concept_to_inputs(concept: Concept) -> List[Concept]:
    """Given a concept, return all relevant root inputs"""
    output = []
    if not concept.lineage:
        return [concept]
    for source in concept.sources:
        # if something is a transformation of something with a lineage
        # then we need to persist the original type
        # ex: avg() of sum() @ grain
        output += concept_to_inputs(source.with_default_grain())
    return output


def get_datasource_from_direct_select(
    concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    """Return a datasource a concept can be directly selected from at
    appropriate grain. Think select * from table."""
    all_concepts = concept_to_inputs(concept)
    for datasource in environment.datasources.values():
        if not datasource.grain.issubset(grain):
            continue
        all_found = True
        for req_concept in all_concepts:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except nx.exception.NodeNotFound as e:
                all_found = False
                break
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                break
            if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                all_found = False
                break
        if all_found:
            if datasource.grain.issubset(grain):
                final_grain = datasource.grain
            else:
                final_grain = grain
            logger.debug(
                f"got {datasource.identifier} for {concept} from direct select"
            )
            base_outputs = [concept] + final_grain.components
            for item in datasource.concepts:
                if item.address in [c.address for c in grain.components]:
                    base_outputs.append(item)
            return QueryDatasource(
                output_concepts=unique(base_outputs, "address"),
                input_concepts=all_concepts,
                source_map={concept.name: {datasource} for concept in all_concepts},
                datasources=[datasource],
                grain=final_grain,
                joins=[],
            )
    raise ValueError(f"No direct select for {concept} and grain {grain}")


def get_datasource_from_property_lookup(
    concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    """Return a datasource that can be grouped to a value and grain.
    Think unique order ids in order product table."""
    all_concepts = concept_to_inputs(concept)
    for datasource in environment.datasources.values():
        if datasource.grain.issubset(grain):
            all_found = True
            for req_concept in all_concepts:
                try:
                    path = nx.shortest_path(
                        g,
                        source=datasource_to_node(datasource),
                        target=concept_to_node(req_concept),
                    )
                except nx.exception.NetworkXNoPath as e:
                    all_found = False
                    break
                if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                    all_found = False
                    break
            if all_found:
                return QueryDatasource(
                    input_concepts=all_concepts + datasource.grain.components,
                    output_concepts=[concept] + datasource.grain.components,
                    source_map={concept.name: {datasource} for concept in all_concepts},
                    datasources=[datasource],
                    grain=datasource.grain,
                    joins=[],
                )
    raise ValueError(f"No property lookup for {concept}")


def get_datasource_from_group_select(
    concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    """Return a datasource that can be grouped to a value and grain.
    Unique values in a column, for example"""
    all_concepts = concept_to_inputs(concept.with_default_grain()) + grain.components
    for datasource in environment.datasources.values():
        all_found = True
        for req_concept in all_concepts:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except (nx.exception.NetworkXNoPath, nx.exception.NodeNotFound) as e:
                all_found = False
                break
            if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                all_found = False
                break
        if all_found:
            # if the dataset that can reach every concept
            # is in fact a subset of the concept
            # assume property lookup
            if datasource.grain.issubset(grain):
                final_grain = datasource.grain
            else:
                final_grain = grain
            logger.debug(
                f"got {datasource.identifier} for {concept} from grouped select"
            )
            return QueryDatasource(
                input_concepts=all_concepts,
                output_concepts=[concept] + grain.components,
                source_map={concept.name: {datasource} for concept in all_concepts},
                datasources=[datasource],
                grain=final_grain,
                joins=[],
            )
    raise ValueError(f"No grouped select for {concept}")


def parse_path_to_matches(
    input: List[str]
) -> List[Tuple[Optional[str], Optional[str], List[str]]]:
    """Parse a networkx path to a set of join relations"""
    left_ds = None
    right_ds = None
    concept = None
    output: List[Tuple[Optional[str], Optional[str], List[str]]] = []
    while input:
        ds = None
        next = input.pop(0)
        if next.startswith("ds~"):
            ds = next
        elif next.startswith("c~"):
            concept = next
        if ds and not left_ds:
            left_ds = ds
            continue
        elif ds and concept:
            right_ds = ds
            output.append((left_ds, right_ds, [concept]))
            left_ds = right_ds
            concept = None
    if left_ds and concept and not right_ds:
        output.append((left_ds, None, [concept]))
    return output


def path_to_joins(input: List[str], g: ReferenceGraph) -> List[BaseJoin]:
    """ Build joins and ensure any required CTEs are also created/tracked"""
    out = []
    zipped = parse_path_to_matches(input)
    for row in zipped:
        left_ds, right_ds, raw_concepts = row
        concepts = [g.nodes[concept]["concept"] for concept in raw_concepts]
        left_value = g.nodes[left_ds]["datasource"]
        if not right_ds:
            continue
        right_value = g.nodes[right_ds]["datasource"]
        out.append(
            BaseJoin(
                left_datasource=left_value,
                right_datasource=right_value,
                join_type=JoinType.LEFT_OUTER,
                concepts=concepts,
            )
        )
    return out


class PathInfo(TypedDict):
    paths: Dict[str, List[str]]
    datasource: Datasource


def get_datasource_by_joins(
    concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    join_candidates: List[PathInfo] = []

    all_requirements = unique(concept_to_inputs(concept) + grain.components, "address")

    for datasource in environment.datasources.values():
        all_found = True
        paths = {}
        for item in all_requirements:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(item),
                )
                paths[concept_to_node(item)] = path
            except nx.exception.NodeNotFound as e:
                all_found = False
                continue
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                continue
        if all_found:
            join_candidates.append({"paths": paths, "datasource": datasource})
    join_candidates.sort(key=lambda x: sum([len(v) for v in x["paths"].values()]))
    if not join_candidates:
        raise ValueError(f"No joins to get to {concept} and grain {grain}")
    shortest: PathInfo = join_candidates[0]
    source_map = defaultdict(set)
    join_paths = []
    parents = []
    all_datasets: Set = set()
    all_concepts: Set = set()
    for key, value in shortest["paths"].items():
        datasource_nodes = [v for v in value if v.startswith("ds~")]
        concept_nodes = [v for v in value if v.startswith("c~")]
        all_datasets = all_datasets.union(set(datasource_nodes))
        all_concepts = all_concepts.union(set(concept_nodes))
        root = datasource_nodes[-1]
        source_concept = g.nodes[value[-1]]["concept"]
        parents.append(source_concept)

        new_joins = path_to_joins(value, g=g)

        join_paths += new_joins
        source_map[source_concept.address].add(g.nodes[root]["datasource"])
        # ensure we add in all keys required for joins as inputs
        # even if they are not selected out
        for join in new_joins:
            for jconcept in join.concepts:
                source_map[jconcept.address].add(join.left_datasource)
                source_map[jconcept.address].add(join.right_datasource)
                all_requirements.append(jconcept)
    all_requirements = unique(all_requirements, "address")

    output = QueryDatasource(
        output_concepts=[concept] + grain.components,
        input_concepts=all_requirements,
        source_map=source_map,
        grain=grain,
        datasources=sorted(
            [g.nodes[key]["datasource"] for key in all_datasets],
            key=lambda x: x.identifier,
        ),
        joins=join_paths,
    )
    return output


def get_datasource_from_complex_lineage(concept: Concept, grain: Grain, environment, g):
    # always true if window item
    complex_lineage_flag = isinstance(concept.lineage, WindowItem)
    all_requirements = []
    all_datasets = []
    source_map = {}
    # for anything in function lineage, calculate to current grain
    if not isinstance(concept.lineage, Function):
        raise ValueError(
            "Attempting to get complex lineage from non-function declaration"
        )
    for sub_concept in concept.lineage.arguments:
        # if aggregate of aggregate
        if sub_concept.derivation in (PurposeLineage.AGGREGATE, PurposeLineage.WINDOW):
            complex_lineage_flag = True
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, sub_concept.grain + grain, environment=environment, g=g
        )
        all_datasets.append(sub_datasource)
        all_requirements.append(sub_concept)
        source_map[sub_concept.name] = {sub_datasource}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}

    # for grain components, build in CTE if required
    for sub_concept in grain.components:
        if sub_concept.derivation in (PurposeLineage.AGGREGATE, PurposeLineage.WINDOW):
            complex_lineage_flag = True
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, sub_concept.grain, environment=environment, g=g
        )
        all_datasets.append(sub_datasource)
        all_requirements.append(sub_concept)
        source_map[sub_concept.name] = {sub_datasource}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}
    if complex_lineage_flag:
        logger.debug(f"Complex lineage found for {concept}")
        logger.debug(f"Grain {grain}")
        qds = QueryDatasource(
            output_concepts=[concept] + grain.components,
            input_concepts=all_requirements,
            source_map=source_map,
            grain=grain,
            datasources=all_datasets,
            joins=[]
            # joins=join_paths,
        )
        source_map[concept.name] = {qds}
        return qds


def get_datasource_from_window_function(concept: Concept, grain: Grain, environment, g):
    if not isinstance(concept.lineage, WindowItem):
        raise ValueError(
            "Attempting to use windowed derivation for non window function"
        )
    window: WindowItem = concept.lineage
    all_requirements = []
    all_datasets: Dict = {}
    source_map = {}
    cte_grain = Grain(components=[window.content.output])
    sub_concepts = unique([window.content.output] + grain.components, "identifier")
    for arg in window.order_by:
        sub_concepts += [arg.output]
    for sub_concept in sub_concepts:
        sub_concept = sub_concept.with_grain(cte_grain)
        sub_datasource = get_datasource_by_concept_and_grain(
            sub_concept, cte_grain, environment=environment, g=g
        )
        if sub_datasource.identifier in all_datasets:
            all_datasets[sub_datasource.identifier] = (
                all_datasets[sub_datasource.identifier] + sub_datasource
            )
        else:
            all_datasets[sub_datasource.identifier] = sub_datasource
        all_requirements.append(sub_concept)
        source_map[sub_concept.name] = {all_datasets[sub_datasource.identifier]}
        if isinstance(sub_datasource, QueryDatasource):
            source_map = {**source_map, **sub_datasource.source_map}
    dataset_list = list(all_datasets.values())
    base = dataset_list[0]

    joins = []
    for right_value in dataset_list[1:]:
        joins.append(
            BaseJoin(
                left_datasource=base,
                right_datasource=right_value,
                join_type=JoinType.LEFT_OUTER,
                concepts=cte_grain.components,
            )
        )
    qds = QueryDatasource(
        output_concepts=[concept] + grain.components,
        input_concepts=all_requirements,
        source_map=source_map,
        grain=grain,
        datasources=list(all_datasets.values()),
        joins=joins,
    )
    source_map[concept.name] = {qds}
    return qds


def get_datasource_by_concept_and_grain(
    concept, grain: Grain, environment: Environment, g: Optional[ReferenceGraph] = None
) -> Union[Datasource, QueryDatasource]:
    """Determine if it's possible to get a certain concept at a certain grain.
    """
    g = g or generate_graph(environment)
    if concept.lineage:
        if concept.derivation == PurposeLineage.WINDOW:
            logger.debug("Checking for complex window function")
            complex = get_datasource_from_window_function(
                concept, grain, environment, g
            )
        elif concept.derivation == PurposeLineage.AGGREGATE:
            logger.debug("Checking for complex function derivation")
            complex = get_datasource_from_complex_lineage(
                concept, grain, environment, g
            )
        else:
            complex = None
        if complex:
            logger.debug(f"Returning complex lineage for {concept}")
            return complex
        logger.debug(f"Can satisfy query with basic lineage for {concept}")
    # the concept is available directly on a datasource at appropriate grain
    if concept.purpose in (Purpose.KEY, Purpose.PROPERTY):
        try:
            return get_datasource_from_property_lookup(
                concept.with_default_grain(), grain, environment, g
            )
        except ValueError as e:
            logger.error(e)
    # the concept is available on a datasource, but at a higher granularity
    try:
        return get_datasource_from_group_select(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)
    # the concept and grain together can be gotten via
    # a join from a root dataset to enrichment datasets
    try:
        return get_datasource_by_joins(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)
    from itertools import combinations

    for x in range(1, len(grain.components)):
        for combo in combinations(grain.components, x):
            ngrain = Grain(components=list(combo))
            try:
                return get_datasource_by_joins(
                    concept.with_grain(ngrain), grain, environment, g
                )
            except ValueError as e:
                logger.error(e)

    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(f"No source for {concept} found, neighbors {neighbors}")


def base_join_to_join(base_join: BaseJoin, ctes: List[CTE]) -> Join:

    left_cte = [
        cte
        for cte in ctes
        if (
            cte.source.datasources[0].identifier == base_join.left_datasource.identifier
            or cte.source.identifier == base_join.left_datasource.identifier
        )
    ][0]
    right_cte = [
        cte
        for cte in ctes
        if (
            cte.source.datasources[0].identifier
            == base_join.right_datasource.identifier
            or cte.source.identifier == base_join.right_datasource.identifier
        )
    ][0]

    return Join(
        left_cte=left_cte,
        right_cte=right_cte,
        joinkeys=[JoinKey(concept=concept) for concept in base_join.concepts],
        jointype=base_join.join_type,
    )


def datasource_to_ctes(query_datasource: QueryDatasource) -> List[CTE]:
    int_id = string_to_hash(query_datasource.identifier)
    group_to_grain = (
        False
        if sum([ds.grain for ds in query_datasource.datasources])
        == query_datasource.grain
        else True
    )
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
                sub_select = {
                    key: item
                    for key, item in query_datasource.source_map.items()
                    if datasource in item
                }
                concepts = [
                    c for c in datasource.concepts if c.address in sub_select.keys()
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
            output += sub_cte
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

    output.append(
        CTE(
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
            joins=[base_join_to_join(join, output) for join in query_datasource.joins],
            related_columns=query_datasource.input_concepts,
            grain=query_datasource.grain,
            group_to_grain=group_to_grain,
            parent_ctes=children,
        )
    )
    return output


def get_query_datasources(
    environment: Environment, statement: Select, graph: Optional[ReferenceGraph] = None
):
    concept_map: Dict = defaultdict(list)
    graph = graph or generate_graph(environment)
    datasource_map: Dict = {}
    for concept in statement.output_components + statement.grain.components:
        datasource = get_datasource_by_concept_and_grain(
            concept, statement.grain, environment, graph
        )

        if concept not in concept_map[datasource.identifier]:
            concept_map[datasource.identifier].append(concept)
        if datasource.identifier in datasource_map:
            # concatenate to add new fields
            datasource_map[datasource.identifier] = (
                datasource_map[datasource.identifier] + datasource
            )
        else:
            datasource_map[datasource.identifier] = datasource
    return concept_map, datasource_map


def process_query(
    environment: Environment,
    statement: Select,
    hooks: Optional[List[BaseProcessingHook]] = None,
) -> ProcessedQuery:
    """Turn the raw query input into an instantiated execution tree."""
    graph = generate_graph(environment)
    concepts, datasources = get_query_datasources(
        environment=environment, graph=graph, statement=statement
    )
    ctes = []
    joins = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)

    final_ctes_dict: Dict[str, CTE] = {}
    # merge CTEs
    for cte in ctes:
        if cte.name not in final_ctes_dict:
            final_ctes_dict[cte.name] = cte
        else:
            final_ctes_dict[cte.name] = final_ctes_dict[cte.name] + cte
    final_ctes = list(final_ctes_dict.values())
    base_list = [cte for cte in final_ctes if cte.grain == statement.grain]
    if base_list:
        base = base_list[0]
    else:
        base_list = [cte for cte in ctes if cte.grain.issubset(statement.grain)]
        base = base_list[0]
    others = [cte for cte in final_ctes if cte != base]
    for cte in others:
        joinkeys = [
            JoinKey(c) for c in statement.grain.components if c in cte.output_columns
        ]
        if joinkeys:
            joins.append(
                Join(
                    left_cte=base,
                    right_cte=cte,
                    joinkeys=joinkeys,
                    jointype=JoinType.LEFT_OUTER,
                )
            )
    return ProcessedQuery(
        order_by=statement.order_by,
        grain=statement.grain,
        limit=statement.limit,
        where_clause=statement.where_clause,
        output_columns=statement.output_components,
        ctes=final_ctes,
        base=base,
        joins=joins,
    )
