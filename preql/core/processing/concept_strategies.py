from collections import defaultdict
from typing import List, Optional, Union, Set, Dict

import networkx as nx

from preql.constants import logger
from preql.core.enums import Purpose, PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.models import (
    Concept,
    Environment,
    Datasource,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin,
    Function,
    WindowItem,
)
from preql.core.processing.utility import concept_to_inputs, PathInfo, path_to_joins
from preql.utility import unique


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
    concept: Concept,
    grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    is_filter: bool = False,
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
                logger.debug(f"Can satisfy query from property lookup for {concept}")
                if is_filter:
                    outputs = datasource.grain.components
                    filters = [concept]
                else:
                    outputs = [concept] + datasource.grain.components
                    filters = []
                return QueryDatasource(
                    input_concepts=all_concepts + datasource.grain.components,
                    output_concepts=outputs,
                    source_map={concept.name: {datasource} for concept in all_concepts},
                    datasources=[datasource],
                    grain=datasource.grain,
                    joins=[],
                    filter_concepts=filters,
                )
    raise ValueError(f"No property lookup for {concept}")


def get_datasource_from_group_select(
    concept: Concept,
    grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    is_filter: bool = False,
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
            if is_filter:
                outputs = grain.components
                filters = [concept]
            else:
                outputs = [concept] + grain.components
                filters = []
            return QueryDatasource(
                input_concepts=all_concepts,
                output_concepts=outputs,
                source_map={concept.name: {datasource} for concept in all_concepts},
                datasources=[datasource],
                grain=final_grain,
                joins=[],
                filter_concepts=filters,
            )
    raise ValueError(f"No grouped select for {concept}")


def get_datasource_by_joins(
    concept: Concept,
    grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    is_filter: bool = False,
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
    join_paths: List[BaseJoin] = []
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
    if is_filter:
        outputs = grain.components
        filters = [concept]
    else:
        outputs = [concept] + grain.components
        filters = []
    output = QueryDatasource(
        output_concepts=outputs,
        input_concepts=all_requirements,
        source_map=source_map,
        grain=grain,
        datasources=sorted(
            [g.nodes[key]["datasource"] for key in all_datasets],
            key=lambda x: x.identifier,
        ),
        joins=join_paths,
        filter_concepts=filters,
    )
    logger.debug(f"got {concept} from joins")
    return output


def get_datasource_from_complex_lineage(
    concept: Concept, grain: Grain, environment, g, is_filter: bool = False
):
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


def get_datasource_from_window_function(
    concept: Concept, grain: Grain, environment, g, is_filter: bool = False
):
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
    concept,
    grain: Grain,
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
    is_filter: bool = False,
) -> Union[Datasource, QueryDatasource]:
    """Determine if it's possible to get a certain concept at a certain grain.
    """
    g = g or generate_graph(environment)
    if concept.lineage:
        if concept.derivation == PurposeLineage.WINDOW:
            logger.debug("Checking for complex window function")
            complex = get_datasource_from_window_function(
                concept, grain, environment, g, is_filter=is_filter
            )
        elif concept.derivation == PurposeLineage.AGGREGATE:
            logger.debug("Checking for complex function derivation")
            complex = get_datasource_from_complex_lineage(
                concept, grain, environment, g, is_filter=is_filter
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
                concept.with_default_grain(), grain, environment, g, is_filter=is_filter
            )
        except ValueError as e:
            logger.error(e)
    # the concept is available on a datasource, but at a higher granularity
    try:
        return get_datasource_from_group_select(
            concept, grain, environment, g, is_filter=is_filter
        )
    except ValueError as e:
        logger.error(e)
    # the concept and grain together can be gotten via
    # a join from a root dataset to enrichment datasets
    try:
        return get_datasource_by_joins(
            concept, grain, environment, g, is_filter=is_filter
        )
    except ValueError as e:
        logger.error(e)
    from itertools import combinations

    for x in range(1, len(grain.components)):
        for combo in combinations(grain.components, x):
            ngrain = Grain(components=list(combo))
            try:
                return get_datasource_by_joins(
                    concept.with_grain(ngrain),
                    grain,
                    environment,
                    g,
                    is_filter=is_filter,
                )
            except ValueError as e:
                logger.error(e)

    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(f"No source for {concept} found, neighbors {neighbors}")
