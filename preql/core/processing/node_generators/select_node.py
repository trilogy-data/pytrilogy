from collections import defaultdict
from itertools import combinations
from typing import List, Optional

from preql.core.enums import Purpose
from preql.core.models import (
    Concept,
    Environment,
    BaseJoin,
    QueryDatasource,
    Datasource,
)
from typing import Set
from preql.core.processing.nodes import (
    StrategyNode,
    SelectNode,
    MergeNode,
    NodeJoin,
)
from preql.core.exceptions import NoDatasourceException
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.processing.utility import PathInfo, path_to_joins
from preql.constants import logger
from preql.core.processing.nodes import StaticSelectNode
from preql.utility import unique


LOGGER_PREFIX = "[GEN_SELECT_NODE_FROM_JOIN_VERBOSE]"


def gen_select_node_from_table(
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    accept_partial: bool = False,
) -> Optional[SelectNode]:
    # if we have only constants
    # we don't need a table
    # so verify nothing, select node will render
    if all([c.purpose == Purpose.CONSTANT for c in all_concepts]):
        return SelectNode(
            output_concepts=all_concepts,
            input_concepts=[],
            environment=environment,
            g=g,
            parents=[],
            depth=depth,
            # no partial for constants
            partial_concepts=[],
        )
    # otherwise, we need to look for a table
    for datasource in environment.datasources.values():
        all_found = True
        for raw_concept in all_concepts:
            # look for connection to abstract grain
            req_concept = raw_concept.with_default_grain()
            # if we don't have a concept in the graph
            # exit early
            if concept_to_node(req_concept) not in g.nodes:
                return None
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except nx.NodeNotFound as e:
                candidates = [
                    datasource_to_node(datasource),
                    concept_to_node(req_concept),
                ]
                for candidate in candidates:
                    try:
                        g.nodes[candidate]
                    except KeyError:
                        raise SyntaxError(
                            "Could not find node for {}".format(candidate)
                        )
                raise e
            except nx.exception.NetworkXNoPath:
                all_found = False
                break
            # 2023-10-18 - more strict condition then below
            # 2023-10-20 - we need this to get through abstract concepts
            # but we may want to add a filter to avoid using this for anything with lineage
            # if len(path) != 2:
            #     all_found = False
            #     break
            if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                all_found = False
                break
            for node in path:
                if g.nodes[node]["type"] == "datasource":
                    continue
                if g.nodes[node]["concept"].address == raw_concept.address:
                    continue
                all_found = False
                break

        if all_found:
            partial_concepts = [
                c.concept
                for c in datasource.columns
                if not c.is_complete
                and c.concept.address in [x.address for x in all_concepts]
            ]
            if not accept_partial and partial_concepts:
                continue
            return SelectNode(
                input_concepts=[c.concept for c in datasource.columns],
                output_concepts=all_concepts,
                environment=environment,
                g=g,
                parents=[],
                depth=depth,
                partial_concepts=[
                    c.concept for c in datasource.columns if not c.is_complete
                ],
            )
    return None


def gen_select_node_from_join(
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    accept_partial: bool = False,
) -> Optional[MergeNode]:
    all_input_concepts = [*all_concepts]

    join_candidates: List[PathInfo] = []
    for datasource in environment.datasources.values():
        all_found = True
        paths = {}
        for bitem in all_concepts:
            item = bitem.with_default_grain()
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(item),
                )
                paths[concept_to_node(item)] = path
            except nx.exception.NodeNotFound as e:
                # TODO: support Verbose logging mode configuration and reenable these
                logger.debug(
                    f"{LOGGER_PREFIX} could not find node for {item.address} with {item.grain} and {item.lineage}: {str(e)}"
                )
                all_found = False

                continue
            except nx.exception.NetworkXNoPath:
                logger.debug(
                    f"{LOGGER_PREFIX} could not get to {concept_to_node(item)} from {datasource_to_node(datasource)}"
                )
                all_found = False
                continue
        if all_found:
            partial = [
                c.concept
                for c in datasource.columns
                if not c.is_complete
                and c.concept.address in [x.address for x in all_concepts]
            ]
            if partial and not accept_partial:
                continue
            join_candidates.append({"paths": paths, "datasource": datasource})
    join_candidates.sort(key=lambda x: sum([len(v) for v in x["paths"].values()]))
    if not join_candidates:
        return None
    shortest: PathInfo = join_candidates[0]
    source_map = defaultdict(set)
    join_paths: List[BaseJoin] = []
    parents = []
    all_datasets: Set = set()
    all_search_concepts: Set = set()
    for key, value in shortest["paths"].items():
        datasource_nodes = [v for v in value if v.startswith("ds~")]
        concept_nodes = [v for v in value if v.startswith("c~")]
        all_datasets = all_datasets.union(set(datasource_nodes))
        all_search_concepts = all_search_concepts.union(set(concept_nodes))
        root = datasource_nodes[-1]
        source_concept: Concept = g.nodes[value[-1]]["concept"]
        parents.append(source_concept)
        new_joins = path_to_joins(value, g=g)

        join_paths += new_joins
        source_map[source_concept.address].add(g.nodes[root]["datasource"])
        for join in new_joins:
            for jconcept in join.concepts:
                source_map[jconcept.address].add(join.left_datasource)
                source_map[jconcept.address].add(join.right_datasource)
                all_input_concepts.append(jconcept)
    datasources: List[Datasource] = sorted(
        [g.nodes[key]["datasource"] for key in all_datasets],
        key=lambda x: x.full_name,
    )
    parent_nodes: List[StrategyNode] = []
    ds_to_node_map = {}
    for datasource in datasources:
        partial = [x for x in datasource.partial_concepts if x in all_concepts]
        local_all: List[Concept] = datasource.output_concepts
        node = StaticSelectNode(
            input_concepts=local_all,
            output_concepts=local_all,
            environment=environment,
            g=g,
            datasource=QueryDatasource(
                input_concepts=unique(local_all, "address"),
                output_concepts=unique(local_all, "address"),
                source_map={concept.address: {datasource} for concept in local_all},
                datasources=[datasource],
                grain=datasource.grain,
                joins=[],
                partial_concepts=[
                    c.concept for c in datasource.columns if not c.is_complete
                ],
            ),
            depth=depth,
            partial_concepts=[
                c.concept for c in datasource.columns if not c.is_complete
            ],
        )
        parent_nodes.append(node)
        ds_to_node_map[datasource.identifier] = node

    final_joins = []
    for join in join_paths:
        left = ds_to_node_map[join.left_datasource.identifier]
        right = ds_to_node_map[join.right_datasource.identifier]
        concepts = join.concepts
        join_type = join.join_type
        final_joins.append(
            NodeJoin(
                left_node=left,
                right_node=right,
                concepts=concepts,
                join_type=join_type,
                filter_to_mutual=join.filter_to_mutual,
            )
        )
    all_partial = [
        c
        for c in all_concepts
        if all(
            [c.address in [x.address for x in p.partial_concepts] for p in parent_nodes]
        )
    ]
    return MergeNode(
        input_concepts=all_input_concepts,
        output_concepts=all_concepts,
        environment=environment,
        g=g,
        parents=parent_nodes,
        depth=depth,
        node_joins=final_joins,
        partial_concepts=all_partial,
    )


def gen_select_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    accept_partial: bool = False,
) -> MergeNode | SelectNode:
    basic_inputs = [
        x
        for x in local_optional
        if x.address in [z.address for z in environment.materialized_concepts]
    ]
    ds = None
    ds = gen_select_node_from_table(
        [concept] + local_optional,
        g=g,
        environment=environment,
        depth=depth,
        accept_partial=accept_partial,
    )
    if ds:
        return ds
    # then look for joins
    for x in reversed(range(1, len(basic_inputs) + 1)):
        for combo in combinations(basic_inputs, x):
            all_concepts = [concept, *combo]
            ds = gen_select_node_from_table(
                all_concepts,
                g=g,
                environment=environment,
                depth=depth,
                accept_partial=accept_partial,
            )
            if ds:
                return ds
            joins = gen_select_node_from_join(
                all_concepts,
                g=g,
                environment=environment,
                depth=depth,
                accept_partial=accept_partial,
            )
            if joins:
                return joins
    ds = gen_select_node_from_table(
        [concept],
        g=g,
        environment=environment,
        depth=depth,
        accept_partial=accept_partial,
    )
    if not ds:
        raise NoDatasourceException(f"No datasource exists for {concept}")
    return ds
