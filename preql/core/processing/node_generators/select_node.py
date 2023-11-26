from collections import defaultdict
from itertools import combinations
from typing import List, Optional


from preql.core.enums import Purpose
from preql.core.models import (
    Concept,
    Environment,
)
from typing import Set
from preql.core.processing.nodes import (
    StrategyNode,
    SelectNode,
    StaticSelectNode,
    MergeNode,
    NodeJoin,
)
from preql.core.exceptions import NoDatasourceException
from preql.core.models import (
    BaseJoin,
)
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.processing.utility import PathInfo, path_to_joins, JoinType


def gen_select_node_from_table(
    all_concepts: List[Concept], g, environment: Environment, depth: int
) -> Optional[SelectNode]:
    # if we have only constants
    # we don't need a table
    # so verify nothing, select node will render
    if all([c.purpose == Purpose.CONSTANT for c in all_concepts]):
        return SelectNode(
            mandatory_concepts=all_concepts,
            optional_concepts=[],
            environment=environment,
            g=g,
            parents=[],
            depth=depth,
        )
    # otherwise, we need to look for a table
    for datasource in environment.datasources.values():
        all_found = True
        for raw_concept in all_concepts:
            # look for connection to abstract grain
            req_concept = raw_concept.with_default_grain()
            if concept_to_node(req_concept) not in g.nodes:
                raise ValueError(f"concept {req_concept} not found in graph")
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
        if all_found:
            return SelectNode(
                mandatory_concepts=all_concepts,
                optional_concepts=[],
                environment=environment,
                g=g,
                parents=[],
                depth=depth,
            )
    return None


def gen_select_node_from_join(
    all_concepts: List[Concept],
    g,
    environment: Environment,
    depth: int,
    source_concepts,
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
            except nx.exception.NodeNotFound:
                # TODO: support Verbose logging mode configuration and reenable these
                # logger.debug(f'{LOGGER_PREFIX} could not find node for {item.address}')
                all_found = False

                continue
            except nx.exception.NetworkXNoPath:
                # logger.debug(f'{LOGGER_PREFIX} could not get to {item.address} at {item.grain} from {datasource}')
                all_found = False
                continue
        if all_found:
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
    datasources = sorted(
        [g.nodes[key]["datasource"] for key in all_datasets],
        key=lambda x: x.full_name,
    )
    parent_nodes: List[StrategyNode] = []
    ds_to_node_map = {}
    for datasource in datasources:
        if datasource.output_concepts == all_concepts:
            raise SyntaxError(
                "This would result in infinite recursion, each source should be partial"
            )
        node = source_concepts(
            datasource.output_concepts,
            [],
            environment,
            g,
            depth=depth + 1,
        )
        parent_nodes.append(node)
        ds_to_node_map[datasource.identifier] = node

    final_joins = []
    for join in join_paths:
        left = ds_to_node_map[join.left_datasource.identifier]
        right = ds_to_node_map[join.right_datasource.identifier]
        if isinstance(left, StaticSelectNode) or isinstance(right, StaticSelectNode):
            concepts = []
            join_type = JoinType.FULL
        else:
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

    return MergeNode(
        mandatory_concepts=all_concepts,
        optional_concepts=[],
        environment=environment,
        g=g,
        parents=parent_nodes,
        depth=depth,
        node_joins=final_joins,
    )


def gen_select_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
) -> MergeNode | SelectNode:
    basic_inputs = [x for x in local_optional if x in environment.materialized_concepts]
    ds = None
    for x in reversed(range(1, len(basic_inputs) + 1)):
        for combo in combinations(basic_inputs, x):
            all_concepts = [concept, *combo]
            ds = gen_select_node_from_table(
                all_concepts, g=g, environment=environment, depth=depth
            )
            if ds:
                return ds
            joins = gen_select_node_from_join(
                all_concepts,
                g=g,
                environment=environment,
                depth=depth,
                source_concepts=source_concepts,
            )
            if joins:
                return joins
    ds = gen_select_node_from_table(
        [concept], g=g, environment=environment, depth=depth
    )
    if not ds:
        raise NoDatasourceException(f"No datasource exists for {concept}")
    return ds
