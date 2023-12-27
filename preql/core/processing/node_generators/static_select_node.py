from typing import List


from preql.core.models import (
    Concept,
)
from preql.core.processing.nodes import StaticSelectNode
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.models import (
    QueryDatasource,
    Environment,
)
from preql.utility import unique


def gen_static_select_node(
    all_concepts: List[Concept], environment: Environment, g, depth
) -> StaticSelectNode | None:
    for datasource in environment.datasources.values():
        all_found = True
        for raw_concept in unique(all_concepts, "address"):
            path = []
            if raw_concept.grain and not raw_concept.grain.components:
                target = concept_to_node(raw_concept.with_default_grain())
            else:
                target = concept_to_node(raw_concept)
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=target,
                )
            except nx.exception.NetworkXNoPath:
                all_found = False
                break
            except nx.exception.NodeNotFound:
                all_found = False
                break
            # if it's not a two node hop, not a direct select
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
            # keep all concepts on the output, until we get to a node which requires reduction
            return StaticSelectNode(
                input_concepts=all_concepts,
                output_concepts=all_concepts,
                environment=environment,
                g=g,
                datasource=QueryDatasource(
                    input_concepts=unique(all_concepts, "address"),
                    output_concepts=unique(all_concepts, "address"),
                    source_map={
                        concept.address: {datasource} for concept in all_concepts
                    },
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
    return None
