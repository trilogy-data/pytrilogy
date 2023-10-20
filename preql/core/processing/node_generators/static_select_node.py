from typing import List


from preql.core.models import (
    Concept,
)
from preql.core.processing.nodes import StaticSelectNode
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.models import (
    QueryDatasource,
)
from preql.utility import unique


def gen_static_select_node(
    all_concepts: List[Concept], environment, g, depth
) -> StaticSelectNode | None:
    for datasource in environment.datasources.values():
        all_found = True
        for raw_concept in all_concepts:
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
            # if it's not a two node hop, not a direct select
            if len(path) != 2:
                all_found = False
                break
        if all_found:
            # keep all concepts on the output, until we get to a node which requires reduction
            return StaticSelectNode(
                mandatory_concepts=all_concepts,
                optional_concepts=[],
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
                ),
                depth=depth,
            )
    return None
