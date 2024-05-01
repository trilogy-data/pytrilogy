from typing import List


from preql.core.models import (
    Concept,
)
from preql.core.processing.nodes import StaticSelectNode
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.models import (
    QueryDatasource,
    Datasource,
    Environment,
)
from preql.utility import unique
from networkx import Graph


def source_loop(
    raw_concept: Concept, datasource: Datasource, g: Graph, fail: bool = False
) -> bool:
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
        if fail:
            raise
        return False
    except nx.exception.NodeNotFound:
        if fail:
            raise
        return False
    # if it's not a two node hop, not a direct select
    if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
        if fail:
            raise SyntaxError
        return False
    for node in path:
        if g.nodes[node]["type"] == "datasource":
            continue
        if g.nodes[node]["concept"].address == raw_concept.address:
            continue
        if fail:
            raise SyntaxError
        return False
    return True


def gen_static_select_node(
    all_concepts: List[Concept],
    environment: Environment,
    g: Graph,
    depth: int,
    fail_on_no_datasource: bool = False,
) -> StaticSelectNode | None:
    for datasource in environment.datasources.values():
        all_found = True
        for raw_concept in unique(all_concepts, "address"):
            all_found = source_loop(raw_concept, datasource, g)
            # break on any failure to find
            if not all_found:
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
    if fail_on_no_datasource:
        datasources = ",\n".join(
            [
                f"{key}:" + ",".join([str(conc) for conc in c.output_concepts])
                for key, c in environment.datasources.items()
            ]
        )

        raise SyntaxError(
            f"Could not find a datasource for static selection for {[c.address for c in all_concepts]} from datasources:\n{datasources}"
        )
    return None
