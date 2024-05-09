from typing import List, Optional

from preql.core.models import (
    Concept,
    Environment,
)
from preql.core.processing.nodes import (
    MergeNode,
)
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.processing.utility import PathInfo
from preql.constants import logger
from preql.utility import unique

LOGGER_PREFIX = "[GEN_MERGE_NODE]"

pad = lambda x: x*'\t'

def gen_merge_node(
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    source_concepts,
) -> Optional[MergeNode]:
    join_candidates: List[PathInfo] = []
    # anchor on datasources
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
                all_found = False

                continue
            except nx.exception.NetworkXNoPath:
                all_found = False
                continue
        if all_found:
            partial = [
                c.concept
                for c in datasource.columns
                if not c.is_complete
                and c.concept.address in [x.address for x in all_concepts]
            ]
            if partial:
                continue
            join_candidates.append({"paths": paths, "datasource": datasource})
    join_candidates.sort(key=lambda x: sum([len(v) for v in x["paths"].values()]))
    if not join_candidates:
        return None
    for join_candidate in join_candidates:
        logger.info(f"{pad(depth)}{LOGGER_PREFIX} Join candidate: {join_candidate['paths']}")
    shortest: PathInfo = join_candidates[0]
    concept_nodes: List[Concept] = []
    # along our path, find all the concepts required
    for key, value in shortest["paths"].items():
        concept_nodes += [g.nodes[v]["concept"] for v in value if v.startswith("c~")]
    final:List[Concept] = unique(concept_nodes, "address")
    logger.info([str(c) for c in final])
    if final == all_concepts:
        # no point in recursing
        # if we could not find an answer
        logger.info(f"{pad(depth)}{LOGGER_PREFIX} No additional join candidates could be found")
        return None
    new = {c.address for c in final}.difference({c.address for c in all_concepts})
    logger.info(f"{pad(depth)}{LOGGER_PREFIX} sourcing with new concepts {new}")
    return source_concepts(
        mandatory_list=unique(concept_nodes, "address"),
        environment=environment,
        g=g,
        depth=depth + 1,
    )
