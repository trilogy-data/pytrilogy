from typing import List, Optional

from trilogy.core.models import Concept, Environment, Datasource, Conditional
from trilogy.core.processing.nodes import MergeNode, History
import networkx as nx
from trilogy.core.graph_models import concept_to_node, datasource_to_node
from trilogy.core.processing.utility import PathInfo
from trilogy.constants import logger
from trilogy.utility import unique
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.processing.utility import padding
from trilogy.core.processing.graph_utils import extract_mandatory_subgraphs
from trilogy.core.enums import PurposeLineage

LOGGER_PREFIX = "[GEN_MERGE_NODE]"


def reduce_path_concepts(paths, g) -> set[str]:
    concept_nodes: List[Concept] = []
    # along our path, find all the concepts required
    for _, value in paths.items():
        concept_nodes += [g.nodes[v]["concept"] for v in value if v.startswith("c~")]
    final: List[Concept] = unique(concept_nodes, "address")
    return set([x.address for x in final])


def identify_ds_join_paths(
    all_concepts: List[Concept],
    g,
    datasource: Datasource,
    accept_partial: bool = False,
    fail: bool = False,
) -> PathInfo | None:
    all_found = True
    any_direct_found = False
    paths = {}
    for bitem in all_concepts:
        item = bitem.with_default_grain()
        target_node = concept_to_node(item)
        try:
            path = nx.shortest_path(
                g,
                source=datasource_to_node(datasource),
                target=target_node,
            )
            paths[target_node] = path
            if sum([1 for x in path if x.startswith("ds~")]) == 1:
                any_direct_found = True
        except nx.exception.NodeNotFound:
            # TODO: support Verbose logging mode configuration and reenable these
            all_found = False
            if fail:
                raise
            return None
        except nx.exception.NetworkXNoPath:
            all_found = False
            if fail:
                raise
            return None
    if all_found and any_direct_found:
        partial = [
            c.concept
            for c in datasource.columns
            if not c.is_complete
            and c.concept.address in [x.address for x in all_concepts]
        ]
        if partial and not accept_partial:
            return None

        return PathInfo(
            paths=paths,
            datasource=datasource,
            reduced_concepts=reduce_path_concepts(paths, g),
            concept_subgraphs=extract_mandatory_subgraphs(paths, g),
        )
    return None


def gen_merge_node(
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    source_concepts,
    accept_partial: bool = False,
    history: History | None = None,
    conditions: Conditional | None = None,
) -> Optional[MergeNode]:
    join_candidates: List[PathInfo] = []
    # anchor on datasources
    final_all_concepts = []
    for x in all_concepts:
        final_all_concepts.append(x)
    for datasource in environment.datasources.values():
        path = identify_ds_join_paths(final_all_concepts, g, datasource, accept_partial)
        if path and path.reduced_concepts:
            join_candidates.append(path)
    join_candidates.sort(key=lambda x: sum([len(v) for v in x.paths.values()]))
    if not join_candidates:
        return None
    join_additions: list[set[str]] = []
    for candidate in join_candidates:
        join_additions.append(candidate.reduced_concepts)

    common: set[str] = set()
    final_candidates: list[set[str]] = []
    # find all values that show up in every join_additions
    for ja in join_additions:
        if not common:
            common = ja
        else:
            common = common.intersection(ja)
        if all(ja.issubset(y) for y in join_additions):
            final_candidates.append(ja)

    if not final_candidates:
        filtered_paths = [x.difference(common) for x in join_additions]
        raise AmbiguousRelationshipResolutionException(
            f"Ambiguous concept join resolution fetching {[x.address for x in all_concepts]} - unique values in possible paths = {filtered_paths}. Include an additional concept to disambiguate",
            join_additions,
        )
    if not join_candidates:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} No additional join candidates could be found"
        )
        return None
    shortest: PathInfo = sorted(
        [x for x in join_candidates if x.reduced_concepts in final_candidates],
        key=lambda x: len(x.reduced_concepts),
    )[0]
    logger.info(f"{padding(depth)}{LOGGER_PREFIX} final path is {shortest.paths}")
    # logger.info(f'{padding(depth)}{LOGGER_PREFIX} final reduced concepts are {shortest.concs}')
    parents = []
    for graph in shortest.concept_subgraphs:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching subgraph {[c.address for c in graph]}"
        )
        parent = source_concepts(
            mandatory_list=graph,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Unable to instantiate target subgraph"
            )
            return None
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} finished subgraph fetch for {[c.address for c in graph]}, have parent {type(parent)}"
        )
        parents.append(parent)

    return MergeNode(
        input_concepts=[
            environment.concepts[x]
            for x in shortest.reduced_concepts
            if environment.concepts[x].derivation != PurposeLineage.MERGE
        ],
        output_concepts=[
            x for x in all_concepts if x.derivation != PurposeLineage.MERGE
        ],
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
        conditions=conditions,
    )
