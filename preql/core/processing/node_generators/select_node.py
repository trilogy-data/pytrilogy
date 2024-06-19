from itertools import combinations
from typing import List, Optional

from preql.core.enums import PurposeLineage
from preql.core.models import Concept, Environment, Grain, LooseConceptList
from preql.core.processing.nodes import (
    StrategyNode,
    SelectNode,
    MergeNode,
    GroupNode,
    ConstantNode,
)
from preql.core.exceptions import NoDatasourceException
import networkx as nx
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.constants import logger
from preql.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_SELECT_NODE]"


def gen_select_node_from_table(
    target_concept: Concept,
    all_concepts: List[Concept],
    g: nx.DiGraph,
    environment: Environment,
    depth: int,
    target_grain: Grain,
    accept_partial: bool = False,
) -> Optional[StrategyNode]:
    # if we have only constants
    # we don't need a table
    # so verify nothing, select node will render
    all_lcl = LooseConceptList(concepts=all_concepts)
    if all([c.derivation == PurposeLineage.CONSTANT for c in all_concepts]):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} All concepts {[x.address for x in all_concepts]} are constants, returning constant node"
        )
        return ConstantNode(
            output_concepts=all_concepts,
            input_concepts=[],
            environment=environment,
            g=g,
            parents=[],
            depth=depth,
            # no partial for constants
            partial_concepts=[],
            force_group=False,
        )
    candidates: dict[str, StrategyNode] = {}
    scores: dict[str, int] = {}
    # otherwise, we need to look for a table
    for datasource in environment.datasources.values():
        all_found = True
        for raw_concept in all_concepts:
            # look for connection to abstract grain
            req_concept = raw_concept.with_default_grain()
            # if we don't have a concept in the graph
            # exit early
            if concept_to_node(req_concept) not in g.nodes:
                raise ValueError(concept_to_node(req_concept))
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except nx.NodeNotFound as e:
                # just to provide better error
                ncandidates = [
                    datasource_to_node(datasource),
                    concept_to_node(req_concept),
                ]
                for ncandidate in ncandidates:
                    try:
                        g.nodes[ncandidate]
                    except KeyError:
                        raise SyntaxError(
                            "Could not find node for {}".format(ncandidate)
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
                if not c.is_complete and c.concept in all_lcl
            ]
            partial_lcl = LooseConceptList(concepts=partial_concepts)
            if not accept_partial and target_concept in partial_lcl:
                continue
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} target grain is {str(target_grain)}"
            )
            if target_grain and target_grain.issubset(datasource.grain):

                if all([x in all_lcl for x in target_grain.components]):
                    force_group = False
                # if we are not returning the grain
                # we have to group
                else:
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} not all grain components are in output {str(all_lcl)}, group to actual grain"
                    )
                    force_group = True
            elif all([x in all_lcl for x in datasource.grain.components]):
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} query output includes all grain components, no reason to group further"
                )
                force_group = False
            else:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} target grain is not subset of datasource grain {datasource.grain}, required to group"
                )
                force_group = True

            bcandidate: StrategyNode = SelectNode(
                input_concepts=[c.concept for c in datasource.columns],
                output_concepts=all_concepts,
                environment=environment,
                g=g,
                parents=[],
                depth=depth,
                partial_concepts=[c for c in all_concepts if c in partial_lcl],
                accept_partial=accept_partial,
                datasource=datasource,
                grain=Grain(components=all_concepts),
            )
            # we need to ntest the group node one further
            if force_group is True:
                candidate: StrategyNode = GroupNode(
                    output_concepts=all_concepts,
                    input_concepts=all_concepts,
                    environment=environment,
                    g=g,
                    parents=[bcandidate],
                    depth=depth,
                    partial_concepts=bcandidate.partial_concepts,
                )
            else:
                candidate = bcandidate
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found select node with {datasource.identifier}, returning {candidate.output_lcl}"
            )
            candidates[datasource.identifier] = candidate
            scores[datasource.identifier] = -len(partial_concepts)
    if not candidates:
        return None
    final = max(candidates, key=lambda x: scores[x])
    return candidates[final]


def gen_select_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    accept_partial: bool = False,
    fail_if_not_found: bool = True,
    accept_partial_optional: bool = True,
    target_grain: Grain | None = None,
) -> StrategyNode | None:
    all_concepts = [concept] + local_optional
    all_lcl = LooseConceptList(concepts=all_concepts)
    materialized_lcl = LooseConceptList(
        concepts=[
            x
            for x in all_concepts
            if x.address in [z.address for z in environment.materialized_concepts]
            or x.derivation == PurposeLineage.CONSTANT
        ]
    )
    if not target_grain:
        target_grain = Grain()
        for ac in all_concepts:
            target_grain += ac.grain
    if materialized_lcl != all_lcl:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Skipping select node generation for {concept.address} "
            f" as it + optional (looking for all {all_lcl}) includes non-materialized concepts {all_lcl.difference(materialized_lcl)} vs materialized: {materialized_lcl}"
        )
        if fail_if_not_found:
            raise NoDatasourceException(f"No datasource exists for {concept}")
        return None

    ds: StrategyNode | None = None

    # attempt to select all concepts from table
    ds = gen_select_node_from_table(
        concept,
        [concept] + local_optional,
        g=g,
        environment=environment,
        depth=depth,
        accept_partial=accept_partial,
        target_grain=target_grain,
    )
    if ds:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Found select node with all target concepts, force group is {ds.force_group}, target grain {target_grain}"
        )
        return ds
    # if we cannot find a match
    parents: List[StrategyNode] = []
    found: List[Concept] = []
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} looking for multiple sources that can satisfy"
    )
    all_found = False
    for x in reversed(range(1, len(local_optional) + 1)):
        if all_found:
            break
        for combo in combinations(local_optional, x):
            if all_found:
                break
            # filter to just the original ones we need to get
            local_combo = [x for x in combo if x not in found]
            # skip if nothing new in this combo
            if not local_combo:
                continue
            # include core concept as join
            all_concepts = [concept, *local_combo]

            ds = gen_select_node_from_table(
                concept,
                all_concepts,
                g=g,
                environment=environment,
                depth=depth + 1,
                accept_partial=accept_partial,
                target_grain=Grain(components=all_concepts),
            )
            if ds:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} found a source with {[x.address for x in all_concepts]}"
                )
                parents.append(ds)
                found += [x for x in ds.output_concepts if x != concept]
                if {x.address for x in found} == {c.address for c in local_optional}:
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found all optional {[c.address for c in local_optional]}"
                    )
                    all_found = True
    if parents and (all_found or accept_partial_optional):
        if all_found:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found all optional {[c.address for c in local_optional]} via joins"
            )
        else:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} found some optional {[c.address for c in found]}, returning"
            )
        all_partial = [
            c
            for c in all_concepts
            if all(
                [c.address in [x.address for x in p.partial_concepts] for p in parents]
            )
        ]
        force_group = False
        for candidate in parents:
            if candidate.grain and not candidate.grain.issubset(target_grain):
                force_group = True
        if len(parents) == 1:
            candidate = parents[0]
        else:
            candidate = MergeNode(
                output_concepts=[concept] + found,
                input_concepts=[concept] + found,
                environment=environment,
                g=g,
                parents=parents,
                depth=depth,
                partial_concepts=all_partial,
                grain=sum([x.grain for x in parents if x.grain], Grain()),
            )
        candidate.depth += 1
        source_grain = candidate.grain
        if force_group:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} datasource grain {source_grain} does not match target grain {target_grain} for select, adding group node"
            )
            return GroupNode(
                output_concepts=candidate.output_concepts,
                input_concepts=candidate.output_concepts,
                environment=environment,
                g=g,
                parents=[candidate],
                depth=depth,
                partial_concepts=candidate.partial_concepts,
            )
        return candidate

    if not accept_partial_optional:
        return None
    ds = gen_select_node_from_table(
        concept,
        [concept],
        g=g,
        environment=environment,
        depth=depth,
        accept_partial=accept_partial,
        target_grain=Grain(components=[concept]),
    )

    if not ds and fail_if_not_found:
        raise NoDatasourceException(f"No datasource exists for {concept}")
    return ds
