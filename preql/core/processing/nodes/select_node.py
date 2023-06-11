from collections import defaultdict
from itertools import combinations
from typing import List, Optional, Set

import networkx as nx

from preql.constants import logger
from preql.core.constants import CONSTANT_DATASET
from preql.core.enums import Purpose
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.models import (
    BaseJoin,
    Datasource,
    Grain,
    QueryDatasource,
    SourceType,
)
from preql.core.processing.utility import (
    PathInfo,
    path_to_joins,
)
from preql.utility import unique
from preql.core.processing.nodes.base_node import StrategyNode

LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


class SelectNode(StrategyNode):
    """Select nodes actually fetch raw data, either
    directly from a table or via joins"""

    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )

    def resolve_joins_pass(self, all_concepts) -> Optional[QueryDatasource]:
        all_input_concepts = [*all_concepts]
        # for key, value in self.environment.datasources.items():
        #     print(key)
        #     print(value.name)

        join_candidates: List[PathInfo] = []
        for datasource in self.environment.datasources.values():
            all_found = True
            paths = {}
            for bitem in all_concepts:
                item = bitem.with_default_grain()

                try:
                    path = nx.shortest_path(
                        self.g,
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
            source_concept = self.g.nodes[value[-1]]["concept"]
            parents.append(source_concept)
            new_joins = path_to_joins(value, g=self.g)

            join_paths += new_joins
            source_map[source_concept.address].add(self.g.nodes[root]["datasource"])
            # ensure we add in all keys required for joins as inputs
            # even if they are not selected out
            for join in new_joins:
                for jconcept in join.concepts:
                    source_map[jconcept.address].add(join.left_datasource)
                    source_map[jconcept.address].add(join.right_datasource)
                    all_input_concepts.append(jconcept)
        final_grain = Grain()
        datasources = sorted(
            [self.g.nodes[key]["datasource"] for key in all_datasets],
            key=lambda x: x.full_name,
        )
        all_outputs = []
        for datasource in datasources:
            final_grain += datasource.grain
            all_outputs += datasource.output_concepts
        output = QueryDatasource(
            output_concepts=unique(all_concepts, "address"),
            input_concepts=unique(all_input_concepts, "address"),
            source_map=source_map,
            grain=final_grain,
            datasources=datasources,
            joins=join_paths,
        )
        return output

    def resolve_join(self) -> QueryDatasource:
        for x in reversed(range(1, len(self.optional_concepts) + 1)):
            for combo in combinations(self.optional_concepts, x):
                all_concepts = self.mandatory_concepts + list(combo)
                required = [c.address for c in all_concepts]
                logger.info(
                    f"{LOGGER_PREFIX} Attempting to resolve joins to reach"
                    f" {','.join(required)}"
                )
                ds = self.resolve_joins_pass(all_concepts)
                if ds:
                    return ds
        required = [c.address for c in self.mandatory_concepts]
        raise ValueError(
            f"Could not find any way to associate required concepts {required}"
        )

    def resolve_from_raw_datasources(self) -> Optional[QueryDatasource]:
        whole_grain = True
        # TODO evaluate above
        if whole_grain:
            valid_matches = ["all"]
        else:
            valid_matches = ["all", "partial"]
        all_concepts = self.mandatory_concepts + self.optional_concepts
        # 2023-03-30 strategy is not currently used
        # but keeping it here for future use
        for strategy in valid_matches[:1]:
            for datasource in self.environment.datasources.values():
                # whole grain determines
                # if we can get a partial grain match
                # such as joining through a table with a PK to get properties
                # sometimes we need a source with all grain keys, in which case we
                # force this not to match

                # if strategy == "partial":
                #     if not datasource.grain.issubset(grain):
                #         continue
                # else:
                #     # either an exact match
                #     # or it's a key on the table
                #     if not datasource.grain == grain:
                #         continue
                all_found = True
                for raw_concept in all_concepts:
                    # look for connection to abstract grain
                    req_concept = raw_concept.with_default_grain()
                    try:
                        path = nx.shortest_path(
                            self.g,
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
                                self.g.nodes[candidate]
                            except KeyError:
                                raise SyntaxError(
                                    "Could not find node for {}".format(candidate)
                                )
                        raise e
                    except nx.exception.NetworkXNoPath:
                        all_found = False
                        break
                    if (
                        len(
                            [p for p in path if self.g.nodes[p]["type"] == "datasource"]
                        )
                        != 1
                    ):
                        all_found = False
                        break
                if all_found:
                    # keep all concepts on the output, until we get to a node which requires reduction
                    return QueryDatasource(
                        input_concepts=unique(all_concepts, "address"),
                        output_concepts=unique(self.all_concepts, "address"),
                        source_map={
                            concept.address: {datasource}
                            for concept in self.all_concepts
                        },
                        datasources=[datasource],
                        grain=datasource.grain,
                        joins=[],
                    )
        return None

    def resolve_from_constant_datasources(self) -> QueryDatasource:
        datasource = Datasource(
            identifier=CONSTANT_DATASET, address=CONSTANT_DATASET, columns=[]
        )
        return QueryDatasource(
            input_concepts=unique(self.all_concepts, "address"),
            output_concepts=unique(self.all_concepts, "address"),
            source_map={concept.address: {datasource} for concept in self.all_concepts},
            datasources=[datasource],
            grain=datasource.grain,
            joins=[],
        )

    def _resolve(self) -> QueryDatasource:
        # if we have parent nodes, treat this as a normal select
        if self.parents:
            return super()._resolve()

        if all([c.purpose == Purpose.CONSTANT for c in self.all_concepts]):
            return self.resolve_from_constant_datasources()
        # otherwise, look if there is a datasource in the graph
        raw = self.resolve_from_raw_datasources()

        if raw:
            return raw
        # if we don't have a select, see if we can join
        return self.resolve_join()
