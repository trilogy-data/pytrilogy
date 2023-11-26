# from collections import defaultdict
# from itertools import combinations
# from typing import List, Optional, Set

# import networkx as nx

# from preql.constants import logger
# from preql.core.constants import CONSTANT_DATASET
# from preql.core.enums import Purpose
# from preql.core.graph_models import concept_to_node, datasource_to_node
# from preql.core.models import (
#     BaseJoin,
#     Datasource,
#     Grain,
#     QueryDatasource,
#     SourceType,
#     Environment,
# )
# from preql.core.processing.utility import (
#     PathInfo,
#     path_to_joins,
# )
# from preql.utility import unique
# from preql.core.processing.nodes.base_node import StrategyNode

# LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


# class StaticSelectNode(StrategyNode):
#     """Direct select from a table"""
#     source_type = SourceType.SELECT

#     def __init__(
#         self,
#         mandatory_concepts,
#         optional_concepts,
#         environment: Environment,
#         g,
#         datasource: QueryDatasource,
#         depth: int = 0,
#     ):
#         super().__init__(
#             mandatory_concepts,
#             optional_concepts,
#             environment,
#             g,
#             whole_grain=True,
#             parents=[],
#             depth=depth,
#         )
#         self.datasource = datasource

#     def _resolve(self):
#         return self.datasource


# class SelectNode(StrategyNode):
#     """Select nodes actually fetch raw data, either
#     directly from a table or via joins"""

#     source_type = SourceType.SELECT

#     def __init__(
#         self,
#         mandatory_concepts,
#         optional_concepts,
#         environment: Environment,
#         g,
#         whole_grain: bool = False,
#         parents: List["StrategyNode"] | None = None,
#         depth: int = 0,
#     ):
#         super().__init__(
#             mandatory_concepts,
#             optional_concepts,
#             environment,
#             g,
#             whole_grain=whole_grain,
#             parents=parents,
#             depth=depth,
#         )

#     def resolve_joins_pass(self, all_concepts) -> Optional[QueryDatasource]:
#         all_input_concepts = [*all_concepts]

#         join_candidates: List[PathInfo] = []
#         for datasource in self.environment.datasources.values():
#             all_found = True
#             paths = {}
#             for bitem in all_concepts:
#                 item = bitem.with_default_grain()

#                 try:
#                     path = nx.shortest_path(
#                         self.g,
#                         source=datasource_to_node(datasource),
#                         target=concept_to_node(item),
#                     )
#                     paths[concept_to_node(item)] = path
#                 except nx.exception.NodeNotFound:
#                     # TODO: support Verbose logging mode configuration and reenable these
#                     # logger.debug(f'{LOGGER_PREFIX} could not find node for {item.address}')
#                     all_found = False

#                     continue
#                 except nx.exception.NetworkXNoPath:
#                     # logger.debug(f'{LOGGER_PREFIX} could not get to {item.address} at {item.grain} from {datasource}')
#                     all_found = False
#                     continue
#             if all_found:
#                 join_candidates.append({"paths": paths, "datasource": datasource})
#         join_candidates.sort(key=lambda x: sum([len(v) for v in x["paths"].values()]))
#         if not join_candidates:
#             return None
#         shortest: PathInfo = join_candidates[0]
#         source_map = defaultdict(set)
#         join_paths: List[BaseJoin] = []
#         parents = []
#         all_datasets: Set = set()
#         all_search_concepts: Set = set()
#         for key, value in shortest["paths"].items():
#             datasource_nodes = [v for v in value if v.startswith("ds~")]
#             concept_nodes = [v for v in value if v.startswith("c~")]
#             all_datasets = all_datasets.union(set(datasource_nodes))
#             all_search_concepts = all_search_concepts.union(set(concept_nodes))
#             root = datasource_nodes[-1]
#             source_concept = self.g.nodes[value[-1]]["concept"]
#             parents.append(source_concept)
#             new_joins = path_to_joins(value, g=self.g)

#             join_paths += new_joins
#             source_map[source_concept.address].add(self.g.nodes[root]["datasource"])
#             # if source_concept.with_grain(self.g.nodes[root]["datasource"].grain) not in self.g.nodes[root]["datasource"].output_concepts:
#             #     raise NotImplementedError(f"need to inject in dynamic discovery node to support getting {source_concept.address} from {root}")
#             # # ensure we add in all keys required for joins as inputs
#             # even if they are not selected out
#             for join in new_joins:
#                 for jconcept in join.concepts:
#                     source_map[jconcept.address].add(join.left_datasource)
#                     source_map[jconcept.address].add(join.right_datasource)
#                     all_input_concepts.append(jconcept)
#         final_grain = Grain()
#         datasources = sorted(
#             [self.g.nodes[key]["datasource"] for key in all_datasets],
#             key=lambda x: x.full_name,
#         )
#         all_outputs = []
#         for datasource in datasources:
#             final_grain += datasource.grain
#             all_outputs += datasource.output_concepts
#         output = QueryDatasource(
#             output_concepts=unique(all_concepts, "address"),
#             input_concepts=unique(all_input_concepts, "address"),
#             source_map=source_map,
#             grain=final_grain,
#             datasources=datasources,
#             joins=join_paths,
#         )
#         logger.info(
#             f"{self.logging_prefix}{LOGGER_PREFIX} Found joined source from {[d.address for d in datasources]}"
#         )
#         return output

#     # def resolve_join(self) -> Optional[QueryDatasource]:
#     #     for x in reversed(range(1, len(self.optional_concepts) + 1)):
#     #         for combo in combinations(self.optional_concepts, x):
#     #             all_concepts = self.mandatory_concepts + list(combo)
#     #             required = [c.address for c in all_concepts]
#     #             logger.info(
#     #                 f"{self.logging_prefix}{LOGGER_PREFIX} Attempting to resolve joins to reach"
#     #                 f" {','.join(required)}"
#     #             )
#     #             ds = self.resolve_joins_pass(all_concepts)
#     #             if ds:
#     #                 return ds

#     #     ds = self.resolve_joins_pass(self.mandatory_concepts)
#     #     if ds:
#     #         return ds

#     def resolve_as_many_as_possible(self) -> Optional[QueryDatasource]:
#         ds = None
#         for x in reversed(range(1, len(self.optional_concepts) + 1)):
#             for combo in combinations(self.optional_concepts, x):
#                 all_concepts = self.mandatory_concepts + list(combo)
#                 required = [c.address for c in all_concepts]
#                 logger.info(
#                     f"{self.logging_prefix}{LOGGER_PREFIX} Attempting to resolve select to reach"
#                     f" {','.join(required)}, have {len(self.optional_concepts)} optional concept, pass {x}"
#                 )
#                 ds = self.resolve_from_raw_datasources(all_concepts)
#                 if ds:
#                     return ds
#                 joins = self.resolve_joins_pass(all_concepts)
#                 if joins:
#                     return joins
#         ds = self.resolve_from_raw_datasources(self.mandatory_concepts)

#         if ds:
#             return ds
#         joins = self.resolve_joins_pass(self.mandatory_concepts)
#         if joins:
#             return joins
#         return None

#     def resolve_from_raw_datasources(self, all_concepts) -> Optional[QueryDatasource]:
#         for datasource in self.environment.datasources.values():
#             all_found = True
#             for raw_concept in all_concepts:
#                 # look for connection to abstract grain
#                 req_concept = raw_concept.with_default_grain()
#                 try:
#                     path = nx.shortest_path(
#                         self.g,
#                         source=datasource_to_node(datasource),
#                         target=concept_to_node(req_concept),
#                     )
#                 except nx.NodeNotFound as e:
#                     candidates = [
#                         datasource_to_node(datasource),
#                         concept_to_node(req_concept),
#                     ]
#                     for candidate in candidates:
#                         try:
#                             self.g.nodes[candidate]
#                         except KeyError:
#                             raise SyntaxError(
#                                 "Could not find node for {}".format(candidate)
#                             )
#                     raise e
#                 except nx.exception.NetworkXNoPath:
#                     all_found = False
#                     break
#                 # 2023-10-18 - more strict condition then below
#                 if len(path) != 2:
#                     all_found = False
#                     break
#                 if (
#                     len([p for p in path if self.g.nodes[p]["type"] == "datasource"])
#                     != 1
#                 ):
#                     all_found = False
#                     break
#             if all_found:
#                 # keep all concepts on the output, until we get to a node which requires reduction
#                 logger.info(
#                     f"{self.logging_prefix}{LOGGER_PREFIX} found direct select from {datasource.address}"
#                 )

#                 return QueryDatasource(
#                     input_concepts=unique(all_concepts, "address"),
#                     output_concepts=unique(all_concepts, "address"),
#                     source_map={
#                         concept.address: {datasource} for concept in all_concepts
#                     },
#                     datasources=[datasource],
#                     grain=datasource.grain,
#                     joins=[],
#                 )
#         return None

#     def resolve_from_constant_datasources(self) -> QueryDatasource:
#         datasource = Datasource(
#             identifier=CONSTANT_DATASET, address=CONSTANT_DATASET, columns=[]
#         )
#         return QueryDatasource(
#             input_concepts=unique(self.all_concepts, "address"),
#             output_concepts=unique(self.all_concepts, "address"),
#             source_map={concept.address: {datasource} for concept in self.all_concepts},
#             datasources=[datasource],
#             grain=datasource.grain,
#             joins=[],
#         )

#     def _resolve(self) -> QueryDatasource:
#         # if we have parent nodes, treat this as a normal select
#         if self.parents:
#             return super()._resolve()

#         if all([c.purpose == Purpose.CONSTANT for c in self.all_concepts]):
#             return self.resolve_from_constant_datasources()
#         # otherwise, look if there is a datasource in the graph
#         # raw = self.resolve_raw_select()
#         # # raw = self.resolve_direct_select()
#         # if raw:
#         #     return raw
#         # if we don't have a select, see if we can join
#         # but don't do transformations, as we should get a separate select for this
#         # if all([x.lineage is None for x in self.all_concepts]):
#         # joins = self.resolve_join()
#         # if joins:
#         #     return joins
#         raw = self.resolve_as_many_as_possible()
#         if raw:
#             return raw
#         required = [c.address for c in self.mandatory_concepts]
#         raise ValueError(
#             f"Could not find any way to associate required concepts {required}"
#         )
