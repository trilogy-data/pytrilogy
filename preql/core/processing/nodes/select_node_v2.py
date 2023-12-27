from typing import List, Optional

import networkx as nx

from preql.constants import logger
from preql.core.constants import CONSTANT_DATASET
from preql.core.enums import Purpose
from preql.core.graph_models import concept_to_node, datasource_to_node
from preql.core.models import (
    Datasource,
    QueryDatasource,
    SourceType,
    Environment,
    Concept,
)
from preql.utility import unique
from preql.core.processing.nodes.base_node import StrategyNode
from preql.core.exceptions import NoDatasourceException


LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


class StaticSelectNode(StrategyNode):
    """Static select nodes."""

    source_type = SourceType.SELECT

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment: Environment,
        g,
        datasource: QueryDatasource,
        depth: int = 0,
        partial_concepts: List[Concept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            whole_grain=True,
            parents=[],
            depth=depth,
            partial_concepts=partial_concepts,
        )
        self.datasource = datasource

    def _resolve(self):
        return self.datasource


class SelectNode(StrategyNode):
    """Select nodes actually fetch raw data from a table
    Responsible for selecting the cheapest option from which to select.
    """

    source_type = SourceType.SELECT

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment: Environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: List[Concept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
        )

    def resolve_from_raw_datasources(self, all_concepts) -> Optional[QueryDatasource]:
        for datasource in self.environment.datasources.values():
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
                # 2023-10-18 - more strict condition then below
                # if len(path) != 2:
                #     all_found = False
                #     break
                if (
                    len([p for p in path if self.g.nodes[p]["type"] == "datasource"])
                    != 1
                ):
                    all_found = False
                    break
            if all_found:
                # partial_concepts = [c.concept for c in datasource.columns if not c.is_complete]
                # if any([c not in partial_concepts for c in all_concepts]):
                #     logger.info(
                #     f"{self.logging_prefix}{LOGGER_PREFIX} skipp direct select from {datasource.address} for due to partial concepts {[c.address for c in partial_concepts]}"
                # )
                #     continue
                # keep all concepts on the output, until we get to a node which requires reduction
                logger.info(
                    f"{self.logging_prefix}{LOGGER_PREFIX} found direct select from {datasource.address} for {[c.address for c in all_concepts]}"
                )
                return QueryDatasource(
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
                )
        return None

    def resolve_from_constant_datasources(self) -> QueryDatasource:
        datasource = Datasource(
            identifier=CONSTANT_DATASET, address=CONSTANT_DATASET, columns=[]
        )
        return QueryDatasource(
            input_concepts=[],
            output_concepts=unique(self.all_concepts, "address"),
            source_map={concept.address: set() for concept in self.all_concepts},
            datasources=[datasource],
            grain=datasource.grain,
            joins=[],
            partial_concepts=[],
        )

    def _resolve(self) -> QueryDatasource:
        # if we have parent nodes, we do not need to go to a datasource
        if self.parents:
            return super()._resolve()

        if all([c.purpose == Purpose.CONSTANT for c in self.all_concepts]):
            return self.resolve_from_constant_datasources()
        resolution = self.resolve_from_raw_datasources(self.all_concepts)
        if resolution:
            return resolution
        required = [c.address for c in self.all_concepts]
        raise NoDatasourceException(
            f"Could not find any way to associate required concepts {required}"
        )


class ConstantNode(SelectNode):
    """Represents a constant value."""

    pass
