from collections import defaultdict
from typing import List, Optional, Union, Set, Dict

import networkx as nx

from preql.constants import logger
from preql.core.enums import Purpose, PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.models import (
    Concept,
    Environment,
    Datasource,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin,
    Function,
    WindowItem,
    FilterItem,
SourceType
)
from preql.core.processing.utility import (
    concept_to_inputs,
    PathInfo,
    path_to_joins,
    concepts_to_conditions_mapping,
    get_nested_source_for_condition,
)
from preql.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL]"

class StrategyNode():

    def __init__(self, mandatory_concepts, optional_concepts,  environment, g, whole_grain:bool=False, parents:List["StrategyNode"]=None):
        self.mandatory_concepts = mandatory_concepts
        self.optional_concepts = optional_concepts
        self.environment = environment
        self.g = g
        self.whole_grain = whole_grain
        self.parents = parents or []

    def __repr__(self):
        concepts = self.mandatory_concepts + self.optional_concepts
        contents = ','.join([c.address for c in concepts])
        return f'{self.__class__.__name__}<{contents}>'

    def resolve(self)->QueryDatasource:
        raise NotImplementedError

class WindowNode(StrategyNode):

        def __init__(self, mandatory_concepts, optional_concepts, environment, g, whole_grain:bool=False, parents:List["StrategyNode"]=None):
            super().__init__(mandatory_concepts, optional_concepts,  environment, g, whole_grain=whole_grain, parents=parents)

        def resolve(self)->QueryDatasource:
            pass

def resolve_concept_map(inputs:List[QueryDatasource]):
    concept_map = defaultdict(set)
    for input in inputs:
        for concept in input.output_concepts:
            concept_map[concept.address].add(input)
    return concept_map
class FilterStrategyNode(StrategyNode):
    def __init__(self, mandatory_concepts, optional_concepts, environment, g, whole_grain: bool = False,
                 parents: List["StrategyNode"] = None):
        super().__init__(mandatory_concepts, optional_concepts, environment, g, whole_grain=whole_grain,
                         parents=parents)

    def resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]
        return QueryDatasource(
            output_concepts = self.mandatory_concepts + self.optional_concepts,
            datasources = parent_sources,
            source_type = SourceType.FILTER,
            source_map = resolve_concept_map(parent_sources),


        )

class GroupNode(StrategyNode):

    def __init__(self, mandatory_concepts, optional_concepts, environment, g, whole_grain: bool = False,
                 parents: List["StrategyNode"] = None):
        super().__init__(mandatory_concepts, optional_concepts, environment, g, whole_grain=whole_grain,
                         parents=parents)

    def resolve(self) -> QueryDatasource:
        pass

class SelectNode(StrategyNode):

    def __init__(self, mandatory_concepts, optional_concepts, environment, g, whole_grain: bool = False,
                 parents: List["StrategyNode"] = None):
        super().__init__(mandatory_concepts, optional_concepts, environment, g, whole_grain=whole_grain,
                         parents=parents)

    def resolve(self) -> QueryDatasource:
        pass

class OutputNode(StrategyNode):

    def __init__(self, mandatory_concepts, optional_concepts, environment, g, whole_grain: bool = False,
                 parents: List["StrategyNode"] = None):
        super().__init__(mandatory_concepts, optional_concepts, environment, g, whole_grain=whole_grain,
                         parents=parents)

    def resolve(self) -> QueryDatasource:
        pass
def resolve_window_parent_concepts(concept:Concept)->List[Concept]:
    return concept_to_inputs(concept)

def resolve_filter_parent_concepts(concept:Concept)->List[Concept]:
    return concept_to_inputs(concept)

def resolve_aggregate_parent_concepts(concept:Concept)->List[Concept]:
    if not isinstance(concept.lineage, Function):
        raise ValueError(f"Concept {concept} is not an aggregate function")
    return concept.lineage.concept_arguments
def source_concepts(mandatory_concepts:List[Concept], optional_concepts:List[Concept], environment:Environment,  g: Optional[ReferenceGraph] = None,)->List[StrategyNode]:
    g = g or generate_graph(environment)
    stack:List[StrategyNode] = []
    all_concepts = mandatory_concepts + optional_concepts

    #TODO
    #Loop through all possible grains + subgrains
    #Starting with the most grain
    for concept in all_concepts:
        if concept.lineage:
            if concept.derivation == PurposeLineage.WINDOW:
                parent_concepts = resolve_window_parent_concepts(concept)
                stack.append(WindowNode([concept], optional_concepts, environment, g, parents=source_concepts(parent_concepts, optional_concepts,
                                                                                                                         environment, g)))
            elif concept.derivation == PurposeLineage.FILTER:
                parent_concepts = resolve_filter_parent_concepts(concept)
                stack.append(FilterStrategyNode([concept], optional_concepts, environment, g, parents=source_concepts(parent_concepts, optional_concepts,
                                                                                                                         environment, g)))
            elif concept.derivation == PurposeLineage.AGGREGATE:
                # aggregates MUST always group to the proper grain
                # todo: is this true?
                parent_concepts = resolve_aggregate_parent_concepts(concept) + optional_concepts
                stack.append(GroupNode([concept], optional_concepts, environment, g, parents=source_concepts(parent_concepts, [],
                                                                                                                         environment, g)))
            elif concept.derivation == PurposeLineage.BASIC:
                # directly select out a basic derivation
                parent_concepts =  resolve_aggregate_parent_concepts(concept)
                stack.append(SelectNode([concept], optional_concepts, environment, g, parents=source_concepts(parent_concepts, [],
                                                                                                                         environment, g)))
            else:
                raise ValueError(f"Unknown lineage type {concept.derivation}")
        else:
            stack.append(SelectNode([concept], optional_concepts, environment, g))
    return stack

def _get_datasource_by_concept_and_grain(
    concept,
    grain: Grain,
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
    whole_grain: bool = False,
) -> Union[Datasource, QueryDatasource]:
    """Determine if it's possible to get a certain concept at a certain grain.
    """
    g = g or generate_graph(environment)
    logger.debug(f"{LOGGER_PREFIX} Starting sub search for {concept} at {grain}")
    if concept.lineage:
        if concept.derivation == PurposeLineage.WINDOW:
            logger.debug(f"{LOGGER_PREFIX} Returning complex window function")
            complex = get_datasource_from_window_function(
                concept, grain, environment, g, whole_grain=whole_grain
            )
        elif concept.derivation == PurposeLineage.FILTER:
            logger.debug(f"{LOGGER_PREFIX} Returning filtration")
            complex = get_datasource_for_filter(
                concept, grain, environment, g, whole_grain=whole_grain
            )
        elif concept.derivation == PurposeLineage.AGGREGATE:
            logger.debug(f"{LOGGER_PREFIX} Checking for complex function derivation")
            try:
                complex = get_datasource_from_complex_lineage(
                    concept, grain, environment, g, whole_grain=whole_grain
                )
            except ValueError:
                complex = None
                logger.debug(
                    f"{LOGGER_PREFIX} Cannot retrieve complex lineage for aggregate {concept}"
                )
        else:
            complex = None
        if complex:
            logger.debug(f"{LOGGER_PREFIX} Returning complex lineage for {concept}")
            return complex
        logger.debug(f"{LOGGER_PREFIX} Can satisfy query with basic lineage")
    # the concept is available directly on a datasource at appropriate grain
    if concept.purpose in (Purpose.KEY, Purpose.PROPERTY):
        try:

            out = get_datasource_from_property_lookup(
                concept.with_default_grain(),
                grain,
                environment,
                g,
                whole_grain=whole_grain,
            )
            logger.debug(f"{LOGGER_PREFIX} Got {concept} from property lookup from {out.name}")
            return out
        except ValueError as e:
            logger.debug(f"{LOGGER_PREFIX} {str(e)}")
    # the concept is available on a datasource, but at a higher granularity
    try:
        out = get_datasource_from_group_select(
            concept, grain, environment, g, whole_grain=whole_grain
        )

        logger.debug(f"{LOGGER_PREFIX} Got {concept} from grouped select from {out.name}")
        return out
    except ValueError as e:
        logger.debug(f"{LOGGER_PREFIX} {str(e)}")
    # the concept and grain together can be gotten via
    # a join from a root dataset to enrichment datasets
    try:
        out = get_datasource_by_joins(
            concept, grain, environment, g, whole_grain=whole_grain
        )
        logger.debug(f"{LOGGER_PREFIX} Got {concept} from joins")
        return out
    except ValueError as e:
        logger.debug(f"{LOGGER_PREFIX} {str(e)}")

    try:
        out = get_property_group_by_without_key(
            concept, grain, environment, g, whole_grain=whole_grain
        )
        logger.debug(
            f"{LOGGER_PREFIX} {concept} from property lookup via transversing key based grain"
        )
        return out
    except ValueError as e:
        logger.debug(f"{LOGGER_PREFIX} failed to get property lookup")

    logger.debug(f"{LOGGER_PREFIX} Full grain search exhausted ")
    if whole_grain:
        raise ValueError(
            f"Cannot find {concept} at {grain}, full grain search exhausted and whole grain is set to true."
        )
    from itertools import combinations

    for x in range(1, len(grain.components_copy)):
        for combo in combinations(grain.components_copy, x):
            logger.debug(f"{LOGGER_PREFIX} looking at reduced grain {grain}")
            ngrain = Grain(components=list(combo))
            try:
                # out = get_datasource_by_joins(
                #     concept.with_grain(ngrain),
                #     ngrain,
                #     environment,
                #     g,
                #     whole_grain=whole_grain,
                # )
                nout = get_datasource_by_concept_and_grain(
                    concept.with_grain(ngrain),
                    ngrain,
                    environment,
                    g,
                    whole_grain=whole_grain,
                )

                logger.debug(
                    f"{LOGGER_PREFIX} Got {concept} from a sub-portion of grain"
                )
                return nout
            except ValueError as e:
                logger.debug(f"{LOGGER_PREFIX} {str(e)}")

    # if there is a property in the grain, see if we can find a datasource
    # with all the keys of the property, which we can then join to

    try:
        neighbors = list(g.predecessors(concept_to_node(concept)))
    # node is not in graph
    except nx.exception.NetworkXError:
        neighbors = []
    raise ValueError(f"No source for {concept} found, neighbors {neighbors}")