from copy import deepcopy
from typing import List, Optional
from collections import defaultdict

from preql.core.models import (
    FilterItem,
    Grain,
    QueryDatasource,
    SourceType,
    Concept,
    Environment,
)
from preql.core.enums import Purpose
from preql.utility import unique


def concept_list_to_grain(
    inputs: List[Concept], parent_sources: List[QueryDatasource]
) -> Grain:
    candidates = [c for c in inputs if c.purpose == Purpose.KEY]
    for x in inputs:
        if x.purpose == Purpose.PROPERTY and not any(
            [key in candidates for key in (x.keys or [])]
        ):
            candidates.append(x)
        # TODO: figure out how to avoid this?
        elif x.purpose == Purpose.CONSTANT:
            candidates.append(x)
        elif x.purpose == Purpose.METRIC:
            # metrics that were previously calculated must be included in grain
            if any([x in parent.output_concepts for parent in parent_sources]):
                candidates.append(x)

    return Grain(components=candidates)


def resolve_concept_map(inputs: List[QueryDatasource]):
    concept_map = defaultdict(set)
    for input in inputs:
        for concept in input.output_concepts:
            concept_map[concept.address].add(input)
    return concept_map


class StrategyNode:
    source_type = SourceType.SELECT

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment: Environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        partial_concepts: List[Concept] | None = None,
    ):
        self.mandatory_concepts = mandatory_concepts
        self.optional_concepts = deepcopy(optional_concepts)
        self.environment = environment
        self.g = g
        self.whole_grain = whole_grain
        self.parents = parents or []
        self.resolution_cache: Optional[QueryDatasource] = None
        self.partial_concepts = partial_concepts or []

    @property
    def all_concepts(self):
        return unique(
            deepcopy(self.mandatory_concepts + self.optional_concepts), "address"
        )

    def __repr__(self):
        concepts = self.all_concepts
        contents = ",".join([c.address for c in concepts])
        return f"{self.__class__.__name__}<{contents}>"

    def _resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]
        input_concepts = []
        for p in parent_sources:
            input_concepts += p.output_concepts
        conditions = [
            c.lineage.where.conditional
            for c in self.mandatory_concepts
            if isinstance(c.lineage, FilterItem)
        ]
        conditional = conditions[0] if conditions else None
        if conditional:
            for condition in conditions[1:]:
                conditional += condition
        grain = Grain()
        output_concepts = self.all_concepts
        for source in parent_sources:
            grain += source.grain
            output_concepts += source.grain.components_copy
        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=unique(self.all_concepts, "address"),
            datasources=parent_sources,
            source_type=self.source_type,
            source_map=resolve_concept_map(parent_sources),
            joins=[],
            grain=grain,
            condition=conditional,
        )

    def resolve(self) -> QueryDatasource:
        if self.resolution_cache:
            return self.resolution_cache
        qds = self._resolve()
        self.resolution_cache = qds
        return qds
