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
from preql.core.enums import Purpose, JoinType
from preql.utility import unique
from dataclasses import dataclass


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


def resolve_concept_map(inputs: List[QueryDatasource], targets: Optional[List[Concept]] = None):
    targets = targets or []
    concept_map = defaultdict(set)
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in input.non_partial_concept_addresses:
                continue
            if concept.address not in concept_map:
                concept_map[concept.address].add(input)
    for target in targets:
        if target.lineage and not any(
            target in input.output_concepts for input in inputs
        ):
            # an empty source means it is defined in this CTE
            concept_map[target.address] = set()
    if all([target.address in concept_map for target in targets]):
        return concept_map

    # second loop, include partials
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in concept_map:
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
        depth: int = 0,
    ):
        self.mandatory_concepts = mandatory_concepts
        self.optional_concepts = deepcopy(optional_concepts)
        self.environment = environment
        self.g = g
        self.whole_grain = whole_grain
        self.parents = parents or []
        self.resolution_cache: Optional[QueryDatasource] = None
        self.partial_concepts = partial_concepts or []
        self.depth = depth

    @property
    def logging_prefix(self) -> str:
        return "\t" * self.depth

    @property
    def all_concepts(self) -> list[Concept]:
        return unique(
            deepcopy(self.mandatory_concepts + self.optional_concepts), "address"
        )

    def __repr__(self):
        concepts = self.all_concepts
        contents = ",".join(sorted([c.address for c in concepts]))
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
        source_map = resolve_concept_map(
            parent_sources, unique(self.all_concepts, "address")
        )
        for c in self.all_concepts:
            if c.address not in source_map:
                raise SyntaxError(f"missing EXPORT FOR {c.address}")
        return QueryDatasource(
            input_concepts=unique(input_concepts, "address"),
            output_concepts=unique(self.all_concepts, "address"),
            datasources=parent_sources,
            source_type=self.source_type,
            source_map=source_map,
            joins=[],
            grain=grain,
            condition=conditional,
            partial_concepts=self.partial_concepts,
        )

    def resolve(self) -> QueryDatasource:
        if self.resolution_cache:
            return self.resolution_cache
        qds = self._resolve()
        self.resolution_cache = qds
        return qds


@dataclass
class NodeJoin:
    left_node: StrategyNode
    right_node: StrategyNode
    concepts: List[Concept]
    join_type: JoinType
    filter_to_mutual: bool = False

    def __post_init__(self):
        final_concepts = []
        for concept in self.concepts:
            include = True
            for ds in [self.left_node, self.right_node]:
                if concept.address not in [c.address for c in ds.all_concepts]:
                    if self.filter_to_mutual:
                        include = False
                    else:
                        raise SyntaxError(
                            f"Invalid join, missing {concept} on {str(ds)}, have"
                            f" {[c.address for c in ds.all_concepts]}"
                        )
            if include:
                final_concepts.append(concept)
        if not final_concepts and self.concepts:
            # if one datasource only has constants
            # we can join on 1=1
            for ds in [self.left_node, self.right_node]:
                if all([c.purpose == Purpose.CONSTANT for c in ds.all_concepts]):
                    self.concepts = []
                    return

            left_keys = [c.address for c in self.left_node.all_concepts]
            right_keys = [c.address for c in self.right_node.all_concepts]
            match_concepts = [c.address for c in self.concepts]
            raise SyntaxError(
                "No mutual join keys found between"
                f" {self.left_node} and"
                f" {self.right_node}, left_keys {left_keys},"
                f" right_keys {right_keys},"
                f" provided join concepts {match_concepts}"
            )
        self.concepts = final_concepts

    @property
    def unique_id(self) -> str:
        return str(self.left_node) + str(self.right_node) + self.join_type.value

    def __str__(self):
        return (
            f"{self.join_type.value} JOIN {self.left_node} and"
            f" {self.right_node} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )
