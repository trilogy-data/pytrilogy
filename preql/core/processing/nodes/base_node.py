from typing import List, Optional, Sequence
from collections import defaultdict

from preql.core.models import (
    Grain,
    QueryDatasource,
    SourceType,
    Concept,
    Environment,
    Conditional,
    UnnestJoin,
    Datasource,
    Comparison,
    Parenthetical,
    LooseConceptList,
)
from preql.core.enums import Purpose, JoinType, PurposeLineage, Granularity
from preql.utility import unique
from dataclasses import dataclass


def concept_list_to_grain(
    inputs: List[Concept], parent_sources: Sequence[QueryDatasource | Datasource]
) -> Grain:
    candidates = [
        c
        for c in inputs
        if c.purpose == Purpose.KEY and c.granularity != Granularity.SINGLE_ROW
    ]
    for x in inputs:
        if x.granularity == Granularity.SINGLE_ROW:
            continue
        if x.purpose == Purpose.PROPERTY and not any(
            [key in candidates for key in (x.keys or [])]
        ):
            candidates.append(x)
        elif x.purpose == Purpose.CONSTANT:
            candidates.append(x)
        elif x.purpose == Purpose.METRIC:
            # metrics that were previously calculated must be included in grain
            if any([x in parent.output_concepts for parent in parent_sources]):
                candidates.append(x)

    return Grain(components=candidates)


def resolve_concept_map(
    inputs: List[QueryDatasource],
    targets: List[Concept],
    inherited_inputs: List[Concept],
    full_joins: List[Concept] | None = None,
) -> dict[str, set[Datasource | QueryDatasource | UnnestJoin]]:
    targets = targets or []
    concept_map: dict[str, set[Datasource | QueryDatasource | UnnestJoin]] = (
        defaultdict(set)
    )
    full_addresses = {c.address for c in full_joins} if full_joins else set()
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in input.non_partial_concept_addresses:
                continue
            if concept.address not in [t.address for t in inherited_inputs]:
                continue
            if concept.address in full_addresses:
                concept_map[concept.address].add(input)
            elif concept.address not in concept_map:
                concept_map[concept.address].add(input)

    # second loop, include partials
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in [t.address for t in inherited_inputs]:
                continue
            if len(concept_map.get(concept.address, [])) == 0:
                concept_map[concept.address].add(input)
    # this adds our new derived metrics, which are not created in this CTE
    for target in targets:
        if target not in inherited_inputs:
            # an empty source means it is defined in this CTE
            concept_map[target.address] = set()
    return concept_map


def get_all_parent_partial(all_concepts: List[Concept], parents: List["StrategyNode"]):
    return [
        c
        for c in all_concepts
        if len([c.address in [x.address for x in p.partial_concepts] for p in parents])
        >= 1
        and all([c.address in [x.address for x in p.partial_concepts] for p in parents])
    ]


class StrategyNode:
    source_type = SourceType.ABSTRACT

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment: Environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        partial_concepts: List[Concept] | None = None,
        depth: int = 0,
        conditions: Conditional | Comparison | Parenthetical | None = None,
        force_group: bool | None = None,
        grain: Optional[Grain] = None,
    ):
        self.input_concepts: List[Concept] = (
            unique(input_concepts, "address") if input_concepts else []
        )
        self.input_lcl = LooseConceptList(concepts=self.input_concepts)
        self.output_concepts: List[Concept] = unique(output_concepts, "address")
        self.output_lcl = LooseConceptList(concepts=self.output_concepts)

        self.environment = environment
        self.g = g
        self.whole_grain = whole_grain
        self.parents = parents or []
        self.resolution_cache: Optional[QueryDatasource] = None
        self.partial_concepts = partial_concepts or get_all_parent_partial(
            self.output_concepts, self.parents
        )
        self.partial_lcl = LooseConceptList(concepts=self.partial_concepts)
        self.depth = depth
        self.conditions = conditions
        self.grain = grain
        self.force_group = force_group
        self.tainted = False
        for parent in self.parents:
            if not parent:
                raise SyntaxError("Unresolvable parent")

    def add_output_concept(self, concept: Concept):
        self.output_concepts.append(concept)
        self.output_lcl = LooseConceptList(concepts=self.output_concepts)
        self.rebuild_cache()

    @property
    def logging_prefix(self) -> str:
        return "\t" * self.depth

    @property
    def all_concepts(self) -> list[Concept]:
        return [*self.output_concepts]

    @property
    def all_used_concepts(self) -> list[Concept]:
        return [*self.input_concepts]

    def __repr__(self):
        concepts = self.all_concepts
        contents = ",".join(sorted([c.address for c in concepts]))
        return f"{self.__class__.__name__}<{contents}>"

    def _resolve(self) -> QueryDatasource:
        parent_sources = [p.resolve() for p in self.parents]

        # if conditional:
        #     for condition in conditions[1:]:
        #         conditional += condition
        grain = Grain(components=self.output_concepts)
        source_map = resolve_concept_map(
            parent_sources, self.output_concepts, self.input_concepts
        )
        return QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=self.output_concepts,
            datasources=parent_sources,
            source_type=self.source_type,
            source_map=source_map,
            joins=[],
            grain=grain,
            condition=self.conditions,
            partial_concepts=self.partial_concepts,
        )

    def rebuild_cache(self) -> QueryDatasource:
        self.tainted = True
        if not self.resolution_cache:
            return self.resolve()
        self.resolution_cache = None
        return self.resolve()

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
                if all(
                    [c.derivation == PurposeLineage.CONSTANT for c in ds.all_concepts]
                ):
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
        nodes = sorted([self.left_node, self.right_node], key=lambda x: str(x))
        return str(nodes) + self.join_type.value

    def __str__(self):
        return (
            f"{self.join_type.value} JOIN {self.left_node} and"
            f" {self.right_node} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )
