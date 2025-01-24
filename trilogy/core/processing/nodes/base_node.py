from collections import defaultdict
from dataclasses import dataclass
from typing import List, Optional

from trilogy.core.enums import (
    BooleanOperator,
    Derivation,
    JoinType,
    SourceType,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildGrain,
    BuildOrderBy,
    BuildParenthetical,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import ConceptPair, QueryDatasource, UnnestJoin
from trilogy.utility import unique


def resolve_concept_map(
    inputs: List[QueryDatasource | BuildDatasource],
    targets: List[BuildConcept],
    inherited_inputs: List[BuildConcept],
    full_joins: List[BuildConcept] | None = None,
) -> dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]]:
    targets = targets or []
    concept_map: dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]] = (
        defaultdict(set)
    )
    full_addresses = {c.address for c in full_joins} if full_joins else set()
    inherited = set([t.address for t in inherited_inputs])
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in input.full_concepts:
                continue
            if (
                isinstance(input, QueryDatasource)
                and concept.address in input.hidden_concepts
            ):
                continue
            if concept.address in full_addresses:
                concept_map[concept.address].add(input)
            elif concept.address not in concept_map:
                # equi_targets = [x for x in targets if concept.address in x.pseudonyms or x.address in concept.pseudonyms]
                # if equi_targets:
                #     for equi in equi_targets:
                #         concept_map[equi.address] = set()
                concept_map[concept.address].add(input)

    # second loop, include partials
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in [t for t in inherited_inputs]:
                continue
            if (
                isinstance(input, QueryDatasource)
                and concept.address in input.hidden_concepts
            ):
                continue
            if len(concept_map.get(concept.address, [])) == 0:
                concept_map[concept.address].add(input)
    # this adds our new derived metrics, which are not created in this CTE
    for target in targets:
        if target.address not in inherited:
            # an empty source means it is defined in this CTE
            concept_map[target.address] = set()

    return concept_map


def get_all_parent_partial(
    all_concepts: List[BuildConcept], parents: List["StrategyNode"]
) -> List[BuildConcept]:
    return unique(
        [
            c
            for c in all_concepts
            if len(
                [
                    p
                    for p in parents
                    if c.address in [x.address for x in p.partial_concepts]
                ]
            )
            >= 1
            and all(
                [
                    c.address in p.partial_lcl
                    for p in parents
                    if c.address in p.output_lcl
                ]
            )
        ],
        "address",
    )


def get_all_parent_nullable(
    all_concepts: List[BuildConcept], parents: List["StrategyNode"]
) -> List[BuildConcept]:
    return unique(
        [
            c
            for c in all_concepts
            if len(
                [
                    p
                    for p in parents
                    if c.address in [x.address for x in p.nullable_concepts]
                ]
            )
            >= 1
        ],
        "address",
    )


class StrategyNode:
    source_type = SourceType.ABSTRACT

    def __init__(
        self,
        input_concepts: List[BuildConcept],
        output_concepts: List[BuildConcept],
        environment: BuildEnvironment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        partial_concepts: List[BuildConcept] | None = None,
        nullable_concepts: List[BuildConcept] | None = None,
        depth: int = 0,
        conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        preexisting_conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        force_group: bool | None = None,
        grain: Optional[BuildGrain] = None,
        hidden_concepts: set[str] | None = None,
        existence_concepts: List[BuildConcept] | None = None,
        virtual_output_concepts: List[BuildConcept] | None = None,
        ordering: BuildOrderBy | None = None,
    ):
        self.input_concepts: List[BuildConcept] = (
            unique(input_concepts, "address") if input_concepts else []
        )
        self.input_lcl = LooseBuildConceptList(concepts=self.input_concepts)
        self.output_concepts: List[BuildConcept] = unique(output_concepts, "address")
        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)

        self.environment = environment
        self.whole_grain = whole_grain
        self.parents = parents or []
        self.resolution_cache: Optional[QueryDatasource] = None
        self.partial_concepts = partial_concepts or get_all_parent_partial(
            self.output_concepts, self.parents
        )
        self.nullable_concepts = nullable_concepts or get_all_parent_nullable(
            self.output_concepts, self.parents
        )
        self.ordering = ordering
        self.depth = depth
        self.conditions = conditions
        self.grain = grain
        self.force_group = force_group
        self.tainted = False
        self.hidden_concepts = hidden_concepts or set()
        self.existence_concepts = existence_concepts or []
        self.virtual_output_concepts = virtual_output_concepts or []
        self.preexisting_conditions = preexisting_conditions
        if self.conditions and not self.preexisting_conditions:
            self.preexisting_conditions = self.conditions
        elif (
            self.conditions
            and self.preexisting_conditions
            and self.conditions != self.preexisting_conditions
        ):
            self.preexisting_conditions = BuildConditional(
                left=self.conditions,
                right=self.preexisting_conditions,
                operator=BooleanOperator.AND,
            )
        self.validate_parents()
        self.validate_inputs()
        self.log = True

    def validate_inputs(self):
        if not self.parents:
            return
        non_hidden = set()
        for x in self.parents:
            for z in x.usable_outputs:
                non_hidden.add(z.address)
                for psd in z.pseudonyms:
                    non_hidden.add(psd)
        if not all([x.address in non_hidden for x in self.input_concepts]):
            missing = [x for x in self.input_concepts if x.address not in non_hidden]
            raise ValueError(
                f"Invalid input concepts; {missing} are missing non-hidden parent nodes"
            )

    def add_parents(self, parents: list["StrategyNode"]):
        self.parents += parents
        self.validate_parents()
        return self

    def set_preexisting_conditions(
        self, conditions: BuildConditional | BuildComparison | BuildParenthetical
    ):
        self.preexisting_conditions = conditions
        return self

    def add_condition(
        self, condition: BuildConditional | BuildComparison | BuildParenthetical
    ):
        if self.conditions and condition == self.conditions:
            return self
        if self.conditions:
            self.conditions = BuildConditional(
                left=self.conditions, right=condition, operator=BooleanOperator.AND
            )
        else:
            self.conditions = condition
        self.set_preexisting_conditions(condition)
        self.rebuild_cache()
        return self

    def validate_parents(self):
        # validate parents exist
        # assign partial values where needed
        for parent in self.parents:
            if not parent:
                raise SyntaxError("Unresolvable parent")

        # TODO: make this accurate
        if self.parents:
            self.partial_concepts = get_all_parent_partial(
                self.output_concepts, self.parents
            )

        self.partial_lcl = LooseBuildConceptList(concepts=self.partial_concepts)

    def add_output_concepts(self, concepts: List[BuildConcept], rebuild: bool = True):
        for concept in concepts:
            if concept.address not in self.output_lcl.addresses:
                self.output_concepts.append(concept)
        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)
        if rebuild:
            self.rebuild_cache()
        return self

    def add_partial_concepts(self, concepts: List[BuildConcept], rebuild: bool = True):
        for concept in concepts:
            if concept.address not in self.partial_lcl.addresses:
                self.partial_concepts.append(concept)
        self.partial_lcl = LooseBuildConceptList(concepts=self.partial_concepts)
        if rebuild:
            self.rebuild_cache()
        return self

    def add_existence_concepts(
        self, concepts: List[BuildConcept], rebuild: bool = True
    ):
        for concept in concepts:
            if concept.address not in self.output_concepts:
                self.existence_concepts.append(concept)
        if rebuild:
            self.rebuild_cache()
        return self

    def set_output_concepts(self, concepts: List[BuildConcept], rebuild: bool = True):
        # exit if no changes
        if self.output_concepts == concepts:
            return self
        self.output_concepts = concepts
        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)

        if rebuild:
            self.rebuild_cache()
        return self

    def add_output_concept(self, concept: BuildConcept, rebuild: bool = True):
        return self.add_output_concepts([concept], rebuild)

    def hide_output_concepts(self, concepts: List[BuildConcept], rebuild: bool = True):
        for x in concepts:
            self.hidden_concepts.add(x.address)
        if rebuild:
            self.rebuild_cache()
        return self

    def unhide_output_concepts(
        self, concepts: List[BuildConcept], rebuild: bool = True
    ):
        self.hidden_concepts = set(x for x in self.hidden_concepts if x not in concepts)
        if rebuild:
            self.rebuild_cache()
        return self

    def remove_output_concepts(
        self, concepts: List[BuildConcept], rebuild: bool = True
    ):
        for x in concepts:
            self.hidden_concepts.add(x.address)
        addresses = [x.address for x in concepts]
        self.output_concepts = [
            x for x in self.output_concepts if x.address not in addresses
        ]
        if rebuild:
            self.rebuild_cache()
        return self

    @property
    def usable_outputs(self) -> list[BuildConcept]:
        return [
            x for x in self.output_concepts if x.address not in self.hidden_concepts
        ]

    @property
    def logging_prefix(self) -> str:
        return "\t" * self.depth

    @property
    def all_concepts(self) -> list[BuildConcept]:
        return [*self.output_concepts]

    @property
    def all_used_concepts(self) -> list[BuildConcept]:
        return [*self.input_concepts, *self.existence_concepts]

    def __repr__(self):
        concepts = self.all_concepts
        addresses = [c.address for c in concepts]
        contents = ",".join(sorted(addresses[:3]))
        if len(addresses) > 3:
            extra = len(addresses) - 3
            contents += f"...{extra} more"
        return f"{self.__class__.__name__}<{contents}>"

    def _resolve(self) -> QueryDatasource:
        parent_sources: List[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]

        grain = (
            self.grain if self.grain else BuildGrain.from_concepts(self.output_concepts)
        )
        source_map = resolve_concept_map(
            parent_sources,
            targets=self.output_concepts,
            inherited_inputs=self.input_concepts + self.existence_concepts,
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
            nullable_concepts=self.nullable_concepts,
            force_group=self.force_group,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
        )

    def rebuild_cache(self) -> QueryDatasource:
        self.tainted = True
        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)
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

    def copy(self) -> "StrategyNode":
        return self.__class__(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=list(self.parents),
            partial_concepts=list(self.partial_concepts),
            nullable_concepts=list(self.nullable_concepts),
            depth=self.depth,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            force_group=self.force_group,
            grain=self.grain,
            hidden_concepts=set(self.hidden_concepts),
            existence_concepts=list(self.existence_concepts),
            virtual_output_concepts=list(self.virtual_output_concepts),
            ordering=self.ordering,
        )


@dataclass
class NodeJoin:
    left_node: StrategyNode
    right_node: StrategyNode
    concepts: List[BuildConcept]
    join_type: JoinType
    filter_to_mutual: bool = False
    concept_pairs: list[ConceptPair] | None = None

    def __post_init__(self):
        if self.left_node == self.right_node:
            raise SyntaxError("Invalid join, left and right nodes are the same")
        if self.concept_pairs:
            return
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
                if all([c.derivation == Derivation.CONSTANT for c in ds.all_concepts]):
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
