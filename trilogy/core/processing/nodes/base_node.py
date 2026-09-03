from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field

from trilogy.core.enums import (
    JoinType,
    Modifier,
    SetOperator,
    SourceType,
)
from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildOrderBy,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import ConceptPair, QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import (
    condition_proves_non_null,
    merge_conditions_and_dedup,
)
from trilogy.utility import unique


def resolve_concept_map(
    inputs: list[QueryDatasource | BuildDatasource],
    targets: list[BuildConcept],
    inherited_inputs: list[BuildConcept],
    full_joins: list[BuildConcept] | None = None,
) -> dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]]:

    targets = targets or []
    concept_map: dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]] = (
        defaultdict(set)
    )
    full_addresses = {c.address for c in full_joins} if full_joins else set()
    inherited = {t.address for t in inherited_inputs}
    for input in inputs:
        # ``full_concepts`` is a property that rebuilds an address set each
        # call; bind it once instead of rescanning per output concept.
        full_addr = input.full_concepts
        for concept in input.output_concepts:
            # skip partials unless they are full join keys
            if (
                concept.address not in full_addr
                and concept.address not in full_addresses
            ):
                continue
            if (
                isinstance(input, QueryDatasource)
                and concept.address in input.hidden_concepts
            ):
                continue
            if concept.address in full_addresses or concept.address not in concept_map:
                concept_map[concept.address].add(input)

    # second loop, include partials
    for input in inputs:
        for concept in input.output_concepts:
            if concept.address not in inherited and not (
                concept.pseudonyms and any(s in inherited for s in concept.pseudonyms)
            ):
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
            for input in inputs:
                for concept in input.output_concepts:
                    if (
                        concept.address != target.address
                        and target.address not in concept.pseudonyms
                    ):
                        continue
                    if (
                        isinstance(input, QueryDatasource)
                        and concept.address in input.hidden_concepts
                    ):
                        continue
                    concept_map[target.address].add(input)
            if concept_map.get(target.address):
                continue
            # an empty source means it is defined in this CTE
            concept_map[target.address] = set()
    return concept_map


def resolve_existence_map(
    inputs: list[QueryDatasource | BuildDatasource],
    existence_concepts: list[BuildConcept],
) -> dict[str, set[BuildDatasource | QueryDatasource]]:
    existence_addresses = {c.address for c in existence_concepts}
    if not existence_addresses:
        return {}
    raw = resolve_concept_map(
        inputs,
        targets=[],
        inherited_inputs=existence_concepts,
    )
    return {
        address: {
            source
            for source in sources
            if isinstance(source, BuildDatasource | QueryDatasource)
        }
        for address, sources in raw.items()
        if address in existence_addresses
    }


def get_all_parent_partial(
    all_concepts: list[BuildConcept], parents: list["StrategyNode"]
) -> list[BuildConcept]:
    partial_addrs = [{x.address for x in p.partial_concepts} for p in parents]
    return unique(
        [
            c
            for c in all_concepts
            if any(c.address in addrs for addrs in partial_addrs)
            and all(
                c.address in p.partial_lcl for p in parents if c.address in p.output_lcl
            )
        ],
        "address",
    )


def get_all_parent_nullable(
    all_concepts: list[BuildConcept],
    parents: Sequence["StrategyNode | QueryDatasource | BuildDatasource"],
) -> list[BuildConcept]:
    """Accepts nodes or RESOLVED sources; only `nullable_concepts` is read.
    Prefer resolved sources where available: a node's attribute is a
    construction-time snapshot that grows at its first resolve."""
    for x in parents:
        if not x:
            raise ValueError(parents)
    nullable_addrs = [{x.address for x in p.nullable_concepts} for p in parents]
    return unique(
        [
            c
            for c in all_concepts
            if any(c.address in addrs for addrs in nullable_addrs)
            # a scalar derivation of a nullable input is itself nullable; infer
            # it at the computing node so address-based propagation carries it up
            or (
                propagates_argument_nulls(c)
                and any(
                    arg.address in addrs
                    for addrs in nullable_addrs
                    for arg in c.concept_arguments
                )
            )
        ],
        "address",
    )


class StrategyNode:
    source_type = SourceType.ABSTRACT
    # A node that only projects or filters emits its parents' rows. Subclasses
    # that set this take their parents' grain when none was passed, rather than
    # deriving one from their own outputs; see `_default_grain`.
    inherits_parent_grain: bool = False
    # Only UnionNode carries a non-default combinator; it must be set at
    # construction so QueryDatasource.__post_init__ preserves arm order for
    # EXCEPT.
    set_operator: SetOperator = SetOperator.UNION_ALL

    def __init__(
        self,
        input_concepts: list[BuildConcept],
        output_concepts: list[BuildConcept],
        environment: BuildEnvironment,
        parents: list["StrategyNode"] | None = None,
        partial_concepts: list[BuildConcept] | None = None,
        rollup_concepts: list[BuildConcept] | None = None,
        nullable_concepts: list[BuildConcept] | None = None,
        depth: int = 0,
        conditions: BoolExpr | None = None,
        preexisting_conditions: BoolExpr | None = None,
        force_group: bool | None = None,
        grain: BuildGrain | None = None,
        hidden_concepts: set[str] | None = None,
        existence_concepts: list[BuildConcept] | None = None,
        ordering: BuildOrderBy | None = None,
    ):
        self.input_concepts: list[BuildConcept] = (
            unique(input_concepts, "address") if input_concepts else []
        )
        self.input_lcl = LooseBuildConceptList(concepts=self.input_concepts)
        self.output_concepts: list[BuildConcept] = unique(output_concepts, "address")
        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)

        self.environment = environment
        self.parents = parents or []
        self.resolution_cache: QueryDatasource | None = None

        self.nullable_concepts = nullable_concepts or get_all_parent_nullable(
            self.output_concepts, self.parents
        )
        self.ordering = ordering
        # Row limit applied at this node's output (rowset body `limit N`).
        # Attribute rather than an init param: only the rowset translation
        # wrapper sets it, and copy()/_resolve() thread it through.
        self.limit: int | None = None
        self.depth = depth
        self.conditions = conditions
        self._refine_nullable_for_conditions()
        self.grain = grain
        self.force_group = force_group
        # Set when this source's own group was deferred past a merge (so a
        # pushed WHERE could apply post-join); the merge must regroup to its
        # output grain or the deferred normalization is silently lost.
        self.group_deferred = False
        self.hidden_concepts = hidden_concepts or set()
        self.existence_concepts = existence_concepts or []
        self.preexisting_conditions = preexisting_conditions
        if self.conditions and not self.preexisting_conditions:
            self.preexisting_conditions = self.conditions
        elif (
            self.conditions
            and self.preexisting_conditions
            and self.conditions != self.preexisting_conditions
        ):
            self.preexisting_conditions = merge_conditions_and_dedup(
                self.conditions,
                self.preexisting_conditions,
            )
        self.partial_concepts: list[BuildConcept] = self.derive_partials(
            partial_concepts
        )
        self.rollup_concepts = rollup_concepts or []
        self.validate_inputs()

    def validate_inputs(self):
        if not self.parents:
            return
        non_hidden = set()
        non_hidden_canonical = set()
        hidden = set()
        usable_outputs = set()
        for x in self.parents:
            for z in x.usable_outputs:
                usable_outputs.add(z.address)
                non_hidden.add(z.address)
                non_hidden_canonical.add(z.canonical_address)
                for psd in z.pseudonyms:
                    non_hidden.add(psd)
            for z in x.hidden_concepts:
                hidden.add(z)
        # Inputs may match a parent's output by canonical_address: two addresses
        # with the same lineage render from the same SQL expression, so a parent
        # producing one satisfies the other.
        missing = [
            x.address
            for x in self.input_concepts
            if x.address not in non_hidden
            and x.canonical_address not in non_hidden_canonical
        ]
        if missing:

            raise ValueError(
                f"Invalid input concepts to node! {missing} are missing non-hidden parent nodes; have {non_hidden} and hidden {hidden} from root {usable_outputs}"
            )

    def add_parents(self, parents: list["StrategyNode"]):
        self.parents += parents
        self.partial_concepts = self.derive_partials(None)
        return self

    def _refine_nullable_for_conditions(self) -> None:
        """Strip concepts from ``nullable_concepts`` that this node's own
        ``conditions`` null-rejects, so each node's nullability reflects its own
        rows and downstream nodes inherit that via ``get_all_parent_nullable``.

        After this refinement, absence from ``nullable_concepts`` conflates
        "never nullable" with "proven non-null by this node's own condition".
        Anything that reasons about the condition itself (e.g.
        ``StripRedundantNotNull``) must not treat absence as ground truth.
        """
        if not self.conditions or not self.nullable_concepts:
            return
        proven = condition_proves_non_null(self.conditions)
        if not proven:
            return
        self.nullable_concepts = [
            c for c in self.nullable_concepts if c.address not in proven
        ]

    def derive_partials(
        self, partial_concepts: list[BuildConcept] | None = None
    ) -> list[BuildConcept]:
        for parent in self.parents:
            if not parent:
                raise SyntaxError("Unresolvable parent")

        # TODO: make this accurate
        if self.parents and partial_concepts is None:
            partials = get_all_parent_partial(self.output_concepts, self.parents)
        elif partial_concepts is None:
            partials = []
        else:
            partials = partial_concepts
        self.partial_lcl = LooseBuildConceptList(concepts=partials)
        return partials

    def add_output_concepts(
        self, concepts: list[BuildConcept], rebuild: bool = True, unhide: bool = True
    ):
        # nullability was computed at construction; keep it current for
        # post-construction outputs (a derived concept computed over a nullable
        # input is nullable wherever the input is)
        upstream_nullable = {x.address for x in self.nullable_concepts}
        for p in self.parents:
            upstream_nullable |= {x.address for x in p.nullable_concepts}
        nullable_addresses = {x.address for x in self.nullable_concepts}
        for concept in concepts:
            if concept.address not in self.output_lcl.addresses:
                self.output_concepts.append(concept)
                if concept.address not in nullable_addresses and (
                    concept.address in upstream_nullable
                    or (
                        propagates_argument_nulls(concept)
                        and any(
                            a.address in upstream_nullable
                            for a in concept.concept_arguments
                        )
                    )
                ):
                    self.nullable_concepts.append(concept)
                    nullable_addresses.add(concept.address)
            if unhide and concept.address in self.hidden_concepts:
                self.hidden_concepts.remove(concept.address)
        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)
        if rebuild:
            self.rebuild_cache()
        return self

    def add_partial_concepts(self, concepts: list[BuildConcept], rebuild: bool = True):
        for concept in concepts:
            if concept.address not in self.partial_lcl.addresses:
                self.partial_concepts.append(concept)
        self.partial_lcl = LooseBuildConceptList(concepts=self.partial_concepts)
        if rebuild:
            self.rebuild_cache()
        return self

    def add_existence_concepts(
        self, concepts: list[BuildConcept], rebuild: bool = True
    ):
        for concept in concepts:
            if concept.address not in self.output_concepts:
                self.existence_concepts.append(concept)
        if rebuild:
            self.rebuild_cache()
        return self

    def set_output_concepts(
        self,
        concepts: list[BuildConcept],
        rebuild: bool = True,
        change_visibility: bool = True,
    ):
        if self.output_concepts == concepts:
            return self
        self.output_concepts = concepts
        if self.hidden_concepts and change_visibility:
            self.hidden_concepts = {
                x for x in self.hidden_concepts if x not in concepts
            }

        self.output_lcl = LooseBuildConceptList(concepts=self.output_concepts)

        if rebuild:
            self.rebuild_cache()
        return self

    def hide_output_concepts(
        self, concepts: list[BuildConcept] | list[str] | set[str], rebuild: bool = True
    ):
        for x in concepts:
            if isinstance(x, BuildConcept):
                self.hidden_concepts.add(x.address)
            else:
                self.hidden_concepts.add(x)
        if rebuild:
            self.rebuild_cache()
        return self

    def unhide_output_concepts(
        self, concepts: list[BuildConcept], rebuild: bool = True
    ):
        self.hidden_concepts = {x for x in self.hidden_concepts if x not in concepts}
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

    def __repr__(self):
        concepts = self.all_concepts
        addresses = [c.address for c in concepts]
        contents = ",".join(sorted(addresses[:3]))
        if len(addresses) > 3:
            extra = len(addresses) - 3
            contents += f"...{extra} more"
        return f"{self.__class__.__name__}<{contents}>"

    def _default_grain(
        self, parent_sources: list[QueryDatasource | BuildDatasource]
    ) -> BuildGrain:
        """The grain to declare when the planner passed none.

        A row-preserving node reports its parents' grain: claiming the grain of
        its own (possibly narrower) projection would read as already deduped,
        and consumers would skip the GROUP BY that makes it true.
        """
        if self.inherits_parent_grain and parent_sources and not self.force_group:
            # An existence feeder is read through a subselect: it rejects rows,
            # it never supplies them, so its grain is not part of ours.
            existence = {concept.address for concept in self.existence_concepts}
            inherited = BuildGrain()
            for source in parent_sources:
                supplied = {concept.address for concept in source.output_concepts}
                if existence and supplied <= existence:
                    continue
                inherited += source.grain
            if inherited.components:
                return inherited
        return BuildGrain.from_concepts(self.output_concepts)

    def _resolve(self) -> QueryDatasource:
        parent_sources: list[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]

        grain = self.grain if self.grain else self._default_grain(parent_sources)
        source_map = resolve_concept_map(
            parent_sources,
            targets=self.output_concepts,
            inherited_inputs=self.input_concepts + self.existence_concepts,
        )

        # Nullability is recomputed from the RESOLVED parents rather than the
        # construction-time snapshot: the parent NODE attribute only carries
        # join analysis after its first resolve, so copies built before and
        # after that resolve would otherwise plan differently. The node's own
        # condition refinement still applies on top.
        nullable = unique(
            self.nullable_concepts
            + get_all_parent_nullable(self.output_concepts, parent_sources),
            "address",
        )
        if self.conditions:
            proven = condition_proves_non_null(self.conditions)
            if proven:
                nullable = [c for c in nullable if c.address not in proven]

        return QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=self.output_concepts,
            datasources=parent_sources,
            source_type=self.source_type,
            set_operator=self.set_operator,
            source_map=source_map,
            existence_source_map=resolve_existence_map(
                parent_sources, self.existence_concepts
            ),
            joins=[],
            grain=grain,
            condition=self.conditions,
            partial_concepts=self.partial_concepts,
            rollup_concepts=self.rollup_concepts,
            nullable_concepts=nullable,
            force_group=self.force_group,
            hidden_concepts=self.hidden_concepts,
            ordering=self.ordering,
            limit=self.limit,
            base_datasource=parent_sources[0] if len(parent_sources) == 1 else None,
        )

    def rebuild_cache(self) -> QueryDatasource:
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
        # Resolve-time nullability (outer-join null extension, ROLLUP padding) is
        # stamped on the QueryDatasource, but downstream nodes read the node
        # attribute; sync it back or joins on a null-extended column render a
        # plain `=` and silently drop rows.
        self.nullable_concepts = unique(
            self.nullable_concepts + list(qds.nullable_concepts), "address"
        )
        return qds

    def copy(self) -> "StrategyNode":
        node = self.__class__(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            parents=list(self.parents),
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            nullable_concepts=list(self.nullable_concepts),
            depth=self.depth,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            force_group=self.force_group,
            grain=self.grain,
            hidden_concepts=set(self.hidden_concepts),
            existence_concepts=list(self.existence_concepts),
            ordering=self.ordering,
        )
        node.limit = self.limit
        return node


@dataclass
class NodeJoin:
    left_node: StrategyNode
    right_node: StrategyNode
    concepts: list[BuildConcept]
    join_type: JoinType
    concept_pairs: list[ConceptPair] | None = None
    modifiers: list[Modifier] = field(default_factory=list)

    def __post_init__(self):
        if self.left_node == self.right_node:
            raise SyntaxError("Invalid join, left and right nodes are the same")
        if self.concept_pairs:
            return
        for concept in self.concepts:
            for ds in [self.left_node, self.right_node]:
                if concept.address not in [c.address for c in ds.all_concepts]:
                    raise SyntaxError(
                        f"Invalid join, missing {concept} on {ds!s}, have"
                        f" {[c.address for c in ds.all_concepts]}"
                    )

    def __str__(self):
        return (
            f"{self.join_type.value} JOIN {self.left_node} and"
            f" {self.right_node} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )
