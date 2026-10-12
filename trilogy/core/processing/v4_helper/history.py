"""Per-request caches for one v4 discovery run.

Lives beside the stages rather than in `concept_strategies_v4` so the helper
modules and the node generators can type against it directly instead of
lazy-importing the planner module they are imported *by*.
"""

from dataclasses import dataclass, field

from trilogy.core.enums import JoinType
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildMultiSelectLineage,
    BuildSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes import BuildCaches, StrategyNode

from .constants import NO_WITNESS_FLOOR
from .keyspace import RowsetWitness
from .models import BuildInfo
from .network_model import SearchResult

# (built lineage, its build env, its WHERE, the reference graph it plans in)
NestedBuild = tuple[
    BuildSelectLineage | BuildMultiSelectLineage,
    BuildEnvironment,
    BuildWhereClause | None,
    ReferenceGraph,
]
# (select id, excluded handles, scoped joins)
NestedBuildKey = tuple[int, tuple[str, ...], tuple[tuple[str, str, JoinType], ...]]


@dataclass
class V4History:
    """Per-request discovery caches: the StrategyNode cache serving the
    datasource-selection sub-searches, and the BuildInfo bundles the planner
    returns."""

    base_environment: Environment
    select_history: dict[str, StrategyNode | None] = field(default_factory=dict)
    # Coalescing axes whose all-member assembly is mid-flight: sourcing a
    # member SIDE re-enters discovery for that member, which must resolve the
    # side alone rather than re-assembling the axis (balanced add/discard).
    coalescing_axis_in_progress: set[str] = field(default_factory=set)
    build_caches: BuildCaches = field(default_factory=BuildCaches)
    build_history: dict[str, BuildInfo | None] = field(default_factory=dict)
    # Derived-connector origin addresses currently mid-plan, used by the root
    # source planner to break the self-referential bridge recursion (a merged
    # recursive connector whose own input search re-routes through it).
    connectors_in_progress: set[str] = field(default_factory=set)
    # `SourceNetwork.signature()` -> the search's verdict. Holds no build
    # information (a SourceSolution is node names, addresses and integers) and
    # is scoped to one build request, since a fresh V4History is minted per
    # statement and per nested sub-build. The ROOT planner asks the same question
    # several times per query.
    search_cache: dict[tuple, SearchResult] = field(default_factory=dict)
    # `_network_source` outcomes that hand out NO network objects: "none"
    # (decline to the fall-through planners) and "defer" (a one-scan solution
    # that is `_direct_source`'s job). Keyed on addresses + conditions like
    # `_v4_key`, plus the group's promoted spans (`SpanScope.extent_free`, read
    # by the candidate labelling), safe within one history for the same reason
    # `build_history` is; a hit skips rebuilding a SourceNetwork just to
    # re-learn "not mine". Solution-bearing outcomes are NOT cached here:
    # emission needs the network, whose candidates are build-scoped objects a
    # later request must not reuse.
    network_verdicts: dict[
        tuple[str, str, bool, tuple[str, ...], bool, tuple[str, ...]], str
    ] = field(default_factory=dict)
    # Outputs of every nested construct enclosing the scope being planned
    # (rowset handles, merge/union align outputs), hidden from its connectivity
    # check only. Accumulates DOWNWARD: a union arm inside a rowset body must
    # hide both, or whichever it can still see bridges the check through the
    # construct being defined. Managed by `plan_nested_select`.
    nested_exclusions: frozenset[str] = frozenset()
    # Each rowset the statement reads, as a source of the plans reading it
    # (`keyspace.rowset_witness`): a fact of the rowset, computed once.
    rowset_witnesses: dict[str, RowsetWitness] = field(default_factory=dict)
    # Witnesses mid-computation (`rowset_witnesses()`), each by its depth,
    # standing in the cache as an empty placeholder.
    live_witnesses: dict[str, int] = field(default_factory=dict)
    # The shallowest live placeholder read since the innermost computation
    # began: a result that read one above its own frame understates it.
    witness_floor: int = NO_WITNESS_FLOOR
    # `build_nested_select` results; the select itself is held so its id is
    # never recycled.
    nested_builds: dict[
        NestedBuildKey, tuple[SelectLineage | MultiSelectLineage, NestedBuild]
    ] = field(default_factory=dict)
    # Spans of the body regions the plan reading a rowset holds the rows of:
    # the body, and every plan under it, is built without them. Managed by
    # `plan_nested_select`; part of the build key.
    owned_spans: frozenset[str] = frozenset()

    def _v4_key(
        self,
        search: list[BuildConcept],
        conditions: list[BuildWhereClause],
        complete_partials: bool,
        staged_conditions: list[BuildWhereClause] | None = None,
    ) -> str:
        base = "-".join(sorted(c.address for c in search))
        conditioned = base + str(conditions) if conditions else base
        if staged_conditions:
            conditioned += f"|staged={staged_conditions}"
        if self.owned_spans:
            conditioned += f"|owned={sorted(self.owned_spans)}"
        return f"{conditioned}|complete_partials={complete_partials}"

    def get_build_history(
        self,
        search: list[BuildConcept],
        conditions: list[BuildWhereClause],
        complete_partials: bool = True,
        staged_conditions: list[BuildWhereClause] | None = None,
    ) -> BuildInfo | None | bool:
        key = self._v4_key(search, conditions, complete_partials, staged_conditions)
        if key in self.build_history:
            node = self.build_history[key]
            return node.copy() if node else node
        return False

    def build_to_history(
        self,
        search: list[BuildConcept],
        output: BuildInfo | None,
        conditions: list[BuildWhereClause],
        complete_partials: bool = True,
        staged_conditions: list[BuildWhereClause] | None = None,
    ) -> None:
        self.build_history[
            self._v4_key(search, conditions, complete_partials, staged_conditions)
        ] = output

    def read_rowset_witness(self, name: str) -> RowsetWitness | None:
        """The cached witness of `name`; reading a live placeholder lowers the
        witness floor."""
        depth = self.live_witnesses.get(name)
        if depth is not None:
            self.witness_floor = min(self.witness_floor, depth)
        return self.rowset_witnesses.get(name)

    def _concepts_to_lookup(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        conditions: BuildWhereClause | None = None,
        promoted: frozenset[str] = frozenset(),
    ) -> str:
        base = sorted([c.address for c in search])
        key = "-".join(base) + str(accept_partial)
        if conditions:
            key += str(conditions)
        # a `~` key promoted to a full binding (`SpanScope.extent_free`) picks a
        # different scan for the same outputs: the solid stream of a region
        # reads the partial fact, its domain must read the complete one
        if promoted:
            key += f"|promoted={sorted(promoted)}"
        return key

    def gen_select_node(
        self,
        concepts: list[BuildConcept],
        environment: BuildEnvironment,
        g,
        depth: int,
        fail_if_not_found: bool = False,
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ) -> StrategyNode | None:
        from trilogy.core.processing.node_generators.select_node import gen_select_node

        fingerprint = self._concepts_to_lookup(
            concepts,
            accept_partial,
            conditions=conditions,
            promoted=environment.span_scope.extent_free,
        )
        if fingerprint in self.select_history:
            rval = self.select_history[fingerprint]
            if rval:
                # all nodes must be copied before returning
                return rval.copy()
            return rval
        gen = gen_select_node(
            concepts,
            environment,
            g,
            depth + 1,
            fail_if_not_found=fail_if_not_found,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        self.select_history[fingerprint] = gen
        if gen:
            return gen.copy()
        return gen
