"""Per-request caches for one v4 discovery run.

Lives beside the stages rather than in `concept_strategies_v4` so the helper
modules and the node generators can type against it directly instead of
lazy-importing the planner module they are imported *by*.
"""

from dataclasses import dataclass, field

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.processing.nodes import History

from .models import BuildInfo
from .network_model import SearchResult


@dataclass
class V4History(History):
    """History fork for the v4 discovery prototype. The inherited StrategyNode
    cache still serves the v3 sub-searches v4 dispatches into; this fork adds a
    parallel, correctly-typed cache for the BuildInfo bundles v4 returns."""

    build_history: dict[str, BuildInfo | None] = field(default_factory=dict)
    # Derived-connector origin addresses currently mid-plan, used by the root
    # source planner to break the self-referential bridge recursion (a merged
    # recursive connector whose own input search re-routes through it).
    connectors_in_progress: set[str] = field(default_factory=set)
    # `SourceNetwork.signature()` -> the search's verdict. Holds no build
    # information — a SourceSolution is node names, addresses and integers — and
    # is scoped to one build request, since a fresh V4History is minted per
    # statement and per nested sub-build. The ROOT planner asks the same question
    # several times per query.
    search_cache: dict[tuple, SearchResult] = field(default_factory=dict)
    # Outputs of every nested construct enclosing the scope being planned
    # (rowset handles, merge/union align outputs), hidden from its connectivity
    # check only. Accumulates DOWNWARD: a union arm inside a rowset body must
    # hide both, or whichever it can still see bridges the check through the
    # construct being defined. Managed by `plan_nested_select`.
    nested_exclusions: frozenset[str] = frozenset()

    def _v4_key(
        self,
        search: list[BuildConcept],
        conditions: list[BuildWhereClause],
        complete_partials: bool,
    ) -> str:
        base = "-".join(sorted(c.address for c in search))
        conditioned = base + str(conditions) if conditions else base
        return f"{conditioned}|complete_partials={complete_partials}"

    def get_build_history(
        self,
        search: list[BuildConcept],
        conditions: list[BuildWhereClause],
        complete_partials: bool = True,
    ) -> BuildInfo | None | bool:
        key = self._v4_key(search, conditions, complete_partials)
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
    ) -> None:
        self.build_history[self._v4_key(search, conditions, complete_partials)] = output
