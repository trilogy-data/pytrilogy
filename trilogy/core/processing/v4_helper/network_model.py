"""The source-network vocabulary: what a candidate, a cover and a cost ARE.

Every other `network_*` module is stated in these terms. This one depends on
nothing in the search: it holds the labeled network and the answers derivable
from the labels alone, so the stages above it cannot disagree about what they
are reasoning over.

See `network_search` for the search itself and
docs/v4_network_discovery_design.md for the model.
"""

from __future__ import annotations

from dataclasses import astuple, dataclass, field
from enum import Enum
from typing import Any

from trilogy.core.models.build import BuildDatasource, BuildUnionDatasource

# Bounds the cover enumeration. A truncated search is reported, never silent.
COVER_LIMIT = 4096
# Bounds the obligation search's visited states, independent of how many
# complete covers it has found. A truncated search is reported, never silent.
# Sized an order of magnitude above the largest state count a corpus query
# reaches. An UNSOURCEABLE request explores states until this budget stops it,
# so every increment here is paid in full on the failure path before the
# fall-through raises UnresolvableQueryException.
STATE_LIMIT = 10_000

CONNECTOR_NODE_PREFIX = "connector~"


class SearchLimit(Enum):
    """Which budget the enumeration exhausted. Reported so a truncated search is
    never mistaken for a complete one."""

    COVERS = "cover_limit"
    STATES = "state_limit"


class BindingStrength(Enum):
    FULL = "full"
    PARTIAL = "partial"


class ConditionFit(Enum):
    """How this datasource stands to the request's WHERE.

    `disqualifying` (SENSITIVE) removes a candidate; `partial_is_full`
    (IMPLIED_EXACT) makes a partial binding authoritative. Where the WHERE
    lands (scan vs post-merge) is a shape question, not a selection input."""

    # No conditions bear on selection.
    NEUTRAL = "neutral"
    # A `complete where` partial whose predicate the query implies: pre-filtered
    # to exactly the requested rows, so it is authoritative and its partiality is
    # never dominance evidence.
    IMPLIED_EXACT = "implied_exact"
    # Carries an aggregate the filter would invalidate.
    SENSITIVE = "sensitive"

    @property
    def disqualifying(self) -> bool:
        return self is ConditionFit.SENSITIVE

    @property
    def partial_is_full(self) -> bool:
        return self is ConditionFit.IMPLIED_EXACT


@dataclass(frozen=True)
class Binding:
    address: str
    strength: BindingStrength
    # A stored column, as opposed to a value the scan can derive inline from
    # complete columns. Joining on a stored key beats joining on a computed one.
    stored: bool
    # Added by the search itself (`_pin_unoffered_probes`), not the graph: the
    # bridge emitter can render it, but `_direct_source`'s graph-scored select
    # cannot, so a single-scan solution leaning on one must stay on the bridge.
    injected: bool = False

    @property
    def partial(self) -> bool:
        return self.strength is BindingStrength.PARTIAL


@dataclass(frozen=True)
class SourceCandidate:
    node: str
    # None for a derived-connector candidate (`connector~*`): a merged key with
    # a non-BASIC origin has no scan of its own; `_derived_connector_nodes`
    # materializes its `alias_origin_lookup` lineage as a subplan.
    datasource: BuildDatasource | BuildUnionDatasource | None
    bindings: dict[str, Binding]
    condition: ConditionFit
    is_union: bool
    # The source's own row identity, in equivalence-class terms.
    grain: frozenset[str]

    def binds(self, address: str) -> bool:
        return address in self.bindings

    def binds_fully(self, address: str) -> bool:
        binding = self.bindings.get(address)
        if binding is None:
            return False
        return not binding.partial or self.condition.partial_is_full


def _row_complete(candidate: SourceCandidate) -> bool:
    """Whether this candidate's row population is total for its own grain:
    every grain-key binding is FULL, or the request's WHERE implies its
    `complete where` predicate. A row-partial candidate (an enum arm, a
    returns-side table) may TERMINATE a labeling chain it fully binds, but
    never EXTEND one: a lookup routed through it silently drops the rows it
    lacks."""
    if candidate.condition.partial_is_full:
        return True
    return all(
        not binding.partial
        for address, binding in candidate.bindings.items()
        if address in candidate.grain
    )


def datasource_identifiers(
    datasource: BuildDatasource | BuildUnionDatasource,
) -> set[str]:
    if isinstance(datasource, BuildUnionDatasource):
        return {child.identifier for child in datasource.children}
    return {datasource.identifier}


@dataclass(frozen=True)
class JoinRequirement:
    """A declared relation (`merge`, or the scope-blind query `subset`/`union
    join`) whose two ROOT members canonicalize to one build address.

    Sourcing the merged key ONCE satisfies coverage but not the declaration:
    the equality relates the sides, so each side the cover carries must
    materialize the key against its OWN key set. Otherwise that side has no way
    to produce the authored equality and the sides silently pair on whatever
    else they happen to share. This is the search-side statement of what
    `inject_authored_join_key_terminals` asks the connector walk for."""

    canonical: str
    # Each side's own keys, in equivalence-class terms. A source binding these
    # is a carrier for that side; a source binding these AND the canonical is
    # the hop that materializes the equality there.
    left_keys: frozenset[str]
    right_keys: frozenset[str]

    def sides(self) -> tuple[frozenset[str], ...]:
        return tuple(keys for keys in (self.left_keys, self.right_keys) if keys)


def _memo() -> Any:
    """A per-network memo table. Pure over the immutable candidate set, so it is
    excluded from equality and repr. Declared through one helper because there
    are a dozen of them and the spec must not drift between them."""
    return field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True, eq=False)
class SourceNetwork:
    """`eq=False` deliberately: a generated structural `__eq__`/`__hash__` would
    raise on the dict fields, so a network that LOOKED hashable would blow up
    the first time one entered a set. Structural comparison is `signature()`."""

    terminals: tuple[str, ...]
    candidates: dict[str, SourceCandidate]
    # Address -> its equivalence-class representative (pseudonym / merge twins).
    equivalence: dict[str, str]
    # Address -> the grain its own value lives at (a measure at fact grain, a
    # dimension key at its dimension's grain).
    address_grain: dict[str, frozenset[str]]
    join_requirements: tuple[JoinRequirement, ...] = ()
    # Requested coalescing (`full`/`union` join) axis classes whose request is
    # NOT pinned to one arm, mapped to per-MEMBER carrier candidates (best
    # first). The unified axis is the union of the members' domains, so such a
    # class is fully bound only by a cover carrying EVERY member's own column;
    # a single arm's read silently drops the other arms' rows.
    axis_families: dict[str, tuple[tuple[str, ...], ...]] = field(default_factory=dict)
    # Partition-arm node -> the union candidate that subsumes it, for arms the
    # request's WHERE does not pin (see `_subsumed_arms`). Input to the search,
    # not a memo: `_prune_subsumed_arms` reads it to keep an obligation from
    # branching onto arms its own satisfier list already covers with the union.
    subsumed_arms: dict[str, str] = field(default_factory=dict)
    # Pure memo tables over the immutable candidate set (see `join_keys`).
    _join_key_cache: dict[tuple[str, str], frozenset[str]] = _memo()
    _binding_key_cache: dict[str, frozenset[str]] = _memo()
    _binder_cache: dict[str, tuple[str, ...]] = _memo()
    # Source-set-keyed memos filled by their computing modules: obligations by
    # `network_obligations.pending_obligations`, topology by
    # `network_topology.joined_pairs`/`components`/`blend_joins`. All are pure
    # over the network; `_reduce` and the enumeration re-ask the same source
    # sets thousands of times per search.
    _obligation_cache: dict[frozenset[str], tuple[Obligation, ...]] = _memo()
    _pair_cache: dict[frozenset[str], tuple[tuple[str, str], ...]] = _memo()
    _component_cache: dict[frozenset[str], tuple[frozenset[str], ...]] = _memo()
    _blend_cache: dict[frozenset[str], int] = _memo()
    # Binding profiles by `network_search._binding_profile`; keyed by targets
    # too, though every current caller passes the network's own terminals.
    _profile_cache: dict[tuple[frozenset[str], tuple[str, ...]], dict[str, int]] = (
        _memo()
    )
    _full_binder_cache: dict[str, frozenset[str]] = _memo()
    _completer_cache: dict[str, frozenset[str]] = _memo()
    _row_complete_cache: dict[str, bool] = _memo()
    _sorted_cache: dict[int, tuple[str, ...]] = _memo()
    _adjacency_cache: dict[
        int, tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]
    ] = _memo()
    _partner_cache: dict[
        int, tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]
    ] = _memo()
    _binder_set_cache: dict[str, frozenset[str]] = _memo()
    _bound_terminal_cache: dict[str, frozenset[str]] = _memo()

    def signature(self) -> tuple:
        """Everything `search_sources` reads, as a hashable value: two networks
        sharing one have the same solution. Lets a caller memo the search across
        the repeated ROOT requests of ONE build (see `V4History.search_cache`).
        Deliberately structural rather than identity-based (it holds only
        addresses and node names, never a BuildConcept), so a stale environment
        cannot be smuggled through it."""
        return (
            self.terminals,
            tuple(
                (
                    node,
                    candidate.condition,
                    candidate.is_union,
                    tuple(sorted(candidate.grain)),
                    tuple(
                        (address, binding.strength, binding.stored)
                        for address, binding in sorted(candidate.bindings.items())
                    ),
                )
                for node, candidate in sorted(self.candidates.items())
            ),
            tuple(
                (address, tuple(sorted(grain)))
                for address, grain in sorted(self.address_grain.items())
            ),
            self.join_requirements,
            tuple(sorted(self.axis_families.items())),
            tuple(sorted(self.subsumed_arms.items())),
        )

    def fans_out(self, node: str, contributed: frozenset[str]) -> bool:
        """This scan holds more rows per contributed value than that value's own
        grain: a fact table standing in for a dimension. Judged ONLY against
        what the source contributes: any wider yardstick (the rest of the
        solution, or the keys this source is joined on) can be widened by adding
        a source, which would let a solution launder its own fan-out away."""
        candidate = self.candidates[node]
        if not candidate.grain or not contributed:
            return False
        closure = set(contributed)
        for address in contributed:
            closure |= self.address_grain.get(address, frozenset())
        return not candidate.grain <= closure

    def joins_functionally(self, left: str, right: str) -> bool:
        """The keys these two share identify one side's rows, so the join is a
        LOOKUP: it can restrict, never multiply. A join covering neither grain is
        a BLEND: legitimate when two facts are related only through conformed
        dimensions and nothing can co-locate a finer key, a wrong-rows defect
        when something can (a dimension joined on a low-cardinality
        discriminator). Which of the two it is, is a property of the whole
        cover, not of the pair, so this reports the pair and `_blend_joins`
        decides."""
        keys = self.join_keys(left, right)
        if not keys:
            return False
        return (
            self.candidates[left].grain <= keys or self.candidates[right].grain <= keys
        )

    def binders(self, address: str) -> tuple[str, ...]:
        # Memoized: the repair search asks per (cover, source, terminal).
        cached = self._binder_cache.get(address)
        if cached is None:
            cached = tuple(
                sorted(node for node, c in self.candidates.items() if c.binds(address))
            )
            self._binder_cache[address] = cached
        return cached

    def binder_set(self, address: str) -> frozenset[str]:
        # `binders` as a set, for the per-state "is this terminal covered yet"
        # test: one intersection against the cover instead of a `binds` per
        # member.
        cached = self._binder_set_cache.get(address)
        if cached is None:
            cached = frozenset(self.binders(address))
            self._binder_set_cache[address] = cached
        return cached

    def bound_terminals(self, node: str) -> frozenset[str]:
        # Transpose of `binder_set` over the request's terminals: the labelable
        # scan asks "which terminals does this source contribute" per (state,
        # source).
        cached = self._bound_terminal_cache.get(node)
        if cached is None:
            cached = frozenset(
                terminal
                for terminal in self.terminals
                if node in self.binder_set(terminal)
            )
            self._bound_terminal_cache[node] = cached
        return cached

    def join_keys(self, left: str, right: str) -> frozenset[str]:
        # Memoized: the cover search asks this O(n^2) times per candidate cover, for
        # thousands of covers, and the operand sets are large (a wide fact scan).
        key = (left, right) if left <= right else (right, left)
        cached = self._join_key_cache.get(key)
        if cached is None:
            cached = self.binding_keys(left) & self.binding_keys(right)
            self._join_key_cache[key] = cached
        return cached

    def binding_keys(self, node: str) -> frozenset[str]:
        cached = self._binding_key_cache.get(node)
        if cached is None:
            cached = frozenset(self.candidates[node].bindings)
            self._binding_key_cache[node] = cached
        return cached

    def sorted_candidates(self) -> tuple[str, ...]:
        cached = self._sorted_cache.get(0)
        if cached is None:
            cached = tuple(sorted(self.candidates))
            self._sorted_cache[0] = cached
        return cached

    def full_binders(self, address: str) -> frozenset[str]:
        """Candidates binding this address FULLY. The hot loops ask per (cover,
        source, terminal), so this is one membership test against a table sized
        by the candidate set."""
        cached = self._full_binder_cache.get(address)
        if cached is None:
            cached = frozenset(
                node
                for node, candidate in self.candidates.items()
                if candidate.binds_fully(address)
            )
            self._full_binder_cache[address] = cached
        return cached

    def _adjacency(self) -> tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]:
        """The functional-lookup digraph, both directions, built once: an edge
        u -> v means one `v` row per `u` row (`functional_into`).

        Every chain question in the search is a walk over this graph, so it is
        built as adjacency SETS rather than re-asked pairwise: one pass over the
        V^2 pairs instead of a pairwise test per walk step."""
        cached = self._adjacency_cache.get(0)
        if cached is None:
            nodes = self.sorted_candidates()
            succ: dict[str, set[str]] = {node: set() for node in nodes}
            pred: dict[str, set[str]] = {node: set() for node in nodes}
            for origin in nodes:
                for target in nodes:
                    if origin != target and self.functional_into(origin, target):
                        succ[origin].add(target)
                        pred[target].add(origin)
            cached = (
                {node: frozenset(targets) for node, targets in succ.items()},
                {node: frozenset(origins) for node, origins in pred.items()},
            )
            self._adjacency_cache[0] = cached
        return cached

    def _partners(self) -> tuple[dict[str, frozenset[str]], dict[str, frozenset[str]]]:
        """The two UNDIRECTED pair predicates as adjacency sets, built once:
        index 0 is "shares any binding key" and index 1 is
        `joins_functionally`. The obligation scan asks both per (state,
        source, candidate); an unsourceable request walks the full state
        budget, so a pairwise call per ask would dominate the cost of
        concluding "no solution". Symmetric, so each unordered pair is asked
        once."""
        cached = self._partner_cache.get(0)
        if cached is None:
            nodes = self.sorted_candidates()
            joined: dict[str, set[str]] = {node: set() for node in nodes}
            functional: dict[str, set[str]] = {node: set() for node in nodes}
            for i, left in enumerate(nodes):
                for right in nodes[i + 1 :]:
                    if not self.join_keys(left, right):
                        continue
                    joined[left].add(right)
                    joined[right].add(left)
                    if self.joins_functionally(left, right):
                        functional[left].add(right)
                        functional[right].add(left)
            cached = (
                {node: frozenset(others) for node, others in joined.items()},
                {node: frozenset(others) for node, others in functional.items()},
            )
            self._partner_cache[0] = cached
        return cached

    def functional_successors(self, node: str) -> frozenset[str]:
        return self._adjacency()[0][node]

    def functional_predecessors(self, node: str) -> frozenset[str]:
        return self._adjacency()[1][node]

    def chain_completers(self, address: str) -> frozenset[str]:
        """Candidates that can END a labeling chain for this address: they bind
        it fully, or a lookup chain off their own keys reaches something that
        does. Cover-independent (which chain is IN the cover is the caller's
        question), so it is asked once per address, not once per state.

        This is the ANCESTOR set of the full binders, so it is walked backwards
        from them once rather than forwards from every candidate. An ancestor is
        ADMITTED unconditionally (it is the chain's ORIGIN, and the forward
        walk never gates the origin) but is EXPANDED further only when it is
        row-complete, since expanding it makes it an INTERMEDIATE on a longer
        chain. `_forward_reach` in the tests states the forward form the two
        must agree on."""
        cached = self._completer_cache.get(address)
        if cached is None:
            full = self.full_binders(address)
            seen = set(full)
            stack = list(full)
            while stack:
                for origin in self.functional_predecessors(stack.pop()):
                    if origin in seen:
                        continue
                    seen.add(origin)
                    if self.row_complete(origin):
                        stack.append(origin)
            cached = frozenset(seen)
            self._completer_cache[address] = cached
        return cached

    def functional_into(self, origin: str, target: str) -> bool:
        """One `target` row per `origin` row: the keys the two share cover the
        TARGET's whole grain, a lookup INTO the target, which can label or
        restrict the origin's rows but never multiply them. DIRECTIONAL, unlike
        `joins_functionally`: the undirected form cannot see the shared-dimension
        diamond, where two facts each look up the same dimension while neither
        pins the other.

        Unmemoized: `_adjacency` asks each ordered pair exactly once and every
        walk reads the adjacency sets, so a memo here could never hit."""
        grain = self.candidates[target].grain
        return bool(grain) and grain <= self.join_keys(origin, target)

    def row_complete(self, node: str) -> bool:
        cached = self._row_complete_cache.get(node)
        if cached is None:
            cached = _row_complete(self.candidates[node])
            self._row_complete_cache[node] = cached
        return cached

    def axis_complete(self, sources: frozenset[str], address: str) -> bool:
        """Every member of this coalescing axis class has a carrier in the
        cover, so the emitters can assemble the mandatory coalesce of every
        member side. Only meaningful for `axis_families` entries."""
        family = self.axis_families.get(address)
        if family is None:
            return False
        return all(any(node in sources for node in nodes) for nodes in family)


@dataclass(frozen=True)
class SolutionCost:
    """Lower is better on every axis. `search_sources` compares LEXICOGRAPHICALLY
    in declared order, which is a total order: the axes are ranked, not merely
    incomparable, so no frontier is kept."""

    unpaired_join_keys: int
    partial_terminals: int
    completions: int
    blend_joins: int
    fanout_sources: int
    sources: int
    connectors: int
    derived_joins: int

    def axes(self) -> tuple[int, ...]:
        # Derived from the fields in declaration order, never restated: an axis
        # added to the class but forgotten here would silently drop out of the
        # search's `min`.
        return astuple(self)


@dataclass(frozen=True)
class SourceSolution:
    sources: tuple[str, ...]
    # ds node -> the terminal addresses it is chosen to provide.
    assignments: dict[str, frozenset[str]]
    join_keys: dict[tuple[str, str], frozenset[str]]
    # Terminals that survive only as a partial binding.
    partial_terminals: frozenset[str]
    # Partial terminals another candidate binds fully: a completion join.
    completions: frozenset[str]
    # Non-terminal addresses the solution joins on.
    connectors: frozenset[str]
    cost: SolutionCost


@dataclass
class SearchResult:
    solution: SourceSolution | None = None
    unreachable: frozenset[str] = frozenset()
    # Terminals no single join-component of the WHOLE candidate pool can cover
    # alongside the rest. A cover's joins are a subgraph of the pool's, so this
    # is a PROOF no connected cover exists: a considered decline, unlike
    # `limit`, which is only an exhausted budget.
    split: frozenset[str] = frozenset()
    # The budget the enumeration ran out of, if any.
    limit: SearchLimit | None = None

    @property
    def truncated(self) -> bool:
        return self.limit is not None

    @property
    def exhausted(self) -> bool:
        """Out of budget before any usable cover was emitted. NOT a decline: the
        search makes no claim that no solution exists, so a caller falling
        through to another planner is guessing, not following evidence."""
        return self.truncated and self.solution is None


class ObligationKind(str, Enum):
    """What a pending requirement IS. One vocabulary for every correctness
    invariant:

    - ``COVER``      : a terminal no chosen source binds. Subject: (address,).
    - ``AXIS``       : a coalescing-axis member arm with no carrier in the
                       cover. Subject: (class representative, member index).
    - ``PAIRED``     : a declared relation side the cover carries without
                       materializing the merged key on it. Subject:
                       (canonical, *side keys).
    - ``LABELABLE``  : a chosen source whose rows cannot be labeled with a
                       requested terminal through any in-cover functional
                       lookup, though some candidate could supply one.
                       Subject: (source, terminal).
    - ``COLOCATED``  : a chosen source none of whose in-cover joins covers its
                       grain, when some candidate could put its grain key
                       beside it. Subject: (source,).
    - ``CONNECTED``  : the cover is in pieces and some candidate bridges them.
                       Subject: the components' minimum members.

    `str` mixin so `Obligation.identity` orders and hashes as a bare string;
    the enumeration's scarcest-first tiebreak (`min(pending, key=...)`)
    compares it."""

    COVER = "cover"
    AXIS = "axis"
    PAIRED = "paired"
    LABELABLE = "labelable"
    COLOCATED = "colocated"
    CONNECTED = "connected"


@dataclass(frozen=True)
class Obligation:
    """A requirement of the cover under construction, with the candidate nodes
    that can discharge it (see `ObligationKind` for the kinds and subjects).

    Obligations are monotone (adding a source never re-opens one) and are
    minted only when at least one satisfier exists: a requirement nothing
    could satisfy is the request's own shape, not a cover defect."""

    kind: ObligationKind
    subject: tuple[str, ...]
    satisfiers: tuple[str, ...]

    @property
    def identity(self) -> tuple[ObligationKind, tuple[str, ...]]:
        return (self.kind, self.subject)


def node_address(node: str) -> str:
    return node.split("~", maxsplit=1)[1].split("@", maxsplit=1)[0]


def find(parent: dict[str, str], node: str) -> str:
    while parent.get(node, node) != node:
        parent[node] = parent.get(parent[node], parent[node])
        node = parent[node]
    return node


def union(parent: dict[str, str], left: str, right: str) -> bool:
    a, b = find(parent, left), find(parent, right)
    if a == b:
        return False
    lo, hi = sorted((a, b))
    parent[hi] = lo
    return True
