"""Concept domain graph (docs/domain_graph_design.md, phase 3).

One directed graph over concept ADDRESSES carrying two edge families:

* Domain edges — how concepts' VALUE SETS relate: SUBSET (source ⊑ target),
  EQUAL (source ≡ target), INCOMPARABLE (source ∦ target — declared
  never-narrow). Each edge carries a provenance, the scope the declaration
  was authored at, and an optional condition the relation holds under
  (a filtered derivation is a subset edge carrying its filter).
* FD edges — which concept tuples uniquely determine which others, scoped to
  the population where the dependency holds (None = the concept's full
  domain; a datasource identifier = that source's row population).

Author environments carry NO graph state: declared edges derive on demand
from ``Environment.merges`` (global scope) and statement join clauses
(overlay scopes), so authoring stays light. The full graph — declared +
structural + binding + FD edges — is assembled once per build and lives on
``BuildEnvironment.domain_graph``. Query-scoped joins are an overlay over
the declared base, never a mutation.

Bindings are recorded as ``BindingEdge`` population facts (datasource
projection ⊑/≡ concept) rather than synthetic domain nodes; FD scopes
reference the same populations by datasource identifier.

Invariants (phase 2): domains are value sets — NULL is not a value and
nullability is a separate axis; row multiplicity is the FD family's job.
Declared edges are trusted (a lying declaration is an author error);
structural and binding edges are true by construction. All derivations are
deterministic in edge insertion order.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Optional

from trilogy.core.enums import JoinType


class DomainRelation(Enum):
    SUBSET = "subset"
    EQUAL = "equal"
    INCOMPARABLE = "incomparable"


class EdgeProvenance(Enum):
    DECLARED = "declared"
    STRUCTURAL = "structural"
    BINDING = "binding"


class EdgeScope(Enum):
    GLOBAL = "global"  # environment `merge` statement
    STATEMENT = "statement"  # outer query join clause
    ROWSET = "rowset"  # join clause inside a rowset body


class ResolvedRelation(Enum):
    SUBSET = "subset"
    SUPERSET = "superset"
    EQUAL = "equal"
    INCOMPARABLE = "incomparable"
    UNKNOWN = "unknown"


@dataclass
class DomainEdge:
    """source RELATION target; SUBSET reads `source ⊑ target`."""

    source: str
    target: str
    relation: DomainRelation
    provenance: EdgeProvenance = EdgeProvenance.DECLARED
    scope: EdgeScope = EdgeScope.GLOBAL
    # BoolExpr-ish object the relation holds under (e.g. a filter derivation's
    # WHERE). Opaque to the graph; compared via condition_implies by consumers.
    condition: Any = None

    def identity(self) -> tuple:
        return (
            self.source,
            self.target,
            self.relation,
            self.provenance,
            self.scope,
            str(self.condition) if self.condition is not None else None,
        )


@dataclass
class BindingEdge:
    """A datasource column binding: the source's projection of `concept`.

    complete=True means the projection carries the concept's full domain
    (projection ≡ concept); complete=False (`~` binding) means a proper
    projection ⊑ concept. `condition` carries non_partial_for when present.
    """

    datasource: str
    safe_identifier: str
    concept: str
    complete: bool = True
    condition: Any = None

    def identity(self) -> tuple:
        return (self.datasource, self.concept, self.complete)


@dataclass
class FDEdge:
    """determinants → dependent, holding on `scope` (a datasource identifier
    population) or globally (scope=None)."""

    determinants: frozenset[str]
    dependent: str
    provenance: EdgeProvenance = EdgeProvenance.DECLARED
    scope: Optional[str] = None

    def identity(self) -> tuple:
        return (self.determinants, self.dependent, self.scope)


def declared_edge_from_join(
    source: str, target: str, join_type: JoinType, scope: EdgeScope
) -> DomainEdge | None:
    """Translate a scoped-join/merge tuple into its domain declaration.

    Tuple convention (merge_to_join / join normalization): LEFT_OUTER(s, t)
    anchors on `s` with `t` the subset side, so it declares t ⊑ s. FULL
    declares EQUAL when authored globally (`merge a into b` asserts one
    identity) but INCOMPARABLE at query scope (`union join` / `full join`
    assert neither domain contains the other). Other join types declare
    nothing about domains.
    """
    if join_type is JoinType.LEFT_OUTER:
        return DomainEdge(
            source=target, target=source, relation=DomainRelation.SUBSET, scope=scope
        )
    if join_type is JoinType.FULL:
        relation = (
            DomainRelation.EQUAL
            if scope is EdgeScope.GLOBAL
            else DomainRelation.INCOMPARABLE
        )
        return DomainEdge(source=source, target=target, relation=relation, scope=scope)
    return None


class DomainGraph:
    def __init__(
        self,
        edges: Iterable[DomainEdge] | None = None,
        binding_edges: Iterable[BindingEdge] | None = None,
        fd_edges: Iterable[FDEdge] | None = None,
    ):
        self.edges: list[DomainEdge] = []
        self.binding_edges: list[BindingEdge] = []
        self.fd_edges: list[FDEdge] = []
        self._edge_keys: set[tuple] = set()
        self._binding_keys: set[tuple] = set()
        self._fd_keys: set[tuple] = set()
        self._canonical: dict[str, str] | None = None
        for e in edges or []:
            self.add_edge(e)
        for b in binding_edges or []:
            self.add_binding(b)
        for f in fd_edges or []:
            self.add_fd(f)

    @classmethod
    def from_scoped_joins(
        cls, joins: Iterable[tuple[tuple[str, str, JoinType], EdgeScope]]
    ) -> "DomainGraph":
        """Build a declared-edge graph from scope-tagged join tuples, in order
        (canonical roots are order-sensitive, matching the historical
        union-find contract)."""
        graph = cls()
        for (source, target, join_type), scope in joins:
            edge = declared_edge_from_join(source, target, join_type, scope)
            if edge is not None:
                graph.add_edge(edge)
        return graph

    def add_edge(self, edge: DomainEdge) -> bool:
        key = edge.identity()
        if key in self._edge_keys:
            return False
        self._edge_keys.add(key)
        self.edges.append(edge)
        self._canonical = None
        return True

    def add_binding(self, edge: BindingEdge) -> bool:
        key = edge.identity()
        if key in self._binding_keys:
            return False
        self._binding_keys.add(key)
        self.binding_edges.append(edge)
        return True

    def add_fd(self, edge: FDEdge) -> bool:
        key = edge.identity()
        if key in self._fd_keys:
            return False
        self._fd_keys.add(key)
        self.fd_edges.append(edge)
        return True

    def with_overlay(self, edges: Iterable[DomainEdge] | None = None) -> "DomainGraph":
        """Copy-on-write view: a new graph with this graph's edges plus the
        statement overlay. Never mutates the base."""
        return DomainGraph(
            edges=[*self.edges, *(edges or [])],
            binding_edges=self.binding_edges,
            fd_edges=self.fd_edges,
        )

    # --- canonical collapse (union-find over declared edges) ---------------

    def canonical_map(self) -> dict[str, str]:
        """Collapse declared edges into address -> canonical-root map (only
        non-root entries), reproducing the historical scoped-merge union-find:
        a SUBSET edge roots on its superset (target); EQUAL/INCOMPARABLE root
        on the authored target. Processed in insertion order."""
        if self._canonical is not None:
            return self._canonical
        parent: dict[str, str] = {}

        def find(x: str) -> str:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for edge in self.edges:
            if edge.provenance is not EdgeProvenance.DECLARED:
                continue
            if edge.relation is DomainRelation.SUBSET:
                # preserve historical find() call order: anchor (superset)
                # first, then the subset side, rooting on the anchor.
                anchor, other = find(edge.target), find(edge.source)
                if anchor != other:
                    parent[other] = anchor
            else:
                source_root, target_root = find(edge.source), find(edge.target)
                if source_root != target_root:
                    parent[source_root] = target_root
        self._canonical = {a: find(a) for a in parent if find(a) != a}
        return self._canonical

    def canonical(self, address: str) -> str:
        return self.canonical_map().get(address, address)

    # --- registry derivations (phase 3 step 1 compat shims) ----------------

    def subset_sources(self) -> set[str]:
        """The subset side of every declared SUBSET relation, by its OWN
        address (historical scoped_partial_sources)."""
        return {
            e.source
            for e in self.edges
            if e.relation is DomainRelation.SUBSET
            and e.provenance is EdgeProvenance.DECLARED
        }

    def subset_join_map(self) -> dict[str, str]:
        """subset address -> its canonical superset counterpart."""
        canonical = self.canonical_map()
        return {
            addr: canonical[addr] for addr in self.subset_sources() if addr in canonical
        }

    def outer_relation_keys(self) -> set[str]:
        """Canonicalized endpoints of every EQUAL/INCOMPARABLE declaration —
        the keys whose authored preservation the outer-join upgrade must never
        collapse (historical full_join_keys / scoped_full_join_keys)."""
        return {
            self.canonical(addr)
            for e in self.edges
            if e.relation in (DomainRelation.EQUAL, DomainRelation.INCOMPARABLE)
            and e.provenance is EdgeProvenance.DECLARED
            for addr in (e.source, e.target)
        }

    def statement_incomparable_keys(self) -> set[str]:
        """Canonicalized endpoints of statement-scoped ∦ declarations
        (historical statement_full_keys)."""
        return {
            self.canonical(addr)
            for e in self.edges
            if e.relation is DomainRelation.INCOMPARABLE
            and e.scope is EdgeScope.STATEMENT
            for addr in (e.source, e.target)
        }

    def equal_narrowable_keys(self) -> set[str]:
        """Canonicalized endpoints of EQUAL declarations, minus keys also
        authored as a statement-scoped ∦ (historical equal_join_keys): an
        EQUAL key may narrow to INNER once completeness tests pass; a ∦ key
        keeps the veto."""
        return {
            self.canonical(addr)
            for e in self.edges
            if e.relation is DomainRelation.EQUAL
            and e.provenance is EdgeProvenance.DECLARED
            for addr in (e.source, e.target)
        } - self.statement_incomparable_keys()

    def left_anchor_keys(self) -> set[str]:
        """Canonicalized superset anchors of declared SUBSET relations
        (historical scoped_left_anchor_keys)."""
        return {
            self.canonical(e.target)
            for e in self.edges
            if e.relation is DomainRelation.SUBSET
            and e.provenance is EdgeProvenance.DECLARED
        }

    def join_key_groups(self) -> dict[str, set[str]]:
        """Authored join-key equivalence groups, canonical -> all members."""
        groups: dict[str, set[str]] = {}
        for source, target in self.canonical_map().items():
            groups.setdefault(target, {target}).add(source)
        return groups

    def binding_sources(self, address: str) -> set[str]:
        """Identifiers of datasources natively binding `address`."""
        out: set[str] = set()
        for b in self.binding_edges:
            if b.concept == address:
                out.add(b.datasource)
                out.add(b.safe_identifier)
        return out

    # --- resolution semantics ----------------------------------------------

    def _equivalence_classes(self) -> dict[str, str]:
        """Representative map under EQUAL edges (declared or structural,
        unconditioned) — the ≡-classes the partial order lives over."""
        parent: dict[str, str] = {}

        def find(x: str) -> str:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for edge in self.edges:
            if edge.relation is DomainRelation.EQUAL and edge.condition is None:
                left_root, right_root = find(edge.source), find(edge.target)
                if left_root != right_root:
                    parent[left_root] = right_root
        return {a: find(a) for a in parent}

    def _subset_reachable(self, start: str, goal: str, rep: dict[str, str]) -> bool:
        """Directed reachability start ⊑ ... ⊑ goal over SUBSET edges between
        ≡-classes. Edge conditions identify WHICH subset, so they never weaken
        containment."""
        successors: dict[str, set[str]] = {}
        for edge in self.edges:
            if edge.relation is DomainRelation.SUBSET:
                successors.setdefault(rep.get(edge.source, edge.source), set()).add(
                    rep.get(edge.target, edge.target)
                )
        seen = {start}
        frontier = [start]
        while frontier:
            node = frontier.pop()
            for nxt in sorted(successors.get(node, ())):
                if nxt == goal:
                    return True
                if nxt not in seen:
                    seen.add(nxt)
                    frontier.append(nxt)
        return False

    def relation(
        self,
        left: str,
        right: str,
        left_condition: Any = None,
        right_condition: Any = None,
    ) -> ResolvedRelation:
        """Resolve how left's domain relates to right's: ⊑ / ⊒ / ≡ / ∦ /
        unknown. Conditions refine same-class comparisons: (x|c1) ⊑ (x|c2)
        when c1 ⇒ c2 (condition_implies)."""
        rep = self._equivalence_classes()
        left_class, right_class = rep.get(left, left), rep.get(right, right)
        if left_class == right_class:
            return self._conditioned_same_class(left_condition, right_condition)
        for edge in self.edges:
            if edge.relation is not DomainRelation.INCOMPARABLE:
                continue
            ends = {
                rep.get(edge.source, edge.source),
                rep.get(edge.target, edge.target),
            }
            if ends == {left_class, right_class}:
                return ResolvedRelation.INCOMPARABLE
        forward = self._subset_reachable(left_class, right_class, rep)
        backward = self._subset_reachable(right_class, left_class, rep)
        if forward and backward:
            return ResolvedRelation.EQUAL
        if forward:
            return ResolvedRelation.SUBSET
        if backward:
            return ResolvedRelation.SUPERSET
        return ResolvedRelation.UNKNOWN

    @staticmethod
    def _conditioned_same_class(
        left_condition: Any, right_condition: Any
    ) -> ResolvedRelation:
        if left_condition is None and right_condition is None:
            return ResolvedRelation.EQUAL
        if right_condition is None:
            return ResolvedRelation.SUBSET
        if left_condition is None:
            return ResolvedRelation.SUPERSET
        from trilogy.core.processing.condition_utility import condition_implies

        forward = condition_implies(left_condition, right_condition)
        backward = condition_implies(right_condition, left_condition)
        if forward and backward:
            return ResolvedRelation.EQUAL
        if forward:
            return ResolvedRelation.SUBSET
        if backward:
            return ResolvedRelation.SUPERSET
        return ResolvedRelation.UNKNOWN

    def determines(
        self,
        determinants: Iterable[str],
        dependent: str,
        population: Optional[str] = None,
    ) -> bool:
        """FD closure membership: does the determinant tuple uniquely fix
        `dependent` for the rows of `population` (a datasource identifier, or
        None for the global domain)?

        An FD scoped to a source population applies globally only when that
        source binds all involved concepts completely (the complete-binding
        rule); otherwise it applies only when queried against its own
        population. Closure is transitive over ≡-classes.
        """
        rep = self._equivalence_classes()

        def canon(x: str) -> str:
            return rep.get(x, x)

        complete_bindings: dict[str, set[str]] = {}
        for b in self.binding_edges:
            if b.complete and b.condition is None:
                complete_bindings.setdefault(b.datasource, set()).add(canon(b.concept))

        def applies(fd: FDEdge) -> bool:
            if fd.scope is None or fd.scope == population:
                return True
            involved = {canon(a) for a in fd.determinants} | {canon(fd.dependent)}
            return involved <= complete_bindings.get(fd.scope, set())

        closure = {canon(a) for a in determinants}
        goal = canon(dependent)
        changed = True
        while changed:
            if goal in closure:
                return True
            changed = False
            for fd in self.fd_edges:
                if not applies(fd):
                    continue
                if {canon(a) for a in fd.determinants} <= closure:
                    dep = canon(fd.dependent)
                    if dep not in closure:
                        closure.add(dep)
                        changed = True
        return goal in closure

    # --- contradiction lint --------------------------------------------------

    def contradicts(self, edge: DomainEdge) -> str | None:
        """Reason the declaration contradicts what the graph already knows,
        or None. Declared edges are trusted, so this fires only on internal
        inconsistency — the lying-declaration DATA check stays opt-in
        (validate_domains)."""
        existing = self.relation(edge.source, edge.target)
        if edge.relation is DomainRelation.INCOMPARABLE:
            if existing in (
                ResolvedRelation.SUBSET,
                ResolvedRelation.SUPERSET,
                ResolvedRelation.EQUAL,
            ):
                return (
                    f"'{edge.source}' and '{edge.target}' are already related "
                    f"({existing.value}); declaring them incomparable (union) "
                    "contradicts that relation"
                )
            return None
        if existing is ResolvedRelation.INCOMPARABLE:
            return (
                f"'{edge.source}' and '{edge.target}' are declared incomparable "
                f"(union); declaring a {edge.relation.value} relation contradicts that"
            )
        if edge.relation is DomainRelation.SUBSET:
            reversed_structural = any(
                e.relation is DomainRelation.SUBSET
                and e.provenance is EdgeProvenance.STRUCTURAL
                and e.condition is not None
                and e.source == edge.target
                and e.target == edge.source
                for e in self.edges
            )
            if reversed_structural:
                return (
                    f"'{edge.target}' is derived from '{edge.source}' with a filter, "
                    f"so '{edge.target}' ⊑ '{edge.source}' structurally; declaring "
                    f"'{edge.source}' as the subset side is contradictory — "
                    "reversed operands?"
                )
        return None


# --- build-time edge minting (author facts -> full graph) --------------------
# Imports are deferred: this module must stay importable from environment.py
# and build.py without model-import cycles.


def structural_domain_edge(concept: Any) -> DomainEdge | None:
    """The domain edge a derived concept's lineage implies, if any.

    Filter derivation `x' <- x ? cond` and a filtered rowset body give a
    subset edge carrying the filter; an unfiltered rowset output is
    value-equal to its body concept (grouping preserves the value set).
    BASIC image edges are deferred (they transfer completeness but not
    distinctness without an injectivity table — design doc open question 2).
    """
    from trilogy.core.models.author import ConceptRef, FilterItem, RowsetItem

    lineage = getattr(concept, "lineage", None)
    if isinstance(lineage, FilterItem) and isinstance(lineage.content, ConceptRef):
        return DomainEdge(
            source=concept.address,
            target=lineage.content.address,
            relation=DomainRelation.SUBSET,
            provenance=EdgeProvenance.STRUCTURAL,
            condition=lineage.where.conditional,
        )
    if isinstance(lineage, RowsetItem):
        body = lineage.rowset.select
        where = getattr(body, "where_clause", None)
        having = getattr(body, "having_clause", None)
        restricting = where if where is not None else having
        if restricting is not None:
            # a HAVING also restricts the body; the edge carries the WHERE
            # when both exist (condition completeness is a consumer concern
            # once the narrowing pass migrates — direction is what matters).
            condition = restricting.conditional
            return DomainEdge(
                source=concept.address,
                target=lineage.content.address,
                relation=DomainRelation.SUBSET,
                provenance=EdgeProvenance.STRUCTURAL,
                condition=condition,
            )
        return DomainEdge(
            source=concept.address,
            target=lineage.content.address,
            relation=DomainRelation.EQUAL,
            provenance=EdgeProvenance.STRUCTURAL,
        )
    return None


def _unique_concepts(environment: Any) -> Iterable[Any]:
    seen: set[str] = set()
    for concept in environment.concepts.values():
        address = getattr(concept, "address", None)
        if address is None or address in seen:
            continue
        seen.add(address)
        yield concept


def mint_structural_edges(environment: Any) -> list[DomainEdge]:
    out: list[DomainEdge] = []
    for concept in _unique_concepts(environment):
        edge = structural_domain_edge(concept)
        if edge is not None:
            out.append(edge)
    return out


def mint_binding_edges(environment: Any) -> list[BindingEdge]:
    out: list[BindingEdge] = []
    for datasource in environment.datasources.values():
        non_partial_for = getattr(datasource, "non_partial_for", None)
        for column in datasource.columns:
            complete = column.is_complete
            out.append(
                BindingEdge(
                    datasource=datasource.identifier,
                    safe_identifier=datasource.safe_identifier,
                    concept=column.concept.address,
                    complete=complete,
                    condition=(
                        non_partial_for.conditional
                        if not complete and non_partial_for is not None
                        else None
                    ),
                )
            )
    return out


def mint_fd_edges(environment: Any) -> list[FDEdge]:
    out: list[FDEdge] = []
    for concept in _unique_concepts(environment):
        keys = getattr(concept, "keys", None)
        if keys:
            out.append(
                FDEdge(
                    determinants=frozenset(keys),
                    dependent=concept.address,
                    provenance=EdgeProvenance.DECLARED,
                )
            )
    for datasource in environment.datasources.values():
        grain = {c for c in datasource.grain.components}
        if not grain:
            continue
        for column in datasource.columns:
            address = column.concept.address
            if address in grain:
                continue
            out.append(
                FDEdge(
                    determinants=frozenset(grain),
                    dependent=address,
                    provenance=EdgeProvenance.BINDING,
                    scope=datasource.identifier,
                )
            )
    return out


def assemble_full_graph(environment: Any, declared: DomainGraph) -> DomainGraph:
    """The BuildEnvironment graph: the declared edges (global + this build's
    scoped overlay, already collected in `declared`) plus structural, binding
    and FD edges minted from the author environment. Linear in model size."""
    graph = DomainGraph(edges=declared.edges)
    for edge in mint_structural_edges(environment):
        graph.add_edge(edge)
    for binding in mint_binding_edges(environment):
        graph.add_binding(binding)
    for fd in mint_fd_edges(environment):
        graph.add_fd(fd)
    return graph
