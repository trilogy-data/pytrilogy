"""What each group's node renders, stated from stage-2 facts.

`output_concepts` is what readers read off a group; its node renders a little
more and a little less: the preserve keys its readers ask that it can hold,
its row identity, the relation members a scan computes, and never a value a
twin read through (`final_cover.unrendered`). The FINAL add passes read this
instead of built nodes. See `docs/join_axis_audit_2026_10.md` §7.
"""

from collections import defaultdict

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.models.build_environment import BuildEnvironment

from .constants import FINAL_NODE_ID, GROUPING_DERIVATIONS
from .edges import EdgeKind, EdgeMap, edge_kind
from .final_cover import _single_valued, coalescing_mates, exposes, unrendered
from .group_graph import read_row_parents
from .join_plan import (
    _determined,
    _is_handle,
    _over_own_rows,
    _renderable,
    _widenable,
    lineage_pinned_grain,
)
from .models import GroupAttrs


def renders(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
) -> dict[str, frozenset[str]]:
    dropped = unrendered(group_graph, group_edges, attrs, environment)
    asked: dict[str, set[str]] = defaultdict(set)
    for group in attrs.values():
        for contract in group.input_contracts:
            asked[contract.parent_group_id] |= contract.preserve_keys
    return {
        gid: _renders(group_graph, group_edges, attrs, gid, asked[gid], environment)
        - dropped.get(gid, frozenset())
        for gid in attrs
        if gid != FINAL_NODE_ID
    }


def _renders(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    asked: set[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """A one-value group renders its value. Otherwise a group renders what it
    exposes, and each preserve key a reader asks that it can hold; a rowset
    handle or relation member only under a name it reads (an aggregate by
    `kb` over side a's rows renders the axis as `ka`)."""
    group = attrs[gid]
    if _single_valued(group, environment):
        return exposes(group, environment)
    members = _relation_members(environment)
    held = (
        set(exposes(group, environment))
        | _over_own_rows(group)
        | {
            a
            for a in asked & _widenable(group_graph, attrs, gid, environment)
            if a in group.row_columns
            or not (_is_handle(a, environment) or a in members)
        }
    )
    if group.derivation in GROUPING_DERIVATIONS:
        # its GROUP BY projects its outputs: a carried key it groups by
        # without outputting is no column of it
        held -= set(group.members) - set(group.output_concepts)
        held |= _side_handles(group_graph, group_edges, attrs, gid, environment)
    else:
        # a scan projects what it is asked; a member the demand pass dropped
        # is no column of it
        held |= set(
            group.output_concepts
            if group.derivation == Derivation.ROOT and group.output_concepts
            else group.members
        )
        # a row stream over source rows holds the row keys it is grained at
        if group.derivation != Derivation.ROWSET and not lineage_pinned_grain(
            group.output_concepts or group.members, environment
        ):
            held |= group.grain_components
    if group.derivation in (Derivation.ROOT, Derivation.ROWSET):
        # a scan or boundary computes an expression member off its arguments
        held |= _renderable(frozenset(held), members, environment)
    if group.derivation == Derivation.ROOT:
        held |= _advertised(held, environment)
    if group.derivation == Derivation.ROWSET:
        held = {
            a
            for a in held
            if a in group.primary_members or not _is_handle(a, environment)
        }
    return frozenset(held)


def _relation_members(environment: BuildEnvironment) -> set[str]:
    return {m for relation in environment.scoped_join_relations() for m in relation}


def _side_handles(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    environment: BuildEnvironment,
) -> set[str]:
    """A grouping group reading the rowsets of a coalescing relation's sides
    renders each side's handle beside its own (`pop:a|b` by `b.k` renders
    `a.k`)."""
    boundaries: set[str] = set().union(
        *(
            attrs[p].output_concepts
            for p in read_row_parents(group_graph, group_edges, attrs, gid)
            if attrs[p].derivation == Derivation.ROWSET
        )
    )
    return {
        mate
        for address in attrs[gid].output_concepts
        for mate in coalescing_mates(environment, address)
        if mate in boundaries and _is_handle(mate, environment)
    }


def _advertised(held: set[str], environment: BuildEnvironment) -> set[str]:
    """A scan rendering one member of a relation renders every member a
    datasource can bind (`ka`, `kb`, `kc` beside a `union join`); a rowset
    handle is no scan's column."""
    return {
        mate
        for relation in environment.scoped_join_relations()
        if relation & held
        for mate in relation
        if not _is_handle(mate, environment)
    }


def parents_render(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    rendered: dict[str, frozenset[str]],
    environment: BuildEnvironment,
) -> set[str]:
    """What `gid`'s node reads off its parents. A grouping reader slices its
    ROOT parents to what its grain determines; a row stream beside a grouped
    parent projects its row parents to its grain
    (`_satisfy_parent_projection_contract`). A CONSTRAINT parent is joined in
    whole to apply its condition."""
    group = attrs[gid]
    rows = read_row_parents(group_graph, group_edges, attrs, gid)
    sliced: set[Derivation] = set()
    if group.grain_components:
        if group.derivation in GROUPING_DERIVATIONS:
            sliced = {Derivation.ROOT}
        elif any(attrs[p].derivation in GROUPING_DERIVATIONS for p in rows):
            sliced = {Derivation.ROOT, Derivation.BASIC, Derivation.FILTER}
    determined = _determined(group.grain_components, environment)
    constraints = [
        p
        for p in group_graph.predecessors(gid)
        if p in rendered and edge_kind(group_edges, p, gid) == EdgeKind.CONSTRAINT
    ]
    return set().union(
        *(
            rendered[p] & determined if attrs[p].derivation in sliced else rendered[p]
            for p in rows
        ),
        *(rendered[p] for p in constraints),
    )


def declared_partial(group: GroupAttrs, environment: BuildEnvironment) -> set[str]:
    """What a group holds only partially by declaration: a declared subset
    side of a relation."""
    subset = environment.domain_graph.subset_join_map()
    return {a for a in (*group.members, *group.output_concepts) if a in subset}
