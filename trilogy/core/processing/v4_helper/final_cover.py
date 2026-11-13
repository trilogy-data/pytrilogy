"""The FINAL merge's cover: which group supplies each requested concept.

Elected from stage-2 facts (group attrs, the group graph and the statement's
extent ownership), never from built nodes, so the join plan is stated over
the contributors that actually merge. See `docs/join_axis_audit_2026_10.md`.
"""

from collections import defaultdict
from collections.abc import Collection, Sequence

from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_ADDRESS
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment

from .concept_graph import _scope_and_phase
from .condition_placement import args_holding_on_absent, feeder_conditions_implied
from .constants import AGGREGATING_DERIVATIONS, FINAL_NODE_ID, GROUPING_DERIVATIONS
from .edges import EdgeKind, EdgeMap, edges_of_kind
from .group_graph import read_row_parents
from .models import ExtentOwnership, GroupAttrs
from .projection import concept_satisfiable


def exposes(group: GroupAttrs, environment: BuildEnvironment) -> frozenset[str]:
    """What a group's rows present to FINAL: its outputs; a row stream also
    the grain it is computed at, and the key of a property grain (`label`
    keyed by `tree_id`): the columns it carries its rows by. Only what its
    rows hold: a grain key it is computed at but never reads is no column of
    it, and a one-value filter (`avg(x) by * ? yr = 1999`) is one row."""
    outputs = frozenset(group.output_concepts)
    if (
        group.derivation in GROUPING_DERIVATIONS
        or group.derivation in (Derivation.ROOT, Derivation.ROWSET)
        or _single_valued(group, environment)
    ):
        return outputs
    keys = {
        key
        for address in group.grain_components
        if (c := environment.concepts.get(address)) is not None
        and c.purpose == Purpose.PROPERTY
        for key in c.keys or ()
    }
    return outputs | group.emitted_grain | (keys & group.row_columns)


def record_twin_reuse(order: Sequence[str], attrs: dict[str, GroupAttrs]) -> None:
    """Set each aggregate's `twin_reused`: the values a grouping group at the
    SAME grain, built before it in `order`, already outputs (its pre-condition
    twin). Grain equality gates out a coarser re-aggregation (avg-of-sum),
    which genuinely recomputes."""
    earlier: list[str] = []
    for gid in order:
        group = attrs.get(gid)
        if group is None or gid == FINAL_NODE_ID:
            continue
        if group.derivation in AGGREGATING_DERIVATIONS:
            group.twin_reused = frozenset(
                address
                for address in group.primary_members
                if any(
                    attrs[other].grain_components == group.grain_components
                    and address in attrs[other].output_concepts
                    for other in earlier
                )
            )
        if group.derivation in GROUPING_DERIVATIONS:
            earlier.append(gid)


def unrendered(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
) -> dict[str, frozenset[str]]:
    """What each group exposes but cannot render. A value read through a twin
    (`record_twin_reuse`) is recomputed only off a parent that survives as a
    feeder (`_feeders`); with none, the aggregate is built as its population
    alone. A value a parent lost is opaque to its readers (they read it, never
    its lineage). The named combo in `test_v4_window_basic_merge`: the
    select-phase window's value is its condition twin's, and the select-phase
    chain renders `wk`."""
    dropped: dict[str, frozenset[str]] = {}
    for gid in nx.topological_sort(group_graph):
        if gid == FINAL_NODE_ID:
            continue
        parents = read_row_parents(group_graph, group_edges, attrs, gid)
        if not parents:
            continue
        shown = {
            p: exposes(attrs[p], environment) - dropped.get(p, frozenset())
            for p in parents
        }
        available: set[str] = set().union(*shown.values())
        opaque = set().union(*(dropped.get(p, frozenset()) for p in parents))
        if attrs[gid].twin_reused:
            fed = set().union(
                *(shown[p] for p in _feeders(group_graph, attrs, gid, shown))
            )
            opaque |= {
                address
                for address in attrs[gid].twin_reused
                if (concept := environment.concepts.get(address)) is None
                or not concept_satisfiable(concept, fed)
            }
        opaque -= available
        if not opaque:
            continue
        drops = frozenset(
            address
            for address in attrs[gid].output_concepts
            if (concept := environment.concepts.get(address)) is not None
            and concept_satisfiable(concept, available | opaque)
            and not concept_satisfiable(
                concept, available, cache=dict.fromkeys(opaque, False)
            )
        )
        if drops:
            dropped[gid] = drops
    return dropped


def _feeders(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    shown: dict[str, frozenset[str]],
) -> list[str]:
    """The row parents of an aggregate reading values through its twin that
    survive as feeders: one whose every column the aggregate reads a grouping
    sibling at its grain also holds, under conditions at least as narrow, is
    a redundant re-scan (`_parent_nodes_for`)."""
    group = attrs[gid]
    reads = (set(group.output_concepts) - group.twin_reused) | {
        arg.address for atom in group.condition_atoms for arg in atom.row_arguments
    }
    return [
        p
        for p in shown
        if attrs[p].derivation in GROUPING_DERIVATIONS
        or not any(
            q != p
            and attrs[q].derivation in GROUPING_DERIVATIONS
            and shown[p] & reads <= shown[q]
            and feeder_conditions_implied(group_graph, attrs, p, q)
            for q in shown
        )
    ]


def _single_valued(group: GroupAttrs, environment: BuildEnvironment) -> bool:
    concepts = [environment.concepts.get(a) for a in group.primary_members]
    return bool(concepts) and all(
        c is not None and c.granularity == Granularity.SINGLE_ROW for c in concepts
    )


def coalescing_mates(environment: BuildEnvironment, address: str) -> frozenset[str]:
    """The other members of the COALESCING key group `address` belongs to.

    Only a coalescing relation (`union`/`full`) fuses its members onto one axis
    that the merge emits under every member's own alias, which is what lets a
    mate answer for the address. A subset or global-merge member keeps its own
    column name, so reading it under the other member's address dangles."""
    coalescing = environment.domain_graph.coalescing_relation_members()
    if address not in coalescing:
        return frozenset()
    for group in environment.scoped_join_relations():
        if address in group:
            return frozenset(group - {address}) & coalescing
    return frozenset()


def output_covers(output: BuildConcept, concept: BuildConcept) -> bool:
    """Whether a node output supplies `concept`, directly or via pseudonym.

    A struct field selected as `unnest_array.a` parses to the bare key
    `local.a` but is produced under its derivable pseudonym `unnest_array.a`;
    the CTE layer maps the two by pseudonym, so coverage matching must too."""
    return (
        output.address == concept.address
        or output.address in concept.pseudonyms
        or concept.address in output.pseudonyms
    )


def _selected_keys(
    concept: BuildConcept, mandatory_list: Sequence[BuildConcept]
) -> frozenset[str]:
    if concept.purpose != Purpose.PROPERTY or not concept.keys:
        return frozenset()
    return frozenset(concept.keys) & {c.address for c in mandatory_list}


def _row_atoms(attrs: dict[str, GroupAttrs], groups: Collection[str]) -> set[str]:
    """The WHERE atoms over row columns alone (`city = 'B'`) the groups host."""
    return {
        str(atom)
        for gid in groups
        for atom in attrs[gid].condition_atoms
        if atom.row_arguments and not any(c.is_aggregate for c in atom.row_arguments)
    }


def _rows_pass(
    gid: str,
    atom: object,
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    memo: dict[str, bool],
) -> bool:
    """Whether every row `gid` presents passed `atom`: it hosts it, or the rows
    it is built over did. An aggregate value joined beside a row stream filters
    none of its rows (`SelectGrain.gate_host_skips_select_rows`)."""
    if atom in attrs[gid].condition_atoms:
        return True
    if gid in memo:
        return memo[gid]
    memo[gid] = False
    parents = [
        p
        for p in read_row_parents(group_graph, group_edges, attrs, gid)
        if p in attrs and p != FINAL_NODE_ID
    ]
    rows = [p for p in parents if attrs[p].derivation not in GROUPING_DERIVATIONS]
    memo[gid] = bool(rows or parents) and all(
        _rows_pass(p, atom, group_graph, group_edges, attrs, memo)
        for p in rows or parents
    )
    return memo[gid]


def _single_row_value(concept: BuildConcept) -> bool:
    """A total (`count_distinct(tag) by *`): one row, which adds none wherever
    it is read."""
    return concept.is_aggregate and set(
        concept.grain.components if concept.grain else ()
    ) <= {ALL_ROWS_ADDRESS}


def _where_atoms(attrs: dict[str, GroupAttrs], groups: Collection[str]) -> list:
    seen: list = []
    for gid in groups:
        if _scope_and_phase(attrs[gid].label)[1] == "condition":
            continue
        for atom in attrs[gid].condition_atoms:
            if atom not in seen and args_holding_on_absent(atom):
                seen.append(atom)
    return seen


def elect_cover(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    groups: Collection[str],
    mandatory_list: Sequence[BuildConcept],
    environment: BuildEnvironment,
    ownership: ExtentOwnership,
) -> dict[str, list[BuildConcept]]:
    """For each mandatory concept, the most-downstream of `groups` exposing it
    (more ancestors among `groups` = further downstream). Returns `{gid:
    [concepts that group provides]}` in discovery order, so the merge renders
    with a stable join layout.

    A group carrying the concept's authored scoped-join MATE counts as exposing
    it: the merge coalesces the two members onto one axis, and the mate is
    surfaced under the concept's own address once the merge resolves.

    A ``~``-licensed key comes from its elected owner and nowhere else: that
    group is the one built with permission to manufacture the key's extension
    rows, so any other candidate exposes only the members the fact bound.

    A group exposes nothing it cannot render (`unrendered`): a select-phase
    aggregate reading its value through a condition twin is built as its
    population alone."""
    per_group: dict[str, list[BuildConcept]] = defaultdict(list)
    coalescing = environment.domain_graph.coalescing_relation_members()
    dropped = unrendered(group_graph, group_edges, attrs, environment)
    exposed = {
        gid: exposes(attrs[gid], environment) - dropped.get(gid, frozenset())
        for gid in groups
    }
    # a side-channel set feeds no rows: reading a host through one is not
    # reading that host's side
    row_graph = group_graph.copy()
    row_graph.remove_edges_from(edges_of_kind(group_edges, EdgeKind.EXISTENCE))
    row_atoms = _row_atoms(attrs, groups)

    where_atoms = _where_atoms(attrs, groups)
    memos: list[dict[str, bool]] = [{} for _ in where_atoms]

    def rows_filtered(gid: str) -> bool:
        return all(
            _rows_pass(gid, atom, group_graph, group_edges, attrs, memo)
            for atom, memo in zip(where_atoms, memos)
        )

    def skips_row_atoms(gid: str) -> bool:
        if not row_atoms or _scope_and_phase(attrs[gid].label)[1] != "condition":
            return False
        held = {
            str(atom)
            for g in nx.ancestors(group_graph, gid) | {gid}
            if g in attrs
            for atom in attrs[g].condition_atoms
        }
        return not row_atoms <= held

    for concept in mandatory_list:
        addr = concept.address
        mates = coalescing_mates(environment, addr)
        candidates = [gid for gid in groups if exposed[gid] & (mates | {addr})]
        # Only fall back to pseudonym coverage (struct fields produced under
        # their derivable origin address) when nothing provides the concept
        # directly; a plain alias keeps its own contributor.
        if not candidates:
            candidates = [
                gid
                for gid in groups
                if any(
                    output_covers(c, concept)
                    for address in exposed[gid]
                    if (c := environment.concepts.get(address)) is not None
                )
            ]
        if not candidates:
            continue
        # A COALESCING mate carries only its own side's domain: a group that
        # never read the one hosting the concept itself misses that side's rows.
        if addr in coalescing:
            hosts = {gid for gid in candidates if addr in attrs[gid].members}
            if hosts:
                candidates = [
                    gid
                    for gid in candidates
                    if gid in hosts or hosts & nx.ancestors(row_graph, gid)
                ]
        # A dim peeled off a row stream decorates the stream's keys; its own
        # key carries every entity, not the rows the stream's WHERE kept.
        # A property read beside its selected key (`tree_form` beside
        # `species`) comes with the key: a group holding it without the key
        # (`max(x) by city, tree_form`) pairs it with the key's rows on the
        # other columns alone (`SelectGrain.property_without_its_key_misjoins`).
        # A WHERE aggregate's own group reads every row, never the rows the
        # WHERE kept: an output read off it skips the row atoms placed on the
        # select's rows (`SelectGrain.gate_group_skips_row_atoms`).
        keyed = _selected_keys(concept, mandatory_list)
        candidates.sort(
            key=lambda gid: (
                keyed <= exposed[gid],
                not skips_row_atoms(gid),
                _single_row_value(concept) or rows_filtered(gid),
                sum(1 for a in nx.ancestors(group_graph, gid) if a in groups),
                addr not in attrs[gid].dim_keys,
                addr in attrs[gid].members,
            ),
            reverse=True,
        )
        winner = candidates[0]
        owner = ownership.owner_of(addr)
        # a group that READ the owner carries its rows too, and more besides
        if (
            owner is not None
            and owner in candidates
            and not any(owner in nx.ancestors(group_graph, gid) for gid in candidates)
        ):
            winner = owner
        per_group[winner].append(concept)
    return _keys_off_one_tuple(per_group, exposed, mandatory_list, groups, group_graph)


def _keys_off_one_tuple(
    per_group: dict[str, list[BuildConcept]],
    exposed: dict[str, frozenset[str]],
    mandatory_list: Sequence[BuildConcept],
    groups: Collection[str],
    group_graph: nx.DiGraph,
) -> dict[str, list[BuildConcept]]:
    """The selected keys come off one group's rows when no winner holds them
    all: `city` off a group by `(city, tree_form)` and `species` off one by
    `species` pair every city with every species of a form, tuples no row
    holds (`SelectGrain.keys_off_two_groups_pair_unheld`)."""
    keys = [
        c for c in mandatory_list if c.purpose == Purpose.KEY and not c.is_aggregate
    ]
    addrs = frozenset(c.address for c in keys)
    winners = {
        gid for gid, concepts in per_group.items() for c in concepts if c in keys
    }
    if len(winners) < 2 or any(addrs <= exposed[gid] for gid in winners):
        return per_group
    holders = [gid for gid in groups if addrs <= exposed[gid]]
    if not holders:
        return per_group
    holder = max(
        holders,
        key=lambda gid: (
            sum(1 for a in nx.ancestors(group_graph, gid) if a in groups),
            gid,
        ),
    )
    out: dict[str, list[BuildConcept]] = defaultdict(list)
    for gid, concepts in per_group.items():
        for c in concepts:
            out[holder if c.address in addrs else gid].append(c)
    return out
