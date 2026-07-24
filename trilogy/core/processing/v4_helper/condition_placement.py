"""Condition placement for the v4 group graph.

The planner is intentionally side-effect free: it inspects the group graph and
returns the groups that should receive each decomposed WHERE atom. The caller is
responsible for mutating ``GroupAttrs.condition_atoms``.
"""

from dataclasses import dataclass
from enum import Enum

from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.enums import Derivation
from trilogy.core.models.build import BoolExpr, BuildConcept, BuildWhereClause
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.discovery_utility import _output_is_rootless
from trilogy.core.processing.node_generators.presence_probe import is_presence_probe

from .constants import FINAL_NODE_ID, DepthLabel, EdgeKind
from .edges import EdgeMap, lineage_subgraph, subgraph_of_kinds
from .models import GroupBucket

ROOT_D1_DEPTH = DepthLabel.ROOT_D1

_CANNOT_HOST_OWN_OUTPUT: set[Derivation] = {
    Derivation.WINDOW,
    Derivation.UNNEST,
}

_EMITS_GROUP_BY: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.GROUP_TO,
}


class PlacementReason(Enum):
    UPSTREAM_MOST = "upstream_most"
    FINAL_RECONVERGENCE = "final_reconvergence"
    FINAL_CROSS_GRAIN_AGGREGATE = "final_cross_grain_aggregate"
    DISCONNECTED_GATE = "disconnected_gate"


@dataclass(frozen=True)
class ConditionPlacement:
    atom: BoolExpr
    group_ids: tuple[str, ...]
    reason: PlacementReason


def main_lineage_groups(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    buckets: dict[str, GroupBucket],
    mandatory_list: list[BuildConcept],
) -> set[str]:
    """Groups in the user-facing pipeline.

    These are the groups whose primary members produce a mandatory concept,
    plus all their lineage-edge ancestors. Used to bias condition placement
    away from existence-only side channels.
    """
    mandatory_addrs = {c.address for c in mandatory_list}
    seeds = {
        gid for gid, b in buckets.items() if mandatory_addrs & set(b.primary_members)
    }
    if not seeds:
        return set(buckets.keys())
    lineage_only = lineage_subgraph(group_graph, group_edges)
    main: set[str] = set(seeds)
    for seed in seeds:
        main |= nx.ancestors(lineage_only, seed)
    return main


def _reachable_input(
    gid: str,
    lineage_ancestors_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
    group_members: dict[str, set[str]],
) -> set[str]:
    own = (
        set(group_members.get(gid, set()))
        if buckets.get(gid) and buckets[gid].derivation == Derivation.ROOT
        else set()
    )
    ancestors = nx.ancestors(lineage_ancestors_graph, gid)
    if not ancestors:
        return own | set(group_members.get(gid, set()))
    reachable: set[str] = set(own)
    for anc in ancestors:
        reachable |= group_members.get(anc, set())
    return reachable


def _candidate_groups(
    atom: BoolExpr,
    row_inputs: set[str],
    group_members: dict[str, set[str]],
    d1_root_ids: set[str],
    lineage_ancestors_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
) -> list[str]:
    candidates = [
        gid
        for gid in group_members
        if gid not in d1_root_ids
        and row_inputs
        <= _reachable_input(gid, lineage_ancestors_graph, buckets, group_members)
    ]
    if not candidates:
        # No single group (with its ancestors) covers every input, but each
        # input may still be produced by SOME group — e.g. a rowset output
        # compared against a bare aggregate co-grained over that same rowset
        # (`sct.total_spent > 0.5 * max_total`): the branches only reconverge
        # at FINAL, so return empty and let the caller route the atom there.
        # Raise only for an input no group produces at all — that predicate
        # would otherwise be silently dropped.
        produced = set().union(*group_members.values()) if group_members else set()
        missing = row_inputs - produced
        if missing:
            raise ValueError(
                f"Could not place condition atom {atom}: row inputs "
                f"{sorted(missing)} not produced by any group."
            )
        return []
    return [
        gid
        for gid in candidates
        if buckets.get(gid) is None
        or buckets[gid].derivation not in _CANNOT_HOST_OWN_OUTPUT
    ]


def _producer_closures(
    row_inputs: set[str],
    group_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
) -> list[set[str]]:
    closures: list[set[str]] = []
    for addr in row_inputs:
        producers = [
            gid
            for gid, b in buckets.items()
            if b.depth_label == DepthLabel.D0 and addr in set(b.primary_members)
        ]
        if not producers:
            continue
        reach: set[str] = set(producers)
        for producer in producers:
            reach |= nx.descendants(group_graph, producer)
        closures.append(reach)
    return closures


def _producer_groups(
    row_inputs: set[str],
    buckets: dict[str, GroupBucket],
) -> set[str]:
    producer_groups: set[str] = set()
    for addr in row_inputs:
        producers = [
            gid for gid, b in buckets.items() if addr in set(b.primary_members)
        ]
        main_producers = [
            gid
            for gid in producers
            if buckets[gid].depth_label not in (DepthLabel.D1, ROOT_D1_DEPTH)
        ]
        producer_groups.update(main_producers or producers)
    return producer_groups


def _aggregate_outputs(
    row_inputs: set[str],
    buckets: dict[str, GroupBucket],
) -> set[str]:
    return {
        addr
        for addr in row_inputs
        if any(
            b.derivation in _EMITS_GROUP_BY and addr in set(b.primary_members)
            for b in buckets.values()
        )
    }


def _post_aggregation_producers(
    row_inputs: set[str],
    buckets: dict[str, GroupBucket],
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
) -> set[str]:
    """Producer groups of row inputs that are GLOBAL post-aggregation VALUES:
    a `by *` aggregate output (grain = the abstract all_rows marker, or no
    grain at all), or a derived concept (e.g. a bool BASIC wrapping one) whose
    producer sits lineage-downstream of such an aggregate. An atom referencing
    a global value is a 0/1-row gate — it may only be hosted at the value's
    producer (HAVING) or downstream of it; an upstream scan carrying the
    address as a computable member re-renders the aggregate inline at the
    HOSTING group's grain, silently turning the global gate into a per-grain
    HAVING. Deliberately untouched: a GRAINED aggregate value (its keyed
    consumers are legitimate join hosts — `web_total > store_total` joins both
    aggregate CTEs on the shared grain) and a MIXED atom pairing a global
    value with row-level inputs (`account_balance > avg_bal by *`, TPC-H q22
    — there the host is the row group with the global CTE cross-joined in, so
    pinning to the producer chain strands the row side). Pin only when EVERY
    row input is a global post-aggregation value."""
    all_rows_address = f"{INTERNAL_NAMESPACE}.{ALL_ROWS_CONCEPT}"
    lineage_only = lineage_subgraph(group_graph, group_edges)

    def _is_global(gid: str) -> bool:
        return set(buckets[gid].grain_components) <= {all_rows_address}

    producers: set[str] = set()
    for addr in row_inputs:
        producer: str | None = None
        for gid, b in buckets.items():
            if addr not in set(b.primary_members):
                continue
            if b.derivation in _EMITS_GROUP_BY and _is_global(gid) or (
                b.derivation not in _EMITS_GROUP_BY
                and _is_global(gid)
                and gid in lineage_only
                and any(
                    anc in buckets
                    and buckets[anc].derivation in _EMITS_GROUP_BY
                    and _is_global(anc)
                    for anc in nx.ancestors(lineage_only, gid)
                )
            ):
                producer = gid
            break
        if producer is None:
            return set()
        producers.add(producer)
    return producers


def _routes_to_final_for_cross_grain_aggregates(
    agg_outputs: set[str],
    buckets: dict[str, GroupBucket],
) -> bool:
    if not agg_outputs:
        return False
    agg_grains = {
        frozenset(b.grain_components)
        for b in buckets.values()
        if b.derivation in _EMITS_GROUP_BY and set(b.primary_members) & agg_outputs
    }
    return len(agg_grains) > 1


def _upstream_most(
    candidates: list[str],
    lineage_ancestors_graph: nx.DiGraph,
) -> list[str]:
    cand_set = set(candidates)
    return [
        gid
        for gid in candidates
        if not (cand_set & nx.ancestors(lineage_ancestors_graph, gid))
    ]


def _choose_groups(
    candidates: list[str],
    lineage_ancestors_graph: nx.DiGraph,
    main_lineage: set[str],
) -> tuple[str, ...]:
    upstream_most = _upstream_most(candidates, lineage_ancestors_graph)
    main_upstream = [gid for gid in upstream_most if gid in main_lineage]
    main_cands = [gid for gid in candidates if gid in main_lineage]
    if main_upstream:
        return tuple(main_upstream)
    if upstream_most:
        return tuple(upstream_most)
    if main_cands:
        return tuple(main_cands)
    return tuple(candidates)


def _validate_not_pushed_past_independent_barrier(
    atom: BoolExpr,
    chosen_groups: tuple[str, ...],
    producer_groups: set[str],
    d0_group_ids: set[str],
    group_graph: nx.DiGraph,
) -> None:
    consumed_barriers: set[str] = set(producer_groups)
    for producer in producer_groups:
        consumed_barriers |= nx.ancestors(group_graph, producer)
    for chosen in chosen_groups:
        chosen_ancestors = nx.ancestors(group_graph, chosen)
        offending = (d0_group_ids & chosen_ancestors) - consumed_barriers
        if offending:
            raise ValueError(
                f"Atom {atom} would be injected at {chosen}, which is "
                f"downstream of d0 barrier(s) {sorted(offending)}; "
                f"conditions cannot be pushed past row-shape changes."
            )


def plan_condition_placements(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    scoped_join_key_groups: dict[str, set[str]] | None = None,
) -> list[ConditionPlacement]:
    """Return where each decomposed condition atom should be injected."""
    scoped_join_key_groups = scoped_join_key_groups or {}
    scoped_join_member_addresses = frozenset(
        addr
        for canonical, members in scoped_join_key_groups.items()
        for addr in (canonical, *members)
    )
    d0_group_ids = {gid for gid, b in buckets.items() if b.depth_label == DepthLabel.D0}
    d1_root_ids = {gid for gid, b in buckets.items() if b.depth_label == ROOT_D1_DEPTH}
    main_lineage = (
        main_lineage_groups(group_graph, group_edges, buckets, mandatory_list)
        if mandatory_list
        else set(buckets.keys())
    )
    lineage_ancestors_graph = subgraph_of_kinds(
        group_graph, group_edges, EdgeKind.LINEAGE, EdgeKind.CONSTRAINT
    )
    group_members: dict[str, set[str]] = {
        gid: set(b.primary_members) | set(b.secondary_members)
        for gid, b in buckets.items()
    }
    # Every concept that is the RHS set of some membership (`x in <set>`) in this
    # query, and the groups that PRODUCE one. A membership must never be injected
    # on a group that produces a set: the set is a side-channel subselect that
    # must stay unfiltered, and filtering its producer by `x in set` is
    # self-referential (the set defines the rows it's filtered against -> the
    # IN-RHS renders against a dangling CTE).
    all_existence_addrs: set[str] = set()
    for clause in conditions:
        for atom in decompose_condition(clause.conditional):
            all_existence_addrs |= {
                c.address for group in atom.existence_arguments for c in group
            }
    existence_set_producers = {
        gid
        for gid, b in buckets.items()
        if all_existence_addrs & set(b.primary_members)
    }

    def _boundary_in_active_relation(gid: str) -> bool:
        """True when ``gid`` is a ROWSET boundary whose scoped-relation mate is
        also present in THIS graph — i.e. the completion merge that null-extends
        this boundary's rows happens in this query. A WHERE atom over such a
        boundary's outputs is a post-join predicate: hosting it at the boundary
        pre-filters one side of a preserving relation (an `is not null`
        intersection idiom becomes a tautology, and a filtered side breaks the
        EQUAL-declaration narrowing evidence downstream). A boundary whose
        relation mate is outside this scope (a nested arm reading one rowset)
        keeps boundary hosting — no merge here can null-extend it."""
        b = buckets.get(gid)
        if b is None or b.derivation != Derivation.ROWSET:
            return False
        # the boundary's key handle can live in the axis bucket rather than in
        # its own member list, but always names the boundary's grain
        members = group_members.get(gid, set()) | set(b.grain_components)
        keys = members & scoped_join_member_addresses
        if not keys:
            return False
        mates: set[str] = set()
        for canonical, group in scoped_join_key_groups.items():
            relation = set(group) | {canonical}
            if keys & relation:
                mates |= relation - keys
        if not mates:
            return False
        return any(
            other_gid != gid and (mates & other_members)
            for other_gid, other_members in group_members.items()
        )

    placements: list[ConditionPlacement] = []
    for clause in conditions:
        for atom in decompose_condition(clause.conditional):
            row_inputs = {c.address for c in atom.row_arguments}
            candidates = _candidate_groups(
                atom,
                row_inputs,
                group_members,
                d1_root_ids,
                lineage_ancestors_graph,
                buckets,
            )
            # A presence probe's null test is only meaningful ABOVE the merge
            # that null-extends it: hosting it at the member's own rowset
            # boundary reads the probe one-sided (never NULL for `is not
            # null`, all-NULL for `is null` — the anti-join filters the wrong
            # side). Drop boundary hosts; the atom lands at FINAL (or a ROOT
            # group, whose plan itself contains the completion merge). The
            # same applies to a scoped-join KEY-GROUP MEMBER itself: a member
            # reference reads as the coalesced group axis, which only exists
            # post-merge (v3 renders `WHERE coalesce(b_store, a_store) is not
            # null` at the final select) — filtering one boundary by its own
            # key both no-ops locally and perturbs the anchor-LEFT join shape.
            if any(
                is_presence_probe(addr) or addr in scoped_join_member_addresses
                for addr in row_inputs
            ) or any(_boundary_in_active_relation(gid) for gid in candidates):
                non_rowset_candidates = [
                    gid
                    for gid in candidates
                    if buckets.get(gid) is None
                    or buckets[gid].derivation != Derivation.ROWSET
                ]
                # A member of a scoped join whose producer is a ROWSET
                # boundary reads as the coalesced axis above the completion
                # merge; once a boundary host is off the table the surviving
                # candidates are downstream derivations of the OTHER side
                # (the `fut_period.wk + 53` derived-key group), which can
                # neither see the axis nor be pushed past their own boundary.
                # Route straight to FINAL. Same for a probe whose only hosts
                # are condition-only side branches (the mixed
                # root-member-vs-rowset-anchor shape): applying the atom there
                # filters a group FINAL never merges, silently dropping the
                # WHERE — FINAL pulls the probe's producer in as a keyed side
                # input instead.
                dropped_rowset_host = len(non_rowset_candidates) != len(candidates)
                candidates = non_rowset_candidates
                if dropped_rowset_host or (
                    candidates and not any(gid in main_lineage for gid in candidates)
                ):
                    placements.append(
                        ConditionPlacement(
                            atom=atom,
                            group_ids=(FINAL_NODE_ID,),
                            reason=PlacementReason.FINAL_RECONVERGENCE,
                        )
                    )
                    continue
            # An atom referencing an aggregate OUTPUT is a post-aggregation
            # predicate: it may only be hosted at that aggregate's producer
            # group (HAVING) or downstream of it. An upstream scan can carry
            # the address as a computable member, but hosting there re-renders
            # the aggregate inline at the HOSTING group's grain — a `by *`
            # global gate silently becomes a per-output-grain HAVING.
            producer_gids = _post_aggregation_producers(
                row_inputs, buckets, group_graph, group_edges
            )
            if producer_gids:
                # LINEAGE-only descendants: a CONSTRAINT successor is the
                # d0 consumer the gate must sit ABOVE, not a group that
                # can evaluate the post-aggregation value.
                lineage_only = lineage_subgraph(group_graph, group_edges)
                allowed: set[str] | None = None
                for gid in producer_gids:
                    reach = {gid}
                    if gid in lineage_only:
                        reach |= nx.descendants(lineage_only, gid)
                    allowed = reach if allowed is None else (allowed & reach)
                candidates = [gid for gid in candidates if gid in (allowed or set())]
                if not candidates:
                    # The producers' branches only reconverge at FINAL.
                    placements.append(
                        ConditionPlacement(
                            atom=atom,
                            group_ids=(FINAL_NODE_ID,),
                            reason=PlacementReason.FINAL_RECONVERGENCE,
                        )
                    )
                    continue
            closures = _producer_closures(row_inputs, group_graph, buckets)
            restricted = [
                gid for gid in candidates if all(gid in reach for reach in closures)
            ]
            # A self-contained membership whose ONLY hosts are membership-set
            # producers (output and set share one scan, so the set's producer is
            # the sole candidate) has nowhere neutral to land -- placing it on a
            # producer is self-referential. Route to FINAL, where each set is a
            # subselect feeder. Memberships with a real consumer candidate (the
            # common TPC-DS `x in <set>` over a separate output aggregate) are
            # untouched.
            if (
                atom.existence_arguments
                and restricted
                and all(gid in existence_set_producers for gid in restricted)
            ):
                placements.append(
                    ConditionPlacement(
                        atom=atom,
                        group_ids=(FINAL_NODE_ID,),
                        reason=PlacementReason.FINAL_RECONVERGENCE,
                    )
                )
                continue
            # A gate whose row inputs are only producible by groups disconnected
            # from the mandatory outputs (e.g. `where x = 1` beside a rootless
            # `unnest([...])`/constant output) has no covering contributor to host
            # it -- its own root group is pruned from FINAL assembly, silently
            # dropping the filter. Route it to FINAL, which cross-joins the gate's
            # scan (the FINAL merge dedups to the output grain, so the gate acts as
            # a 0/1-row EXISTS gate). Restricted to rootless outputs: a disconnected
            # filter beside a real datasource output is a missing join and already
            # raised at the connectivity pre-gate, so it never reaches here.
            if (
                mandatory_list
                and candidates
                and _output_is_rootless(mandatory_list)
                and all(gid not in main_lineage for gid in candidates)
            ):
                placements.append(
                    ConditionPlacement(
                        atom=atom,
                        group_ids=(FINAL_NODE_ID,),
                        reason=PlacementReason.DISCONNECTED_GATE,
                    )
                )
                continue
            agg_outputs = _aggregate_outputs(row_inputs, buckets)
            if _routes_to_final_for_cross_grain_aggregates(agg_outputs, buckets):
                placements.append(
                    ConditionPlacement(
                        atom=atom,
                        group_ids=(FINAL_NODE_ID,),
                        reason=PlacementReason.FINAL_CROSS_GRAIN_AGGREGATE,
                    )
                )
                continue
            # Drop hosts sitting downstream of a d0 row-shape barrier the
            # atom's own producers don't already consume (the same criterion
            # `_validate_not_pushed_past_independent_barrier` enforces): a
            # cross-boundary atom (an OR of two boundaries' presence probes)
            # can end up with only such hosts — its correct home is FINAL,
            # above every barrier, not a crash.
            if restricted:
                producer_groups = _producer_groups(row_inputs, buckets)
                consumed_barriers: set[str] = set(producer_groups)
                for producer in producer_groups:
                    consumed_barriers |= nx.ancestors(group_graph, producer)
                restricted = [
                    gid
                    for gid in restricted
                    if not (
                        (d0_group_ids & nx.ancestors(group_graph, gid))
                        - consumed_barriers
                    )
                ]
            if not restricted:
                placements.append(
                    ConditionPlacement(
                        atom=atom,
                        group_ids=(FINAL_NODE_ID,),
                        reason=PlacementReason.FINAL_RECONVERGENCE,
                    )
                )
                continue
            chosen_groups = _choose_groups(
                restricted, lineage_ancestors_graph, main_lineage
            )
            _validate_not_pushed_past_independent_barrier(
                atom,
                chosen_groups,
                _producer_groups(row_inputs, buckets),
                d0_group_ids,
                group_graph,
            )
            placements.append(
                ConditionPlacement(
                    atom=atom,
                    group_ids=chosen_groups,
                    reason=PlacementReason.UPSTREAM_MOST,
                )
            )
    return placements
