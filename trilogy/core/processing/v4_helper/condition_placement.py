"""Condition placement for the v4 group graph.

The planner is intentionally side-effect free: it inspects the group graph and
returns the groups that should receive each decomposed WHERE atom. The caller is
responsible for mutating ``GroupAttrs.condition_atoms``.
"""

from dataclasses import dataclass
from enum import Enum

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.models.build import BoolExpr, BuildConcept, BuildWhereClause
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.discovery_utility import _output_is_rootless

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
        raise ValueError(
            f"Could not place condition atom {atom}: row inputs "
            f"{sorted(row_inputs)} not reachable from any group."
        )
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
) -> list[ConditionPlacement]:
    """Return where each decomposed condition atom should be injected."""
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
