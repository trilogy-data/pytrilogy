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
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.discovery_utility import _output_is_rootless
from trilogy.core.processing.node_generators.presence_probe import is_presence_probe

from .constants import FINAL_NODE_ID, DepthLabel, EdgeKind
from .edges import EdgeMap, lineage_subgraph, subgraph_of_kinds
from .models import ConceptAttrs, GroupBucket

ROOT_D1_DEPTH = DepthLabel.ROOT_D1

_EMITS_GROUP_BY: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.GROUP_TO,
}


class PlacementReason(Enum):
    UPSTREAM_MOST = "upstream_most"
    FINAL_RECONVERGENCE = "final_reconvergence"
    FINAL_CROSS_GRAIN_AGGREGATE = "final_cross_grain_aggregate"
    DISCONNECTED_GATE = "disconnected_gate"
    FINAL_UNCOVERED_CONTRIBUTOR = "final_uncovered_contributor"


@dataclass(frozen=True)
class ConditionPlacement:
    atom: BoolExpr
    group_ids: tuple[str, ...]
    reason: PlacementReason


def _output_rowset_body_condition_addresses(
    mandatory_list: list[BuildConcept],
) -> set[str]:
    """Row-arg addresses of the WHERE/HAVING clauses inside every rowset body
    reachable from the mandatory outputs. An outer WHERE over one of these is a
    restatement the rowset scope already consumes (q44: outer `store.sk = 1`
    over two rowsets whose bodies filter `store.sk = 1`) — not a missing join."""
    addrs: set[str] = set()
    seen: set[str] = set()
    stack = list(mandatory_list)
    while stack:
        concept = stack.pop()
        if concept.address in seen:
            continue
        seen.add(concept.address)
        lineage = concept.lineage
        if isinstance(lineage, BuildRowsetItem):
            select = lineage.rowset.select
            for clause in (select.where_clause, select.having_clause):
                if clause is not None:
                    addrs |= {a.address for a in clause.row_arguments}
        for arg in concept.concept_arguments:
            if isinstance(arg, BuildConcept):
                stack.append(arg)
    return addrs


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
    hosts: list[str] = []
    for gid in candidates:
        bucket = buckets.get(gid)
        if bucket is None:
            hosts.append(gid)
            continue
        if bucket.derivation == Derivation.UNNEST:
            continue
        # A WINDOW group can host an atom as a pre-window input filter (the
        # strategy builder peels it into a wrapper below the window), but an
        # atom over the window's OWN output has no pre-window rendering.
        if bucket.derivation == Derivation.WINDOW and (
            row_inputs & set(bucket.primary_members)
        ):
            continue
        hosts.append(gid)
    return hosts


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
            if b.derivation in _EMITS_GROUP_BY and _is_global(gid):
                producer = gid
            elif (
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


def _uncovered_exposing_output_contributor(
    chosen_groups: tuple[str, ...],
    row_inputs: set[str],
    buckets: dict[str, GroupBucket],
    group_graph: nx.DiGraph,
    mandatory_addrs: set[str],
) -> bool:
    """Whether some select-phase group producing a mandatory output sits outside
    the chosen hosts' downstream cover AND can expose every atom input — i.e. an
    unfiltered projection of the same row universe re-enters the FINAL merge.

    Hosting an atom at its upstream-most candidate filters that branch, but a
    sibling output projection (`vehicle_label` beside a name-grain window) joins
    FINAL on a coarser key and fans the filtered rows back out to the unfiltered
    universe — v3 re-applies the WHERE on the final select. Only trip when the
    uncovered group can expose the inputs (else the FINAL WHERE would reference
    an unsourced concept; an uncovered contributor keyed 1:1 by row identity is
    pinned by the join and needs no re-filter — its groups can't expose derived
    condition inputs and skip here). Condition-phase (d1) groups are population
    scope and never receive row atoms.

    Skipped entirely under non-standard grouping (ROLLUP/CUBE/GROUPING SETS):
    those groups NULL-inject rolled-up dims on subtotal rows, and a WHERE
    re-applied above the merge would drop every subtotal (q05 lost its rollup
    totals)."""
    if any(b.nulls_grouping_keys for b in buckets.values()):
        return False
    covered: set[str] = set(chosen_groups)
    for gid in chosen_groups:
        covered |= nx.descendants(group_graph, gid)
    uncovered_exposing = False
    for gid, b in buckets.items():
        if gid in covered:
            continue
        if b.depth_label in (DepthLabel.D1, ROOT_D1_DEPTH):
            continue
        members = set(b.primary_members) | set(b.secondary_members)
        if not (members & mandatory_addrs):
            continue
        if row_inputs <= members:
            uncovered_exposing = True
            break
    return uncovered_exposing


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


def _grouping_barrier_host(
    candidates: list[str],
    dropped_hosts: list[str],
    row_inputs: set[str],
    group_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
) -> str | None:
    """The last group at which the atom's inputs still exist as raw columns.

    A GROUP BY candidate downstream of EVERY dropped boundary host has the
    completion merge as its own input, so the coalesced axis is a real column
    there. Above it the axis is gone: a grouping group can only emit a column
    it groups by, and this one's grain excludes the inputs — routing to FINAL
    makes FINAL demand an un-grouped column and the binder rejects the CTE
    (`by rollup` + a multi-key `subset join`).

    This is about column SURVIVAL, and so applies to any GROUP BY, standard or
    not. It is a different question from `nulls_grouping_keys` (which is about
    subtotal rows NULLing keys that do survive); rollup happens to fail both."""
    if not dropped_hosts:
        return None
    for gid in candidates:
        bucket = buckets.get(gid)
        if bucket is None or bucket.derivation not in _EMITS_GROUP_BY:
            continue
        if row_inputs <= set(bucket.grain_components):
            continue
        ancestors = nx.ancestors(group_graph, gid)
        if all(host in ancestors for host in dropped_hosts):
            return gid
    return None


def plan_condition_placements(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    scoped_join_key_groups: dict[str, set[str]] | None = None,
    concept_attrs: dict[str, ConceptAttrs] | None = None,
    statement_relation_addresses: frozenset[str] = frozenset(),
    environment: BuildEnvironment | None = None,
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
    # Addresses a group can pair a relation on beyond its listed members: the
    # KEYS of its members (a group hosting only `a.aw` still joins on a.aw's
    # key `a.aid` — assembly carries it). `group_relatable` additionally folds
    # in member pseudonyms (a collapsed scoped-join mate answers under the
    # canonical address) — safe for LOCATING a relation mate, but NOT for a
    # group's own relation-side identity: a boundary key's pseudonym IS the
    # other side, and counting it would swallow the whole relation (empty
    # mates) and misclassify the boundary as self-contained.
    attrs_by_address: dict[str, ConceptAttrs] = {}
    for ca in (concept_attrs or {}).values():
        attrs_by_address.setdefault(ca.address, ca)
    group_own_keys: dict[str, set[str]] = {}
    group_relatable: dict[str, set[str]] = {}
    for gid, members in group_members.items():
        own = set(members)
        relatable = set(members)
        for member in members:
            member_attrs = attrs_by_address.get(member)
            if member_attrs is not None:
                own |= set(member_attrs.keys)
                relatable |= set(member_attrs.keys) | set(member_attrs.pseudonyms)
        group_own_keys[gid] = own
        group_relatable[gid] = relatable
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

    def _group_in_active_relation(gid: str) -> bool:
        """True when ``gid`` is one SIDE of a scoped relation whose mate lives
        in a DIFFERENT group of THIS graph — i.e. the completion merge that
        null-extends this group's rows happens above it in this query. A WHERE
        atom over such a group's outputs is a post-join predicate: hosting it
        at the group pre-filters one side of a preserving relation (an `is not
        null` intersection idiom becomes a tautology, a filtered side breaks
        the EQUAL-declaration narrowing evidence downstream, and a preserved
        anchor re-admits the filtered rows NULL-extended). A group whose
        relation mate is outside this scope (a nested arm reading one rowset)
        or INSIDE itself (a single ROOT scan covering the whole join) keeps
        local hosting — its own SELECT applies the WHERE post-join."""
        b = buckets.get(gid)
        if b is None:
            return False
        # A non-ROWSET group only defers when its rows flow STRAIGHT to the
        # FINAL merge — that merge is then the relation's completion merge and
        # the atom is genuinely post-join. A group feeding any intermediate
        # consumer (q72: the inv scan feeding a joined aggregation) must keep
        # local hosting: its atoms are pre-aggregation predicates, and a
        # FINAL-deferred copy would filter aggregated rows instead of input
        # rows.
        if b.derivation != Derivation.ROWSET and any(
            succ != FINAL_NODE_ID for succ in group_graph.successors(gid)
        ):
            return False
        # a ROWSET boundary's key handle can live in the axis bucket rather
        # than its own member list, but always names the boundary's grain; a
        # plain group participates through a member's key (`a.aw` keyed by
        # relation member `a.aid`). Own-side identity deliberately excludes
        # pseudonyms — see `group_own_keys` — and for a ROWSET boundary also
        # excludes member KEYS: a handle's key resolves through the OUTER
        # scoped join's canonical (the mate's address), which would swallow
        # the relation and un-flag the boundary (q64 second-fact join hoist).
        if b.derivation == Derivation.ROWSET:
            members = group_members.get(gid, set()) | set(b.grain_components)
            # A boundary participates through its member HANDLE even when the
            # select never demands it (`union join return_demos.demo_id =
            # c_demo` selecting only grain keys): the handle lives on this
            # boundary by namespace, and the completion merge still
            # null-extends these rows.
            boundary_namespaces = {
                member.rpartition(".")[0]
                for member in members
                if (ca := attrs_by_address.get(member)) is not None
                and ca.derivation == Derivation.ROWSET
            }
            members |= {
                addr
                for addr in scoped_join_member_addresses
                if addr.rpartition(".")[0] in boundary_namespaces
            }
        else:
            members = group_own_keys.get(gid, set()) | set(b.grain_components)
        keys = members & scoped_join_member_addresses
        if not keys:
            return False
        mates: set[str] = set()
        for canonical, group in scoped_join_key_groups.items():
            relation = set(group) | {canonical}
            if not (keys & relation):
                continue
            # A non-ROWSET group only counts as a preserved SIDE of a
            # STATEMENT-scoped join: a global `merge` is an identity
            # declaration (INNER pairing, no null-extension), so pre-filtering
            # a scan that shares a merged key is sound — and required, or a
            # rowset body's own WHERE floats above its aggregate. Rowset
            # boundaries keep the wider criterion (cross-rowset `merge X.a
            # into Y.b` completion merges are global-scoped yet preserving).
            if b.derivation != Derivation.ROWSET and not (
                relation & statement_relation_addresses
            ):
                continue
            mates |= relation - keys
        mates -= keys
        if not mates:
            return False
        # A ROOT mate (`c_demo`) is often not itself a member of any group —
        # the pairing scan carries it as an FD attribute of a member's key
        # (customers hosts c_name keyed by c_id; c_demo's key is c_id). Let a
        # ROWSET boundary locate such a mate through the mate's keys; an
        # undemanded mate has no ConceptAttrs, so fall back to the environment.
        if b.derivation == Derivation.ROWSET:
            for mate in list(mates):
                mate_attrs = attrs_by_address.get(mate)
                if mate_attrs is not None:
                    mates |= set(mate_attrs.keys)
                elif environment is not None:
                    mate_concept = environment.concepts.get(mate)
                    if mate_concept is not None and mate_concept.keys:
                        mates |= set(mate_concept.keys)
            mates -= keys
        return any(
            other_gid != gid and (mates & other_relatable)
            for other_gid, other_relatable in group_relatable.items()
        )

    # Rowset boundaries whose rows flow STRAIGHT to the FINAL merge. When two or
    # more such boundaries merge there, that merge is a cross-rowset completion
    # join that can null-extend a side — the offset/derived-key subset join
    # (`subset join b.oid + 1 = a.oid`) is the motivating case: its endpoint is a
    # derived expression, so it registers no scoped-join axis and
    # `_group_in_active_relation` cannot see it. A WHERE over such a boundary's
    # output is a post-merge predicate the same way (hosting `b.amt is not null`
    # inside b's boundary filters nothing pre-join, then the LEFT completion
    # re-admits the row null-extended). Route it to FINAL — always correct, a
    # no-op when the merge is INNER.
    rowset_final_groups = {
        gid
        for gid, b in buckets.items()
        if b.derivation == Derivation.ROWSET
        and set(group_graph.successors(gid)) == {FINAL_NODE_ID}
    }

    def _rowset_boundary_deferred(gid: str) -> bool:
        return len(rowset_final_groups) > 1 and gid in rowset_final_groups

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
            active_relation_hosts = {
                gid
                for gid in candidates
                if _group_in_active_relation(gid) or _rowset_boundary_deferred(gid)
            }
            # A flagged NON-rowset host (a FINAL contributor that is one side
            # of an active preserving relation — the aligns read-back's
            # enrichment scan) leaves the pool quietly: any surviving upstream
            # host still wins (its SELECT applies the WHERE pre-merge within
            # the pipeline, q72), and when nothing survives the tail routes
            # the atom to FINAL.
            relation_candidates = list(candidates)
            candidates = [
                gid
                for gid in candidates
                if gid not in active_relation_hosts
                or (
                    buckets.get(gid) is not None
                    and buckets[gid].derivation == Derivation.ROWSET
                )
            ]
            if any(
                is_presence_probe(addr) or addr in scoped_join_member_addresses
                for addr in row_inputs
            ) or any(gid in active_relation_hosts for gid in candidates):
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
                dropped_hosts = [
                    gid for gid in candidates if gid not in non_rowset_candidates
                ]
                candidates = non_rowset_candidates
                # A GROUP BY candidate every dropped boundary must flow THROUGH
                # to reach FINAL is where the coalesced axis last exists as a
                # raw column: routing past it makes FINAL demand the axis from
                # an aggregate that cannot group by it (`by rollup` + a
                # multi-key `subset join` — the axis columns come out ungrouped
                # and the binder rejects them). Host it there instead, a
                # pre-aggregation WHERE above the completion merge.
                barrier = _grouping_barrier_host(
                    relation_candidates, dropped_hosts, row_inputs, group_graph, buckets
                )
                if dropped_rowset_host and barrier is not None:
                    placements.append(
                        ConditionPlacement(
                            atom=atom,
                            group_ids=(barrier,),
                            reason=PlacementReason.UPSTREAM_MOST,
                        )
                    )
                    continue
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
            # A row atom whose EVERY candidate host lies outside the main
            # lineage (with real datasource outputs — the rootless case became
            # a FINAL EXISTS gate above) has no host FINAL assembly will keep:
            # the condition-only group covers no mandatory output, so it is
            # pruned and the WHERE silently vanishes (`where year = 2001` over
            # rowset outputs that never expose year). No join relates the
            # gate's rows to the outputs — v3's rowset islanding diagnoses the
            # same shape as disconnected; raise the same typed error. EXEMPT an
            # atom the output rowsets' own bodies already filter on (q44's
            # outer `store.sk = 1` restates both rowsets' WHERE): that scope
            # consumes the concept, so this is a redundant restatement, not a
            # missing join — placement proceeds and the drop is harmless.
            if (
                mandatory_list
                and candidates
                and not atom.existence_arguments
                and all(gid not in main_lineage for gid in candidates)
                and not row_inputs
                <= _output_rowset_body_condition_addresses(mandatory_list)
            ):
                input_addrs = sorted(row_inputs)
                output_addrs = sorted({c.address for c in mandatory_list})
                raise DisconnectedConceptsException(
                    f"WHERE input(s) {input_addrs} cannot be related to the "
                    f"query outputs {output_addrs}: no join or merge connects "
                    "the filter's source to any output-producing source. Add a "
                    "join/merge relating them, or select a concept from the "
                    "filter's model.",
                    subgraphs=[output_addrs, input_addrs],
                )
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
            if (
                mandatory_list
                and not atom.existence_arguments
                and _uncovered_exposing_output_contributor(
                    chosen_groups,
                    row_inputs,
                    buckets,
                    group_graph,
                    {c.address for c in mandatory_list},
                )
            ):
                placements.append(
                    ConditionPlacement(
                        atom=atom,
                        group_ids=(FINAL_NODE_ID,),
                        reason=PlacementReason.FINAL_UNCOVERED_CONTRIBUTOR,
                    )
                )
    return placements
