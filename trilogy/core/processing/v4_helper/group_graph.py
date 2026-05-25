"""Stage 2: collapse the concept graph into compatible-concept groups and
append a single FINAL sink.

Pipeline:
    assign groups via per-derivation grouping rules →
    attach secondary members → wire group-level lineage edges →
    inject condition clauses → color edges by pre/post-condition phase →
    attach FINAL sink

The assignment pass is a single uniform loop: for each derivation, look up
its rule in `group_rules.GROUPING_RULES` and let the rule produce its
buckets. The rule decides whether to merge by equality (the default,
keying on `(depth_label, grain)`) or by some other relation (BASIC merges
by grain subset; ROOT collapses everything to a single bucket). No
derivation is privileged in the orchestrator — each one's grouping logic
lives next to its rule.
"""

from collections import defaultdict

import networkx as nx

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildWhereClause

from .constants import FINAL_NODE_ID, GROUPING_DERIVATIONS
from .group_rules import DEFAULT_RULE, GROUPING_RULES
from .models import GroupBucket


def _lineage_parents(concept_graph: nx.DiGraph, node: str) -> frozenset[str]:
    return frozenset(
        u
        for u, _, d in concept_graph.in_edges(node, data=True)
        if d.get("kind") == "lineage"
    )


def _group_id_for(bucket: GroupBucket) -> str:
    grain_key = "|".join(sorted(bucket.grain_components)) or "∅"
    return f"grp:{bucket.derivation}:{bucket.depth_label}:{grain_key}"


def _assign_groups(
    concept_graph: nx.DiGraph,
) -> tuple[dict[str, str], dict[str, GroupBucket]]:
    """Group every concept by dispatching to its derivation's rule. Each rule
    is responsible for both the grouping key and the merge semantics for its
    derivation, so the orchestrator just collects per-rule buckets without
    any special-case branches."""
    by_derivation: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for node, data in concept_graph.nodes(data=True):
        by_derivation[data.get("derivation", "")].append((node, data))

    primary_group: dict[str, str] = {}
    buckets: dict[str, GroupBucket] = {}
    for derivation, items in by_derivation.items():
        rule = GROUPING_RULES.get(derivation, DEFAULT_RULE)
        for bucket in rule(items):
            group_id = _group_id_for(bucket)
            buckets[group_id] = bucket
            for member in bucket.primary_members:
                primary_group[member] = group_id
    return primary_group, buckets


def _attach_secondary_members(
    concept_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
) -> None:
    """Each group lists every concept it can also expose (grain columns from
    a GROUP BY/PARTITION BY; roots and lineage parents that co-project
    alongside a basic). Secondary membership is for display/availability only
    — it does NOT suppress upstream lineage edges, since the downstream group
    still needs the upstream scan to feed it."""
    root_addresses = {
        n
        for n, d in concept_graph.nodes(data=True)
        if d.get("derivation") == Derivation.ROOT.value
    }

    def add(bucket: GroupBucket, address: str) -> None:
        if address in bucket.primary_members or address in bucket.secondary_members:
            return
        if address not in concept_graph.nodes:
            return
        bucket.secondary_members.append(address)
        bucket.member_depths[address] = concept_graph.nodes[address].get(
            "depth_label", "d*"
        )

    for bucket in buckets.values():
        if bucket.derivation in GROUPING_DERIVATIONS:
            for grain_addr in bucket.grain_components:
                add(bucket, grain_addr)
        elif bucket.derivation == Derivation.BASIC.value:
            for root_addr in root_addresses:
                add(bucket, root_addr)
            for member in list(bucket.primary_members):
                for parent in _lineage_parents(concept_graph, member):
                    add(bucket, parent)


def _materialize_group_graph(
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
    buckets: dict[str, GroupBucket],
) -> nx.DiGraph:
    """Realize the in-flight `GroupBucket` map as an nx.DiGraph with node
    attributes the downstream consumers (strategy walker, visualizer) read."""
    group_graph: nx.DiGraph = nx.DiGraph()
    for gid, bucket in buckets.items():
        members = tuple(bucket.primary_members) + tuple(bucket.secondary_members)
        group_graph.add_node(
            gid,
            depth_label=bucket.depth_label,
            derivation=bucket.derivation,
            grain_components=bucket.grain_components,
            members=members,
            primary_members=tuple(bucket.primary_members),
            secondary_members=tuple(bucket.secondary_members),
            member_depths=dict(bucket.member_depths),
            conditions=[],
            condition_objects=[],
        )

    for u, v, edata in concept_graph.edges(data=True):
        if edata.get("kind") != "lineage":
            continue
        gu, gv = primary_group[u], primary_group[v]
        if gu == gv:
            continue
        group_graph.add_edge(gu, gv, kind="lineage")

    return group_graph


def _inject_conditions(
    group_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
) -> set[str]:
    """Place each clause on the furthest-upstream group that already exposes
    every input the clause references. Errors out if the chosen group sits
    downstream of a d0 barrier (a filter cannot be pushed past a row-shape
    change). Returns the set of groups that received at least one condition."""
    d0_group_ids = {gid for gid, b in buckets.items() if b.depth_label == "d0"}
    group_members: dict[str, set[str]] = {
        gid: set(b.primary_members) | set(b.secondary_members)
        for gid, b in buckets.items()
    }
    condition_group_ids: set[str] = set()

    for clause in conditions:
        inputs = {c.address for c in clause.concept_arguments}
        candidates = [gid for gid, mems in group_members.items() if inputs <= mems]
        if not candidates:
            continue
        cand_set = set(candidates)
        upstream_most = [
            gid
            for gid in candidates
            if not (cand_set & nx.ancestors(group_graph, gid))
        ]
        chosen = upstream_most[0] if upstream_most else candidates[0]
        chosen_ancestors = nx.ancestors(group_graph, chosen)
        offending = d0_group_ids & chosen_ancestors
        if offending:
            raise ValueError(
                f"Condition {clause} would be injected at {chosen}, which is "
                f"downstream of d0 barrier(s) {sorted(offending)}; conditions "
                f"cannot be pushed past row-shape changes."
            )
        group_graph.nodes[chosen]["conditions"].append(str(clause))
        group_graph.nodes[chosen]["condition_objects"].append(clause)
        condition_group_ids.add(chosen)

    return condition_group_ids


def _color_phases(
    group_graph: nx.DiGraph,
    condition_group_ids: set[str],
) -> set[str]:
    """Mark each edge as `pre_condition` or `post_condition`. Any edge from a
    condition-bearing group (or any descendant of one) is post-condition;
    everything earlier is pre-condition. Returns the downstream set so the
    FINAL-sink wiring can use the same coloring."""
    downstream: set[str] = set(condition_group_ids)
    for cgid in condition_group_ids:
        downstream |= nx.descendants(group_graph, cgid)
    for u, v in list(group_graph.edges()):
        group_graph.edges[u, v]["phase"] = (
            "post_condition" if u in downstream else "pre_condition"
        )
    return downstream


def _add_final_node(
    group_graph: nx.DiGraph,
    concept_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    downstream: set[str],
) -> None:
    """Attach a single FINAL sink that collects every non-d1 concept, with a
    merge edge from every group, phase-colored to match the rest of the graph."""
    non_condition_members = tuple(
        n for n, d in concept_graph.nodes(data=True) if d.get("depth_label") != "d1"
    )
    group_graph.add_node(
        FINAL_NODE_ID,
        depth_label="final",
        derivation="final",
        grain_components=frozenset(),
        members=non_condition_members,
        conditions=[str(c) for c in conditions],
    )
    for gid in buckets:
        phase = "post_condition" if gid in downstream else "pre_condition"
        group_graph.add_edge(gid, FINAL_NODE_ID, kind="merge", phase=phase)


def build_group_graph(
    concept_graph: nx.DiGraph,
    conditions: list[BuildWhereClause],
) -> nx.DiGraph:
    """Collapse compatible concepts into groups and append a single FINAL sink.

    Grouping is delegated to per-derivation rules in `group_rules.py`:
    most derivations group by equality on `(depth_label, grain)`; ROOT
    collapses to one bucket; BASIC merges by grain subset/equality.
    """
    primary_group, buckets = _assign_groups(concept_graph)
    _attach_secondary_members(concept_graph, buckets)
    group_graph = _materialize_group_graph(concept_graph, primary_group, buckets)
    condition_group_ids = _inject_conditions(group_graph, buckets, conditions)
    downstream = _color_phases(group_graph, condition_group_ids)
    _add_final_node(group_graph, concept_graph, buckets, conditions, downstream)
    return group_graph
