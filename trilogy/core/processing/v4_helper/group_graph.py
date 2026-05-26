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
from trilogy.core.processing.condition_utility import decompose_condition

from .constants import FINAL_NODE_ID, GROUPING_DERIVATIONS
from .group_rules import DEFAULT_RULE, GROUPING_RULES
from .models import GroupBucket

# depth_label used for the secondary root bucket dedicated to feeding d1
# (in-WHERE) aggregate calculations. Distinct from "root" so the bucket
# gets its own group id and doesn't collide with the main root bucket.
ROOT_D1_DEPTH = "root_d1"

# Derivations that compute over an input row POPULATION — only these need
# to be sourced from a pristine, unfiltered scan when they appear in a
# WHERE clause. AGGREGATE/WINDOW/GROUP_TO are population-sensitive: avg/
# sum/rank depend on which rows are present. ROWSET / SUBSELECT / UNNEST
# / UNION / RECURSIVE are NOT — a rowset used inside `IN` is its own
# subquery scope, an unnest is pointwise, etc. Including them would split
# roots for every `x IN some_rowset` clause and break parent wiring
# downstream (q23 regression).
_POPULATION_SENSITIVE_DERIVATIONS = GROUPING_DERIVATIONS


def _lineage_parents(concept_graph: nx.DiGraph, node: str) -> frozenset[str]:
    return frozenset(
        u
        for u, _, d in concept_graph.in_edges(node, data=True)
        if d.get("kind") == "lineage"
    )


def _group_id_for(bucket: GroupBucket) -> str:
    grain_key = "|".join(sorted(bucket.grain_components)) or "∅"
    return f"grp:{bucket.derivation}:{bucket.depth_label}:{grain_key}"


def _lineage_only_subgraph(concept_graph: nx.DiGraph) -> nx.DiGraph:
    edges = [
        (u, v)
        for u, v, ed in concept_graph.edges(data=True)
        if ed.get("kind") == "lineage"
    ]
    sub = concept_graph.edge_subgraph(edges).copy()
    for n in concept_graph.nodes:
        if n not in sub:
            sub.add_node(n)
    return sub


def _d1_calc_subgraph(concept_graph: nx.DiGraph) -> tuple[set[str], set[str]]:
    """Identify (d1_calc_roots, d1_subgraph_nodes).

    A 'd1 calc node' is a derived concept (non-root) classified as d1 — an
    aggregate (or window/etc.) referenced inside a WHERE clause. Its lineage
    inputs must be sourced from a scan that does NOT have sibling WHERE atoms
    pushed onto it (an avg-in-where has to run over the unfiltered population,
    not the rows that survive other filters).

    - d1_calc_roots: root concepts that are lineage-ancestors of any d1 calc.
      These become primary members of an extra ROOT bucket (R_d1).
    - d1_subgraph_nodes: the d1 calc nodes plus all their lineage ancestors.
      Used by edge routing to decide whether a root→X edge should source from
      R_d1 (X in d1_subgraph) or R_other (the default root bucket)."""
    d1_calc_nodes = [
        n
        for n, d in concept_graph.nodes(data=True)
        if d.get("depth_label") == "d1"
        and d.get("derivation") in _POPULATION_SENSITIVE_DERIVATIONS
    ]
    if not d1_calc_nodes:
        return set(), set()
    lineage_only = _lineage_only_subgraph(concept_graph)
    subgraph_nodes: set[str] = set(d1_calc_nodes)
    for n in d1_calc_nodes:
        subgraph_nodes.update(nx.ancestors(lineage_only, n))
    d1_calc_roots = {
        n
        for n in subgraph_nodes
        if concept_graph.nodes[n].get("derivation") == Derivation.ROOT.value
    }
    return d1_calc_roots, subgraph_nodes


def _add_d1_root_bucket(
    concept_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
    d1_calc_roots: set[str],
) -> str | None:
    """Add an extra ROOT bucket containing just the d1-feeding roots. Returns
    the new bucket's group id (or None if no d1 calc roots)."""
    if not d1_calc_roots:
        return None
    bucket = GroupBucket(
        depth_label=ROOT_D1_DEPTH,
        derivation=Derivation.ROOT.value,
        grain_components=frozenset(),
    )
    for addr in sorted(d1_calc_roots):
        bucket.primary_members.append(addr)
        bucket.member_depths[addr] = concept_graph.nodes[addr].get(
            "depth_label", "d*"
        )
    gid = _group_id_for(bucket)
    buckets[gid] = bucket
    return gid


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
    d1_root_gid: str | None = None,
    d1_calc_roots: set[str] | None = None,
    d1_subgraph: set[str] | None = None,
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
            # Atoms (BoolExpr) applied AT this group. A clause like
            # `state='TN' AND year=2000` is decomposed and each atom finds its
            # own highest-allowed group independently — so a single clause
            # may live at multiple groups, or one group may collect atoms
            # from several clauses.
            condition_atoms=[],
            # String renderings of the atoms above, just for graph visualization.
            conditions=[],
        )

    # Propagate concept-level edges to the group level. Both `lineage` and
    # `constraint` edges become group predecessor relationships: lineage is
    # a computational dependency; constraint is a d1→d0 must-be-above
    # ordering that downstream consumers (the strategy walker, condition
    # placement) treat identically — both mean "this group's outputs must
    # be in the input CTE for the consumer." Without propagating
    # constraints, a d1 aggregate (e.g. `avg(price) by category`, when used
    # in a filter) ends up an island — the filter atom has nowhere to land
    # and the d0 aggregate that consumes the filtered rows never gets the
    # d1 group as a parent.
    #
    # Root edge routing: when a d1 calc node exists (an aggregate inside a
    # WHERE clause), its lineage-feeding roots are duplicated into a second
    # ROOT bucket (R_d1). Any root → d1-subgraph edge sources from R_d1 so
    # the d1 calc reads from a pristine scan; root → anything-else still
    # routes through the default R_other bucket and inherits its pushed-down
    # WHEREs. Without the split, sibling filters pollute the avg's input.
    d1_calc_roots = d1_calc_roots or set()
    d1_subgraph = d1_subgraph or set()
    for u, v, edata in concept_graph.edges(data=True):
        if edata.get("kind") not in ("lineage", "constraint"):
            continue
        if (
            d1_root_gid is not None
            and u in d1_calc_roots
            and v in d1_subgraph
        ):
            gu = d1_root_gid
        else:
            gu = primary_group[u]
        gv = primary_group[v]
        if gu == gv:
            continue
        group_graph.add_edge(gu, gv, kind="lineage")

    return group_graph


def _inject_conditions(
    group_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
) -> set[str]:
    """Decompose each clause into AND-atoms (via `decompose_condition`) and
    place each atom *independently* at the furthest-upstream group that can
    serve its inputs. An atom like `state='TN'` can fly all the way up to
    ROOT even if its sibling atom needs a downstream group — the two no
    longer share fate.

    Errors out if a chosen group sits downstream of a d0 barrier (a filter
    cannot be pushed past a row-shape change). Returns the set of groups
    that received at least one atom."""
    d0_group_ids = {gid for gid, b in buckets.items() if b.depth_label == "d0"}
    # The d1-feeding root bucket exists exactly so its scan stays unfiltered
    # — sibling WHERE atoms would corrupt the avg-in-where. Conditions whose
    # row inputs only fit in R_d1 still have to land somewhere, so we exclude
    # R_d1 from the candidate set and let the d1's downstream paths host the
    # filter (R_other, or a basic/aggregate below it).
    d1_root_ids = {
        gid for gid, b in buckets.items() if b.depth_label == ROOT_D1_DEPTH
    }
    group_members: dict[str, set[str]] = {
        gid: set(b.primary_members) | set(b.secondary_members)
        for gid, b in buckets.items()
    }
    condition_group_ids: set[str] = set()

    # A group can host an atom iff every row-arg is in its INPUT row
    # stream — i.e. what its FROM clause provides. That's:
    #   - for a source group (no ancestors): its own members (the
    #     datasource scan IS its input)
    #   - for any other group: the union of its ancestors' members (its
    #     parents' CTEs feed its FROM)
    # NOT including its own primaries — those are the *outputs* of this
    # group's derivation. You can't WHERE on `avg(price)` inside the same
    # SELECT that computes it; that filter must live downstream where
    # `avg(price)` is an input column. The "ancestors-only" rule below
    # forces those atoms to land on a consumer.
    def _reachable_input(gid: str) -> set[str]:
        ancestors = nx.ancestors(group_graph, gid)
        if not ancestors:
            return set(group_members.get(gid, set()))
        reachable: set[str] = set()
        for anc in ancestors:
            reachable |= group_members.get(anc, set())
        return reachable

    for clause in conditions:
        for atom in decompose_condition(clause.conditional):
            # Only the row arguments need to live in the host group's row
            # stream. Existence arguments (the right-hand side of an IN
            # subquery) are reached via a side-channel subselect — they
            # don't constrain placement and are threaded into the host
            # node as `existence_concepts` later. Without this distinction,
            # an atom like `week_seq IN relevent_week_seq` finds no group
            # that contains both inputs and drops on the floor.
            row_inputs = {c.address for c in atom.row_arguments}
            candidates = [
                gid
                for gid in group_members
                if gid not in d1_root_ids
                and row_inputs <= _reachable_input(gid)
            ]
            if not candidates:
                # Fail fast — silently dropping an atom changes query
                # semantics. If you hit this, the atom needs either a
                # synthetic merge group to land on or a richer
                # row-args/existence-args split.
                raise ValueError(
                    f"Could not place condition atom {atom}: row inputs "
                    f"{sorted(row_inputs)} not reachable from any group."
                )
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
                    f"Atom {atom} would be injected at {chosen}, which is "
                    f"downstream of d0 barrier(s) {sorted(offending)}; "
                    f"conditions cannot be pushed past row-shape changes."
                )
            group_graph.nodes[chosen]["condition_atoms"].append(atom)
            group_graph.nodes[chosen]["conditions"].append(str(atom))
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
    d1_calc_roots, d1_subgraph = _d1_calc_subgraph(concept_graph)
    d1_root_gid = _add_d1_root_bucket(concept_graph, buckets, d1_calc_roots)
    _attach_secondary_members(concept_graph, buckets)
    group_graph = _materialize_group_graph(
        concept_graph,
        primary_group,
        buckets,
        d1_root_gid=d1_root_gid,
        d1_calc_roots=d1_calc_roots,
        d1_subgraph=d1_subgraph,
    )
    condition_group_ids = _inject_conditions(group_graph, buckets, conditions)
    downstream = _color_phases(group_graph, condition_group_ids)
    _add_final_node(group_graph, concept_graph, buckets, conditions, downstream)
    return group_graph
