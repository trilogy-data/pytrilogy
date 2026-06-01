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

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.processing.condition_utility import decompose_condition

from .constants import FINAL_NODE_ID, GROUPING_DERIVATIONS
from .group_behaviors import behavior_for
from .group_rules import DEFAULT_RULE, GROUPING_RULES
from .models import ConceptAttrs, GroupAttrs, GroupBucket

# depth_label used for the secondary root bucket dedicated to feeding d1
# (in-WHERE) aggregate calculations. Distinct from "root" so the bucket
# gets its own group id and doesn't collide with the main root bucket.
ROOT_D1_DEPTH = "root_d1"


def _leaf_inputs(primaries: set[str], lineage_parents: dict[str, set[str]]) -> set[str]:
    """The first non-primary lineage ancestor of each primary — the columns a
    group actually consumes. Walks *through* primaries computed inside the
    group (a primary whose lineage arg is another primary, e.g. q49's chained
    rename) so the demand bottoms out on real inputs, not intermediates."""
    leaves: set[str] = set()
    for c in primaries:
        stack = list(lineage_parents.get(c, set()))
        seen: set[str] = set()
        while stack:
            p = stack.pop()
            if p in seen:
                continue
            seen.add(p)
            if p in primaries:
                stack.extend(lineage_parents.get(p, set()))
            else:
                leaves.add(p)
    return leaves


def _fd_on_key(
    concept_attrs: dict[str, ConceptAttrs], address: str, key: set[str]
) -> bool:
    """Whether `address` is functionally determined by the dimension key set
    `key`: it is a key, has no grain (constant), or its declared grain is a
    subset of the key."""
    if address in key:
        return True
    if address not in concept_attrs:
        return False
    gc = concept_attrs[address].grain_components
    return not gc or set(gc) <= key


def _group_id_for(bucket: GroupBucket) -> str:
    grain_key = "|".join(sorted(bucket.grain_components)) or "∅"
    label_prefix = f"[{bucket.label}]" if bucket.label else ""
    suffix = f":{bucket.discriminator}" if bucket.discriminator else ""
    return (
        f"grp:{label_prefix}{bucket.derivation}:{bucket.depth_label}:"
        f"{grain_key}{suffix}"
    )


def _d1_calc_subgraph(
    concept_graph: nx.DiGraph, concept_attrs: dict[str, ConceptAttrs]
) -> tuple[set[str], set[str]]:
    """Identify (d1_calc_roots, d1_subgraph_nodes).

    Any concept reached via the WHERE recursion lives at a condition-phase
    label (suffix ``@condition``) and is classified d1. Such concepts —
    aggregates, filters, basics, unnests, etc. — feed pre-WHERE compute
    that must not be polluted by sibling WHERE atoms applied to the
    SELECT-side scan. We route root → condition-phase lineage edges
    through a dedicated root_d1 bucket so the condition scan stays
    independent of the blank-phase scan's WHERE atoms.

    - d1_calc_roots: blank-phase roots that feed any condition-phase node.
    - d1_subgraph_nodes: every condition-phase node. Edge routing uses
      this as the destination side of the predicate."""
    d1_subgraph: set[str] = {
        n for n in concept_graph.nodes if concept_attrs[n].depth_label == "d1"
    }
    if not d1_subgraph:
        return set(), set()
    d1_calc_roots: set[str] = set()
    for n in d1_subgraph:
        for pred, _, ed in concept_graph.in_edges(n, data=True):
            if ed.get("kind") != "lineage":
                continue
            pa = concept_attrs[pred]
            if pa.derivation == Derivation.ROOT.value and pa.depth_label != "d1":
                d1_calc_roots.add(pred)
    return d1_calc_roots, d1_subgraph


def _add_d1_root_bucket(
    concept_attrs: dict[str, ConceptAttrs],
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
    for node in sorted(d1_calc_roots):
        address = concept_attrs[node].address
        bucket.primary_members.append(address)
        bucket.primary_node_ids.append(node)
        bucket.member_depths[address] = concept_attrs[node].depth_label
    gid = _group_id_for(bucket)
    buckets[gid] = bucket
    return gid


def _assign_groups(
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str] = frozenset(),
) -> tuple[dict[str, str], dict[str, GroupBucket]]:
    """Group every concept by dispatching to its derivation's rule.

    Rules are invoked once per derivation, lazily, via an `ensure_assigned`
    callback. A rule that needs another derivation already bucketed (BASIC
    walks its lineage parents to compute a stop-signature, and asks for
    each parent's group id) calls `ensure_assigned(parent_derivation)` and
    we run the rule for that derivation on-demand. This avoids any
    privileging in the call order — every derivation looks the same from
    the orchestrator's view; BASIC just happens to use the callback."""
    by_derivation: dict[str, list[tuple[str, ConceptAttrs]]] = defaultdict(list)
    for node in concept_graph.nodes:
        a = concept_attrs[node]
        by_derivation[a.derivation].append((node, a))

    primary_group: dict[str, str] = {}
    buckets: dict[str, GroupBucket] = {}
    assigned: set[str] = set()

    def ensure_assigned(derivation: str) -> None:
        if derivation in assigned:
            return
        assigned.add(derivation)
        rule = GROUPING_RULES.get(derivation, DEFAULT_RULE)
        items = by_derivation.get(derivation, [])
        for bucket in rule(
            items,
            concept_graph,
            concept_attrs,
            primary_group,
            ensure_assigned,
            output_addresses,
        ):
            group_id = _group_id_for(bucket)
            buckets[group_id] = bucket
            for node_id_member in bucket.primary_node_ids:
                primary_group[node_id_member] = group_id

    for derivation in by_derivation:
        ensure_assigned(derivation)
    return primary_group, buckets


def _attach_secondary_members(
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
) -> None:
    """Attach the concepts each grouping bucket implicitly carries beyond
    its primary set. Right now this is only the grain components of a
    GROUP-BY / PARTITION-BY bucket — they appear in the SELECT alongside
    the aggregates, so we record them as members for visualization and
    for the condition-placement pass.

    BASIC groups deliberately get nothing here: their passthrough
    capability is derived from topology in `_compute_concept_sets`
    (parent capability ∩ grain-compatible), not pre-declared. Earlier
    attempts to enumerate BASIC secondaries up front (all roots / lineage
    parents / lineage-parent grains) each leaked or starved a different
    test query."""

    def add(bucket: GroupBucket, address: str) -> None:
        if address in bucket.primary_members or address in bucket.secondary_members:
            return
        if address not in concept_attrs:
            return
        bucket.secondary_members.append(address)
        bucket.member_depths[address] = concept_attrs[address].depth_label

    for bucket in buckets.values():
        if bucket.derivation in GROUPING_DERIVATIONS:
            for grain_addr in bucket.grain_components:
                add(bucket, grain_addr)


def _materialize_group_graph(
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
    buckets: dict[str, GroupBucket],
    d1_root_gid: str | None = None,
    d1_calc_roots: set[str] | None = None,
    d1_subgraph: set[str] | None = None,
) -> tuple[nx.DiGraph, dict[str, GroupAttrs]]:
    """Realize the in-flight `GroupBucket` map as an nx.DiGraph plus a
    side-table of typed `GroupAttrs` keyed by group id. The graph holds
    topology + edge metadata only; downstream consumers read per-group state
    via `attrs[gid]` and get attribute access + type checking."""
    group_graph: nx.DiGraph = nx.DiGraph()
    attrs: dict[str, GroupAttrs] = {}
    for gid, bucket in buckets.items():
        members = tuple(bucket.primary_members) + tuple(bucket.secondary_members)
        attrs[gid] = GroupAttrs(
            depth_label=bucket.depth_label,
            derivation=bucket.derivation,
            grain_components=bucket.grain_components,
            label=bucket.label,
            members=members,
            primary_members=tuple(bucket.primary_members),
            secondary_members=tuple(bucket.secondary_members),
            member_depths=dict(bucket.member_depths),
            dedup_grain=bucket.dedup_grain,
        )
        group_graph.add_node(gid)

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
        edge_kind = edata.get("kind")
        if edge_kind not in ("lineage", "constraint", "existence"):
            continue
        if d1_root_gid is not None and u in d1_calc_roots and v in d1_subgraph:
            gu = d1_root_gid
        else:
            gu = primary_group[u]
        gv = primary_group[v]
        if gu == gv:
            continue
        # The group-edge `kind` records the strongest concept-level edge
        # that maps to it: lineage > constraint > existence. Lineage means
        # the row stream flows along this edge — JOIN partners, demand
        # propagation, sibling-grain projection. Constraint means topo
        # ordering for filter pushdown with implied JOIN. Existence means
        # side-channel only (subselect) — the source must be built and
        # ordered before the consumer, but never JOINed in or projected
        # for sibling JOIN keys. When edges collapse, the stronger
        # guarantee wins.
        _PRIORITY = {"existence": 0, "constraint": 1, "lineage": 2}
        existing = group_graph.get_edge_data(gu, gv)
        if existing is not None:
            current = existing.get("kind", edge_kind)
            if _PRIORITY[edge_kind] > _PRIORITY.get(current, 0):
                group_graph[gu][gv]["kind"] = edge_kind
        else:
            group_graph.add_edge(gu, gv, kind=edge_kind)

    return group_graph, attrs


def _main_lineage_groups(
    group_graph: nx.DiGraph,
    buckets: dict[str, GroupBucket],
    mandatory_list: list[BuildConcept],
) -> set[str]:
    """Groups in the user-facing pipeline: those whose primary members produce
    a mandatory concept, plus all their lineage-edge ancestors. Used to bias
    condition placement away from existence-only side channels (a d1 filter
    group that produces an `IN`-RHS feeder is reachable via row args but
    hosting the atom there self-references the existence concept and leaves
    the main row stream unfiltered)."""
    mandatory_addrs = {c.address for c in mandatory_list}
    seeds = {
        gid for gid, b in buckets.items() if mandatory_addrs & set(b.primary_members)
    }
    if not seeds:
        return set(buckets.keys())
    lineage_only = group_graph.edge_subgraph(
        [
            (u, v)
            for u, v, d in group_graph.edges(data=True)
            if d.get("kind") == "lineage"
        ]
    ).copy()
    for gid in buckets:
        lineage_only.add_node(gid)
    main: set[str] = set(seeds)
    for seed in seeds:
        main |= nx.ancestors(lineage_only, seed)
    return main


def _inject_conditions(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
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
    d1_root_ids = {gid for gid, b in buckets.items() if b.depth_label == ROOT_D1_DEPTH}
    main_lineage = (
        _main_lineage_groups(group_graph, buckets, mandatory_list)
        if mandatory_list
        else set(buckets.keys())
    )
    # Ancestor checks for "upstream-most" must follow lineage only — existence
    # edges go from a side-channel filter back to its consumer (e.g. d1 filter
    # → main root), which would falsely make root a *descendant* of the d1
    # filter and disqualify it from the upstream tiebreak.
    lineage_ancestors_graph = group_graph.edge_subgraph(
        [
            (u, v)
            for u, v, d in group_graph.edges(data=True)
            if d.get("kind") in ("lineage", "constraint")
        ]
    ).copy()
    for gid in buckets:
        lineage_ancestors_graph.add_node(gid)
    group_members: dict[str, set[str]] = {
        gid: set(b.primary_members) | set(b.secondary_members)
        for gid, b in buckets.items()
    }
    condition_group_ids: set[str] = set()

    # A group can host an atom iff every row-arg is in its INPUT row
    # stream — i.e. what its FROM clause provides. That's:
    #   - for a source group (no lineage ancestors): its own members (the
    #     datasource scan IS its input)
    #   - for any other group: the union of its ancestors' members (its
    #     parents' CTEs feed its FROM)
    # NOT including its own primaries — those are the *outputs* of this
    # group's derivation. You can't WHERE on `avg(price)` inside the same
    # SELECT that computes it; that filter must live downstream where
    # `avg(price)` is an input column. The "ancestors-only" rule below
    # forces those atoms to land on a consumer. Existence edges are excluded
    # — they're side-channel (the d1 filter back-edges into its main-graph
    # consumer); counting them as ancestry falsely demotes a true source
    # group like root and excludes its own scan columns from candidacy.
    def _reachable_input(gid: str) -> set[str]:
        ancestors = nx.ancestors(lineage_ancestors_graph, gid)
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
                if gid not in d1_root_ids and row_inputs <= _reachable_input(gid)
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
            # An input that is the primary OUTPUT of a d0 group (an aggregate /
            # row-shape barrier produces it) only exists as a column at that
            # group and below. Keep only candidates at-or-downstream of every
            # such producer (the producer itself hosts a plain HAVING against
            # its own aggregate; consumers host post-aggregate filters). When
            # the atom's inputs come from *separate* producers that never
            # re-converge before the sink (a cross-arm HAVING like
            # `cnt_00 <= cnt_99`, whose two counts are computed in distinct
            # multiselect arms), this empties the candidate set — the atom is a
            # post-merge filter and lands on FINAL, where both columns coexist.
            producer_closures: list[set[str]] = []
            for addr in row_inputs:
                producers = [
                    gid
                    for gid, b in buckets.items()
                    if b.depth_label == "d0" and addr in set(b.primary_members)
                ]
                if producers:
                    reach: set[str] = set(producers)
                    for p in producers:
                        reach |= nx.descendants(group_graph, p)
                    producer_closures.append(reach)
            restricted = [
                gid
                for gid in candidates
                if all(gid in reach for reach in producer_closures)
            ]
            if not restricted:
                attrs[FINAL_NODE_ID].condition_atoms.append(atom)
                attrs[FINAL_NODE_ID].conditions.append(str(atom))
                condition_group_ids.add(FINAL_NODE_ID)
                continue
            candidates = restricted
            cand_set = set(candidates)
            upstream_most = [
                gid
                for gid in candidates
                if not (cand_set & nx.ancestors(lineage_ancestors_graph, gid))
            ]
            # Prefer candidates on the main lineage pipeline. A side-channel
            # d1 filter group that produces this atom's existence arg can
            # show up here (`relevent_week_seq` is reachable through its own
            # row stream) — hosting the WHERE there self-references and
            # leaves the user-facing row stream unfiltered.
            main_upstream = [gid for gid in upstream_most if gid in main_lineage]
            main_cands = [gid for gid in candidates if gid in main_lineage]
            if main_upstream:
                chosen = main_upstream[0]
            elif upstream_most:
                chosen = upstream_most[0]
            elif main_cands:
                chosen = main_cands[0]
            else:
                chosen = candidates[0]
            chosen_ancestors = nx.ancestors(group_graph, chosen)
            offending = d0_group_ids & chosen_ancestors
            if offending:
                raise ValueError(
                    f"Atom {atom} would be injected at {chosen}, which is "
                    f"downstream of d0 barrier(s) {sorted(offending)}; "
                    f"conditions cannot be pushed past row-shape changes."
                )
            attrs[chosen].condition_atoms.append(atom)
            attrs[chosen].conditions.append(str(atom))
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
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
) -> None:
    """Attach a single FINAL sink that collects every non-d1 concept, with a
    merge edge from every group. Added before `_inject_conditions` so a
    cross-arm post-merge filter (which no pre-final group can host) can land on
    it; `_color_phases` colors the merge edges afterward like the rest."""
    non_condition_members = tuple(
        n for n in concept_graph.nodes if concept_attrs[n].depth_label != "d1"
    )
    group_graph.add_node(FINAL_NODE_ID)
    attrs[FINAL_NODE_ID] = GroupAttrs(
        depth_label="final",
        derivation="final",
        members=non_condition_members,
        conditions=[str(c) for c in conditions],
    )
    for gid in buckets:
        group_graph.add_edge(gid, FINAL_NODE_ID, kind="merge")


def _compute_concept_sets(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    mandatory_list: list[BuildConcept],
) -> None:
    """Per-group input/output/hidden concept sets, computed in two passes.

    Pass 1 (forward topo) — `capability`: the upper bound of what each
    group CAN expose. ROOT is its primaries (the scan provides them).
    Every other group's capability = own primaries plus every column from
    a parent's capability that's grain-compatible with this group's
    native grain. The grain filter applies uniformly: a grouping
    derivation (AGGREGATE/GROUP_TO/WINDOW) would change row shape if it
    projected a finer-grain column; a BASIC would inflate its implicit
    GROUP-BY for the same reason (this is what bit q04 when every root
    was attached optimistically).

    Pass 2 (reverse topo) — demand-driven `output_concepts`. For each
    successor S, expose `S.input_concepts ∩ capability`. Add own grain
    when this group is a grouping derivation (its SELECT must include
    those columns anyway). Add sibling-grain join keys: when a successor
    has multiple predecessors, any sibling-grain that we also carry
    becomes the merge key downstream.

    `input_concepts` = lineage parents of primary outputs (we compute
    those columns and need their args) + non-primary outputs
    (passthrough — must come from upstream) + condition row-args at
    this group.

    `hidden_concepts` stays empty at intermediate groups — hiding a
    column there makes it invisible to downstream merges (their join
    keys are read from non-hidden outputs only). The user-facing mask is
    applied to the FINAL contributor in `_assemble_final_node`."""
    mandatory_addresses = {c.address for c in mandatory_list}

    # Concept-level lineage parents indexed by child ADDRESS — addresses are
    # what the rest of this pass works with (output_concepts / primary_of
    # / capability are all address-keyed sets), so the lookup has to match.
    # Pre-phase-split this happened to work because outer-blank node_id ==
    # address; with the @condition phase, [@condition]X != X.
    lineage_parents: dict[str, set[str]] = {}
    for u, v, ed in concept_graph.edges(data=True):
        if ed.get("kind") != "lineage":
            continue
        u_addr = concept_attrs[u].address
        v_addr = concept_attrs[v].address
        lineage_parents.setdefault(v_addr, set()).add(u_addr)

    primary_of: dict[str, set[str]] = {}
    grain_of: dict[str, frozenset[str]] = {}
    derivation_of: dict[str, str] = {}
    native_grain_of: dict[str, frozenset[str]] = {}
    behavior_of = {}
    for gid in group_graph.nodes:
        a = attrs[gid]
        primary_of[gid] = set(a.primary_members)
        grain_of[gid] = frozenset(a.grain_components)
        derivation_of[gid] = a.derivation
    for gid, bucket in buckets.items():
        beh = behavior_for(derivation_of[gid])
        behavior_of[gid] = beh
        native_grain_of[gid] = beh.native_grain(bucket, concept_graph, concept_attrs)

    # Natural row-grain of each concept (the keys it functionally depends on).
    # Used when an aggregating group demands lineage args from a row-level
    # source — without also demanding the source grain, the root scan only
    # projects the aggregate's input columns and v3 inserts an implicit
    # GROUP BY to dedupe to that column shape, destroying the row population
    # AVG/SUM need (q09).
    source_grain_of: dict[str, frozenset[str]] = {}
    for _n in concept_graph.nodes:
        _addr = concept_attrs[_n].address
        _gc = concept_attrs[_n].grain_components
        if _gc:
            source_grain_of[_addr] = frozenset(_gc)

    # Effective-grain override for a basic that rides pointwise on a grouping
    # sibling. `native_grain_basic_inherited` walks CONCEPT lineage, so a basic
    # that renames a raw grain key (q66 `date.year as year_`) reports the raw
    # source grain ({date.id}) even though, once routed onto the aggregate
    # (group-level lineage edge), its rows live at the aggregate's grain. That
    # stale grain blocks the grain key from riding through as a merge join key,
    # so the FINAL merge re-joins on the value and fans out. When every leaf
    # input of the basic is covered by a grouping lineage-parent's grain +
    # outputs (i.e. it's genuinely computed pointwise over that parent), adopt
    # that parent's grain as the basic's effective grain.
    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        d = derivation_of[gid]
        if d == Derivation.ROOT.value or d in GROUPING_DERIVATIONS:
            continue
        leaves = _leaf_inputs(primary_of[gid], lineage_parents)
        if not leaves:
            continue
        grouping_preds = [
            p
            for p in group_graph.predecessors(gid)
            if group_graph.edges[p, gid].get("kind") == "lineage"
            and derivation_of[p] in GROUPING_DERIVATIONS
        ]
        # Same-grain grouping parents (e.g. q59's ratio reads both an aggregate
        # `sun_sales` and a `lead(...)` window, both at (store, week_seq)) are
        # considered collectively: the basic computes pointwise over their
        # shared grain, but each leaf may be a primary of a *different* parent.
        # Check coverage against the union of their primaries so a leaf supplied
        # by the aggregate isn't rejected when matched against the window.
        grains = {grain_of[p] for p in grouping_preds}
        if len(grains) != 1:
            continue
        gp_grain = next(iter(grains))
        # Only correct a grain that's actually stale: when the declared grain is
        # already covered by the grouping parents' grain, the lineage-inherited
        # grain is already right (rollup/window basics — q14/q36 — rely on it).
        # The fix targets a rename/derive whose declared grain (the source row
        # id) is finer than and disjoint from the parents' grain.
        if grain_of[gid] <= gp_grain:
            continue
        covered = set(gp_grain)
        for p in grouping_preds:
            covered |= primary_of[p]
        if leaves <= covered:
            grain_of[gid] = gp_grain
            native_grain_of[gid] = gp_grain

    # Topo includes all dependency edges (lineage / constraint / existence)
    # so a side-channel source builds before its consumer. The JOIN-key
    # and demand passes below treat each edge kind differently.
    dep_edges = [
        (u, v)
        for u, v, ed in group_graph.edges(data=True)
        if ed.get("kind") in ("lineage", "constraint", "existence")
    ]
    dep_graph = group_graph.edge_subgraph(dep_edges).copy()
    for n in group_graph.nodes:
        if n not in dep_graph:
            dep_graph.add_node(n)
    try:
        topo = list(nx.topological_sort(dep_graph))
    except nx.NetworkXUnfeasible:
        # The group-level lineage graph has a cycle — a genuine bug
        # upstream (q05's rowset shape currently triggers this). Skip the
        # capability/demand pass so callers that just want to render the
        # graph (discovery_v4 visualizations) can still proceed; SQL
        # generation will fail downstream where the cycle bites for real.
        try:
            cycle = nx.find_cycle(dep_graph)
        except nx.NetworkXNoCycle:
            cycle = None
        logger.warning(
            "[v4] group-graph lineage cycle, skipping concept-set pass: %s",
            cycle,
        )
        return

    # Pass 1: forward capability propagation. Per-derivation `can_preserve`
    # decides which upstream columns ride through each group.
    capability: dict[str, set[str]] = {gid: set() for gid in group_graph.nodes}
    for gid in topo:
        if gid == FINAL_NODE_ID:
            continue
        cap: set[str] = set(primary_of[gid])
        behavior = behavior_of.get(gid)
        if behavior is None or derivation_of[gid] == Derivation.ROOT.value:
            # Root scans can also project the grain keys of their primaries —
            # `store_sales.ticket_number` is reachable from a store_sales scan
            # even if no consumer asked for it. Exposing it in capability lets
            # an aggregating successor demand it to keep the scan at row-level
            # grain (see `source_grain_of` build above).
            for addr in list(cap):
                cap.update(source_grain_of.get(addr, frozenset()))
            capability[gid] = cap
            continue
        native = native_grain_of[gid]
        for pgid in group_graph.predecessors(gid):
            if pgid == FINAL_NODE_ID:
                continue
            for addr in capability.get(pgid, set()):
                if behavior.can_preserve(concept_graph, concept_attrs, native, addr):
                    cap.add(addr)
        capability[gid] = cap

    # Pass 2: backward demand-driven outputs / inputs.
    output_concepts: dict[str, set[str]] = {gid: set() for gid in group_graph.nodes}
    input_concepts: dict[str, set[str]] = {gid: set() for gid in group_graph.nodes}
    hidden_concepts: dict[str, set[str]] = {gid: set() for gid in group_graph.nodes}

    # Lineage-only reachability, for "does a row-shape-barrier below me already
    # produce this concept?" A mandatory concept produced by a grouping
    # descendant must be contributed by that barrier, not by a pre-barrier
    # group (whose pre-aggregation rows can't merge with the aggregated ones).
    lineage_edges_only = [
        (u, v)
        for u, v, ed in group_graph.edges(data=True)
        if ed.get("kind") == "lineage"
    ]
    lineage_sub = group_graph.edge_subgraph(lineage_edges_only).copy()
    for n in group_graph.nodes:
        if n not in lineage_sub:
            lineage_sub.add_node(n)

    output_concepts[FINAL_NODE_ID] = set(mandatory_addresses)
    input_concepts[FINAL_NODE_ID] = set(mandatory_addresses)

    # Pre-seed existence demand on each source bucket before the walk. An
    # atom like `x IN <subselect>` puts demand on the existence-arg's
    # source bucket, but the demand is side-channel — it doesn't flow
    # through the host's input_concepts (which drives JOIN dedup). Seeding
    # the source bucket directly means its lineage predecessors get
    # demanded via the normal backward walk and the whole compute chain
    # builds correctly without polluting JOIN logic.
    primary_to_gid: dict[str, str] = {}
    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        for addr in attrs[gid].primary_members:
            primary_to_gid.setdefault(addr, gid)
    existence_demand: dict[str, set[str]] = {gid: set() for gid in group_graph.nodes}
    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        for atom in attrs[gid].condition_atoms:
            for arg_group in atom.existence_arguments or ():
                for arg in arg_group:
                    source_gid = primary_to_gid.get(arg.address)
                    if source_gid is None or source_gid == gid:
                        continue
                    existence_demand[source_gid].add(arg.address)
    # A filter-nested semijoin (q08 `zips in substring(p_cust_zip,1,5)`) wires
    # its source via an `existence` group edge rather than a condition atom.
    # The source is a dedicated side-channel group; demand its own outputs so
    # it gets built and can back the `... IN (SELECT src FROM cte)` subselect.
    for src_gid, _, edata in group_graph.edges(data=True):
        if edata.get("kind") == "existence":
            existence_demand[src_gid].update(attrs[src_gid].primary_members)

    for gid in reversed(topo):
        if gid == FINAL_NODE_ID:
            continue
        cap_gid = capability[gid]
        outs: set[str] = existence_demand.get(gid, set()) & cap_gid
        for succ in group_graph.successors(gid):
            if succ == FINAL_NODE_ID:
                mand = cap_gid & mandatory_addresses
                # Drop a mandatory concept that a grouping (row-shape barrier)
                # lineage-descendant already produces — it's the barrier's to
                # contribute. Exposing it here (pre-barrier) leaks a rename of
                # a grain key into the aggregate's input as an ungrouped column
                # (q05 `s_channel`/`s_id` over a ROLLUP). Descendants are earlier
                # in reverse topo, so their outputs are already computed.
                for desc in nx.descendants(lineage_sub, gid):
                    if derivation_of.get(desc) in GROUPING_DERIVATIONS:
                        mand -= output_concepts[desc]
                outs |= mand
                # Same-grain FINAL contributors must expose their shared grain
                # so the merge joins on the grain key, not on whatever columns
                # happen to be shared (q39: a cov basic and the aggregate are
                # both at (item, warehouse) but the cov group exposed only
                # mean/cov, so the merge joined on `mean` values). Only our own
                # grain, only what we can actually produce (capability), and
                # only when a sibling contributor shares it — much narrower than
                # pulling every sibling's grain, which over-exposes constraint-
                # sibling keys (q08).
                my_grain = grain_of.get(gid, frozenset())
                if my_grain:
                    for sibling in group_graph.predecessors(succ):
                        if sibling == gid or sibling == FINAL_NODE_ID:
                            continue
                        # Only an EQUAL-grain sibling is a real merge partner to
                        # join on this key. A merely-overlapping (finer-grained)
                        # sibling — e.g. a d1 filter feeder sharing one key —
                        # isn't; exposing our grain there pins us to a key the
                        # actual partner lacks and blocks routing onto it (q58:
                        # item_id shares item.id with a filter, which kept it
                        # off the item.name aggregate and fanned out).
                        if grain_of.get(sibling, frozenset()) == my_grain:
                            outs |= my_grain & cap_gid
                            break
                continue
            edge_kind = group_graph.edges[gid, succ].get("kind", "lineage")
            # Existence siblings flow via subselect, not the row stream.
            # The source still needs to be built and ordered (topo above
            # handles that), but we don't pull row-stream demand from the
            # consumer or expose JOIN keys for sibling predecessors.
            if edge_kind == "existence":
                continue
            outs |= input_concepts.get(succ, set()) & cap_gid
            # Sibling-grain JOIN keys: when this successor has another
            # predecessor whose grain we also expose, project that key
            # so the downstream merge can JOIN us to the sibling on it.
            # Existence-kind siblings are side-channel only — skip them
            # to avoid pulling their grain columns into JOIN-irrelevant
            # places (q08's blank_root being asked to provide
            # customer.address.* because final_zips's bucket was a
            # constraint sibling of the d0 aggregate).
            for sibling in group_graph.predecessors(succ):
                if sibling == gid or sibling == FINAL_NODE_ID:
                    continue
                sibling_kind = group_graph.edges[sibling, succ].get("kind", "lineage")
                if sibling_kind == "existence":
                    continue
                outs |= grain_of.get(sibling, frozenset()) & cap_gid
        # Grouping derivations: own grain components must be in the SELECT
        # (they're the GROUP-BY / PARTITION-BY keys). Non-grouping groups
        # don't get this — their `grain_components` is the source row
        # identity, projecting it unconditionally would be noise.
        if derivation_of[gid] in GROUPING_DERIVATIONS:
            outs |= grain_of[gid] & cap_gid
        output_concepts[gid] = outs

        # Inputs: a primary's lineage args, a passthrough's own address,
        # plus condition row-args we haven't computed locally.
        primaries = primary_of[gid]
        ins: set[str] = set()
        is_grouping = derivation_of[gid] in GROUPING_DERIVATIONS
        for c in outs:
            if c in primaries:
                # Walk lineage through primaries computed *inside* this group,
                # demanding the first non-primary ancestor of each chain. A
                # primary whose lineage arg is itself another primary (q49:
                # channel -> channel_label -> sales_channel) would otherwise
                # stop at the intermediate and never demand the real input —
                # the renderer then has no row value and constant-folds it.
                stack = list(lineage_parents.get(c, set()))
                seen_chain: set[str] = set()
                while stack:
                    p = stack.pop()
                    if p in seen_chain:
                        continue
                    seen_chain.add(p)
                    if p in primaries:
                        stack.extend(lineage_parents.get(p, set()))
                        continue
                    ins.add(p)
                    # Aggregating derivations sit above a row-level source:
                    # also demand the row-grain of each lineage arg so the
                    # source scan stays at row grain. Without this the source
                    # would project only the requested value columns and v3
                    # inserts an implicit GROUP BY to dedupe (q09: AVG/SUM
                    # over deduped tuples).
                    if is_grouping:
                        for gc in source_grain_of.get(p, frozenset()):
                            if gc not in primaries:
                                ins.add(gc)
            else:
                # A passthrough that's a pure rename of this grouping group's
                # grain keys: demand the keys, not the rename, so the SELECT
                # derives it from the (grouped) key rather than passing through a
                # pre-materialized, ungrouped column — invalid under GROUP BY
                # (q05 `s_channel`/`s_id` over a ROLLUP).
                parents_c = lineage_parents.get(c, set())
                if is_grouping and parents_c and parents_c <= grain_of[gid]:
                    ins |= parents_c
                else:
                    ins.add(c)
        for atom in attrs[gid].condition_atoms:
            for arg in atom.row_arguments:
                if arg.address not in primaries:
                    ins.add(arg.address)
        input_concepts[gid] = ins

    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        attrs[gid].output_concepts = tuple(sorted(output_concepts[gid]))
        attrs[gid].hidden_concepts = tuple(sorted(hidden_concepts[gid]))
        attrs[gid].input_concepts = tuple(sorted(input_concepts[gid]))


def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
) -> tuple[nx.DiGraph, dict[str, GroupAttrs]]:
    """Collapse compatible concepts into groups and append a single FINAL sink.

    Returns the graph (topology + edge metadata) and a side-table of typed
    per-group attributes keyed by group id.

    Grouping is delegated to per-derivation rules in `group_rules.py`:
    most derivations group by equality on `(depth_label, grain)`; ROOT
    collapses to one bucket; BASIC merges by grain subset/equality.
    """
    output_addresses = frozenset(c.address for c in mandatory_list or [])
    primary_group, buckets = _assign_groups(
        concept_graph, concept_attrs, output_addresses
    )
    d1_calc_roots, d1_subgraph = _d1_calc_subgraph(concept_graph, concept_attrs)
    d1_root_gid = _add_d1_root_bucket(concept_attrs, buckets, d1_calc_roots)
    _attach_secondary_members(concept_graph, concept_attrs, buckets)
    group_graph, attrs = _materialize_group_graph(
        concept_graph,
        primary_group,
        buckets,
        d1_root_gid=d1_root_gid,
        d1_calc_roots=d1_calc_roots,
        d1_subgraph=d1_subgraph,
    )
    # FINAL must exist before injection so a cross-arm post-merge filter can
    # land on it (no pre-final group can host one); `_color_phases` then colors
    # its merge edges along with the rest.
    _add_final_node(
        group_graph, attrs, concept_graph, concept_attrs, buckets, conditions
    )
    condition_group_ids = _inject_conditions(
        group_graph, attrs, buckets, conditions, mandatory_list
    )
    _color_phases(group_graph, condition_group_ids)
    if mandatory_list is not None:
        _compute_concept_sets(
            group_graph, attrs, concept_graph, concept_attrs, buckets, mandatory_list
        )
        changed = _route_dimension_enrichments(
            group_graph, attrs, buckets, concept_attrs
        )
        changed = _route_basics_through_richer_siblings(group_graph, attrs) or changed
        if changed:
            # Topology changed — recompute output/input demand so the basic
            # now picks up its new aggregate parent's outputs as passthrough,
            # and `_assemble_final_node` sees a single contributor covering
            # all mandatory concepts (rather than basic + aggregate
            # siblings, which would force a redundant merge).
            _compute_concept_sets(
                group_graph,
                attrs,
                concept_graph,
                concept_attrs,
                buckets,
                mandatory_list,
            )
    return group_graph, attrs


def _route_dimension_enrichments(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
) -> bool:
    """Re-source a dimension-lookup basic from its own grain-keyed scan.

    A basic whose grain is a key K, whose every input is a property
    functionally determined by K, and which currently scans off the shared
    (fact-grain) root, renders at the fact's detail grain — so the FINAL
    merge that joins it to an aggregate on K fans out (q66 warehouse dims:
    one detail row per fact, joined onto the per-(warehouse, year) aggregate).

    When there is an aggregate whose grain contains K (the merge partner that
    makes this a star-schema enrichment, not a standalone dim query), split
    the basic's root source into a dedicated ROOT group scanning only the
    dimension columns. v3's datasource search then resolves that to the bare
    dimension table at grain {K} — one row per key — and the FINAL merge joins
    cleanly. Mirrors the reference's post-aggregate `JOIN warehouse`.
    """
    grouping_gids = [
        g
        for g in group_graph.nodes
        if g != FINAL_NODE_ID and attrs[g].derivation in GROUPING_DERIVATIONS
    ]
    if not grouping_gids:
        return False
    changed = False
    for bgid in list(group_graph.nodes):
        if bgid == FINAL_NODE_ID:
            continue
        b = attrs[bgid]
        if b.derivation != Derivation.BASIC.value:
            continue
        key = set(b.grain_components)
        if not key:
            continue
        lineage_preds = [
            p
            for p in group_graph.predecessors(bgid)
            if group_graph.edges[p, bgid].get("kind") == "lineage"
        ]
        # Pure dimension lookup: every row-stream source is a root scan. A
        # non-root lineage parent means the basic also rides a transform
        # (aggregate/window) and isn't a standalone key→property lookup.
        root_preds = [
            p for p in lineage_preds if attrs[p].derivation == Derivation.ROOT.value
        ]
        if not root_preds or len(root_preds) != len(lineage_preds):
            continue
        inputs = list(b.input_concepts)
        if not inputs:
            continue
        if not all(_fd_on_key(concept_attrs, a, key) for a in inputs):
            continue
        if not any(key <= set(attrs[g].grain_components) for g in grouping_gids):
            continue

        rdim_gid = f"grp:root:root:dim:{'|'.join(sorted(key))}"
        if rdim_gid not in group_graph:
            group_graph.add_node(rdim_gid)
            attrs[rdim_gid] = GroupAttrs(
                depth_label="root",
                derivation=Derivation.ROOT.value,
                grain_components=frozenset(),
                primary_members=tuple(inputs),
                members=tuple(inputs),
            )
            dim_bucket = GroupBucket(
                depth_label="root",
                derivation=Derivation.ROOT.value,
                grain_components=frozenset(),
            )
            dim_bucket.primary_members = list(inputs)
            buckets[rdim_gid] = dim_bucket
        group_graph.add_edge(rdim_gid, bgid, kind="lineage")
        for rp in root_preds:
            group_graph.remove_edge(rp, bgid)
        changed = True
    return changed


def _route_basics_through_richer_siblings(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
) -> bool:
    """For each basic group, find the most-enriched built sibling whose
    outputs already cover the basic's input demand, and add a lineage edge
    from it. The existing `_parent_nodes_for` ancestor-dedup logic in
    strategy_builder then drops the redundant upstream parent automatically.

    Picks the candidate with the most current ancestors (a tie-break on
    label-matching siblings) — i.e. the furthest-downstream sibling — so a
    basic that renames an aggregate's output reads the aggregate's rows,
    and a basic that renames a window's output reads the window's rows,
    etc. Without this routing, the basic builds against the bare source
    and the FINAL merge then has to reconcile a detail-row basic side with
    a transformed-row sibling — for non-standard-grouping aggregates this
    silently drops the rollup's NULL subtotal rows (q14)."""
    changed = False
    candidates_by_basic: list[str] = [
        gid
        for gid in group_graph.nodes
        if gid != FINAL_NODE_ID and attrs[gid].derivation == Derivation.BASIC.value
    ]
    other_groups: list[str] = [
        gid
        for gid in group_graph.nodes
        if gid != FINAL_NODE_ID
        and attrs[gid].derivation != Derivation.BASIC.value
        and attrs[gid].derivation != Derivation.ROOT.value
    ]
    for bgid in candidates_by_basic:
        b = attrs[bgid]
        if not b.input_concepts:
            continue
        needed = set(b.input_concepts)
        my_ancestors = nx.ancestors(group_graph, bgid)
        # Score each label-matching sibling that fully covers our inputs by
        # how many ancestors it has — more ancestors means further
        # downstream in the existing pipeline.
        best_gid: str | None = None
        best_depth = -1
        for ogid in other_groups:
            if ogid == bgid:
                continue
            if attrs[ogid].label != b.label:
                continue
            their_ancestors = nx.ancestors(group_graph, ogid)
            if not (my_ancestors & their_ancestors):
                continue  # not actually a sibling on the same pipeline
            if not needed.issubset(set(attrs[ogid].output_concepts)):
                continue
            if bgid in their_ancestors:
                continue  # would create a cycle
            depth = len(their_ancestors)
            if depth > best_depth:
                best_depth = depth
                best_gid = ogid
        if best_gid is not None and not group_graph.has_edge(best_gid, bgid):
            group_graph.add_edge(best_gid, bgid, kind="lineage")
            changed = True
    return changed
