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
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal, overload

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.author import SelectLineage
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildGrain,
    BuildRowsetItem,
    BuildWhereClause,
    get_grouped_aggregate_wrapper,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
    resolve_rowset_content_address,
)
from trilogy.core.processing.node_generators.presence_probe import is_presence_probe

from .concept_graph import (
    _statement_scoped_relation_members,
    computed_origin_relation_members,
    condition_stage_of_label,
)
from .condition_placement import PlacementReason, plan_condition_placements
from .constants import (
    DEPENDENCY_EDGE_KINDS,
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
    EdgePhase,
)
from .edges import (
    EdgeMap,
    add_edge,
    copy_edges,
    dependency_subgraph,
    edge_kind,
    edges_of_kind,
    lineage_edges,
    lineage_subgraph,
    remove_edge,
)
from .functional_dependency import build_fd_determines, concept_attr_fd_determines
from .group_behaviors import Behavior, behavior_for
from .group_rules import DEFAULT_RULE, GROUPING_RULES
from .models import (
    ConceptAttrs,
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    GroupBucket,
    GroupInputContract,
    InputChannel,
)

# depth_label used for the secondary root bucket dedicated to feeding d1
# (in-WHERE) aggregate calculations. Distinct from ``root`` so the bucket
# gets its own group id and doesn't collide with the main root bucket.
ROOT_D1_DEPTH = DepthLabel.ROOT_D1

_REGRAFTABLE_DERIVATIONS: set[Derivation] = {
    Derivation.BASIC,
    Derivation.WINDOW,
}


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
    return concept_attr_fd_determines(concept_attrs, key, address)


def _group_id_for(bucket: GroupBucket) -> str:
    grain_key = "|".join(sorted(bucket.grain_components)) or "∅"
    label_prefix = f"[{bucket.label}]" if bucket.label else ""
    suffix = f":{bucket.discriminator}" if bucket.discriminator else ""
    return (
        f"grp:{label_prefix}{bucket.derivation.value}:{bucket.depth_label.value}:"
        f"{grain_key}{suffix}"
    )


def _d1_calc_subgraph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> tuple[dict[int | None, set[str]], set[str]]:
    """Identify (d1_calc_roots by `then where` stage, d1_subgraph_nodes).

    Roots are keyed by the condition label's stage qualifier: None is the
    plain condition phase (every query today; the first cross-row-hosting
    stage of a chain), and each later cross-row-hosting stage gets its own
    root set — its feeder scan carries the earlier stages' bounds, so it must
    not be shared with a differently-bounded stage's computations.

    Any concept reached via the WHERE recursion lives at a condition-phase
    label (suffix ``@condition``) and is classified d1. We route a root →
    condition-phase lineage edge through a dedicated root_d1 bucket when the
    condition compute it feeds is a ROW-SHAPE BARRIER (an aggregate/window/etc.
    whose value depends on the row set, e.g. ``where x > avg(price)``): such a
    calc must see a pristine scan, free of sibling WHERE atoms pushed onto the
    SELECT-side scan.

    A condition concept that is only a scalar BASIC (``where flag = 'x'`` with
    ``flag <- case state_code ...``) needs no pristine scan — applying it over
    rows a sibling filter already narrowed still selects exactly the conjunction.
    Splitting its root would re-scan the source as an independent CTE and, when
    that root is co-sourced with the SELECT through a bridge, leave the condition
    re-scan cross-joined ON 1=1.

    A root therefore feeds root_d1 only when it feeds a d1 node that genuinely
    needs an independent scan, i.e. one that is EITHER:
      - a row-shape barrier (the pristine-scan case above); or
      - a condition that constrains a NON-grouping d0 output (a ROOT/BASIC the
        SELECT scans directly). That output is co-sourced into the same root
        bucket, so folding the condition into it would 2-cycle (root → condition
        lineage → root). Keeping the split breaks that cycle. When the constraint
        target is instead an aggregate, it lives in its own group and the
        condition folds into the co-sourced root cleanly.

    - d1_calc_roots: blank-phase roots feeding such a d1 node.
    - d1_subgraph_nodes: every condition-phase node. Edge routing uses
      this as the destination side of the predicate."""
    d1_subgraph: set[str] = {
        n for n in concept_graph.nodes if concept_attrs[n].depth_label == DepthLabel.D1
    }
    if not d1_subgraph:
        return {}, set()

    def _needs_independent_scan(n: str) -> bool:
        if concept_attrs[n].derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
            return True
        for succ in concept_graph.successors(n):
            kind = edge_kind(concept_edges, n, succ)
            # An existence source (a semijoin RHS, `x in <set>`) is a separate
            # discovery: its defining lineage must source from a private root, not
            # the SELECT's common root. Otherwise the fact columns that exist only
            # to define the set (q10's `channel`/`date.year` feeding the buyer-set
            # filters) sit in the shared root and drag the SELECT's dimension
            # projection onto the fact instead of its own dim tables.
            if kind == EdgeKind.EXISTENCE:
                return True
            if kind != EdgeKind.CONSTRAINT:
                continue
            if concept_attrs[succ].derivation not in GROUPING_DERIVATIONS:
                return True
        return False

    # Walk lineage upward from each d1 node that needs an independent scan; the
    # blank-phase ROOT ancestors are the roots whose condition scan must stay
    # separate from the SELECT-side scan. One walk per stage qualifier: a root
    # feeding two stages' computations belongs to both feeders (the scan is
    # duplicated per population, which is the point of the split).
    needing = [n for n in d1_subgraph if _needs_independent_scan(n)]
    roots_by_stage: dict[int | None, set[str]] = {}
    for stage in {condition_stage_of_label(concept_attrs[n].label) for n in needing}:
        d1_calc_roots: set[str] = set()
        visited: set[str] = set()
        stack: list[str] = [
            n
            for n in needing
            if condition_stage_of_label(concept_attrs[n].label) == stage
        ]
        while stack:
            cur = stack.pop()
            for pred, _ in concept_graph.in_edges(cur):
                if edge_kind(concept_edges, pred, cur) != EdgeKind.LINEAGE:
                    continue
                if pred in visited:
                    continue
                visited.add(pred)
                pa = concept_attrs[pred]
                if pa.derivation == Derivation.ROOT and pa.depth_label != DepthLabel.D1:
                    d1_calc_roots.add(pred)
                else:
                    stack.append(pred)
        roots_by_stage[stage] = d1_calc_roots
    return roots_by_stage, d1_subgraph


def _add_d1_root_buckets(
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    d1_calc_roots_by_stage: dict[int | None, set[str]],
) -> dict[int | None, str]:
    """Add an extra ROOT bucket per stage qualifier containing just that
    stage's d1-feeding roots. Returns group ids keyed like the input; the
    plain (None) bucket carries no discriminator, so it keeps its historical
    id. Buckets are added plain-first then by stage so bucket insertion order
    does not depend on set iteration order."""
    gids: dict[int | None, str] = {}
    for stage, d1_calc_roots in sorted(
        d1_calc_roots_by_stage.items(), key=lambda kv: (-1 if kv[0] is None else kv[0])
    ):
        if not d1_calc_roots:
            continue
        bucket = GroupBucket(
            depth_label=ROOT_D1_DEPTH,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
        )
        if stage is not None:
            bucket.discriminator = f"stage:s{stage}"
        for node in sorted(d1_calc_roots):
            address = concept_attrs[node].address
            bucket.primary_members.append(address)
            bucket.primary_node_ids.append(node)
            bucket.member_depths[address] = concept_attrs[node].depth_label
        gid = _group_id_for(bucket)
        buckets[gid] = bucket
        gids[stage] = gid
    return gids


def _prune_existence_exclusive_roots(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    d1_calc_roots: set[str],
    d1_subgraph: set[str],
    protected_addresses: frozenset[str] = frozenset(),
) -> None:
    """Drop from the shared ROOT bucket the roots that exist ONLY to define a
    semijoin-RHS set, once they have been duplicated into the private root_d1
    bucket.

    q10's buyer-set filters (``store_buyers <- pcid ? channel='STORE' and
    date.year=2002 ...``) are sourced as a separate discovery; their defining
    fact columns (``channel``, ``date.year``, ``date.month_of_year``) feed nothing
    but those filters. Left in the common ``grp:root:root:∅`` they force it to
    source from the fact, dragging the customer-dimension projection (demographics)
    onto the fact too. Removing them lets the shared root source the dimension
    standalone (``customer ⋈ demographics ⋈ address``) -- while the
    semijoin's join key (``pcid``, which also feeds the count) stays, sourced both
    from the fact (in root_d1) and the dimension (in the shared root) and joined by
    the ``IN``.

    A root is existence-exclusive when every concept it feeds is a condition
    node and the condition subgraph it feeds reaches an existence source
    (transitively -- a root feeding the set's filter through an intermediate
    BASIC is just as set-exclusive as one feeding it directly). A root that is
    itself a mandatory output or an outer-WHERE row argument is NEVER
    existence-exclusive: the SELECT (or a main-side condition atom) needs it
    from the shared root regardless of what else it feeds."""
    if not d1_calc_roots:
        return
    exclusive_addrs: set[str] = set()
    for root in d1_calc_roots:
        if concept_attrs[root].address in protected_addresses:
            continue
        successors = list(concept_graph.successors(root))
        if not successors or not all(s in d1_subgraph for s in successors):
            continue
        stack = [
            s
            for s in successors
            if edge_kind(concept_edges, root, s) == EdgeKind.LINEAGE
        ]
        seen: set[str] = set()
        feeds_existence_source = False
        while stack and not feeds_existence_source:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            for nxt in concept_graph.successors(cur):
                if edge_kind(concept_edges, cur, nxt) == EdgeKind.EXISTENCE:
                    feeds_existence_source = True
                    break
                if nxt in d1_subgraph:
                    stack.append(nxt)
        if feeds_existence_source:
            exclusive_addrs.add(concept_attrs[root].address)
    if not exclusive_addrs:
        return
    for bucket in buckets.values():
        if (
            bucket.derivation != Derivation.ROOT
            or bucket.depth_label != DepthLabel.ROOT
        ):
            continue
        keep = [
            i
            for i, address in enumerate(bucket.primary_members)
            if address not in exclusive_addrs
        ]
        if len(keep) == len(bucket.primary_members):
            continue
        bucket.primary_members = [bucket.primary_members[i] for i in keep]
        bucket.primary_node_ids = [bucket.primary_node_ids[i] for i in keep]


def _assign_groups(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
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
    by_derivation: dict[Derivation, list[tuple[str, ConceptAttrs]]] = defaultdict(list)
    for node in concept_graph.nodes:
        a = concept_attrs[node]
        # A non-ROWSET node tagged with a rowset (a presence probe over a
        # rowset member) is an obligation of that rowset's boundary: route it
        # to the rowset rule so it buckets with the boundary group and
        # `resolve_rowset` materializes it pre-merge.
        derivation = Derivation.ROWSET if a.rowset_name is not None else a.derivation
        by_derivation[derivation].append((node, a))

    primary_group: dict[str, str] = {}
    buckets: dict[str, GroupBucket] = {}
    assigned: set[Derivation] = set()

    def ensure_assigned(derivation: Derivation) -> None:
        if derivation in assigned:
            return
        assigned.add(derivation)
        rule = GROUPING_RULES.get(derivation, DEFAULT_RULE)
        items = by_derivation.get(derivation, [])
        for bucket in rule(
            items,
            concept_graph,
            concept_edges,
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


def _lineage_leaf_addresses(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    node: str,
) -> set[str]:
    """Addresses of `node`'s lineage ancestors that have no lineage parent of
    their own — the raw inputs the concept ultimately computes from."""
    leaves: set[str] = set()
    visited: set[str] = {node}
    stack = [node]
    while stack:
        cur = stack.pop()
        preds = [
            p
            for p, _ in concept_graph.in_edges(cur)
            if edge_kind(concept_edges, p, cur) == EdgeKind.LINEAGE
        ]
        if cur != node and not preds:
            leaves.add(concept_attrs[cur].address)
        for p in preds:
            if p not in visited:
                visited.add(p)
                stack.append(p)
    return leaves


def _finest_determining_key(
    determiners: list[str], environment: BuildEnvironment
) -> str | None:
    """Among candidate keys that all determine some member, the one determined by
    every other (the finest / most-downstream key). A dim FD by ``customer.id``
    is also transitively FD by the fact grain that determines ``customer.id``;
    the finest key is ``customer.id``. Returns None when no single key is finest
    (the member is FD by ≥2 incomparable entities — q65's cross-entity case —
    and must stay on the fact bucket, riding the aggregate keys)."""
    finest = [
        k
        for k in determiners
        if all(
            other == k
            or build_fd_determines(environment, {other}, k, include_empty_grain=False)
            for other in determiners
        )
    ]
    return finest[0] if len(finest) == 1 else None


def _composite_determining_grain(
    grains: list[frozenset[str]], address: str, environment: BuildEnvironment
) -> frozenset[str] | None:
    """The multi-key grouping grain that functionally determines `address`, when
    no single entity key does.

    A member can be FD by a COMPOSITE key rather than one entity — q20's partsupp
    ``part.available_quantity`` is determined by ``{part.id, part.supplier.id}``
    and by neither alone. Such a member is still a dimension: it can source from
    its own table keyed by that grain and join the aggregate on both columns,
    instead of riding the fact row stream and deduping back to grain in a sibling
    GROUP bucket. Ties break to the coarsest-first deterministic pick."""
    determining = [
        grain
        for grain in grains
        if address not in grain
        and build_fd_determines(
            environment, set(grain), address, include_empty_grain=False
        )
    ]
    if not determining:
        return None
    return min(determining, key=lambda grain: (len(grain), sorted(grain)))


def _row_arg_lineage_closure(arg: BuildConcept) -> set[str]:
    """The arg's address plus every address reachable through its lineage — a
    derived filter arg (``vehicle_label <- concat(name, '-', variant)``) needs
    its ROOT inputs co-located wherever the filter is evaluated."""
    closure: set[str] = set()
    stack: list[BuildConcept] = [arg]
    while stack:
        concept = stack.pop()
        if concept.address in closure:
            continue
        closure.add(concept.address)
        if concept.lineage is not None:
            stack.extend(
                c
                for c in concept.lineage.concept_arguments
                if isinstance(c, BuildConcept)
            )
    return closure


def _pre_aggregate_filter_args(
    conditions: list[BuildWhereClause],
) -> frozenset[str]:
    """Row-arg addresses of WHERE clauses that contain NO aggregate term — pure
    pre-aggregate filters that narrow the rows feeding an aggregate (q98's
    ``item.category in (...)``, which also bounds the class-total window).
    Expanded through each arg's lineage closure: a derived filter arg's ROOT
    inputs must stay on the fact scan too, or the group hosting the filter
    cannot render the derived expression (a concat label filter with
    ``vehicle_name`` peeled to a dim bucket strands ``vehicle_variant``).

    A clause that DOES carry an aggregate is a HAVING-style post-aggregate filter
    (q81's ``customer_state > scaled_state and address.state = 'GA'``): its dim
    args filter the OUTPUT after aggregation, so peeling them to a post-aggregate
    dim join is faithful. A pre-aggregate filter column peeled that way would move
    the WHERE after the aggregate (wrong sums / wrong window denominator)."""
    args: set[str] = set()
    for clause in conditions:
        if any(
            arg.derivation == Derivation.AGGREGATE for arg in clause.concept_arguments
        ):
            continue
        for arg in clause.row_arguments:
            args |= _row_arg_lineage_closure(arg)
    return frozenset(args)


def _post_aggregate_filter_args(
    conditions: list[BuildWhereClause],
) -> frozenset[str]:
    """Row-arg addresses of WHERE clauses that DO contain an aggregate term —
    HAVING-style post-aggregate filters (q30.alt's ``customer_state > scaled and
    billing_customer.address.state = 'GA'``).

    A HAVING dim arg filters the OUTPUT after aggregation, so peeling it to a
    standalone dim scan and semijoining on the entity key is faithful (one
    ``wakeful`` CTE sources the billing-customer dims AND applies
    ``state = 'GA'``). Unlike a selected dim column, a filter-only HAVING arg has no output
    to anchor it — but the placed condition still sources the column at the dim
    scan, so the WHERE is preserved (not dropped)."""
    args: set[str] = set()
    for clause in conditions:
        if any(
            arg.derivation == Derivation.AGGREGATE for arg in clause.concept_arguments
        ):
            args |= {arg.address for arg in clause.row_arguments}
    return frozenset(args)


def _post_aggregate_basic_args(
    mandatory_list: list[BuildConcept],
) -> frozenset[str]:
    """Non-aggregate row args of output BASICs that combine aggregate outputs
    with dimension attributes (q66's per-sqft columns: ``coalesce(sum, 0) /
    warehouse.square_feet``). Such an arg is read at the consumer's
    post-aggregate grouping grain, so peeling it to its entity's dim bucket
    joins it on the entity key exactly like a selected dim column — instead of
    riding the fact row stream and dedup-ing back to the grain. Row-shape
    barriers (aggregate/window/filter/rowset/union) are not descended: an
    aggregate arg marks the output as post-aggregate; any other barrier's
    inputs live at grains this walk can't vouch for."""
    args: set[str] = set()
    for concept in mandatory_list:
        if concept.derivation != Derivation.BASIC or concept.lineage is None:
            continue
        has_aggregate = False
        collected: set[str] = set()
        stack = [
            c for c in concept.lineage.concept_arguments if isinstance(c, BuildConcept)
        ]
        seen: set[str] = set()
        while stack:
            arg = stack.pop()
            if arg.address in seen:
                continue
            seen.add(arg.address)
            if arg.derivation == Derivation.AGGREGATE:
                has_aggregate = True
                continue
            if arg.derivation in (Derivation.ROOT, Derivation.CONSTANT):
                collected.add(arg.address)
                continue
            if arg.derivation == Derivation.BASIC and arg.lineage is not None:
                collected.add(arg.address)
                stack.extend(
                    c
                    for c in arg.lineage.concept_arguments
                    if isinstance(c, BuildConcept)
                )
        if has_aggregate:
            args |= collected
    return frozenset(args)


# Derivations whose output is a per-ROW scalar over its args, so an arg can be
# re-sourced from a dim table and joined back on an entity key without changing
# what the consumer reads. FILTER is included: it is not a row-shape barrier
# (see ROW_SHAPE_BARRIER_DERIVATIONS) and subsets rows without changing any
# surviving row's value — the same call the aggregate-input walk makes in
# `_ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS`.
_SCALAR_PROJECTION_DERIVATIONS = {Derivation.BASIC, Derivation.FILTER}


def _projected_scalar_root_args(
    mandatory_list: list[BuildConcept],
) -> frozenset[str]:
    """ROOT leaves projected through a scalar output alias.

    The peeled arg leaves the row stream it was read from and comes back joined
    on an entity key at post-aggregate grain, so the alias must read it PER ROW
    for that substitution to be faithful. A scalar expression (q66's
    ``sales / square_feet``) qualifies; a barrier reads its arg as a POPULATION
    — an aggregate over the fact rows, a window over its partition — and the
    key-join reintroduces it at the wrong multiplicity, so barrier args stay on
    the fact bucket.

    A filter is scalar in that sense, not a barrier: it subsets rows without
    changing any surviving row's value. Widening the whole walk to every
    non-barrier derivation (MULTISELECT/TVF_UNION/SUBSELECT) is NOT done — those
    are unexamined here."""
    args: set[str] = set()
    for concept in mandatory_list:
        if (
            concept.derivation not in _SCALAR_PROJECTION_DERIVATIONS
            or concept.lineage is None
        ):
            continue
        stack = list(concept.lineage.concept_arguments)
        seen: set[str] = set()
        while stack:
            arg = stack.pop()
            if arg.address in seen:
                continue
            seen.add(arg.address)
            if arg.derivation == Derivation.ROOT:
                args.add(arg.address)
            elif (
                arg.derivation in _SCALAR_PROJECTION_DERIVATIONS
                and arg.lineage is not None
            ):
                stack.extend(arg.lineage.concept_arguments)
    return frozenset(args)


def _finer_filter_grains(
    conditions: list[BuildWhereClause],
) -> frozenset[frozenset[str]]:
    """Grains of non-aggregate filter args that live at a multi-key grain — a
    WHERE/HAVING term needing fact-grain rows finer than any single entity (q20's
    ``part.available_quantity > …``, a partsupp measure at ``{part.id,
    supplier.id}``). An entity whose key sits inside such a grain must NOT be
    peeled: its rows are needed at the finer grain to evaluate the filter, and a
    standalone single-key dim scan can't serve it (condition becomes unplaceable).
    Aggregate args are excluded — they are their own grouping contributor."""
    return frozenset(
        frozenset(arg.grain.components)
        for clause in conditions
        for arg in clause.row_arguments
        if arg.derivation != Derivation.AGGREGATE
        and arg.grain is not None
        and len(arg.grain.components) > 1
    )


def _finer_filter_allows_dimension_key(
    key: str,
    grain: frozenset[str],
    grouping_keys: set[str],
    environment: BuildEnvironment,
) -> bool:
    """Allow a row-identity refinement, but not another independent entity."""
    other = set(grain) - {key}
    independent_keys = grouping_keys | environment.domain_graph.sole_grain_keys()
    return bool(other) and not (other & independent_keys)


def _preaggregate_filter_allows_dimension_member(
    address: str,
    key: str | None,
    grouping_buckets: list[GroupBucket],
    environment: BuildEnvironment,
) -> bool:
    """A pre-aggregate filter column peels only when the filter still drops whole
    groups after the peel: it must be FD by the entity key and that key must be a
    grouping key of every d0 grouping bucket.

    A WINDOW bucket disqualifies the peel outright. It emits one row per input
    row, so its value (rank, lag, row_number) is a function of the whole input
    POPULATION, not of a group's contents — a peeled column filters via a
    post-window entity-key semijoin, which leaves the window computed over the
    unfiltered rows (`where vehicle_name != 'B' select vehicle_name, rank ...`
    ranked all six launches, then dropped B's rows)."""
    if any(b.derivation == Derivation.WINDOW for b in grouping_buckets):
        return False
    concept = environment.concepts[address]
    return (
        key is not None
        and all(key in b.grain_components for b in grouping_buckets)
        and concept.grain is not None
        and set(concept.grain.components) == {key}
    )


def _split_root_dimension_clusters(
    buckets: dict[str, GroupBucket],
    primary_group: dict[str, str],
    environment: BuildEnvironment,
    output_addresses: frozenset[str],
    projected_scalar_root_args: frozenset[str],
    pre_aggregate_filter_args: frozenset[str],
    post_aggregate_args: frozenset[str],
    finer_filter_grains: frozenset[frozenset[str]],
) -> None:
    """Peel single-entity FD dimension clusters out of a keyed ROOT bucket into
    their own ``grp:root:root:dim:<entity_key>`` ROOT buckets.

    A wide output dimension projection (q81's 16 ``billing_customer.*`` columns)
    lands in the single keyed root bucket alongside the fact-grain columns it
    converges with at the FINAL projection. Sourced together they re-root on the
    fact (``catalog_returns ⋈ date_dim ⋈ …``) and dedup back to customer grain —
    those joins are avoided by sourcing the dims from their own tables keyed by
    ``billing_customer.id``.

    When a subset of a root bucket's members is functionally determined by a
    single entity key that is ALSO a downstream grouping key (so the FINAL merge
    already produces that key as a join column), that subset can source
    independently from its own dim tables and join on the key. Each such cluster
    becomes its own ROOT bucket. Per-entity: q65's ``item.desc`` (FD by item.id)
    and ``store.name`` (FD by store.id) each get their own bucket and join the
    aggregate on their key (``wakeful ⋈ item ⋈ store``). A member
    FD by two incomparable entities only co-occurs through the fact and stays put.

    FD is resolved against the full build environment (not the concept-graph
    side-table), so the chain through an intermediate FK that the query never
    names — ``customer.id → customer.current_addr → address.city`` — is visible.
    ``include_empty_grain=False`` so a constant is never treated as a dim member.
    """
    grouping_keys: set[str] = set()
    for bucket in buckets.values():
        if bucket.derivation in GROUPING_DERIVATIONS:
            grouping_keys |= set(bucket.grain_components)
    if not grouping_keys:
        return
    d0_grouping_buckets = [
        bucket
        for bucket in buckets.values()
        if bucket.derivation in GROUPING_DERIVATIONS
        and bucket.depth_label == DepthLabel.D0
    ]
    d0_grouping_grains = [bucket.grain_components for bucket in d0_grouping_buckets]
    for gid in list(buckets):
        bucket = buckets[gid]
        if (
            bucket.derivation != Derivation.ROOT
            or bucket.depth_label != DepthLabel.ROOT
            or bucket.discriminator  # skip single_row / existence / split variants
        ):
            continue
        member_addrs = set(bucket.primary_members)
        # Candidate entity keys: a member that is a downstream grouping key (so a
        # FINAL join column exists) and functionally determines another member.
        # Exclude a key a finer-grain filter needs at fact grain (q20): peeling it
        # to a single-key dim scan strands that filter (unplaceable condition).
        potential_candidates = [
            addr
            for addr in member_addrs
            if addr in grouping_keys
            and any(
                other != addr
                and build_fd_determines(
                    environment, {addr}, other, include_empty_grain=False
                )
                for other in member_addrs
            )
        ]
        candidates = [
            addr
            for addr in potential_candidates
            if not any(
                addr in grain
                and not _finer_filter_allows_dimension_key(
                    addr, grain, grouping_keys, environment
                )
                for grain in finer_filter_grains
            )
        ]
        # Composite dim keys: a downstream d0 grouping grain whose components all
        # live in this bucket. Members FD by the whole grain but by no single
        # entity peel onto it (q20's partsupp measure at {part, supplier}).
        composite_grains = [
            grain
            for grain in d0_grouping_grains
            if len(grain) > 1 and grain <= member_addrs
        ]
        if not candidates and not composite_grains:
            continue
        assignment: dict[str, frozenset[str]] = {}
        for addr in member_addrs:
            if addr in candidates:
                continue
            # Peel a SELECTED dimension column, OR a post-aggregate-only arg: a
            # filter-only HAVING arg (a post-aggregate WHERE arg the query never
            # projects, e.g. q30.alt's `billing_customer.address.state = 'GA'`)
            # or a dim attribute an output BASIC reads beside aggregate outputs
            # (q66's `square_feet` divisor). Both are consumed at post-aggregate
            # grain, so sourcing them at the dim scan joined on the entity key is
            # faithful — a HAVING's condition placed on the dim bucket still
            # applies its WHERE there (`wakeful` sources the dims AND
            # filters `state = 'GA'` in one CTE). A filter-only PRE-aggregate
            # arg is NOT peeled (`pre_aggregate_filter_args` gate below): its
            # WHERE must narrow the fact rows feeding the aggregate, not a
            # post-join dim.
            if addr not in output_addresses and addr not in post_aggregate_args:
                continue
            # A peeled filter-only arg applies as a FINAL entity-key semijoin.
            # That is faithful only when the filter drops WHOLE groups of every
            # output-lineage (d0) grouping bucket — each grain must FD-determine
            # the column (q30.alt: {return_state, customer.sk} → customer's
            # address state). A coarser-grain aggregate ({part} ⊬ segment) needs
            # the filter on its fact input rows; peeling silently drops it from
            # the d0 row stream (dual-scope: outputs recompute over admitted
            # rows). d1 population buckets are exempt — the WHERE never narrows
            # them.
            if addr not in output_addresses and not all(
                grain
                and build_fd_determines(
                    environment, set(grain), addr, include_empty_grain=False
                )
                for grain in d0_grouping_grains
            ):
                continue
            # Never peel a member that is itself a grouping key of some aggregate:
            # it is a grouping DIMENSION the query re-aggregates over (q24 regroups
            # by `customer.first_name, last_name, store.name`), not a passthrough.
            # It must stay at fact grain for that GROUP BY; routing it to a
            # standalone dim scan joined on the entity id breaks the regroup
            # (cross-join fan-out).
            if addr in grouping_keys:
                continue
            # Never peel a pre-aggregate filter column: its WHERE must stay on the
            # fact rows feeding the aggregate, but a peeled column carries its
            # filter to a post-aggregate dim join (q98's `category` would drop from
            # the class-total window). Left in place, the fact applies it correctly.
            determiners = [
                k
                for k in candidates
                if build_fd_determines(
                    environment, {k}, addr, include_empty_grain=False
                )
            ]
            finest = (
                _finest_determining_key(determiners, environment)
                if determiners
                else None
            )
            if (
                addr in pre_aggregate_filter_args
                and not _preaggregate_filter_allows_dimension_member(
                    addr, finest, d0_grouping_buckets, environment
                )
            ):
                continue
            if finest is not None:
                assignment[addr] = frozenset({finest})
                continue
            composite = _composite_determining_grain(
                composite_grains, addr, environment
            )
            if composite is not None:
                assignment[addr] = composite
        if not assignment:
            continue
        clusters: dict[frozenset[str], list[int]] = defaultdict(list)
        for idx, addr in enumerate(bucket.primary_members):
            if addr in assignment:
                clusters[assignment[addr]].append(idx)
        moved: set[int] = set()
        for key, indices in clusters.items():
            dim_bucket = GroupBucket(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain_components=frozenset(),
                label=bucket.label,
            )
            dim_bucket.discriminator = f"dim:{'|'.join(sorted(key))}"
            for idx in indices:
                addr = bucket.primary_members[idx]
                node_id = bucket.primary_node_ids[idx]
                dim_bucket.primary_members.append(addr)
                dim_bucket.primary_node_ids.append(node_id)
                dim_bucket.member_depths[addr] = bucket.member_depths.get(
                    addr, DepthLabel.ROOT
                )
                moved.add(idx)
            if any(
                bucket.primary_members[idx] in projected_scalar_root_args
                for idx in indices
            ):
                for key_address in key:
                    if (
                        key_address in environment.concepts
                        and key_address not in dim_bucket.primary_members
                    ):
                        dim_bucket.secondary_members.append(key_address)
                        dim_bucket.member_depths[key_address] = DepthLabel.ROOT
            dim_gid = _group_id_for(dim_bucket)
            buckets[dim_gid] = dim_bucket
            for idx in indices:
                primary_group[bucket.primary_node_ids[idx]] = dim_gid
        kept = [i for i in range(len(bucket.primary_members)) if i not in moved]
        bucket.primary_members = [bucket.primary_members[i] for i in kept]
        bucket.primary_node_ids = [bucket.primary_node_ids[i] for i in kept]


def _fold_rollup_key_dims(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    buckets: dict[str, GroupBucket],
) -> None:
    """Fold a BASIC dim that is purely a function of a ROLLUP/CUBE/GROUPING_SETS
    group's grouping keys into that group, so it is emitted as a column of the
    GROUP BY ROLLUP node and carries the rolled-up key values on the
    subtotal/grand-total rows.

    Otherwise such a dim (``channel <- case chan``, ``outlet <- concat('x',
    txt)``) buckets by stop-signature/grain into a leaf-grain BASIC group and is
    joined back to the rollup on the raw keys. At a subtotal row the rolled-up
    key is NULL, so the join finds no leaf match and the dim comes back NULL —
    dropping the dimension value that must be preserved (q80). Emit the derived
    dim as a non-aggregate column of the rollup group node instead; such dims
    are already marked nullable for the null-safe
    assembly join.

    Scoped to non-standard grouping: a standard GROUP BY key is never
    NULL-injected, so a downstream dim computed over the key join is already
    correct and folding there would only churn SQL shape."""
    rollups = [
        (gid, b)
        for gid, b in buckets.items()
        if b.derivation == Derivation.AGGREGATE and b.nulls_grouping_keys
    ]
    if not rollups:
        return
    # Most specific (largest key set) first so a dim folds into the rollup whose
    # keys most tightly cover it.
    rollups.sort(key=lambda kv: (-len(kv[1].grain_components), kv[0]))
    moves: dict[str, str] = {}
    for gid, bucket in buckets.items():
        if bucket.derivation != Derivation.BASIC:
            continue
        for node in bucket.primary_node_ids:
            attrs = concept_attrs[node]
            if attrs.depth_label in (DepthLabel.D1, ROOT_D1_DEPTH):
                continue
            # A pure rename is a pseudonym of its source key; the renderer
            # resolves it to the rolled-up key column directly, so it neither
            # needs nor tolerates folding (q86: i_category renames item.category,
            # a rollup key feeding a downstream window/grouping).
            if attrs.is_rename:
                continue
            leaves = _lineage_leaf_addresses(
                concept_graph, concept_edges, concept_attrs, node
            )
            if not leaves:
                continue
            for r_gid, r_bucket in rollups:
                if r_gid == gid or r_bucket.label != bucket.label:
                    continue
                if leaves <= set(r_bucket.grain_components):
                    moves[node] = r_gid
                    break
    for node, target_gid in moves.items():
        src = buckets[primary_group[node]]
        idx = src.primary_node_ids.index(node)
        address = src.primary_members.pop(idx)
        src.primary_node_ids.pop(idx)
        src.member_depths.pop(address, None)
        tgt = buckets[target_gid]
        if node not in tgt.primary_node_ids:
            tgt.primary_node_ids.append(node)
            tgt.primary_members.append(address)
            tgt.member_depths[address] = concept_attrs[node].depth_label
        primary_group[node] = target_gid
    for gid in [
        g
        for g, b in buckets.items()
        if not b.primary_node_ids and not b.secondary_members
    ]:
        del buckets[gid]


def _materialize_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    buckets: dict[str, GroupBucket],
    d1_root_gids: dict[int | None, str] | None = None,
    d1_calc_roots_by_stage: dict[int | None, set[str]] | None = None,
    d1_subgraph: set[str] | None = None,
) -> tuple[nx.DiGraph, dict[str, GroupAttrs], EdgeMap]:
    """Realize the in-flight `GroupBucket` map as an nx.DiGraph plus a
    side-table of typed `GroupAttrs` keyed by group id and the typed
    `group_edges` metadata map. The graph holds topology only; downstream
    consumers read per-group state via `attrs[gid]` and edge kinds via
    `group_edges`, getting attribute access + type checking."""
    group_graph: nx.DiGraph = nx.DiGraph()
    group_edges: EdgeMap = {}
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
            aggregate_input_grain=bucket.aggregate_input_grain,
            aggregate_distinct_addrs=frozenset(bucket.aggregate_distinct_addrs),
            grouping_mode=bucket.grouping_mode,
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
    # With `then where` stage-qualified condition labels, each stage has its
    # own R_d1 — the consuming node's label picks which feeder the edge
    # sources from.
    d1_root_gids = d1_root_gids or {}
    d1_calc_roots_by_stage = d1_calc_roots_by_stage or {}
    d1_subgraph = d1_subgraph or set()
    # The group-edge `kind` records the strongest concept-level edge that maps
    # to it: lineage > constraint > existence. Lineage means the row stream
    # flows along this edge — JOIN partners, demand propagation, sibling-grain
    # projection. Constraint means topo ordering for filter pushdown with
    # implied JOIN. Existence means side-channel only (subselect) — the source
    # must be built and ordered before the consumer, but never JOINed in or
    # projected for sibling JOIN keys. When edges collapse, the stronger
    # guarantee wins.
    priority: dict[EdgeKind, int] = {
        EdgeKind.EXISTENCE: 0,
        EdgeKind.CONSTRAINT: 1,
        EdgeKind.LINEAGE: 2,
    }
    for (u, v), ed in concept_edges.items():
        kind = ed.kind
        if kind not in DEPENDENCY_EDGE_KINDS:
            continue
        gu = primary_group[u]
        if d1_root_gids and v in d1_subgraph:
            stage = condition_stage_of_label(concept_attrs[v].label)
            feeder_gid = d1_root_gids.get(stage)
            if feeder_gid is not None and u in d1_calc_roots_by_stage.get(stage, ()):
                gu = feeder_gid
        gv = primary_group[v]
        if gu == gv:
            continue
        current = edge_kind(group_edges, gu, gv)
        if current is not None:
            if priority[kind] > priority[current]:
                group_edges[(gu, gv)].kind = kind
        else:
            add_edge(group_graph, group_edges, gu, gv, kind)

    # An EXISTENCE edge only orders the subselect source before its consumer.
    # When the source group is itself a LINEAGE-descendant of the consumer
    # group, that ordering is already implied by lineage and the extra edge is
    # a pure back-edge -- it forms a cycle with no new ordering. This happens
    # when the gated row arg and the existence source's input share one node
    # (two unnests merged into one group, then `orid_2 in even_orders` where
    # even_orders descends from that same unnest). Drop those redundant edges.
    lineage_sub = lineage_subgraph(group_graph, group_edges)
    for gu, gv in edges_of_kind(group_edges, EdgeKind.EXISTENCE):
        if gv in lineage_sub and gu in lineage_sub and nx.has_path(lineage_sub, gv, gu):
            remove_edge(group_graph, group_edges, gu, gv)

    return group_graph, attrs, group_edges


def _statement_relation_addresses(environment: BuildEnvironment) -> frozenset[str]:
    """Endpoints of query-scoped (STATEMENT) join declarations — the relations
    whose completion merge can null-extend a side in this query. Global
    `merge` identities are excluded: they pair INNER and never null-extend.
    (Computed-origin merges — which DO null-extend — are widened in per-query
    row-level statements by `plan_condition_placements`, not here: with a
    grouping contributor the completion merge sits below the aggregate and a
    FINAL-deferred side WHERE would filter aggregated rows.)"""
    from trilogy.core.domain_graph import EdgeScope

    return frozenset(
        addr
        for e in environment.domain_graph.edges
        if e.scope is EdgeScope.STATEMENT
        for addr in (e.source, e.target)
    )


def _inject_conditions(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    scoped_join_key_groups: dict[str, set[str]] | None = None,
    concept_attrs: dict[str, ConceptAttrs] | None = None,
    statement_relation_addresses: frozenset[str] = frozenset(),
    environment: BuildEnvironment | None = None,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> set[str]:
    """Apply the typed condition-placement plan to the mutable group attrs."""
    condition_group_ids: set[str] = set()
    placements = plan_condition_placements(
        group_graph,
        group_edges,
        buckets,
        conditions,
        mandatory_list,
        scoped_join_key_groups,
        concept_attrs,
        statement_relation_addresses,
        environment,
        staged_conditions,
    )
    for placement in placements:
        for gid in placement.group_ids:
            if placement.atom not in attrs[gid].condition_atoms:
                attrs[gid].condition_atoms.append(placement.atom)
                attrs[gid].conditions.append(str(placement.atom))
            # A conjunction-coverage copy is tagged so the builder can strip
            # it from a twin-reused aggregate at build time; its host is
            # already colored by the sibling atom that put it in play. A stage
            # precondition likewise leaves phase coloring to the atom's own
            # UPSTREAM_MOST placement: it is an input filter on a d1 side
            # channel, not a gate host.
            if placement.reason is PlacementReason.CONJUNCTION_RECOMPUTE:
                if placement.atom not in attrs[gid].conjunction_atoms:
                    attrs[gid].conjunction_atoms.append(placement.atom)
                continue
            if placement.reason is PlacementReason.STAGE_PRECONDITION:
                continue
            condition_group_ids.add(gid)
    return condition_group_ids


def _virtual_filter_scoped_columns(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    d1_gid: str,
) -> set[str]:
    """Columns already scoped by a `?` virtual-filter (`count(x ? a > b)`)
    downstream of `d1_gid`. Such a predicate is the user's per-aggregate
    selector — its source columns sit in `root_d1` only to feed that CASE-WHEN
    — so a raw outer filter on those columns must NOT be propagated onto the
    shared scan: doing so re-narrows a sibling aggregate that deliberately
    omits the predicate (q21: the unfiltered `distinct_suppliers_per_order`
    next to the late-only `late_suppliers_per_order`)."""
    addr_to_nodes: dict[str, list[str]] = {}
    for nid, ca in concept_attrs.items():
        addr_to_nodes.setdefault(ca.address, []).append(nid)
    scoped: set[str] = set()
    for desc in nx.descendants(group_graph, d1_gid):
        if attrs[desc].derivation != Derivation.FILTER:
            continue
        for addr in attrs[desc].primary_members:
            for nid in addr_to_nodes.get(addr, []):
                for parent in concept_graph.predecessors(nid):
                    if edge_kind(concept_edges, parent, nid) == EdgeKind.LINEAGE:
                        scoped.add(concept_attrs[parent].address)
    return scoped


def _propagate_raw_filters_to_d1_roots(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    datasource_columns: list[frozenset[str]],
) -> set[str]:
    """A `root_d1` bucket is a pristine DUPLICATE of a main fact scan, split off
    so a d1 calc reads rows untouched by *sibling* WHERE atoms — a
    WHERE-referenced cross-row computation gates at POPULATION scope, ignoring
    its peers in the clause (the #599 dual-scope contract; `where f = 1 and
    sum(z) by x > 5` gates on the unfiltered sums).

    The exception is a GROUP-ATOMIC atom: one whose every row arg is
    functionally determined by the grouping grain of EVERY cross-row d1
    aggregate reading the scan. Such an atom keeps or drops whole groups, so
    the population and filtered values coincide — and NOT propagating it leaks
    physical artifacts: q74's `billing_customer.sk is not null` (sk is the
    aggregates' by-key) left a NULL-sk population group that rode the final
    merge into a spurious all-NULL output row. Mirrors
    `_where_is_group_atomic` in where_scope_normalization.

    Additional gates, each still required: (a) root_d1 feeds no existence
    source (`week_seq in relevent_week_seq`, q02 — propagating cycles the CTE
    graph); (b) some single datasource carries BOTH the atom's columns and
    root_d1's scan columns (an item-dim avg is never narrowed by a sales-grain
    date filter, q06); (c) a `?`-virtual-filter-scoped predicate is the user's
    per-aggregate selector and stays off the shared scan (q21); (d) a d1
    WINDOW/GROUP_TO downstream blocks propagation entirely — atomicity for a
    window is partition-key containment, and partition keys are not visible on
    the bucket. Returns the root_d1 gids that gained atoms."""
    existence_sources = {u for u, _ in edges_of_kind(group_edges, EdgeKind.EXISTENCE)}
    d1_roots = [
        gid
        for gid in group_graph.nodes
        if gid in attrs and attrs[gid].depth_label == ROOT_D1_DEPTH
    ]
    main_roots = [
        gid
        for gid in group_graph.nodes
        if gid in attrs
        and attrs[gid].derivation == Derivation.ROOT
        and attrs[gid].depth_label == DepthLabel.ROOT
        and attrs[gid].condition_atoms
    ]
    touched: set[str] = set()
    for d1_gid in d1_roots:
        reach = nx.descendants(group_graph, d1_gid) | {d1_gid}
        if reach & existence_sources:
            continue
        cross_row_grains: list[set[str]] = []
        blocked = False
        for desc in reach - {d1_gid}:
            desc_attrs = attrs.get(desc)
            if desc_attrs is None or desc_attrs.depth_label != DepthLabel.D1:
                continue
            if desc_attrs.derivation in (Derivation.WINDOW, Derivation.GROUP_TO):
                blocked = True
                break
            if desc_attrs.derivation == Derivation.AGGREGATE:
                cross_row_grains.append(set(desc_attrs.grain_components))
        if blocked:
            continue
        d1_members = set(attrs[d1_gid].primary_members)
        # The datasource that supplies the MOST of root_d1's inputs is the table
        # the d1 aggregate actually scans (q74 → the sales fact; q06 → the item
        # table, whose `current_price` the per-category avg reads — NOT the fact,
        # even though they share the `item.id` join key). Propagate a raw filter
        # only when its columns live in THAT table — otherwise a filter on the
        # fact (`customer.id is not null`) wrongly narrows an item-level avg
        # (q06). Requiring all d1_members in one table fails for joined dim
        # attributes that aren't base columns (q74's `date.year`), so pick the
        # best-overlap table instead.
        scan_cols: frozenset[str] = frozenset()
        best = 0
        for cols in datasource_columns:
            overlap = len(d1_members & cols)
            if overlap > best:
                best = overlap
                scan_cols = cols
        if not scan_cols:
            continue
        filter_scoped = _virtual_filter_scoped_columns(
            group_graph, attrs, concept_graph, concept_edges, concept_attrs, d1_gid
        )
        for m_gid in main_roots:
            for atom in attrs[m_gid].condition_atoms:
                row_args = {a.address for a in atom.row_arguments}
                if not row_args <= scan_cols:
                    continue
                # A `?` virtual-filter downstream already scopes this predicate
                # per-aggregate; propagating it to the shared scan would narrow a
                # sibling aggregate that omits it (q21).
                if row_args and row_args <= filter_scoped:
                    continue
                # Group-atomicity: every row arg FD by every downstream d1
                # aggregate's grouping grain, so whole groups drop and the
                # population value is unchanged. A non-atomic peer must never
                # narrow a population gate's input (dual-scope contract).
                if not all(
                    _fd_on_key(concept_attrs, arg, grain)
                    for grain in cross_row_grains
                    for arg in row_args
                ):
                    continue
                if atom not in attrs[d1_gid].condition_atoms:
                    attrs[d1_gid].condition_atoms.append(atom)
                    attrs[d1_gid].conditions.append(str(atom))
                    touched.add(d1_gid)
    return touched


def _color_phases(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    condition_group_ids: set[str],
) -> set[str]:
    """Mark each edge as `pre_condition` or `post_condition`. Any edge from a
    condition-bearing group (or any descendant of one) is post-condition;
    everything earlier is pre-condition. Returns the downstream set so the
    FINAL-sink wiring can use the same coloring."""
    downstream: set[str] = set(condition_group_ids)
    for cgid in condition_group_ids:
        downstream |= nx.descendants(group_graph, cgid)
    for (u, v), attrs in group_edges.items():
        attrs.phase = (
            EdgePhase.POST_CONDITION if u in downstream else EdgePhase.PRE_CONDITION
        )
    return downstream


def _add_final_node(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
) -> None:
    """Attach a single FINAL sink that collects every non-d1 concept, with a
    merge edge from every group. Added before `_inject_conditions` so a
    cross-arm post-merge filter (which no pre-final group can host) can land on
    it; `_color_phases` colors the merge edges afterward like the rest."""
    non_condition_members = tuple(
        n for n in concept_graph.nodes if concept_attrs[n].depth_label != DepthLabel.D1
    )
    group_graph.add_node(FINAL_NODE_ID)
    attrs[FINAL_NODE_ID] = GroupAttrs(
        depth_label=DepthLabel.FINAL,
        members=non_condition_members,
        conditions=[str(c) for c in conditions],
        final_contract=(
            FinalAssemblyContract(
                output_addresses=frozenset(c.address for c in mandatory_list),
                required_grain=frozenset(
                    BuildGrain.from_concepts(mandatory_list).components
                ),
            )
            if mandatory_list is not None
            else None
        ),
    )
    for gid in buckets:
        add_edge(group_graph, group_edges, gid, FINAL_NODE_ID, EdgeKind.MERGE)


def _rowset_join_key_addresses(
    concept: BuildConcept, mandatory_by_address: dict[str, BuildConcept]
) -> set[str]:
    key_addresses = set(concept.keys or set())
    if not key_addresses and concept.grain:
        key_addresses = set(concept.grain.components)
    if not key_addresses:
        # A keyless, grainless rowset handle (a global-aggregate scalar body:
        # `(select max(val)/2 -> half)`) has no join axis. Expanding through its
        # own lineage would put the aggregate VALUE into the merge grain and
        # force the row side to re-render the aggregate at row grain
        # (AGG_GRAIN_MISMATCH). Such a boundary cross-joins ON 1=1.
        return set()
    output: set[str] = set()
    for key_address in key_addresses:
        key_concept = mandatory_by_address.get(key_address)
        if key_concept is None or key_concept.lineage is None:
            output.add(key_address)
            continue
        output |= {arg.address for arg in key_concept.lineage.concept_arguments}
    return output


def _resolved_rowset_grain(
    grain: Iterable[str], environment: BuildEnvironment | None
) -> frozenset[str]:
    """`grain` with every rowset key resolved through its boundary. Sibling
    grains must be compared in these terms — `rs_a.grp_key` and `rs_b.grp_key`
    both wrap `local.grp_key`, so the raw addresses never match even when the
    two sit at the same base grain."""
    return frozenset(resolve_rowset_content_address(a, environment) for a in grain)


def _unwrapped_rowset_grain(
    grain: Iterable[str],
    environment: BuildEnvironment | None,
    rollup_padded: frozenset[str],
) -> frozenset[str]:
    """Just the BASE addresses that `grain`'s rowset keys unwrap to — per
    component, so a key already spelled at its base contributes nothing while a
    sibling that unwraps ONTO that same base still does. ROLLUP-padded keys are
    dropped: their subtotal rows are NULL, so they are not a row identity and
    must never be volunteered as a join axis."""
    return frozenset(
        resolved
        for component in grain
        if (resolved := resolve_rowset_content_address(component, environment))
        != component
        and resolved not in rollup_padded
    )


def _rollup_padded_addresses(
    environment: BuildEnvironment | None,
) -> frozenset[str]:
    """Grouping keys of every ROLLUP/CUBE/GROUPING SETS aggregate in scope.
    The subtotal rows NULL these, so they are not a row identity and must never
    be volunteered as a join axis: pairing on one drops every subtotal row (a
    rolled-up NULL matches nothing). The join resolver applies the same rule
    per-datasource via `rollup_padded_addresses`; this is the environment-wide
    view the demand pass needs before any datasource exists."""
    if environment is None:
        return frozenset()
    padded: set[str] = set()
    for concept in (
        *environment.concepts.values(),
        *environment.alias_origin_lookup.values(),
    ):
        wrapper = get_grouped_aggregate_wrapper(concept)
        if wrapper is not None and wrapper.grouping.nulls_grouping_keys:
            padded |= {c.address for c in wrapper.by}
        # A rowset carries the spec on the SELECT it wraps, not on the
        # aggregate: at demand time the members are still plain STANDARD
        # aggregates and only `select.grouping` says the pass NULL-pads.
        if isinstance(concept.lineage, BuildRowsetItem):
            select = concept.lineage.rowset.select
            if isinstance(select, SelectLineage):
                grouping = select.grouping
                if grouping is not None and grouping.mode.nulls_grouping_keys:
                    padded |= {ref.address for ref in grouping.by}
    return frozenset(padded)


def _lineage_pinned_grain(
    addresses: frozenset[str] | set[str], environment: BuildEnvironment | None
) -> frozenset[str]:
    """Grain components pinned by fixed-grain barriers in the lineage of
    ``addresses``. A BASIC that renames or derives from an aggregate
    (`cust_state_amt as total`) is a fixed-grain barrier exactly like the
    aggregate itself: its rows are unique at the aggregate's grain and cannot
    be regenerated finer. Same for a rename of a rowset member
    (`buyers_b.cust_id as b_cust`): its join axis is what the body value is
    keyed by (`id`), which sibling contributors expose. The walk stops at each
    barrier — grains BELOW it are pre-aggregation / body row grains, not join
    axes. Members of an AUTHORED scoped-join relation are skipped: the
    authored keys are the axis there, and pairing on internal grain too would
    silently narrow the authored fan-out (q59 shape)."""
    if environment is None:
        return frozenset()
    scoped_members = environment.all_scoped_join_group_members()
    stack: list[BuildConcept] = []
    for addr in addresses:
        concept = environment.concepts.get(addr) or environment.alias_origin_lookup.get(
            addr
        )
        if concept is not None:
            stack.append(concept)
    grain: set[str] = set()
    seen: set[str] = set()
    while stack:
        concept = stack.pop()
        if concept.address in seen:
            continue
        seen.add(concept.address)
        # A ROLLUP/CUBE/GROUPING SETS aggregate NULLs its grouping keys on the
        # subtotal rows, so those keys are not a row identity and cannot serve
        # as a join axis: pinning them drops every subtotal row from the merge
        # (a rolled-up NULL never pairs). The `nulls_grouping_keys` wrapper is
        # the same authority `_renders_nonstandard_grouping` uses, and it sees
        # through a rowset handle to the aggregate underneath.
        wrapper = get_grouped_aggregate_wrapper(concept)
        if wrapper is not None and wrapper.grouping.nulls_grouping_keys:
            continue
        if concept.derivation in GROUPING_DERIVATIONS:
            if concept.grain:
                grain |= set(concept.grain.components)
            continue
        if concept.derivation == Derivation.ROWSET:
            axis = set(concept.keys or set())
            if not axis and isinstance(concept.lineage, BuildRowsetItem):
                select_grain = concept.lineage.rowset.select.grain
                if select_grain:
                    axis = set(select_grain.components)
            if not ({concept.address} | axis) & scoped_members:
                grain |= {resolve_rowset_content_address(a, environment) for a in axis}
            continue
        if concept.lineage is not None:
            stack.extend(concept.lineage.concept_arguments)
    return frozenset(grain)


def _final_host_count(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], address: str
) -> int:
    return sum(
        1
        for gid in group_graph.predecessors(FINAL_NODE_ID)
        if gid in attrs and address in attrs[gid].members
    )


def _final_merge_grain(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment | None = None,
    relation_edge_members: frozenset[str] = frozenset(),
) -> frozenset[str]:
    mandatory_by_address = {concept.address: concept for concept in mandatory_list}
    scoped_members = (
        environment.all_scoped_join_group_members()
        if environment is not None
        else frozenset()
    )
    grain: set[str] = set()
    for gid in group_graph.predecessors(FINAL_NODE_ID):
        if gid not in attrs:
            continue
        # Every contributor's grain is a merge axis, not only a grouping one: a
        # BASIC over aggregates (`customer_status <- case ... min(x) by user`)
        # sits at user grain and its CTE emits that key, but with only grouping
        # contributors counted the merge grain came back empty, so no ROOT
        # sibling could be given a join key and the merge fell to ON 1=1.
        # A ROWSET is excluded: its grain components are namespaced internals
        # (`even_orders.order_id`) that pair nothing on their own and must be
        # resolved through the rowset's lineage to the shared base key — which
        # the mandatory-concept pass below already does.
        if attrs[gid].derivation != Derivation.ROWSET:
            grain |= set(attrs[gid].grain_components)
    for concept in mandatory_list:
        if concept.derivation in GROUPING_DERIVATIONS and concept.grain:
            grain |= set(concept.grain.components)
        elif concept.derivation == Derivation.ROWSET:
            # An AUTHORED relation between rowsets pins the merge to exactly
            # the authored keys: two independently-filtered rowsets related by
            # `union join ty.code = ny.code` must not also pair on their shared
            # internal grain (wk pseudonyms, the common base measure) — those
            # extra equalities silently narrow the authored fan-out
            # (independent_rowset_matrix, q59 shape).
            raw_keys = set(concept.keys or set())
            if not raw_keys and concept.grain:
                raw_keys = set(concept.grain.components)
            authored = (raw_keys | {concept.address}) & scoped_members
            if authored:
                grain |= authored
                # The pin controls the JOIN AXIS, but the merge grain also
                # states the result's row grain: with only the authored keys
                # the merge reads COARSER than its rowset contributor and
                # force-groups a redundant final dedup (q46). A remaining
                # grain member that is itself a requested output and hosted
                # by no other FINAL contributor cannot add an unauthored
                # equality — keep it so the row grain stays honest.
                grain |= {
                    key
                    for key in (raw_keys - authored) & set(mandatory_by_address)
                    if _final_host_count(group_graph, attrs, key) <= 1
                }
            else:
                grain |= _rowset_join_key_addresses(concept, mandatory_by_address)
        else:
            # A BASIC rename of a rowset handle (`buyers_a.cust_id as a_cust`)
            # carries the rowset's namespaced grain key (`buyers_a.id`), which
            # sibling rowsets don't share. Resolve it to the base join key
            # (`local.id`) every rowset boundary exposes so the FINAL merge joins
            # on it instead of cross-joining ON 1=1 (test_rowset_alias_name_
            # collision: cartesian). Only fires when a key actually unwraps a
            # rowset, so plain BASIC concepts don't widen the grain.
            for key_address in concept.keys or set():
                resolved = resolve_rowset_content_address(key_address, environment)
                if resolved != key_address:
                    grain.add(resolved)
    # A mixed root↔rowset relation (`union join return_demos.demo_id = c_demo`)
    # whose members are not outputs never enters the grain through the loops
    # above — the rowset boundary and the mate's contributor then share no
    # declared join key and the FINAL merge cross-joins ON 1=1. The authored
    # members ARE the join axis: add them whenever a member lives on a FINAL
    # rowset boundary and another contributor exists to pair with.
    if environment is not None and environment.scoped_join_key_groups:
        finals = [
            gid for gid in group_graph.predecessors(FINAL_NODE_ID) if gid in attrs
        ]
        rowset_namespaces: set[str] = set()
        for gid in finals:
            if attrs[gid].derivation == Derivation.ROWSET:
                for member in attrs[gid].members:
                    namespace, _, _ = member.rpartition(".")
                    if namespace:
                        rowset_namespaces.add(namespace)
        if len(finals) > 1 and rowset_namespaces:
            for canonical, members in environment.scoped_join_key_groups.items():
                relation_addrs = {canonical, *members}
                if any(
                    addr.rpartition(".")[0] in rowset_namespaces
                    for addr in relation_addrs
                ):
                    grain |= relation_addrs
        # A relation carried on the concept graph's RELATION edges (an
        # authored axis whose collapsed side is a first-class computed node)
        # and HOSTED by different FINAL contributors is the merge's join axis:
        # the sides recombine only through the authored equality. This covers
        # the inline computed-member family (`union join rank orders.oid
        # order by orders.amt desc = customers.rnk`), where one side's member
        # is its own row-shape-barrier group and the other side's canonical
        # rides a ROOT scan — no rowset namespace exists for the block above
        # to see. Only the hosted members enter the grain: an unhosted third
        # member of a chained relation is not this merge's to demand.
        if len(finals) > 1 and relation_edge_members:
            for canonical, members in environment.scoped_join_key_groups.items():
                relation_addrs = {canonical, *members}
                if not relation_addrs & relation_edge_members:
                    continue
                hosts: set[str] = set()
                hosted: set[str] = set()
                for gid in finals:
                    have = relation_addrs & set(attrs[gid].members)
                    if have:
                        hosts.add(gid)
                        hosted |= have
                if len(hosts) > 1 and len(hosted) > 1:
                    grain |= hosted
    return frozenset(grain)


def _group_final_grain_contribution(
    attrs: dict[str, GroupAttrs],
    gid: str,
    merge_grain: frozenset[str],
    environment: BuildEnvironment | None = None,
) -> frozenset[str]:
    if gid not in attrs:
        return frozenset()
    if attrs[gid].derivation in GROUPING_DERIVATIONS:
        return attrs[gid].grain_components
    if attrs[gid].derivation == Derivation.ROWSET:
        return merge_grain
    # A BASIC group projecting a rowset rename has a namespaced grain
    # (`buyers_a.id`); resolve it to the shared base key so its projection_grain
    # advertises the join key the merge needs (mirrors `_final_merge_grain`).
    resolved = {
        resolve_rowset_content_address(addr, environment)
        for addr in attrs[gid].grain_components
    }
    rowset_keys = (resolved - set(attrs[gid].grain_components)) & merge_grain
    # A STATEMENT-scoped relation member the group carries is the merge's join
    # axis whether or not it is the group's grain: `subset join
    # best.pair_rank_best = worst.pair_rank_worst` projects only the two product
    # names, so neither BASIC rename group advertises the rank and the FINAL
    # merge cross-joins ON 1=1 (q44's decorrelated best/worst pairing). Global
    # `merge` identities are excluded — they pair INNER and never define a
    # statement's join axis (advertising one strands a partial dimension).
    if environment is not None:
        available = set(attrs[gid].input_concepts) | set(attrs[gid].output_concepts)
        rowset_keys |= (
            _statement_scoped_relation_members(environment) & merge_grain & available
        )
    # A non-grouping contributor whose outputs ride a fixed-grain barrier (a
    # BASIC rename/derivation of an aggregate: `cust_state_amt as total`, or
    # of a rowset member) is grain-pinned at that barrier's grain. Without
    # advertising it the assembly-side merge grain (union of contributor
    # projection grains) can collapse to empty, which disables every join-key
    # preservation guard in `_wrap_for_grain` and cross-joins the FINAL
    # parents ON 1=1 (q30: two keyless group parents fanning out to billions
    # of rows).
    pinned = frozenset(attrs[gid].output_concepts) or frozenset(attrs[gid].members)
    rowset_keys |= _lineage_pinned_grain(pinned, environment)
    return frozenset(rowset_keys)


def _refresh_final_contract(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment | None = None,
    relation_edge_members: frozenset[str] = frozenset(),
) -> None:
    if FINAL_NODE_ID not in attrs:
        return
    output_addresses = frozenset(c.address for c in mandatory_list)
    merge_grain = _final_merge_grain(
        group_graph, attrs, mandatory_list, environment, relation_edge_members
    )
    contributors: list[FinalContributorContract] = []
    for gid in sorted(group_graph.predecessors(FINAL_NODE_ID)):
        if gid not in attrs:
            continue
        preserve_keys = (
            merge_grain if attrs[gid].derivation == Derivation.ROOT else frozenset()
        )
        contributors.append(
            FinalContributorContract(
                group_id=gid,
                output_addresses=frozenset(attrs[gid].output_concepts),
                preserve_keys=preserve_keys,
                projection_grain=_group_final_grain_contribution(
                    attrs, gid, merge_grain, environment
                ),
            )
        )
    # A bare axis-member projection (every output is a member of a scoped
    # join-key relation) has no dedup grain of its own: the output IS the
    # joined relation row-by-row, so a same-key fan-out (two this-year weeks
    # matching one next-year store) must survive into the result (q59 shape,
    # independent_rowset_matrix). Deduping to the members' combined "grain"
    # would collapse those rows to distinct key pairs.
    scoped_members = (
        environment.all_scoped_join_group_members()
        if environment is not None
        else frozenset()
    )
    axis_only_projection = bool(scoped_members) and all(
        concept.address in scoped_members for concept in mandatory_list
    )
    # ...and only when some relation has BOTH sides projected: the row-by-row
    # contract is the author pairing the sides themselves (q59 keys_only fan).
    # A SOLE projected member of its relation is the unified axis — the
    # coalesce of the domains, deduped to the output grain (bare-member
    # projection cell).
    if axis_only_projection and environment is not None:
        mandatory_addresses = {concept.address for concept in mandatory_list}
        axis_only_projection = any(
            len({canonical, *members} & mandatory_addresses) >= 2
            for canonical, members in environment.scoped_join_key_groups.items()
        )
    attrs[FINAL_NODE_ID].final_contract = FinalAssemblyContract(
        output_addresses=output_addresses,
        required_grain=frozenset(BuildGrain.from_concepts(mandatory_list).components),
        merge_grain=merge_grain,
        contributor_contracts=tuple(contributors),
        deduplicate_to_grain=not axis_only_projection,
    )


def _consumer_required_input_grain(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> frozenset[str]:
    # A group's own derived output can be a grain component (a filter/rowset
    # concept whose identity IS its grain). Parents can't supply it — requiring
    # it as an input grain forces a parent to re-derive the concept (e.g. a
    # filter's per-row CASE at a merge that lacks the aggregate arg). Drop it.
    grain: set[str] = set(attrs[gid].grain_components) - set(attrs[gid].primary_members)
    # Likewise a component COMPUTED by a grouping row parent (a window ordering
    # by a coarser-grain aggregate carries the aggregate in its grain) is a
    # column that parent supplies, not a join axis siblings can carry — widening
    # a raw scan with it is unrenderable. The parent's grain (added below) is
    # the joinable identity.
    parent_computed: set[str] = set()
    row_preds = [
        pred
        for pred in group_graph.predecessors(gid)
        if pred != FINAL_NODE_ID
        and pred in attrs
        and edge_kind(group_edges, pred, gid) != EdgeKind.EXISTENCE
    ]
    for pred in row_preds:
        if (
            attrs[pred].derivation in GROUPING_DERIVATIONS
            or attrs[pred].derivation == Derivation.ROWSET
        ):
            # A pred whose row stream reaches this consumer through a GROUPING
            # descendant pred that grouped the axis away has no such axis at
            # this merge: requiring it resurrects the raw pred as a parent CTE
            # beside the descendant, and the two share no joinable key (rollup
            # over union-joined rowsets: the raw `r` rowset beside the rollup
            # aggregate, whose ROLLUP grain is `channel` only). The
            # descendant's own grain, declared below on its own iteration, is
            # the merge axis.
            pred_grain = set(attrs[pred].grain_components)
            axis_grouped_away = any(
                other != pred
                and pred in nx.ancestors(group_graph, other)
                and attrs[other].derivation in GROUPING_DERIVATIONS
                and not (pred_grain <= set(attrs[other].output_concepts))
                for other in row_preds
            )
            if axis_grouped_away:
                continue
            grain |= pred_grain
    return frozenset(grain - parent_computed)


# Consumers that JOIN their row parents (vs. stack/expand them). Only these need
# a bridge join key declared; UNION arms stack and UNNEST/RECURSIVE expand, so a
# shared key there is not a join condition.
_ROW_JOIN_CONSUMER_DERIVATIONS: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.WINDOW,
    Derivation.GROUP_TO,
    Derivation.BASIC,
}


def _transitive_lineage_ancestors(
    seeds: frozenset[str], lineage_parents: dict[str, set[str]]
) -> set[str]:
    """All addresses reachable upward from `seeds` through concept lineage."""
    ancestors: set[str] = set()
    stack = list(seeds)
    while stack:
        addr = stack.pop()
        for parent in lineage_parents.get(addr, ()):
            if parent not in ancestors:
                ancestors.add(parent)
                stack.append(parent)
    return ancestors


def _shared_row_parent_join_keys(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    key_addresses: frozenset[str],
    lineage_parents: dict[str, set[str]],
) -> frozenset[str]:
    """Keys carried by >=2 row-stream parents of `gid` that bridge a fixed-grain
    dimension CTE to the fact scan — the join key their merge must use. A
    dimension derived from the source of the consumer's grouping key (top_x's
    `top_orders <- rank(order)` over the `order`-bearing fact; bound_conversion's
    `date_converted <- date_string` over the `date_string`-bearing scan) is its
    own CTE whose only link to the fact rows is that shared source key. Without
    declaring it, `_consumer_required_input_grain` yields only the consumer grain
    (which the fact parent lacks) and the merge degrades to ON 1=1 (top_x) or
    cross-joins (bound_conversion).

    A key counts only when it is BOTH a lineage ancestor of the consumer's own
    grouping grain (the column the group key is derived from) AND not held by any
    parent as that parent's own grain. A grain-owning parent is the canonical
    dimension at the key and folds with the others, and a key unrelated to the
    grouping grain is co-carried by plain projections of a common scan — declaring
    either a preserve_key over-constrains sourcing (tpc_h q22 / gcat
    parenthetical)."""
    if attrs[gid].derivation not in _ROW_JOIN_CONSUMER_DERIVATIONS:
        return frozenset()
    row_parents = [
        pred
        for pred in group_graph.predecessors(gid)
        if pred != FINAL_NODE_ID
        and pred in attrs
        and edge_kind(group_edges, pred, gid) != EdgeKind.EXISTENCE
    ]
    if len(row_parents) < 2:
        return frozenset()
    grain_ancestors = _transitive_lineage_ancestors(
        attrs[gid].grain_components, lineage_parents
    )
    if not grain_ancestors:
        return frozenset()
    counts: dict[str, int] = defaultdict(int)
    grain_owners: set[str] = set()
    for pred in row_parents:
        for addr in set(attrs[pred].output_concepts):
            counts[addr] += 1
            if addr in attrs[pred].grain_components:
                grain_owners.add(addr)
    return frozenset(
        addr
        for addr, n in counts.items()
        if n >= 2
        and addr in key_addresses
        and addr in grain_ancestors
        and addr not in grain_owners
    )


def _refresh_input_contracts(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_attrs: dict[str, ConceptAttrs],
    concept_edges: EdgeMap,
) -> None:
    key_addresses = frozenset(
        a.address for a in concept_attrs.values() if a.purpose == Purpose.KEY
    )
    lineage_parents = _lineage_parents_by_address(concept_edges, concept_attrs)
    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID or gid not in attrs:
            continue
        required_grain = _consumer_required_input_grain(
            group_graph, group_edges, attrs, gid
        )
        bridge_keys = _shared_row_parent_join_keys(
            group_graph, group_edges, attrs, gid, key_addresses, lineage_parents
        )
        # A non-grouping consumer pairing a GROUPING row parent (a population
        # aggregate at grain G) with row-grain siblings joins them ON G — the
        # aggregate's value repeats per G-group across the row stream (`sum(z)
        # by x + w`: the sum CTE pairs to the w rows on x). Declare G so the
        # sibling projections keep the bridge instead of degrading to 1=1.
        grouping_parent_grain: set[str] = set()
        if attrs[gid].derivation not in GROUPING_DERIVATIONS:
            row_parents = [
                pred
                for pred in group_graph.predecessors(gid)
                if pred != FINAL_NODE_ID
                and pred in attrs
                and edge_kind(group_edges, pred, gid) != EdgeKind.EXISTENCE
            ]
            if len(row_parents) >= 2:
                for pred in row_parents:
                    if attrs[pred].derivation in GROUPING_DERIVATIONS:
                        grouping_parent_grain |= set(attrs[pred].grain_components)
        contracts: list[GroupInputContract] = []
        for pred in sorted(group_graph.predecessors(gid)):
            if pred == FINAL_NODE_ID or pred not in attrs:
                continue
            kind = edge_kind(group_edges, pred, gid)
            is_existence = kind == EdgeKind.EXISTENCE
            required_outputs = frozenset(attrs[pred].output_concepts) & frozenset(
                attrs[gid].input_concepts
            )
            contracts.append(
                GroupInputContract(
                    parent_group_id=pred,
                    consumer_group_id=gid,
                    required_outputs=required_outputs,
                    required_grain=frozenset() if is_existence else required_grain,
                    preserve_keys=(
                        frozenset()
                        if is_existence
                        else required_grain | bridge_keys | grouping_parent_grain
                    ),
                    channel=(
                        InputChannel.EXISTENCE
                        if is_existence
                        else InputChannel.ROW_STREAM
                    ),
                    may_project_dimension=(
                        bool(required_grain)
                        and not is_existence
                        and attrs[pred].derivation not in GROUPING_DERIVATIONS
                    ),
                )
            )
        attrs[gid].input_contracts = tuple(contracts)


@dataclass
class GroupFacts:
    primary: set[str]
    grain: frozenset[str]
    derivation: Derivation | None
    native_grain: frozenset[str] = frozenset()
    behavior: Behavior | None = None


@dataclass
class GroupIOPlan:
    capability: dict[str, set[str]] = field(default_factory=dict)
    outputs: dict[str, set[str]] = field(default_factory=dict)
    inputs: dict[str, set[str]] = field(default_factory=dict)
    hidden: dict[str, set[str]] = field(default_factory=dict)

    @classmethod
    def for_groups(cls, group_graph: nx.DiGraph) -> "GroupIOPlan":
        return cls(
            capability={gid: set() for gid in group_graph.nodes},
            outputs={gid: set() for gid in group_graph.nodes},
            inputs={gid: set() for gid in group_graph.nodes},
            hidden={gid: set() for gid in group_graph.nodes},
        )


def _lineage_parents_by_address(
    concept_edges: EdgeMap, concept_attrs: dict[str, ConceptAttrs]
) -> dict[str, set[str]]:
    lineage_parents: dict[str, set[str]] = {}
    for u, v in lineage_edges(concept_edges):
        u_addr = concept_attrs[u].address
        v_addr = concept_attrs[v].address
        lineage_parents.setdefault(v_addr, set()).add(u_addr)
    return lineage_parents


def _source_grain_by_address(
    concept_graph: nx.DiGraph, concept_attrs: dict[str, ConceptAttrs]
) -> dict[str, frozenset[str]]:
    source_grain: dict[str, frozenset[str]] = {}
    for node in concept_graph.nodes:
        address = concept_attrs[node].address
        grain = concept_attrs[node].grain_components
        if grain:
            source_grain[address] = frozenset(grain)
    return source_grain


def _build_group_facts(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
) -> dict[str, GroupFacts]:
    facts: dict[str, GroupFacts] = {}
    for gid in group_graph.nodes:
        a = attrs[gid]
        facts[gid] = GroupFacts(
            primary=set(a.primary_members),
            grain=frozenset(a.grain_components),
            derivation=a.derivation,
        )
    for gid, bucket in buckets.items():
        behavior = behavior_for(bucket.derivation)
        facts[gid].behavior = behavior
        facts[gid].native_grain = behavior.native_grain(
            bucket, concept_graph, concept_edges, concept_attrs
        )
    return facts


def _grouping_output_contributors(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    rides: dict[str, frozenset[str]],
    gid: str,
) -> list[str]:
    """`gid`'s lineage predecessors that sit on a grouping output — the grouping
    groups themselves plus any group already established as riding one. ROOT
    predecessors never qualify: they carry raw fact columns the grouped rows do
    not have."""
    return [
        pred
        for pred in group_graph.predecessors(gid)
        if edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE and pred in rides
    ]


def _extend_grouping_output_rides(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    facts: dict[str, GroupFacts],
    lineage_parents: dict[str, set[str]],
    rides: dict[str, frozenset[str]],
) -> bool:
    """One pass: mark every non-grouping group whose whole input demand is met
    by grouping-output contributors as riding that grouping grain, re-anchoring
    its grain when it was pinned finer. Returns whether anything changed."""
    changed = False
    for gid, fact in facts.items():
        if gid == FINAL_NODE_ID or gid in rides:
            continue
        if (
            fact.derivation == Derivation.ROOT
            or fact.derivation in GROUPING_DERIVATIONS
        ):
            continue
        leaves = _leaf_inputs(fact.primary, lineage_parents)
        if not leaves:
            continue
        contributors = _grouping_output_contributors(
            group_graph, group_edges, rides, gid
        )
        grains = {rides[pred] for pred in contributors}
        if len(grains) != 1:
            continue
        grouping_grain = next(iter(grains))
        covered = set(grouping_grain)
        for pred in contributors:
            covered |= facts[pred].primary
        if not leaves <= covered:
            continue
        rides[gid] = grouping_grain
        changed = True
        if not fact.grain <= grouping_grain:
            fact.grain = grouping_grain
            fact.native_grain = grouping_grain
    return changed


def _apply_grouping_parent_grain_overrides(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    facts: dict[str, GroupFacts],
    lineage_parents: dict[str, set[str]],
) -> None:
    """Correct non-grouping groups that compute pointwise over grouping parents.

    Iterated to a fixed point because a *chained* named intermediate reaches the
    grouping output one bucket further out than the one-hop spelling: `parent <-
    case when level = 0 then category end` over `level <- grouping(category) +
    grouping(class)` consumes `level`, a primary of a sibling BASIC rather than
    of the ROLLUP itself, so `parent` only reads as pointwise-over-the-rollup
    once that sibling has been marked. Left pinned at its key-transitive fact
    grain, `parent` keeps a redundant fact rescan alive through parent election,
    and that rescan INNER JOINs back onto the ROLLUP rows on their NULL-injected
    grouping keys, fanning every subtotal row out (q36). Each pass only adds to
    `rides`, so this terminates."""
    rides: dict[str, frozenset[str]] = {
        gid: fact.grain
        for gid, fact in facts.items()
        if gid != FINAL_NODE_ID and fact.derivation in GROUPING_DERIVATIONS
    }
    while _extend_grouping_output_rides(
        group_graph, group_edges, facts, lineage_parents, rides
    ):
        pass


def _widen_window_grain_to_grouping_parent(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    facts: dict[str, GroupFacts],
) -> None:
    """A WINDOW runs pointwise over its parent's rows -- it never reduces grain.
    Its bucket grain is the partition-by key, which can be coarser than the rows
    it actually emits: a `rank() over (partition by g1)` over a `sum() by rollup
    g1, g2` produces one row per rollup row (grain g1, g2), not one per g1. Left
    at the partition grain, the window node exposes only g1, so the FINAL merge
    joins it back to the dims on that single non-unique key and fans out the
    ROLLUP subtotal rows (test_window_over_rollup_preserves_grouping_rows). Widen
    to the grouping parent's grain so the window carries the full join key."""
    for gid, fact in facts.items():
        if gid == FINAL_NODE_ID or fact.derivation != Derivation.WINDOW:
            continue
        grouping_grains = {
            facts[pred].grain
            for pred in group_graph.predecessors(gid)
            if edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE
            and facts[pred].derivation in GROUPING_DERIVATIONS
        }
        if len(grouping_grains) != 1:
            continue
        parent_grain = next(iter(grouping_grains))
        if fact.grain < parent_grain:
            fact.grain = parent_grain
            fact.native_grain = parent_grain


def _widen_mixed_scalar_basic_to_final_spine(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    facts: dict[str, GroupFacts],
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment | None,
    relation_edge_members: frozenset[str] = frozenset(),
) -> None:
    """A BASIC scalar mixing row values with a by-key aggregate
    (`order_status <- case when amount = min(amount) by user_id ...`) is
    pointwise over the merged row stream, not over its args' composite grain.
    Left at that coarser grain beside a finer FINAL merge it becomes a
    row-bearing contributor whose key set is disjoint from its siblings'
    (stranded cover, keyless merge), and its joined value NULLs out on
    extension rows where the CASE's ELSE must produce a value. Widen to the
    FINAL merge grain so the group hosts the spine merge itself and computes
    the scalar over the full, extension-bearing stream."""
    if FINAL_NODE_ID not in facts:
        return
    candidates: list[str] = []
    for gid, fact in facts.items():
        if fact.derivation != Derivation.BASIC or not fact.grain:
            continue
        if not group_graph.has_edge(gid, FINAL_NODE_ID):
            continue
        lineage_preds = [
            pred
            for pred in group_graph.predecessors(gid)
            if edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE
        ]
        pred_derivations = {facts[pred].derivation for pred in lineage_preds}
        if Derivation.ROOT not in pred_derivations or not (
            pred_derivations & GROUPING_DERIVATIONS
        ):
            continue
        # A grouping parent that NULL-injects its keys (ROLLUP/CUBE) is not a
        # by-key value stream: joining or re-graining its consumer at the row
        # spine pairs subtotal rows on their NULLed keys (q36/q70/q86).
        if any(
            attrs[pred].nulls_grouping_keys
            for pred in lineage_preds
            if facts[pred].derivation in GROUPING_DERIVATIONS
        ):
            continue
        candidates.append(gid)
    if not candidates:
        return
    spine = _final_merge_grain(
        group_graph, attrs, mandatory_list, environment, relation_edge_members
    )
    # Only a spine that is the result's own row identity qualifies: every
    # spine key is a requested output, or is FD-determined by them (the by-key
    # aggregate's key under an output fact grain). A merge grain carrying
    # genuinely finer internal keys (q59: raw fact keys under a store/week
    # output) means the scalar rides post-dedup — re-graining it onto that
    # finer internal stream is a placement regression, not a fix.
    if not spine:
        return
    mandatory_addresses = {c.address for c in mandatory_list}
    extra_keys = spine - mandatory_addresses
    if extra_keys and (
        environment is None
        or not all(
            build_fd_determines(
                environment, mandatory_addresses, addr, include_empty_grain=False
            )
            for addr in extra_keys
        )
    ):
        return
    for gid in candidates:
        fact = facts[gid]
        if fact.grain < spine:
            fact.grain = spine
            fact.native_grain = spine


def _topological_dependency_order(
    group_graph: nx.DiGraph, group_edges: EdgeMap
) -> list[str] | None:
    dep_graph = dependency_subgraph(group_graph, group_edges)
    try:
        return list(nx.topological_sort(dep_graph))
    except nx.NetworkXUnfeasible:
        try:
            cycle = nx.find_cycle(dep_graph)
        except nx.NetworkXNoCycle:
            cycle = None
        logger.warning(
            "[v4] group-graph lineage cycle, skipping concept-set pass: %s",
            cycle,
        )
        return None


def _scoped_axis_mates(
    environment: BuildEnvironment | None,
) -> dict[str, frozenset[str]]:
    """Each STATEMENT-scoped join axis member -> the OTHER sides' members. They
    are alternative physical columns for one logical axis, so a group that owns
    one of them owns the axis. Global `merge` identities are excluded (they pair
    INNER and never redefine row identity) — EXCEPT a merge whose collapsed
    member keeps a computed origin (`merge recursive_parent into
    root_parent.id`): that is the same axis-equality-between-different-lineages
    as a statement relation, and the concept graph re-injects it with RELATION
    edges, so its members must count as axis mates or the computing group never
    exposes the axis and is dropped from the FINAL cover."""
    if environment is None:
        return {}
    scoped = set(_statement_scoped_relation_members(environment)) | set(
        computed_origin_relation_members(environment)
    )
    mates: dict[str, set[str]] = {}
    for canonical, members in environment.scoped_join_key_groups.items():
        relation = {canonical, *members} & scoped
        for address in relation:
            mates.setdefault(address, set()).update(relation - {address})
    return {address: frozenset(other) for address, other in mates.items()}


def _hosted_condition_outputs(
    atoms: list[BoolExpr], derivation: Derivation | None, capability: set[str]
) -> set[str]:
    if derivation in GROUPING_DERIVATIONS or not any(
        arg.derivation == Derivation.AGGREGATE
        for atom in atoms
        for arg in atom.row_arguments
    ):
        return set()
    return {arg.address for atom in atoms for arg in atom.row_arguments} & capability


def _compute_concept_sets(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    mandatory_list: list[BuildConcept],
    scoped_join_member_addresses: frozenset[str] = frozenset(),
    scoped_axis_mates: dict[str, frozenset[str]] | None = None,
    relation_edge_members: frozenset[str] = frozenset(),
    environment: BuildEnvironment | None = None,
) -> None:
    """Per-group input/output/hidden concept sets.

    The pass has three explicit pieces:
    - stable graph facts (`GroupFacts`): primary members, grain, behavior
    - forward capability: which columns each group can preserve
    - reverse demand: which outputs/inputs each group must expose
    """
    mandatory_addresses = {c.address for c in mandatory_list}
    # Members of authored scoped-join relations: the authored keys are the join
    # axis for the buckets hosting them, so rowset-grain resolution must not
    # volunteer extra equalities there (it would silently narrow the authored
    # fan-out, q59 shape).
    scoped_relation_members = scoped_join_member_addresses | (
        environment.all_scoped_join_group_members()
        if environment is not None
        else frozenset()
    )
    rollup_padded = _rollup_padded_addresses(environment)
    # A struct field demanded as the canonical key (`local.a`) is produced under
    # its derivable pseudonym (`unnest_array.a`); the FINAL demand intersect must
    # match those aliases so the producing group keeps the field as an output.
    # Gate strictly on the canonical key having NO group of its own — i.e. it is
    # only reachable through the pseudonym. A plain alias (`channel_out <-
    # channel_label`) IS a group primary, so it keeps its own projection and must
    # not be matched against its alias's group.
    all_primary_members = {
        member
        for gid, a in attrs.items()
        if gid != FINAL_NODE_ID
        for member in a.primary_members
    }
    mandatory_alias_addresses = set(mandatory_addresses)
    for c in mandatory_list:
        if c.address not in all_primary_members:
            mandatory_alias_addresses.update(c.pseudonyms)
    lineage_parents = _lineage_parents_by_address(concept_edges, concept_attrs)
    source_grain_of = _source_grain_by_address(concept_graph, concept_attrs)
    facts = _build_group_facts(
        group_graph, attrs, concept_graph, concept_edges, concept_attrs, buckets
    )
    _apply_grouping_parent_grain_overrides(
        group_graph, group_edges, facts, lineage_parents
    )
    _widen_window_grain_to_grouping_parent(group_graph, group_edges, facts)
    _widen_mixed_scalar_basic_to_final_spine(
        group_graph,
        group_edges,
        facts,
        attrs,
        mandatory_list,
        environment,
        relation_edge_members,
    )
    topo = _topological_dependency_order(group_graph, group_edges)
    if topo is None:
        return

    pseudonym_mates: dict[str, frozenset[str]] = {}
    mate_accumulator: dict[str, set[str]] = {}
    for ca in concept_attrs.values():
        for twin in ca.pseudonyms:
            mate_accumulator.setdefault(twin, set()).add(ca.address)
            mate_accumulator.setdefault(ca.address, set()).add(twin)
    pseudonym_mates = {k: frozenset(v) for k, v in mate_accumulator.items()}

    io = GroupIOPlan.for_groups(group_graph)
    # Non-ROWSET members of authored statement relations that NO group hosts:
    # the axis vocabulary a fresh scan may advertise below. A member some group
    # already carries as a primary needs no re-sourcing, and advertising it
    # there only widens the scan and the merge keys with equalities the fact
    # grain already determines.
    scan_advertisable_members: list[str] = []
    if environment is not None:
        for member in sorted(_statement_scoped_relation_members(environment)):
            member_concept = environment.concepts.get(member)
            if (
                member_concept is not None
                and member_concept.derivation != Derivation.ROWSET
                and member not in all_primary_members
            ):
                scan_advertisable_members.append(member)
    for gid in topo:
        if gid == FINAL_NODE_ID:
            continue
        fact = facts[gid]
        cap: set[str] = set(fact.primary)
        if fact.behavior is None or fact.derivation == Derivation.ROOT:
            for addr in list(cap):
                cap.update(source_grain_of.get(addr, frozenset()))
            # A fresh scan can re-source an authored scoped-join axis member
            # its columns FD-determine (`billing_customer.sk` determines the
            # unique `billing_customer.id` a `subset join cust.cid = ...id`
            # names). Advertise it so the demand rules can elect the axis for
            # the merge; without it the address intersection fails and the
            # FINAL merge loses the authored join key (all_sales pivot).
            # Rowset handles are excluded: a scan that absorbs one would drop
            # the rowset's internal filter.
            if environment is not None:
                for member in scan_advertisable_members:
                    if member not in cap and build_fd_determines(
                        environment, cap, member, include_empty_grain=False
                    ):
                        cap.add(member)
            # A dim-peel ROOT binds its entity key even though only the peeled
            # members are its primaries (the key rides as a secondary member,
            # outside the primary+source-grain capability). When a non-grouping
            # consumer's row grain names that key (the spine-widened mixed
            # scalar), the scan must be able to supply it or the consumer's
            # merge goes keyless. Grouping consumers are excluded — they source
            # their grain keys through their own fact parents.
            for addr in attrs[gid].secondary_members:
                if addr in cap:
                    continue
                if any(
                    succ != FINAL_NODE_ID
                    and addr in facts[succ].grain
                    and facts[succ].derivation not in GROUPING_DERIVATIONS
                    for succ in group_graph.successors(gid)
                ):
                    cap.add(addr)
            io.capability[gid] = cap
            continue
        # A grouping group's grain component that is a STATEMENT-scoped join axis
        # is equally expressed by any other member of that relation — the members
        # ARE the same value, each rendered on its own side. So a mate a parent
        # supplies is preservable even though it isn't the group's own grain key.
        # A global-merge pseudonym twin is the same story without a statement
        # relation: `merge stages.stage into step` makes the two one value, so a
        # group at grain {step} preserves a parent-supplied `stages.stage`.
        grain_mates: set[str] = set()
        if fact.derivation in GROUPING_DERIVATIONS:
            for component in fact.grain:
                if scoped_axis_mates:
                    grain_mates |= scoped_axis_mates.get(component, frozenset())
                grain_mates |= pseudonym_mates.get(component, frozenset())
        # A rowset boundary namespaces its grain key (`rs_a.grp_key` wraps
        # `local.grp_key`): the base address a parent supplies IS the grain
        # key, so it is preservable through the boundary and everything
        # stacked on it.
        unwrapped_grain = _unwrapped_rowset_grain(
            fact.grain, environment, rollup_padded
        )
        grain_mates |= unwrapped_grain
        # A rowset boundary always materializes its grain keys from its body
        # even with no graph parent to inherit them from, and its key members
        # render under their unwrapped BASE address (the shared join handle).
        # Without this a sibling-rowset merge has no exposable join key and
        # cross-joins ON 1=1. Skipped for authored-relation hosts (see
        # `scoped_relation_members`).
        if fact.derivation == Derivation.ROWSET and not (
            (fact.primary | fact.grain) & scoped_relation_members
        ):
            cap |= unwrapped_grain
        for pgid in group_graph.predecessors(gid):
            if pgid == FINAL_NODE_ID:
                continue
            for addr in io.capability.get(pgid, set()):
                if addr in grain_mates or fact.behavior.can_preserve(
                    concept_graph,
                    concept_edges,
                    concept_attrs,
                    fact.native_grain,
                    addr,
                ):
                    cap.add(addr)
        io.capability[gid] = cap

    lineage_sub = lineage_subgraph(group_graph, group_edges)

    final_condition_args: set[str] = set()
    for atom in attrs[FINAL_NODE_ID].condition_atoms:
        final_condition_args |= {a.address for a in atom.row_arguments}
    final_condition_args -= mandatory_addresses
    io.outputs[FINAL_NODE_ID] = set(mandatory_addresses)
    io.inputs[FINAL_NODE_ID] = set(mandatory_addresses) | final_condition_args

    primary_to_gid: dict[str, str] = {}
    for gid, fact in facts.items():
        if gid == FINAL_NODE_ID:
            continue
        for addr in fact.primary:
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
    for src_gid, _ in edges_of_kind(group_edges, EdgeKind.EXISTENCE):
        existence_demand[src_gid].update(attrs[src_gid].primary_members)

    for gid in reversed(topo):
        if gid == FINAL_NODE_ID:
            continue
        fact = facts[gid]
        cap_gid = io.capability[gid]
        outs: set[str] = existence_demand.get(gid, set()) & cap_gid
        outs |= _hosted_condition_outputs(
            attrs[gid].condition_atoms, fact.derivation, cap_gid
        )
        for succ in group_graph.successors(gid):
            if succ == FINAL_NODE_ID:
                mand = cap_gid & mandatory_alias_addresses
                if fact.derivation in GROUPING_DERIVATIONS:
                    mand &= fact.primary | fact.grain
                for desc in nx.descendants(lineage_sub, gid):
                    if facts[desc].derivation in GROUPING_DERIVATIONS:
                        mand -= io.outputs[desc]
                outs |= mand
                final_args_here = cap_gid & final_condition_args
                outs |= final_args_here
                # A FINAL-deferred presence-probe filter joins its producer
                # back on the probe's KEY (`ord_cust` ~ the anchor's key via
                # the scoped-join pseudonym); expose the key alongside the
                # probe or the side input degrades to a 1=1 cross join.
                for addr in final_args_here:
                    if is_presence_probe(addr):
                        outs |= lineage_parents.get(addr, set()) & cap_gid
                # Same shape for a ROWSET boundary feeding a FINAL-deferred
                # filter (`where return_demos.r_ticket is not null` over a
                # `union join return_demos.demo_id = c_demo`): the feeder must
                # join back on the relation axis, so expose the boundary's own
                # scoped member handles. Not cap-gated — the boundary always
                # materializes its authored handles from its body.
                if final_args_here and fact.derivation == Derivation.ROWSET:
                    boundary_namespaces = {
                        addr.rpartition(".")[0] for addr in fact.primary
                    }
                    outs |= {
                        member
                        for member in scoped_join_member_addresses
                        if member.rpartition(".")[0] in boundary_namespaces
                    }
                # A relation carried on the concept graph's RELATION edges (an
                # authored axis whose collapsed side is a first-class computed
                # node) joins its sides at THIS merge: expose the group's own
                # member whenever a FINAL sibling hosts one of its mates.
                # Without the exposure neither side outputs the axis and the
                # minimum-contributor cover drops the member's group entirely
                # (`union join rank orders.oid order by orders.amt desc =
                # customers.rnk` degraded to a 1=1 cross join).
                if relation_edge_members and scoped_axis_mates:
                    for member in sorted(fact.primary & relation_edge_members):
                        member_mates = scoped_axis_mates.get(member, frozenset())
                        if not member_mates:
                            continue
                        for sibling in group_graph.predecessors(succ):
                            if sibling in (gid, FINAL_NODE_ID):
                                continue
                            if member_mates & facts[sibling].primary:
                                outs.add(member)
                                break
                if fact.grain:
                    # Rowset boundaries namespace their grain keys
                    # (`rs_a.grp_key` vs `rs_b.grp_key` both wrap
                    # `local.grp_key`), so sibling grains must compare after
                    # resolving through the boundary — two aggregates renamed
                    # out of sibling rowsets at the same base grain otherwise
                    # never match, neither exposes the key, and the FINAL
                    # merge cross-joins ON 1=1 (alias-collision aggregates).
                    resolved_grain = _resolved_rowset_grain(fact.grain, environment)
                    for sibling in group_graph.predecessors(succ):
                        if sibling == gid or sibling == FINAL_NODE_ID:
                            continue
                        sibling_fact = facts[sibling]
                        # Same-grain sibling: the grain IS the shared row
                        # identity. A STRICTLY FINER sibling that can also
                        # produce these components is the same story one level
                        # down — this group is a dimension projection joining
                        # back to the fact stream on its FD key (a rowset
                        # body's `cid as s_cid` beside `sum(net)` grouped by
                        # `csk, year`). Without the axis the merge cross-joins
                        # every dimension row onto every fact row.
                        resolved_sibling_grain = _resolved_rowset_grain(
                            sibling_fact.grain, environment
                        )
                        resolved_sibling_match = (
                            resolved_grain == resolved_sibling_grain
                            and not (
                                (
                                    fact.primary
                                    | fact.grain
                                    | facts[sibling].primary
                                    | sibling_fact.grain
                                )
                                & scoped_relation_members
                            )
                            # A resolved match on ROLLUP grouping keys is not a
                            # shared row identity: the subtotal rows NULL them,
                            # so joining there drops every subtotal.
                            and not (
                                (resolved_grain | resolved_sibling_grain)
                                & rollup_padded
                            )
                        )
                        if (
                            sibling_fact.grain == fact.grain
                            or resolved_sibling_match
                            or (
                                fact.grain < sibling_fact.grain
                                and fact.grain <= io.capability[sibling]
                            )
                        ):
                            outs |= (
                                fact.grain | (resolved_grain - rollup_padded)
                            ) & cap_gid
                            break
                continue
            if edge_kind(group_edges, gid, succ) == EdgeKind.EXISTENCE:
                continue
            demanded = io.inputs.get(succ, set()) & cap_gid
            if fact.derivation in GROUPING_DERIVATIONS:
                sibling_providable: set[str] = set()
                for sib in group_graph.predecessors(succ):
                    if sib in (gid, FINAL_NODE_ID):
                        continue
                    if facts[sib].derivation in GROUPING_DERIVATIONS:
                        continue
                    sibling_providable |= io.capability.get(sib, set())
                keep = fact.primary | fact.grain
                demanded -= sibling_providable - keep
            outs |= demanded
            for sibling in group_graph.predecessors(succ):
                if sibling == gid or sibling == FINAL_NODE_ID:
                    continue
                if edge_kind(group_edges, sibling, succ) == EdgeKind.EXISTENCE:
                    continue
                outs |= facts[sibling].grain & cap_gid
        if fact.derivation in GROUPING_DERIVATIONS:
            outs |= fact.grain & cap_gid
            # A grouping group whose grain component is a STATEMENT-scoped join
            # axis it cannot produce still owns that axis — through its OWN side's
            # member of the relation (`subset join fut.period + 53 = agg.period`
            # canonicalizes the axis to `agg.period`, but the fut side's column IS
            # the derived key). Advertise the member it can produce, or the group
            # renders as a grainless global aggregate the FINAL merge can only
            # cross-join (scoped_derived_rowset exp_rows1). A global-merge
            # pseudonym twin licenses the same substitution (an unbound `merge`
            # key whose only physical column is its rowset rename).
            for missing in fact.grain - cap_gid:
                mates = set((scoped_axis_mates or {}).get(missing, frozenset()))
                mates |= pseudonym_mates.get(missing, frozenset())
                for mate in sorted(mates):
                    if mate in cap_gid:
                        outs.add(mate)
                        break
        io.outputs[gid] = outs

        ins: set[str] = set()
        is_grouping = fact.derivation in GROUPING_DERIVATIONS
        for concept_addr in outs:
            if concept_addr in fact.primary:
                stack = list(lineage_parents.get(concept_addr, set()))
                seen_chain: set[str] = set()
                while stack:
                    parent_addr = stack.pop()
                    if parent_addr in seen_chain:
                        continue
                    seen_chain.add(parent_addr)
                    if parent_addr in fact.primary:
                        stack.extend(lineage_parents.get(parent_addr, set()))
                        continue
                    ins.add(parent_addr)
                    if is_grouping and parent_addr not in fact.grain:
                        for gc in source_grain_of.get(parent_addr, frozenset()):
                            if gc not in fact.primary:
                                ins.add(gc)
            else:
                parent_addrs = lineage_parents.get(concept_addr, set())
                if is_grouping and parent_addrs and parent_addrs <= fact.grain:
                    ins |= parent_addrs
                else:
                    ins.add(concept_addr)
        for atom in attrs[gid].condition_atoms:
            for arg in atom.row_arguments:
                if arg.address not in fact.primary:
                    ins.add(arg.address)
        io.inputs[gid] = ins

    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        attrs[gid].output_concepts = tuple(sorted(io.outputs[gid]))
        attrs[gid].hidden_concepts = tuple(sorted(io.hidden[gid]))
        attrs[gid].input_concepts = tuple(sorted(io.inputs[gid]))


@overload
def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    datasource_columns: list[frozenset[str]] | None = None,
    *,
    environment: BuildEnvironment | None = None,
    return_merged_graph: Literal[False] = False,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs]]: ...


@overload
def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    datasource_columns: list[frozenset[str]] | None = None,
    *,
    environment: BuildEnvironment | None = None,
    return_merged_graph: Literal[True],
    staged_conditions: list[BuildWhereClause] | None = None,
) -> tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs], nx.DiGraph, EdgeMap]: ...


def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    datasource_columns: list[frozenset[str]] | None = None,
    *,
    environment: BuildEnvironment | None = None,
    return_merged_graph: bool = False,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> (
    tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs]]
    | tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs], nx.DiGraph, EdgeMap]
):
    """Collapse compatible concepts into groups and append a single FINAL sink.

    Returns the topology graph, its typed `group_edges` metadata map, and a
    side-table of typed per-group attributes keyed by group id (plus the merged
    graph + its edge map when ``return_merged_graph``).

    Grouping is delegated to per-derivation rules in `group_rules.py`:
    most derivations group by equality on `(depth_label, grain)`; ROOT
    collapses to one bucket; BASIC merges by grain subset/equality.
    """
    # Roots reconverge into one row stream not only at the SELECT projection but
    # also at the WHERE: a filter row-arg is a join target the same way an output
    # is. Folding the condition row-args into the convergence set lets
    # `partition_roots` co-source a filter root that descends from a remote model
    # (e.g. `org.flag` from `organizations`, joined to the SELECT's `lv_info` only
    # through the `launch_info` fact) with the SELECT roots, so the bridge planner
    # sees them in one request and discovers the connector instead of cross-joining
    # ON 1=1 at the FINAL merge. Component-gated in `partition_roots`, so this only
    # unions roots already weakly-connected (via the condition's constraint edge).
    condition_arg_addresses = frozenset(
        arg.address for clause in conditions for arg in clause.row_arguments
    )
    output_addresses = frozenset(c.address for c in mandatory_list or [])
    primary_group, buckets = _assign_groups(
        concept_graph,
        concept_edges,
        concept_attrs,
        output_addresses | condition_arg_addresses,
    )
    _fold_rollup_key_dims(
        concept_graph, concept_edges, concept_attrs, primary_group, buckets
    )
    if environment is not None:
        projected_scalar_root_args = _projected_scalar_root_args(mandatory_list or [])
        _split_root_dimension_clusters(
            buckets,
            primary_group,
            environment,
            output_addresses | projected_scalar_root_args,
            projected_scalar_root_args,
            _pre_aggregate_filter_args(conditions),
            _post_aggregate_filter_args(conditions)
            | _post_aggregate_basic_args(mandatory_list or []),
            _finer_filter_grains(conditions),
        )
    d1_calc_roots_by_stage, d1_subgraph = _d1_calc_subgraph(
        concept_graph, concept_edges, concept_attrs
    )
    d1_root_gids = _add_d1_root_buckets(concept_attrs, buckets, d1_calc_roots_by_stage)
    all_d1_calc_roots: set[str] = set().union(*d1_calc_roots_by_stage.values())
    _prune_existence_exclusive_roots(
        concept_graph,
        concept_edges,
        concept_attrs,
        buckets,
        all_d1_calc_roots,
        d1_subgraph,
        protected_addresses=output_addresses | condition_arg_addresses,
    )
    _attach_secondary_members(concept_graph, concept_attrs, buckets)
    group_graph, attrs, group_edges = _materialize_group_graph(
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        buckets,
        d1_root_gids=d1_root_gids,
        d1_calc_roots_by_stage=d1_calc_roots_by_stage,
        d1_subgraph=d1_subgraph,
    )
    # FINAL must exist before injection so a cross-arm post-merge filter can
    # land on it (no pre-final group can host one); `_color_phases` then colors
    # its merge edges along with the rest.
    _add_final_node(
        group_graph,
        group_edges,
        attrs,
        concept_graph,
        concept_attrs,
        buckets,
        conditions,
        mandatory_list,
    )
    merged_group_graph = group_graph.copy()
    merged_group_edges = copy_edges(group_edges)
    if mandatory_list is not None:
        _compute_concept_sets(
            group_graph,
            group_edges,
            attrs,
            concept_graph,
            concept_edges,
            concept_attrs,
            buckets,
            mandatory_list,
            environment=environment,
        )
        merged = _merge_basic_into_window_parent(
            group_graph, group_edges, attrs, buckets, concept_attrs
        )
        changed = _regraft_group_sources(
            group_graph, group_edges, attrs, buckets, concept_attrs
        )
        if changed or merged:
            _compute_concept_sets(
                group_graph,
                group_edges,
                attrs,
                concept_graph,
                concept_edges,
                concept_attrs,
                buckets,
                mandatory_list,
                environment=environment,
            )
    condition_group_ids = _inject_conditions(
        group_graph,
        group_edges,
        attrs,
        buckets,
        conditions,
        mandatory_list,
        environment.scoped_join_key_groups if environment is not None else None,
        concept_attrs,
        (
            _statement_relation_addresses(environment)
            if environment is not None
            else frozenset()
        ),
        environment,
        staged_conditions,
    )
    condition_group_ids |= _propagate_raw_filters_to_d1_roots(
        group_graph,
        group_edges,
        attrs,
        concept_graph,
        concept_edges,
        concept_attrs,
        datasource_columns or [],
    )
    _color_phases(group_graph, group_edges, condition_group_ids)
    if mandatory_list is not None:
        # Members of an authored join-axis equality whose collapsed side keeps
        # its computed lineage as a first-class node (the concept graph's
        # RELATION edges). These relations join above a computation, so the
        # axis members must surface as group outputs and the FINAL merge grain
        # — the same edges that drove the root-side partition.
        relation_edge_members = frozenset(
            concept_attrs[n].address
            for u, v in concept_graph.edges
            if edge_kind(concept_edges, u, v) == EdgeKind.RELATION
            for n in (u, v)
        )
        _compute_concept_sets(
            group_graph,
            group_edges,
            attrs,
            concept_graph,
            concept_edges,
            concept_attrs,
            buckets,
            mandatory_list,
            scoped_join_member_addresses=(
                frozenset(
                    addr
                    for canonical, members in (
                        environment.scoped_join_key_groups or {}
                    ).items()
                    for addr in (canonical, *members)
                )
                if environment is not None
                else frozenset()
            ),
            scoped_axis_mates=_scoped_axis_mates(environment),
            relation_edge_members=relation_edge_members,
            environment=environment,
        )
        _refresh_input_contracts(
            group_graph, group_edges, attrs, concept_attrs, concept_edges
        )
        _refresh_final_contract(
            group_graph,
            attrs,
            mandatory_list,
            environment,
            relation_edge_members=relation_edge_members,
        )
    if return_merged_graph:
        return group_graph, group_edges, attrs, merged_group_graph, merged_group_edges
    return group_graph, group_edges, attrs


def _lineage_predecessors(
    group_graph: nx.DiGraph, group_edges: EdgeMap, gid: str
) -> list[str]:
    return [
        pred
        for pred in group_graph.predecessors(gid)
        if edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE
    ]


def _regraft_candidate(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    *,
    allow_partial: bool,
) -> str | None:
    """Find the richest row-compatible sibling to source `gid`'s inputs.

    A candidate must share an ancestor with `gid` (row-compatible), carry `gid`'s
    label, not be a descendant of `gid`, and cover some of its inputs.

    With `allow_partial=False` the candidate must cover *all* of `gid`'s inputs:
    `gid` is redirected onto it as a single parent, no join, so grain is
    irrelevant.

    With `allow_partial=True` a candidate covering a non-empty *proper subset*
    is also eligible — it becomes the SPINE and the remaining inputs join onto
    it. This lets a scalar BASIC over two sibling aggregates (q47/q57:
    `sum_minus_avg = sum_sales - avg_monthly_sales`) source its spine through the
    most-derived same-grain stream, while the coarser `avg` rides in via the
    BASIC's existing lineage parents — collapsing a redundant intermediate
    merge into a single inline join. The partial case is join-bearing, so it
    is gated for safety: the BASIC's grain must equal the spine's (1:1, no
    fan-out) and every remaining input must already be built by `gid`'s current
    lineage parents. Among eligible spines the DEEPEST (most ancestors) wins —
    see the scoring note. Notably the spine need not be the candidate with the
    *most* coverage: in q47/q57 the lag window (which exposes only the grain
    keys, not `sum_sales`) is preferred over the bare `sum_sales` aggregate
    because it sits downstream of it, so the aggregate folds in and the window —
    itself a required output — never has to be re-joined.
    """
    current = attrs[gid]
    needed = set(current.input_concepts)
    if not needed:
        return None
    # Concepts a partial-coverage spine may lean on for the inputs it lacks.
    pred_outputs: set[str] = set()
    if allow_partial:
        for pred in _lineage_predecessors(group_graph, group_edges, gid):
            pred_outputs |= set(attrs[pred].output_concepts)
    my_ancestors = nx.ancestors(group_graph, gid)
    best_gid: str | None = None
    best_score: tuple[int, ...] = ()
    for candidate in group_graph.nodes:
        if candidate in (gid, FINAL_NODE_ID):
            continue
        cattrs = attrs[candidate]
        if cattrs.derivation in {Derivation.ROOT, Derivation.BASIC}:
            continue
        if cattrs.label != current.label:
            continue
        candidate_ancestors = nx.ancestors(group_graph, candidate)
        if not (my_ancestors & candidate_ancestors):
            continue
        if gid in candidate_ancestors:
            continue
        covered = needed & set(cattrs.output_concepts)
        if not covered:
            continue
        full = covered >= needed
        if not full:
            # Partial spine: join-bearing, so gate for safety only. Any
            # non-ROOT/BASIC sibling at the BASIC's exact grain (1:1, no fan-out)
            # is eligible iff the inputs it lacks are already built upstream; the
            # right one is chosen by depth below, not by derivation type.
            if not allow_partial or current.derivation != Derivation.BASIC:
                continue
            if cattrs.grain_components != current.grain_components:
                continue
            if not (needed - covered) <= pred_outputs:
                continue
        # Selection: full coverage (a single redirect, no join) always wins. Among
        # partial spines, prefer the DEEPEST same-grain candidate (most ancestors)
        # — the most-derived stream at that grain. Every shallower same-grain node
        # folds into it and nothing downstream remains to re-join, so a window
        # built over the base aggregate beats the bare aggregate without naming a
        # type (q47/q57: the lag window, not the sum, is the spine). Coverage then
        # breaks ties (fewer remainder joins), then grouping, then richness.
        score = (
            int(full),
            len(candidate_ancestors),
            len(covered),
            int(cattrs.derivation in GROUPING_DERIVATIONS),
            len(cattrs.output_concepts),
        )
        if score > best_score:
            best_score = score
            best_gid = candidate
    return best_gid


def _grain_needed_by_independent_sibling(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    provider_gid: str,
) -> bool:
    """Whether an independent FINAL-bound group co-grains on the provider's grain.

    Grafting the projection onto the provider fuses it into that group, which
    re-labels the provider's grouping key as this alias. Groups inside the
    provider's own lineage follow that relabeling, but one that merges with it as
    a separate arm does not: it would look for a key that no longer exists and
    the merge would degenerate to a cross join.
    """
    provider_grain = attrs[provider_gid].grain_components
    if not provider_grain:
        return False
    related = (
        nx.ancestors(group_graph, provider_gid)
        | nx.descendants(group_graph, provider_gid)
        | {gid, provider_gid, FINAL_NODE_ID}
    )
    for other in group_graph.nodes:
        if other in related:
            continue
        if FINAL_NODE_ID not in group_graph.successors(other):
            continue
        if attrs[other].grain_components & provider_grain:
            return True
    return False


def _terminal_basic_grouping_provider(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> str | None:
    """Choose a shaped provider for a terminal scalar projection.

    A BASIC inherited from a row stream may retain that stream's physical key
    as a grain-only input even when its expression needs only a grouping key.
    When the projection feeds only FINAL, place it at the deepest grouping
    provider that covers its expression inputs and adopt that provider's grain.
    """
    current = attrs[gid]
    if current.derivation != Derivation.BASIC or current.condition_atoms:
        return None
    if len(current.primary_members) != 1:
        return None
    if any(successor != FINAL_NODE_ID for successor in group_graph.successors(gid)):
        return None
    expression_inputs = set(current.input_concepts) - set(current.grain_components)
    if not expression_inputs:
        return None
    my_ancestors = nx.ancestors(group_graph, gid)
    best_gid: str | None = None
    best_score: tuple[int, int] | None = None
    for candidate in group_graph.nodes:
        if candidate in (gid, FINAL_NODE_ID):
            continue
        candidate_attrs = attrs[candidate]
        if candidate_attrs.derivation not in GROUPING_DERIVATIONS:
            continue
        if candidate_attrs.label != current.label:
            continue
        candidate_ancestors = nx.ancestors(group_graph, candidate)
        if not (my_ancestors & candidate_ancestors):
            continue
        if gid in candidate_ancestors:
            continue
        if not expression_inputs <= set(candidate_attrs.output_concepts):
            continue
        if not candidate_attrs.grain_components <= expression_inputs:
            continue
        score = (len(candidate_ancestors), len(candidate_attrs.output_concepts))
        if best_score is None or score > best_score:
            best_score = score
            best_gid = candidate
    return best_gid


def _synthetic_dimension_regraft_parent(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
    gid: str,
) -> str | None:
    current = attrs[gid]
    if current.derivation != Derivation.BASIC:
        return None
    # A condition-phase (d1) group's real input demand only lands when atoms
    # are injected AFTER this regraft runs; a synthetic bucket built from its
    # pre-injection inputs freezes out the lineage columns the condition's
    # derived arg needs (concat-label WHERE: `vehicle_variant` stranded).
    if current.depth_label == DepthLabel.D1:
        return None
    key = set(current.grain_components)
    if not key:
        return None
    lineage_preds = _lineage_predecessors(group_graph, group_edges, gid)
    root_preds = [
        pred for pred in lineage_preds if attrs[pred].derivation == Derivation.ROOT
    ]
    if not root_preds or len(root_preds) != len(lineage_preds):
        return None
    inputs = list(current.input_concepts)
    if not inputs:
        return None
    if not all(_fd_on_key(concept_attrs, address, key) for address in inputs):
        return None
    if not any(
        key <= set(attrs[candidate].grain_components)
        for candidate in group_graph.nodes
        if candidate != FINAL_NODE_ID
        and attrs[candidate].derivation in GROUPING_DERIVATIONS
    ):
        return None

    dimension_root = _covering_dimension_root(root_preds, inputs, attrs, buckets)
    if dimension_root is not None:
        for other in root_preds:
            if other != dimension_root and group_graph.has_edge(other, gid):
                remove_edge(group_graph, group_edges, other, gid)
        return dimension_root

    root_gid = f"grp:root:root:dim:{'|'.join(sorted(key))}"
    if root_gid not in group_graph:
        group_graph.add_node(root_gid)
        attrs[root_gid] = GroupAttrs(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
            primary_members=tuple(inputs),
            members=tuple(inputs),
        )
        bucket = GroupBucket(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
        )
        bucket.primary_members = list(inputs)
        buckets[root_gid] = bucket
    for pred in root_preds:
        if group_graph.has_edge(pred, gid):
            remove_edge(group_graph, group_edges, pred, gid)
    return root_gid


def _covering_dimension_root(
    root_ids: list[str],
    inputs: list[str],
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
) -> str | None:
    required = set(inputs)
    for root_id in root_ids:
        bucket = buckets.get(root_id)
        if (
            bucket is not None
            and bucket.discriminator.startswith("dim:")
            and required <= set(attrs[root_id].members)
        ):
            return root_id
    return None


def _absorb_group(
    gid: str,
    parent_gid: str,
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
) -> None:
    """Fold group `gid`'s members into `parent_gid` and delete `gid`, so the
    parent node renders `gid`'s outputs in its own SELECT. Repoints `gid`'s
    consumers onto the parent; IO/contract refresh is left to the caller's
    `_compute_concept_sets` rerun."""
    a = attrs[gid]
    pa = attrs[parent_gid]

    def _extend(dst: tuple[str, ...], src: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(dict.fromkeys([*dst, *src]))

    pa.primary_members = _extend(pa.primary_members, a.primary_members)
    pa.members = _extend(pa.members, a.members)
    pa.secondary_members = _extend(pa.secondary_members, a.secondary_members)
    pa.member_depths = {**a.member_depths, **pa.member_depths}

    pb = buckets.get(parent_gid)
    b = buckets.get(gid)
    if pb is not None and b is not None:
        for addr, node_id in zip(b.primary_members, b.primary_node_ids):
            if addr not in pb.primary_members:
                pb.primary_members.append(addr)
                pb.primary_node_ids.append(node_id)
        for addr in b.secondary_members:
            if addr not in pb.secondary_members:
                pb.secondary_members.append(addr)
        pb.member_depths = {**b.member_depths, **pb.member_depths}

    for succ in list(group_graph.successors(gid)):
        if succ == parent_gid or group_graph.has_edge(parent_gid, succ):
            continue
        kind = edge_kind(group_edges, gid, succ) or EdgeKind.LINEAGE
        add_edge(group_graph, group_edges, parent_gid, succ, kind)

    for edge in [e for e in group_edges if gid in e]:
        group_edges.pop(edge, None)
    group_graph.remove_node(gid)
    del attrs[gid]
    buckets.pop(gid, None)


def _merge_basic_into_window_parent(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
) -> bool:
    """Collapse a same-grain scalar BASIC group into a WINDOW parent that already
    supplies all of its inputs, so the projection renders inline in the window's
    own SELECT (the single-CTE window+round shape, q2.1/q2.2) instead of a
    separate node that forces the window to materialize every passthrough column.

    This is the node-MERGE generalization of `_regraft_group_sources` (which only
    routes a source edge). It is gated to the one case the optimizer cannot
    express -- `CollapseSingleParent` blocks BASIC-into-WINDOW because a row
    projection can't ride a window node's SELECT through a generic fold. An
    AGGREGATE parent is deliberately excluded: that fold is already handled
    safely by `CollapseSingleParent`'s `basic_fold_into_group_is_safe`.

    The post-window filter (`x is not null`) needs no handling here: it has not
    been injected yet (this runs before `_inject_conditions`), and placement
    refuses to host a filter on a window group's own output, so it defers to
    FINAL."""
    changed = False
    for gid in list(group_graph.nodes):
        if gid == FINAL_NODE_ID:
            continue
        a = attrs[gid]
        if a.derivation != Derivation.BASIC or a.condition_atoms:
            continue
        # pure scalar projection only: a member that is itself a row-shape
        # barrier (aggregate/window output) can't render in the parent's SELECT.
        if any(
            m in concept_attrs
            and concept_attrs[m].derivation in ROW_SHAPE_BARRIER_DERIVATIONS
            for m in a.primary_members
        ):
            continue
        parent_gid = _regraft_candidate(
            group_graph, group_edges, attrs, gid, allow_partial=False
        )
        if parent_gid is None:
            # The BASIC may also read a sibling the window already sources (q2.1:
            # `round_lag(sunday_sales)` references `sunday_sales` directly AND via
            # the lead, so the round group's parents are the window + the
            # `*_sales` BASIC the window itself reads). Full coverage misses this;
            # fall back to a partial spine and gate on the window carrying every
            # remaining input below.
            parent_gid = _regraft_candidate(
                group_graph, group_edges, attrs, gid, allow_partial=True
            )
        if parent_gid is None or parent_gid == gid:
            continue
        pa = attrs[parent_gid]
        if pa.derivation != Derivation.WINDOW:
            continue
        if pa.grain_components != a.grain_components:
            continue
        # Absorption renders gid inline in the window's own SELECT, so the window
        # must already source every input gid needs -- from its outputs (the
        # leads) or its inputs (the `*_sales` it consumes). A spine whose missing
        # inputs the window doesn't carry would need a real join the absorb can't
        # create, so leave it as a separate joined node.
        if not set(a.input_concepts) <= set(pa.output_concepts) | set(
            pa.input_concepts
        ):
            continue
        _absorb_group(gid, parent_gid, group_graph, group_edges, attrs, buckets)
        changed = True
    return changed


def _regraft_group_sources(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
) -> bool:
    """Topology-only repair for groups whose best row source is already built.

    This is still a grouping-phase concern: it may add group edges or a
    synthetic dimension ROOT bucket so IO demand flows through the right parent,
    but concrete datasource selection remains in `source_planning` and
    StrategyNode construction remains in `strategy_builder`.
    """
    changed = False
    for gid in list(group_graph.nodes):
        if gid == FINAL_NODE_ID:
            continue
        if attrs[gid].derivation not in _REGRAFTABLE_DERIVATIONS:
            continue
        provider_gid = _terminal_basic_grouping_provider(group_graph, attrs, gid)
        parent_gid = None
        if provider_gid is not None:
            attrs[gid].grain_components = attrs[provider_gid].grain_components
            if gid in buckets:
                buckets[gid].grain_components = attrs[provider_gid].grain_components
            if not _grain_needed_by_independent_sibling(
                group_graph, attrs, gid, provider_gid
            ):
                parent_gid = provider_gid
        if parent_gid is None:
            parent_gid = _regraft_candidate(
                group_graph, group_edges, attrs, gid, allow_partial=False
            )
        if parent_gid is None:
            parent_gid = _synthetic_dimension_regraft_parent(
                group_graph, group_edges, attrs, buckets, concept_attrs, gid
            )
        if parent_gid is None:
            parent_gid = _regraft_candidate(
                group_graph, group_edges, attrs, gid, allow_partial=True
            )
        if parent_gid is None or group_graph.has_edge(parent_gid, gid):
            continue
        add_edge(group_graph, group_edges, parent_gid, gid, EdgeKind.LINEAGE)
        changed = True
    return changed
