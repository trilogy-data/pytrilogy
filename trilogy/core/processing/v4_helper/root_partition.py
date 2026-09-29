"""Stage 2, root partition: which root columns are sourced together, and for
which reader.
"""

from collections import defaultdict
from dataclasses import dataclass, field

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import plan_trace

from .concept_graph import condition_stage_of_label
from .constants import (
    GROUPING_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import EdgeMap, edge_kind
from .extent_ownership import span_members
from .functional_dependency import build_fd_determines
from .models import ConceptAttrs, GroupBucket, Keyspace, RootReason
from .region_domains import (
    RegionDomain,
    add_region_domain_buckets,
    carry_spans_to_condition_scans,
    decide_region_domains,
)


def _d1_calc_subgraph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
) -> tuple[dict[int | None, set[str]], set[str]]:
    """Identify (d1_calc_roots by `then where` stage, d1_subgraph_nodes).

    Roots are keyed by the condition label's stage qualifier: None is the
    plain condition phase (the first cross-row-hosting stage of a chain), and
    each later cross-row-hosting stage gets its own root set. Its feeder scan
    carries the earlier stages' bounds, so it must not be shared with a
    differently-bounded stage's computations.

    Any concept reached via the WHERE recursion lives at a condition-phase
    label (suffix ``@condition``) and is classified d1. A root feeds a
    dedicated root_d1 bucket only when it feeds a d1 node that needs an
    independent scan, i.e. one that is EITHER:
      - a row-shape barrier (``where x > avg(price)``) or a semijoin-set
        definition: such a calc must see a pristine scan, free of sibling
        WHERE atoms pushed onto the SELECT-side scan; or
      - a condition that constrains a NON-grouping d0 output (a ROOT/BASIC the
        SELECT scans directly). That output is co-sourced into the same root
        bucket, so folding the condition into it would 2-cycle (root ->
        condition lineage -> root). When the constraint target is an
        aggregate, it lives in its own group and the condition folds into the
        co-sourced root cleanly.

    A condition that is only a scalar BASIC (``where flag = 'x'`` with
    ``flag <- case state_code ...``) needs no pristine scan: applying it over
    rows a sibling filter already narrowed still selects exactly the
    conjunction, and splitting its root would re-scan the source as an
    independent CTE that, when co-sourced with the SELECT through a bridge,
    is cross-joined ON 1=1.

    The second reason is cycle avoidance ONLY, so it yields to the split's own
    precondition: the private scan has to be joinable back to the rows it
    filters. `_split_strands_condition_scan` withholds the split when the
    condition's roots share no join axis with the SELECT's; co-sourcing is
    then the only plan with a key, and `_materialize_group_graph` drops the
    now-redundant constraint back-edge so the fold does not cycle.

    - d1_calc_roots: blank-phase roots feeding such a d1 node.
    - d1_subgraph_nodes: every condition-phase node; edge routing uses this
      as the destination side of the predicate."""
    d1_subgraph: set[str] = {
        n for n in concept_graph.nodes if concept_attrs[n].depth_label == DepthLabel.D1
    }
    if not d1_subgraph:
        return {}, set()

    def _needs_pristine_scan(n: str) -> bool:
        """The calc reads a row POPULATION, so its scan must not see the
        SELECT-side WHERE atoms."""
        if concept_attrs[n].derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
            return True
        # An existence source (a semijoin RHS, `x in <set>`) is a separate
        # discovery: its defining lineage must source from a private root, not
        # the SELECT's common root. Otherwise the fact columns that exist only
        # to define the set sit in the shared root and drag the SELECT's
        # dimension projection onto the fact instead of its own dim tables.
        return any(
            edge_kind(concept_edges, n, succ) == EdgeKind.EXISTENCE
            for succ in concept_graph.successors(n)
        )

    def _constrains_scanned_output(n: str) -> bool:
        """The calc filters a d0 output the SELECT scans directly: the
        cycle-avoidance reason for a split (see the docstring)."""
        for succ in concept_graph.successors(n):
            if edge_kind(concept_edges, n, succ) != EdgeKind.CONSTRAINT:
                continue
            if concept_attrs[succ].derivation not in GROUPING_DERIVATIONS:
                return True
        return False

    def _lineage_roots(seeds: list[str]) -> set[str]:
        """Blank-phase ROOT ancestors of `seeds`, walking lineage edges."""
        roots: set[str] = set()
        visited: set[str] = set()
        stack = list(seeds)
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
                    roots.add(pred)
                else:
                    stack.append(pred)
        return roots

    # Walk lineage upward from each d1 node that needs an independent scan; the
    # blank-phase ROOT ancestors are the roots whose condition scan must stay
    # separate from the SELECT-side scan. One walk per stage qualifier: a root
    # feeding two stages' computations belongs to both feeders (the scan is
    # duplicated per population, which is the point of the split).
    pristine = [n for n in d1_subgraph if _needs_pristine_scan(n)]
    cycle_only = [
        n for n in d1_subgraph if n not in pristine and _constrains_scanned_output(n)
    ]
    stage_of = {
        n: condition_stage_of_label(concept_attrs[n].label) for n in d1_subgraph
    }
    roots_by_stage: dict[int | None, set[str]] = {}
    for stage in {stage_of[n] for n in (*pristine, *cycle_only)}:
        hard = _lineage_roots([n for n in pristine if stage_of[n] == stage])
        soft = _lineage_roots([n for n in cycle_only if stage_of[n] == stage]) - hard
        if soft and _split_strands_condition_scan(
            concept_graph, concept_edges, concept_attrs, soft, d1_subgraph, environment
        ):
            soft = set()
        roots_by_stage[stage] = hard | soft
    return roots_by_stage, d1_subgraph


def _root_join_axis(attrs: ConceptAttrs) -> set[str]:
    """Addresses a standalone scan of this root could join a sibling scan on:
    its own key identity, the keys/grain it hangs off, and any merged pseudonym
    it answers for."""
    axis = set(attrs.grain_components) | set(attrs.keys) | set(attrs.pseudonyms)
    if attrs.purpose == Purpose.KEY:
        axis.add(attrs.address)
    return axis


def _condition_exclusive_root(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    d1_subgraph: set[str],
    root: str,
) -> bool:
    """Whether every concept this root computes lives in the condition phase.

    Such a root is on the SELECT's row stream only by co-sourcing: nothing the
    SELECT projects reads it. A root that also feeds a blank-phase concept is
    the same physical scan the SELECT already has, so its keys ARE available on
    both sides of the merge. Lineage edges only: the condition node's
    CONSTRAINT edge points back at the blank-phase concept it filters, which is
    the relationship being asked about, not evidence against it."""
    stack = [root]
    seen = {root}
    reached = False
    while stack:
        current = stack.pop()
        for succ in concept_graph.successors(current):
            if edge_kind(concept_edges, current, succ) != EdgeKind.LINEAGE:
                continue
            if succ in seen:
                continue
            seen.add(succ)
            reached = True
            if succ not in d1_subgraph:
                return False
            stack.append(succ)
    return reached


def _bound_column_components(environment: BuildEnvironment) -> list[set[str]]:
    """Address components of the PHYSICAL join graph: two datasources land in
    one component when some address (or pseudonym) is a bound column of both.

    Only bound columns count. A concept a datasource could produce by deriving
    it (`unnest(native_ecoregions)`) is exactly what does NOT link two scans on
    its own: realizing that link is bridge planning, and bridge planning only
    happens inside one ROOT request."""
    ds_addresses: list[set[str]] = []
    for datasource in environment.datasources.values():
        addresses: set[str] = set()
        for concept in datasource.output_concepts:
            addresses.add(concept.address)
            addresses.update(concept.pseudonyms)
        if addresses:
            ds_addresses.append(addresses)
    components: list[set[str]] = []
    for addresses in ds_addresses:
        merged = addresses
        rest: list[set[str]] = []
        for component in components:
            if component & merged:
                merged = merged | component
            else:
                rest.append(component)
        rest.append(merged)
        components = rest
    return components


def _split_strands_condition_scan(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    split_roots: set[str],
    d1_subgraph: set[str],
    environment: BuildEnvironment,
) -> bool:
    """Whether scanning `split_roots` privately would leave that scan no join
    key back to the rows it filters.

    A root_d1 scan is only useful if the FINAL merge can pair it with the
    SELECT side. When the condition's roots reach the SELECT's roots only
    through a BRIDGE (`auto x <- unnest(list); merge x into dim_key`, where the
    condition's entity reaches the SELECT's only via an enrichment table keyed
    by a third entity), a scan of the condition side alone shares no key with
    them and the merge degrades to `ON 1=1`. Co-sourcing is then the only plan
    with a key: one ROOT request holding both sides is what lets the bridge
    planner discover the connector.

    The SELECT side is every blank-phase root the SELECT actually reads: a
    condition-exclusive root is not one of them (it reaches the merge only
    through the private scan being decided on), but a root the condition SHARES
    with the SELECT is, and its keys settle the question on their own.

    Both sides must be row-bearing for the question to mean anything: a
    grainless condition root (a constant, a global aggregate) joins by cross
    product by construction and keeps its split.

    Disjoint axes are necessary but NOT sufficient: two dimensions of a star
    schema share no key either, and their private scans still meet over the
    fact table. What makes the bridge case different is that NO chain of bound
    datasource columns relates the two sides at all, so the physical-column
    components decide it.
    """
    condition_axis: set[str] = set()
    condition_addresses: set[str] = set()
    for node in split_roots:
        attrs = concept_attrs[node]
        condition_axis |= _root_join_axis(attrs)
        condition_addresses |= {attrs.address, *attrs.pseudonyms}
    select_axis: set[str] = set()
    select_addresses: set[str] = set()
    for node in concept_graph.nodes:
        attrs = concept_attrs[node]
        if attrs.derivation != Derivation.ROOT or attrs.depth_label == DepthLabel.D1:
            continue
        if _condition_exclusive_root(concept_graph, concept_edges, d1_subgraph, node):
            continue
        select_axis |= _root_join_axis(attrs)
        select_addresses |= {attrs.address, *attrs.pseudonyms}
    if not condition_axis or not select_axis:
        return False
    if not condition_axis.isdisjoint(select_axis):
        return False
    return not any(
        component & condition_addresses and component & select_addresses
        for component in _bound_column_components(environment)
    )


def _add_d1_root_buckets(
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    d1_calc_roots_by_stage: dict[int | None, set[str]],
) -> dict[int | None, str]:
    """Add an extra ROOT bucket per stage qualifier containing just that
    stage's d1-feeding roots. Returns group ids keyed like the input; only
    stage-qualified buckets carry a discriminator. Buckets are added
    plain-first then by stage so insertion order does not depend on set
    iteration order."""
    gids: dict[int | None, str] = {}
    for stage, d1_calc_roots in sorted(
        d1_calc_roots_by_stage.items(), key=lambda kv: (-1 if kv[0] is None else kv[0])
    ):
        if not d1_calc_roots:
            continue
        bucket = GroupBucket(
            depth_label=DepthLabel.ROOT_D1,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
            reason=RootReason.CONDITION,
        )
        if stage is not None:
            bucket.discriminator = f"stage:s{stage}"
        for node in sorted(d1_calc_roots):
            address = concept_attrs[node].address
            bucket.primary_members.append(address)
            bucket.primary_node_ids.append(node)
            bucket.member_depths[address] = concept_attrs[node].depth_label
        gid = bucket.group_id
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

    A semijoin set (``buyers <- pcid ? channel='STORE' and date.year=2002``)
    is sourced as a separate discovery. Its defining fact columns feed nothing
    but that set; left in the common root they force it to source from the
    fact, dragging the dimension projection onto the fact too. Removing them
    lets the shared root source the dimension standalone, while the semijoin's
    join key (which also feeds the outer aggregate) stays, sourced from both
    sides and joined by the ``IN``.

    A root is existence-exclusive when every concept it feeds is a condition
    node and the condition subgraph it feeds reaches an existence source
    (transitively: a root feeding the set's filter through an intermediate
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


def _finest_determining_key(
    determiners: list[str], environment: BuildEnvironment
) -> str | None:
    """Among candidate keys that all determine some member, the one determined
    by every other (the finest / most-downstream key). A dim FD by
    ``customer.id`` is also transitively FD by the fact grain that determines
    ``customer.id``; the finest key is ``customer.id``. Returns None when no
    single key is finest (the member is FD by two or more incomparable
    entities and must stay on the fact bucket, riding the aggregate keys)."""
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
    """The multi-key grouping grain that functionally determines `address`,
    when no single entity key does.

    A member can be FD by a COMPOSITE key rather than one entity (a partsupp
    measure determined by ``{part.id, supplier.id}`` and by neither alone).
    Such a member is still a dimension: it can source from its own table keyed
    by that grain and join the aggregate on both columns, instead of riding
    the fact row stream and deduping back to grain in a sibling GROUP bucket.
    Ties break to the coarsest-first deterministic pick."""
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
    """The arg's address plus every address reachable through its lineage: a
    derived filter arg (``label <- concat(name, '-', variant)``) needs its
    ROOT inputs co-located wherever the filter is evaluated."""
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
    """Row-arg addresses of WHERE clauses that contain NO aggregate term: pure
    pre-aggregate filters that narrow the rows feeding an aggregate (an
    ``item.category in (...)`` that also bounds a class-total window).
    Expanded through each arg's lineage closure: a derived filter arg's ROOT
    inputs must stay on the fact scan too, or the group hosting the filter
    cannot render the derived expression.

    A clause that DOES carry an aggregate is a HAVING-style post-aggregate
    filter: its dim args filter the OUTPUT after aggregation, so peeling them
    to a post-aggregate dim join is faithful. A pre-aggregate filter column
    peeled that way would move the WHERE after the aggregate (wrong sums /
    wrong window denominator)."""
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
    """Row-arg addresses of WHERE clauses that DO contain an aggregate term:
    HAVING-style post-aggregate filters (``total > scaled and
    customer.address.state = 'GA'``).

    A HAVING dim arg filters the OUTPUT after aggregation, so peeling it to a
    standalone dim scan and semijoining on the entity key is faithful: one CTE
    sources the customer dims AND applies ``state = 'GA'``. Unlike a selected
    dim column, a filter-only HAVING arg has no output to anchor it, but the
    placed condition still sources the column at the dim scan, so the WHERE
    is preserved."""
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
    with dimension attributes (``coalesce(sum, 0) / warehouse.square_feet``).
    Such an arg is read at the consumer's post-aggregate grouping grain, so
    peeling it to its entity's dim bucket joins it on the entity key exactly
    like a selected dim column, instead of riding the fact row stream and
    dedup-ing back to the grain. Row-shape barriers
    (aggregate/window/filter/rowset/union) are not descended: an aggregate arg
    marks the output as post-aggregate; any other barrier's inputs live at
    grains this walk can't vouch for."""
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
# surviving row's value, the same call `_ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS`
# makes for aggregate inputs.
_SCALAR_PROJECTION_DERIVATIONS = {Derivation.BASIC, Derivation.FILTER}


def _grouping_keys(buckets: dict[str, GroupBucket]) -> set[str]:
    keys: set[str] = set()
    for bucket in buckets.values():
        if bucket.derivation in GROUPING_DERIVATIONS:
            keys |= set(bucket.grain_components)
    return keys


def _projected_scalar_root_args(
    mandatory_list: list[BuildConcept],
    grouping_keys: set[str],
) -> frozenset[str]:
    """ROOT leaves projected through a scalar output alias.

    The peeled arg leaves the row stream it was read from and comes back
    joined on an entity key at post-aggregate grain, so the alias must read it
    PER ROW for that substitution to be faithful. A scalar expression
    (``sales / square_feet``) qualifies; a barrier reads its arg as a
    POPULATION (an aggregate over the fact rows, a window over its partition)
    and the key-join reintroduces it at the wrong multiplicity, so barrier
    args stay on the fact bucket. A filter is scalar in that sense: it subsets
    rows without changing any surviving row's value. Other non-barrier
    derivations (MULTISELECT/TVF_UNION/SUBSELECT) are not walked.

    Nor is a scalar that is itself a grouping key: the GROUP BY reads it on the
    fact rows, before any post-aggregate join could bring its arg back."""
    args: set[str] = set()
    for concept in mandatory_list:
        if (
            concept.derivation not in _SCALAR_PROJECTION_DERIVATIONS
            or concept.lineage is None
            or concept.address in grouping_keys
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
    """Grains of non-aggregate filter args that live at a multi-key grain: a
    WHERE/HAVING term needing fact-grain rows finer than any single entity
    (a partsupp measure at ``{part.id, supplier.id}``). An entity whose key
    sits inside such a grain must NOT be peeled: its rows are needed at the
    finer grain to evaluate the filter, and a standalone single-key dim scan
    can't serve it (the condition becomes unplaceable). Aggregate args are
    excluded; they are their own grouping contributor."""
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
    POPULATION, not of a group's contents; a peeled column filters via a
    post-window entity-key semijoin, which leaves the window computed over the
    unfiltered rows."""
    if any(b.derivation == Derivation.WINDOW for b in grouping_buckets):
        return False
    concept = environment.concepts[address]
    return (
        key is not None
        and all(key in b.grain_components for b in grouping_buckets)
        and concept.grain is not None
        and set(concept.grain.components) == {key}
    )


def _keep_extension_families_together(
    assignment: dict[str, frozenset[str]],
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> None:
    """Merge the peel clusters that carry a demanded ``~`` extension span, and
    leave the members in the bucket when the merged cluster still mixes nothing.

    Each peeled cluster sources apart and pads its own span, so two families
    hanging off different grain keys (`~product` off the fact grain, `~user` off
    `order_id`) leave no group exposing every span: ownership splits and the
    FINAL merge pairs the families' padding null-safely, inventing a
    (product, user) row. One cluster keyed by both peel keys sources them as one
    span, the shape the same select has without the aggregate.

    A cluster keyed by the span itself reads the dimension's own table and pads
    nothing, so it stays apart. Any other cluster is where
    `add_region_domain_buckets` finds the region's solid source, which it can
    only be if it holds something absent on the region too: carrying the span
    alone it mixes nothing, the region gets no domain, and the span's extent is
    elected to the peel, which pads it off the wrong table
    (`test_field_report_select` read `user_id` through the orders it was peeled
    onto and stopped reading the items' own binding). Un-peeled, the bucket
    they came from mixes the region and owns the extent."""
    demanded_spans = keyspace.output_demanded_spans
    carrying = {
        assignment[address]
        for span in demanded_spans
        for address in span_members(span, assignment, environment)
        if span not in assignment[address]
    }
    if not carrying:
        return
    # after the merge there is one carrying cluster, keyed by every peel key
    # the families hung off
    key = frozenset().union(*carrying)
    for address, cluster in assignment.items():
        if cluster in carrying:
            assignment[address] = key
    members = [a for a, k in assignment.items() if k == key]
    if any(
        all(keyspace.carried_on(member, region) for member in members)
        for region in keyspace.live_regions
        if region.spans & demanded_spans and not region.spans & key
    ):
        for address in members:
            del assignment[address]


def _domain_holding(
    key: frozenset[str],
    node_ids: list[str],
    members: list[str],
    label: str,
    domains: list[RegionDomain],
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
) -> RegionDomain | None:
    """The region domain a cluster is the rows of again: keyed by the region's
    span, every member carried there, and read by nothing but FINAL, which
    reads the domain. A cluster with a reader is peeled all the same: it is
    that reader's SOLID-side provider (`tier` under `tier_amount`, INNER on
    the fact), which the padded domain is not."""
    read = any(
        succ not in node_ids and primary_group.get(succ) is not None
        for node_id in node_ids
        for succ in concept_graph.successors(node_id)
    )
    if read:
        return None
    return next(
        (
            domain
            for domain in domains
            if domain.bucket.label == label
            and domain.bucket.derivation == Derivation.ROOT
            and key <= domain.region.spans
            and set(members) <= domain.carried
        ),
        None,
    )


def _peels_a_cluster(
    key: frozenset[str], members: set[str], environment: BuildEnvironment
) -> bool:
    """`key` determines some other member of the bucket, but not all of them."""
    determined = sum(
        build_fd_determines(environment, set(key), other, include_empty_grain=False)
        for other in members - key
    )
    return 0 < determined < len(members - key)


def _split_root_dimension_clusters(
    buckets: dict[str, GroupBucket],
    primary_group: dict[str, str],
    environment: BuildEnvironment,
    output_addresses: frozenset[str],
    projected_scalar_root_args: frozenset[str],
    pre_aggregate_filter_args: frozenset[str],
    post_aggregate_args: frozenset[str],
    finer_filter_grains: frozenset[frozenset[str]],
    keyspace: Keyspace,
    domains: list[RegionDomain],
    concept_graph: nx.DiGraph,
) -> None:
    """Peel single-entity FD dimension clusters out of a keyed ROOT bucket into
    their own ``grp:root:root:dim:<entity_key>`` ROOT buckets.

    A wide output dimension projection lands in the single keyed root bucket
    alongside the fact-grain columns it converges with at the FINAL
    projection. Sourced together they re-root on the fact and dedup back to
    entity grain; sourcing the dims from their own tables keyed by the entity
    id avoids those joins.

    When a subset of a root bucket's members is functionally determined by a
    single entity key that is ALSO a downstream grouping key (so the FINAL
    merge already produces that key as a join column), that subset can source
    independently from its own dim tables and join on the key. Each such
    cluster becomes its own ROOT bucket, per entity. A member FD by two
    incomparable entities only co-occurs through the fact and stays put.

    FD is resolved against the full build environment (not the concept-graph
    side-table), so the chain through an intermediate FK the query never
    names (``customer.id -> customer.current_addr -> address.city``) is
    visible. ``include_empty_grain=False`` so a constant is never treated as
    a dim member.
    """
    grouping_keys = _grouping_keys(buckets)
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
        if bucket.reason is not RootReason.ROW_STREAM:
            continue
        member_addrs = set(bucket.primary_members)
        # Candidate entity keys: a member that is a downstream grouping key (so a
        # FINAL join column exists) and functionally determines another member,
        # but not every other member: that key is the bucket's own row key (the
        # fact's `id` determining its FK columns), and a peel keyed by it reads
        # the same table again beside nothing at a coarser grain.
        # Exclude a key a finer-grain filter needs at fact grain: peeling it to
        # a single-key dim scan strands that filter (unplaceable condition).
        potential_candidates = [
            addr
            for addr in member_addrs
            if addr in grouping_keys
            and _peels_a_cluster(frozenset({addr}), member_addrs, environment)
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
        # entity peel onto it. The same bound as above: a grain that determines
        # every other member is the bucket's own row key.
        composite_grains = [
            grain
            for grain in d0_grouping_grains
            if len(grain) > 1
            and grain <= member_addrs
            and _peels_a_cluster(grain, member_addrs, environment)
        ]
        if not candidates and not composite_grains:
            continue
        assignment: dict[str, frozenset[str]] = {}
        for addr in member_addrs:
            if addr in candidates:
                continue
            # Peel a SELECTED dimension column, OR a post-aggregate-only arg: a
            # filter-only HAVING arg (a post-aggregate WHERE arg the query never
            # projects) or a dim attribute an output BASIC reads beside
            # aggregate outputs. Both are consumed at post-aggregate grain, so
            # sourcing them at the dim scan joined on the entity key is
            # faithful: a HAVING's condition placed on the dim bucket still
            # applies its WHERE there. A filter-only PRE-aggregate arg is NOT
            # peeled (`pre_aggregate_filter_args` gate below): its WHERE must
            # narrow the fact rows feeding the aggregate, not a post-join dim.
            if addr not in output_addresses and addr not in post_aggregate_args:
                continue
            # A peeled filter-only arg applies as a FINAL entity-key semijoin.
            # That is faithful only when the filter drops WHOLE groups of every
            # output-lineage (d0) grouping bucket, i.e. each grain FD-determines
            # the column. A coarser-grain aggregate needs the filter on its fact
            # input rows; peeling silently drops it from the d0 row stream
            # (dual-scope: outputs recompute over admitted rows). d1 population
            # buckets are exempt; the WHERE never narrows them.
            if addr not in output_addresses and not all(
                grain
                and build_fd_determines(
                    environment, set(grain), addr, include_empty_grain=False
                )
                for grain in d0_grouping_grains
            ):
                continue
            # Never peel a member that is itself a grouping key of some aggregate:
            # it is a grouping DIMENSION the query re-aggregates over, not a
            # passthrough. It must stay at fact grain for that GROUP BY; routing
            # it to a standalone dim scan joined on the entity id breaks the
            # regroup (cross-join fan-out).
            if addr in grouping_keys:
                continue
            # Never peel a pre-aggregate filter column: its WHERE must stay on the
            # fact rows feeding the aggregate, but a peeled column carries its
            # filter to a post-aggregate dim join.
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
        _keep_extension_families_together(assignment, keyspace, environment)
        if not assignment:
            continue
        clusters: dict[frozenset[str], list[int]] = defaultdict(list)
        for idx, addr in enumerate(bucket.primary_members):
            if addr in assignment:
                clusters[assignment[addr]].append(idx)
        moved: set[int] = set()
        for key, indices in clusters.items():
            node_ids = [bucket.primary_node_ids[idx] for idx in indices]
            domain = _domain_holding(
                key,
                node_ids,
                [bucket.primary_members[idx] for idx in indices],
                bucket.label,
                domains,
                concept_graph,
                primary_group,
            )
            if domain is not None:
                for node_id in node_ids:
                    primary_group[node_id] = domain.bucket.group_id
                moved.update(indices)
                continue
            dim_bucket = GroupBucket(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain_components=frozenset(),
                label=bucket.label,
                discriminator=f"dim:{'|'.join(sorted(key))}",
                dim_keys=frozenset(key),
                reason=RootReason.ENTITY,
            )
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
            dim_gid = dim_bucket.group_id
            buckets[dim_gid] = dim_bucket
            for idx in indices:
                primary_group[bucket.primary_node_ids[idx]] = dim_gid
        kept = [i for i in range(len(bucket.primary_members)) if i not in moved]
        bucket.primary_members = [bucket.primary_members[i] for i in kept]
        bucket.primary_node_ids = [bucket.primary_node_ids[i] for i in kept]


@dataclass
class RootPartition:
    """What the partition decided that the group graph's wiring reads: each
    condition stage's private scan, the roots it holds, and the
    condition-phase nodes that read them."""

    condition_scans: dict[int | None, str] = field(default_factory=dict)
    condition_roots: dict[int | None, set[str]] = field(default_factory=dict)
    condition_nodes: set[str] = field(default_factory=set)


@plan_trace.off_clock
def trace_buckets(
    title: str, buckets: dict[str, GroupBucket], primary_group: dict[str, str]
) -> None:
    if plan_trace.active():
        plan_trace.record(
            title,
            plan_trace.BucketsStep(
                buckets={gid: plan_trace.jsonable(b) for gid, b in buckets.items()},
                primary_group=dict(primary_group),
            ),
        )


def partition_root_demand(
    buckets: dict[str, GroupBucket],
    primary_group: dict[str, str],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    keyspace: Keyspace,
) -> RootPartition:
    condition_arg_addresses = frozenset(
        arg.address for clause in conditions for arg in clause.row_arguments
    )
    output_addresses = frozenset(c.address for c in mandatory_list)
    projected_scalar_root_args = _projected_scalar_root_args(
        mandatory_list, _grouping_keys(buckets)
    )
    domains = decide_region_domains(
        buckets,
        concept_attrs,
        environment,
        keyspace,
        condition_arg_addresses,
        mandatory_list,
    )
    _split_root_dimension_clusters(
        buckets,
        primary_group,
        environment,
        output_addresses | projected_scalar_root_args,
        projected_scalar_root_args,
        _pre_aggregate_filter_args(conditions),
        _post_aggregate_filter_args(conditions)
        | _post_aggregate_basic_args(mandatory_list),
        _finer_filter_grains(conditions),
        keyspace,
        domains,
        concept_graph,
    )
    trace_buckets("root dimension clusters split", buckets, primary_group)
    add_region_domain_buckets(buckets, domains, keyspace, environment)
    trace_buckets("region domain buckets added", buckets, primary_group)
    roots_by_stage, condition_nodes = _d1_calc_subgraph(
        concept_graph, concept_edges, concept_attrs, environment
    )
    scans = _add_d1_root_buckets(concept_attrs, buckets, roots_by_stage)
    carry_spans_to_condition_scans(buckets, keyspace)
    _prune_existence_exclusive_roots(
        concept_graph,
        concept_edges,
        concept_attrs,
        buckets,
        set().union(*roots_by_stage.values()),
        condition_nodes,
        protected_addresses=output_addresses | condition_arg_addresses,
    )
    return RootPartition(scans, roots_by_stage, condition_nodes)
