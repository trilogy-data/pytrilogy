"""Stage 1: walk every mandatory concept (and condition input) back to its
roots and produce a DAG of concept-level lineage + d1→d0 constraint edges.

For each concept added, an upstream-fetcher (dispatched on
`concept.derivation`) decides what additional concepts the input CTE for
this node must contain. The default fetcher returns
`lineage.concept_arguments` — the parents the expression directly
consumes. Specialized fetchers (AGGREGATE, FILTER, WINDOW, SUBSELECT) add
row-identity concepts that aren't visible from the lineage walk alone:
property keys, grain components, partition keys. Everything the fetcher
returns gets an `EdgeKind.LINEAGE` edge — an aggregate's grain keys aren't
optional metadata, they're what keeps row identity intact through the SUM.

"""

from collections import defaultdict
from collections.abc import Callable

from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_CONCEPT, GRAIN_SEPARATOR
from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    FunctionType,
    Purpose,
)
from trilogy.core.models.author import SelectLineage
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
    BuildFilterItem,
    BuildFunction,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildWhereClause,
    is_grouping_identity,
    nonstandard_grouping_spec,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.node_generators.presence_probe import (
    is_presence_probe,
    member_binding_datasources,
    probe_member_address,
)
from trilogy.utility import unique

from .constants import (
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import EdgeMap, add_edge, edge_kind
from .functional_dependency import minimize_build_grain
from .models import ConceptAttrs
from .projection import concept_satisfiable, lineage_existence_only
from .staged_where import cross_row_stage_args

UpstreamFetcher = Callable[[BuildConcept, BuildEnvironment], list[BuildConcept]]


# Phase suffix appended to a label when a concept is reached via the WHERE
# recursion. The same concept can appear once in the SELECT (blank) sub-graph
# and once in the WHERE (condition) sub-graph; the suffix is what keeps them
# distinct so we don't try to promote/demote a single shared node.
PHASE_CONDITION_SUFFIX = "@condition"

# A `then where` chain gives each stage's cross-row computations a distinct
# input population (the rows passing the stages before it). Population is
# identity: two gates over different populations must not share a node, a
# bucket, or a feeder scan. Each cross-row-hosting stage after the first
# therefore plans under a stage-qualified condition label, splitting its whole
# lineage subtree — and its root_d1 feeder — from the other stages'.
_STAGE_QUALIFIER_PREFIX = ":s"


def _union_key_siblings(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """Sibling `union(...)` concepts that stack a key for every arm of this
    union — e.g. `all_k <- union(k1, k2)` beside `all_amt <- union(amt, pad)`.
    Such a sibling is the stacked row identity of this union's output."""
    if not isinstance(concept.lineage, BuildFunction):
        return []
    arms = concept.lineage.concept_arguments
    out: list[BuildConcept] = []
    for other in environment.concepts.values():
        if other.address == concept.address or other.derivation != Derivation.UNION:
            continue
        if not isinstance(other.lineage, BuildFunction):
            continue
        other_args = {x.address for x in other.lineage.concept_arguments}
        if all(
            a.address in other_args or (a.keys and set(a.keys) & other_args)
            for a in arms
        ):
            out.append(other)
    return unique(out, "address")


def _walk_aggregate_grain_inputs(
    concept: BuildConcept,
    environment: BuildEnvironment,
    seen: set[str] | None = None,
) -> list[BuildConcept]:
    """Collect row-identity concepts an aggregate needs from its arg's
    upstream — without crossing a row-identity boundary.

    Each concept defines its own row identity if it is:
      - a rowset (row identity = its declared grain)
      - a property with keys (row identity = its keys)

    Walks through grain-preserving wrappers to find the row identity, then
    stops:
      - FilterItem: walk only ``content`` (the value being filtered defines
        row identity; ``where`` predicates do not)
      - Function (BASIC): walk all concept args (a row-level expression
        inherits row identity from its inputs)
      - AGGREGATE / ROWSET: do not descend (the inner aggregate has already
        collapsed its upstream rows to its own ``by`` grain; a rowset
        defines a fresh row identity we've already captured)"""
    seen = seen if seen is not None else set()
    if concept.address in seen:
        return []
    seen.add(concept.address)

    if concept.derivation == Derivation.AGGREGATE:
        return []
    if concept.derivation == Derivation.ROWSET:
        return [
            environment.concepts[c]
            for c in concept.grain.components
            if c in environment.concepts
        ]
    if concept.derivation == Derivation.UNION:
        # A union output's per-arm keys can't be stacked into one column, so
        # its usable row identity is a sibling union over those keys (which a
        # UnionNode CAN output). Without one, fall through to the per-arm key
        # demand — unsatisfiable, but loud, never a silent dedup.
        siblings = _union_key_siblings(concept, environment)
        if siblings:
            return siblings
    if concept.purpose == Purpose.PROPERTY and concept.keys:
        return [
            environment.concepts[c] for c in concept.keys if c in environment.concepts
        ]
    if concept.lineage is None:
        return []
    if isinstance(concept.lineage, BuildFilterItem):
        # A filter's row identity is its content's; the where clause is a
        # predicate, not part of the result's row identity.
        content = concept.lineage.content
        if isinstance(content, BuildConcept):
            return _walk_aggregate_grain_inputs(content, environment, seen)
        return []
    collected: list[BuildConcept] = []
    for arg in concept.lineage.concept_arguments:
        if isinstance(arg, BuildConcept):
            collected.extend(_walk_aggregate_grain_inputs(arg, environment, seen))
    return collected


def _split_condition_label(label: str) -> tuple[str, int | None] | None:
    """Split a condition-phase label into its (scope, stage index) parts, or
    None if it is not a condition-phase label at all. The stage index is None
    for the plain `@condition` phase and an int for a stage-qualified
    `@condition:s<N>` one. Sole parser of the label format."""
    scope, sep, qualifier = label.partition(PHASE_CONDITION_SUFFIX)
    if not sep:
        return None
    if not qualifier:
        return scope, None
    if qualifier.startswith(_STAGE_QUALIFIER_PREFIX):
        stage = qualifier[len(_STAGE_QUALIFIER_PREFIX) :]
        if stage.isdigit():
            return scope, int(stage)
    return None


def _scope_and_phase(label: str) -> tuple[str, str]:
    """Split a label into its (scope, phase) parts. scope is "" for the outer
    query and the rowset name for rowset internals; phase is "blank" or
    "condition" (including stage-qualified `@condition:s<N>` labels, which are
    one phase here — callers that care about the stage ask for it by name)."""
    parsed = _split_condition_label(label)
    if parsed is None:
        return label, "blank"
    return parsed[0], "condition"


def _condition_label(scope_label: str) -> str:
    """Build the condition-phase label from a blank-phase label."""
    return f"{scope_label}{PHASE_CONDITION_SUFFIX}"


def stage_condition_label(scope_label: str, stage_index: int) -> str:
    """The condition-phase label for a specific `then where` stage's cross-row
    computations."""
    return (
        f"{scope_label}{PHASE_CONDITION_SUFFIX}"
        f"{_STAGE_QUALIFIER_PREFIX}{stage_index}"
    )


def condition_stage_of_label(label: str) -> int | None:
    """The `then where` stage index a condition-phase label is qualified with,
    or None for the plain condition phase (and all non-condition labels)."""
    parsed = _split_condition_label(label)
    return parsed[1] if parsed else None


def _effective_label(
    concept: BuildConcept, label: str, materialized_roots: frozenset[str] = frozenset()
) -> str:
    """ROOT concepts represent input columns; their scan is shared between
    the SELECT and WHERE phases, so they always live in the blank-phase
    (scope-only) label. Everything else uses the recursion's label as-is.

    A concept in `materialized_roots` is sourced directly from a datasource
    that materializes it (a precomputed/summary table), so it behaves as a
    ROOT input here even though its lineage is derived. The set also carries
    pinned presence probes (see `pinned_probe_addresses`), which are computed
    on their member's own scan and share it between phases the same way."""
    if concept.address in materialized_roots or concept.derivation == Derivation.ROOT:
        return _scope_and_phase(label)[0]
    return label


def classify_depth(
    concept: BuildConcept, label: str, materialized_roots: frozenset[str] = frozenset()
) -> DepthLabel:
    """Tag a concept by its placement role.

    `d1` is no longer "address appears in a WHERE clause" — it's "this node
    was reached via the condition-phase recursion." The phase is encoded in
    the label, so the SELECT and WHERE walks build disjoint sub-graphs and
    a concept that participates in both gets two distinct nodes.

    A `materialized_roots` concept is a datasource scan, not a row-shape
    barrier, so it never gets the d0 (barrier) tag."""
    _, phase = _scope_and_phase(label)
    if phase == "condition":
        return DepthLabel.D1
    if (
        concept.address not in materialized_roots
        and concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS
    ):
        return DepthLabel.D0
    return DepthLabel.STAR


def pinned_probe_addresses(environment: BuildEnvironment) -> frozenset[str]:
    """Presence probes over datasource-bound (ROOT) key-group members.

    Such a probe pins side identity: it must be computed on a scan that
    physically carries the member's authored column, never derived generically
    (the complement side binds the same canonical and is non-NULL exactly where
    the probe must read NULL). Classifying them as root-like sends them into
    the ROOT scan bucket, so `plan_source` sees them in the datasource request
    and the bridge's `_datasource_renders_probe` gate pins each to its member's
    own scan. Rowset-member probes
    (no binding datasource) keep the BASIC path: their value is computed inside
    the member's rowset body."""
    out: set[str] = set()
    if not environment.scoped_join_key_groups:
        return frozenset()
    for concept in environment.concepts.values():
        if not is_presence_probe(concept.address):
            continue
        member = probe_member_address(concept.address, environment)
        if member is not None and member_binding_datasources(member, environment):
            out.add(concept.address)
    return frozenset(out)


def _lineage_args(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """The default — concepts the lineage's expression directly consumes."""
    if concept.lineage is None:
        return []
    return [
        environment.concepts.get(p.address, p) or p
        for p in concept.lineage.concept_arguments
    ]


def _upstream_default(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    existence_only = lineage_existence_only(concept)
    if not existence_only:
        return _lineage_args(concept, environment)
    return [
        c
        for c in _lineage_args(concept, environment)
        if c.address not in existence_only
    ]


def _relation_mates(address: str, environment: BuildEnvironment) -> set[str]:
    """The other members of every scoped-join relation `address` belongs to."""
    mates: set[str] = set()
    for canonical, members in environment.scoped_join_key_groups.items():
        if address == canonical:
            mates |= members
        elif address in members:
            mates |= (members | {canonical}) - {address}
    return mates


def _relation_crosses_rowset_boundary(
    address: str, environment: BuildEnvironment
) -> bool:
    """Whether a scoped-join relation member is paired with a ROWSET handle.

    Only then is the coalesced axis exclusively post-merge: the rowset is an
    opaque body whose row identity exists no earlier than its boundary, so an
    aggregate riding the relation must sit above the join. When every member is
    a plain concept the axis is a native column of each side's own fact, and
    each side aggregates at its authored grain BEFORE the merge coalesces —
    widening there would leak the axis into the GROUP BY and split the answer
    per joined row (the multileg `union join ss.ticket = sr.ticket` shape).

    The property is the RELATION's, not the member's, so it holds whichever
    side is named — the handle itself answers True as readily as its mate.
    """
    for member in {address, *_relation_mates(address, environment)}:
        member_concept = environment.concepts.get(member)
        if member_concept is not None and isinstance(
            member_concept.lineage, BuildRowsetItem
        ):
            return True
    return False


def _collapsible_anchor(concept: BuildConcept, environment: BuildEnvironment) -> bool:
    """An anchor member eligible for the grain-identity redirect: a handle of
    a plain reprojection rowset (single SelectLineage body) the statement
    never references outside the join declaration. A union / multiselect
    anchor (`subset join x = all_combos.b`) participates for real at its own
    multi-arm grain — its outputs span arms, so the relation axis carries
    multiplicity a reprojection at key grain cannot (pinned by the
    union_reproject direct-RHS cell: fan-out + NULL-extension expected). An
    OUTPUT-authored anchor (any of its handles in the select closure —
    the outputs-only closure excludes the join declarations and WHERE) is a
    first-class row contributor whose canonical co-grain siblings must keep
    sharing (redirecting breaks same-key zip narrowing, both-plain LEFT
    control)."""
    lineage = concept.lineage
    if not (
        isinstance(lineage, BuildRowsetItem)
        and isinstance(lineage.rowset.select, SelectLineage)
    ):
        return False
    rowset_name = lineage.rowset.name
    for addr in environment.statement_output_addresses or ():
        authored = environment.concepts.get(addr)
        if (
            authored is not None
            and isinstance(authored.lineage, BuildRowsetItem)
            and authored.lineage.rowset.name == rowset_name
        ):
            return False
    return True


def _rowset_local_grain_identity(
    grain_concept: BuildConcept,
    rowset_name: str,
    environment: BuildEnvironment,
    keep_rowsets: frozenset[str],
) -> BuildConcept:
    """Map a rowset handle's grain key back to the handle's OWN rowset member
    when the scoped-merge canonicalization re-grained it onto the relation's
    other side (`subset join nov_data.k = qualifying.k` leaves nov_data
    handles at Grain<qualifying.k>). Demanding the canonical would drag the
    anchor rowset in as a real row contributor even when the query never
    references it (union_reproject family); the row identity of a rowset
    handle is its rowset's grain expressed in its own handles. Only an
    identity-path mate redirects — a substituted member (address mismatch)
    is owned by the substitution plan and its canonical stays demanded.
    `keep_rowsets` names anchor rowsets the consuming aggregate already
    groups BY: those are first-class row contributors whose canonical the
    co-grain siblings must keep sharing (redirecting breaks the zip's
    same-key narrowing evidence — both-plain LEFT control cell)."""
    lineage = grain_concept.lineage
    if isinstance(lineage, BuildRowsetItem) and lineage.rowset.name == rowset_name:
        return grain_concept
    if not _collapsible_anchor(grain_concept, environment):
        return grain_concept
    if isinstance(lineage, BuildRowsetItem) and lineage.rowset.name in keep_rowsets:
        return grain_concept
    for mate in sorted(_relation_mates(grain_concept.address, environment)):
        mate_concept = environment.concepts.get(mate)
        if (
            mate_concept is not None
            and mate_concept.address == mate
            and isinstance(mate_concept.lineage, BuildRowsetItem)
            and mate_concept.lineage.rowset.name == rowset_name
        ):
            return mate_concept
    return grain_concept


def _aggregate_by_rowsets(aggregate: BuildConcept) -> frozenset[str]:
    """Rowset names the aggregate's authored `by` keys belong to."""
    lineage = aggregate.lineage
    if not isinstance(lineage, BuildAggregateWrapper):
        return frozenset()
    return frozenset(
        by.lineage.rowset.name
        for by in lineage.by
        if isinstance(by.lineage, BuildRowsetItem)
    )


def _rowset_row_identity(
    rowset: BuildRowsetLineage, environment: BuildEnvironment
) -> list[BuildConcept]:
    """A rowset's row identity expressed in its own members: the authored
    select's grain, prefix-mapped into the rowset namespace.

    Every member of one rowset shares this identity — an aggregate over any
    member consumes the rowset's ROWS, so per-member inherited FD grains must
    not stand in for it (q29: catalog-order vs store-row FD grains split three
    same-output-grain sums into sibling aggregate buckets re-joined null-safe
    on the dim tuple, and dedup'd a deliberately fanned-out member). Empty when
    the select is grainless or a grain key has no member handle — callers fall
    back to the per-member walk."""
    out: list[BuildConcept] = []
    for comp in sorted(rowset.select.grain.components):
        member = environment.concepts.get(f"{rowset.name}.{comp}")
        if member is None:
            return []
        out.append(member)
    return out


def _scoped_canonical(
    concept: BuildConcept, environment: BuildEnvironment
) -> BuildConcept:
    """The canonical a scoped-relation member coalesces to. An inherited member
    grain carries the canonical (the scoped-merge canonicalization re-grains
    handles onto it), so row-identity members must enter the redirect in the
    same space — handing the redirect a side member directly skips the
    `keep_rowsets` guard and breaks the zip's same-key narrowing evidence
    (both-plain LEFT control: the plan widens to FULL)."""
    for canonical, members in environment.scoped_join_key_groups.items():
        if concept.address in members:
            canon = environment.concepts.get(canonical)
            if canon is not None and canon.address == canonical:
                return canon
    return concept


def _expand_aggregate_row_identities(
    inputs: list[BuildConcept], environment: BuildEnvironment
) -> list[BuildConcept]:
    """A row-identity concept that is itself an aggregate stands in for that
    aggregate's output rows, which are keyed by its grouping grain — so its
    usable row identity is those grain keys (`max(lp_avg ? bucket_id = 1)`
    reads bucket-grain rows, not "one row per lp_avg"). Substituting the grain
    lets same-row-grain aggregates over sibling metrics share one input stream
    (q28: the avg/cnt/cntd pivot families all read bucket-grain rows), matching
    how a DIRECT inner-aggregate arg already contributes its output grain in
    `_upstream_aggregate`. The abstract all-rows marker is never a real column;
    a grainless (global) aggregate is one row and contributes nothing."""
    out: list[BuildConcept] = []
    for c in inputs:
        if c.derivation != Derivation.AGGREGATE:
            out.append(c)
            continue
        out.extend(
            environment.concepts[g]
            for g in sorted(c.grain.components)
            if g in environment.concepts
            and environment.concepts[g].name != ALL_ROWS_CONCEPT
        )
    return out


def _walk_scoped_aggregate_grain_inputs(
    aggregate: BuildConcept, concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """`_walk_aggregate_grain_inputs`, with a rowset member contributing its
    rowset's row identity (not its own inherited FD grain) and a rowset
    handle's grain keys redirected to its own rowset members (see
    `_rowset_local_grain_identity`)."""
    lineage = concept.lineage
    if not isinstance(lineage, BuildRowsetItem):
        return _expand_aggregate_row_identities(
            _walk_aggregate_grain_inputs(concept, environment), environment
        )
    inputs = _rowset_row_identity(lineage.rowset, environment)
    if inputs and environment.scoped_join_key_groups:
        inputs = [_scoped_canonical(c, environment) for c in inputs]
    if not inputs:
        inputs = _walk_aggregate_grain_inputs(concept, environment)
    if not inputs or not environment.scoped_join_key_groups:
        return inputs
    keep_rowsets = _aggregate_by_rowsets(aggregate) - {lineage.rowset.name}
    return [
        _rowset_local_grain_identity(c, lineage.rowset.name, environment, keep_rowsets)
        for c in inputs
    ]


def _aggregate_authored_grain(
    concept: BuildConcept, out_grain: frozenset[str], environment: BuildEnvironment
) -> frozenset[str]:
    """Redirect an aggregate's grain component the scoped-merge
    canonicalization moved onto the relation's other side back to the
    aggregate's own authored `by` key. `sum(x) by nov_data.k` under
    `subset join nov_data.k = qualifying.k` builds with
    Grain<qualifying.k> even though its `by` args stay `nov_data.k` (the
    identity path keeps both addresses alive); grouping the bucket on the
    canonical demands the anchor rowset as a real input. A SUBSTITUTED `by`
    arg already reads as the canonical itself, so a participating relation
    (union direct-join) keeps its coalesced-axis grain."""
    lineage = concept.lineage
    if not isinstance(lineage, BuildAggregateWrapper):
        return out_grain
    by_addrs = {c.address for c in lineage.by}
    if not by_addrs:
        return out_grain
    redirected: set[str] = set()
    for g in out_grain:
        if g in by_addrs:
            redirected.add(g)
            continue
        chosen = g
        g_concept = environment.concepts.get(g)
        if g_concept is not None and _collapsible_anchor(g_concept, environment):
            for mate in sorted(_relation_mates(g, environment) & by_addrs):
                mate_concept = environment.concepts.get(mate)
                if mate_concept is not None and mate_concept.address == mate:
                    chosen = mate
                    break
        redirected.add(chosen)
    return frozenset(redirected)


def is_grain_identity(node: object) -> bool:
    """The desugared form of `grain(a, b, ...)`: a hash over the members joined
    by `GRAIN_SEPARATOR` (`grain_hash`). The separator is a control character no
    author can write, so the shape identifies the desugar unambiguously — a
    hand-written `hash(x, md5)` never matches."""
    if not isinstance(node, BuildFunction) or node.operator != FunctionType.HASH:
        return False
    joined = node.arguments[0] if node.arguments else None
    return (
        isinstance(joined, BuildFunction)
        and joined.operator == FunctionType.CONCAT_WS
        and bool(joined.arguments)
        and joined.arguments[0] == GRAIN_SEPARATOR
    )


def _row_identity_components(
    concept: BuildConcept, environment: BuildEnvironment
) -> frozenset[str]:
    """Concepts an aggregate counts a `grain(...)` tuple over.

    `count(grain(a, b))` counts DISTINCT (a, b) combinations: the tuple members
    are the aggregate's own dedup key, consumed by the count, not an axis its
    value varies along. So a relation member reaching the axis widening only
    through one is a false axis — grouping the branch by it slices the count per
    member value, and the outer select can only dedup, never re-aggregate
    (TPC-DS q72: per-item slivers instead of per-week totals)."""
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return frozenset()
    out: set[str] = set()
    stack: list[object] = list(concept.lineage.function.arguments)
    seen: set[int] = set()
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        if is_grain_identity(node):
            assert isinstance(node, BuildFunction)
            out.update(
                arg.address
                for arg in node.concept_arguments
                if isinstance(arg, BuildConcept)
            )
            continue
        if isinstance(node, BuildConcept):
            # `count(grain(...) ? cond)` reaches the tuple through the filter's
            # content, and the desugar itself is hoisted to a virtual concept.
            resolved = environment.concepts.get(node.address, node) or node
            stack.append(resolved.lineage)
        elif isinstance(node, BuildFilterItem):
            stack.append(node.content)
        elif isinstance(node, BuildFunction):
            stack.extend(node.arguments)
    return frozenset(out)


def _aggregate_axis_members(
    concept: BuildConcept,
    environment: BuildEnvironment,
    aggregate_input_grain: frozenset[str],
) -> frozenset[str]:
    """Statement-scoped relation members an aggregate's inputs ride — the axis
    columns to widen its grouping grain by (see the caller in `_add_concept`).

    The MEASURE the aggregate reads can itself be a relation member
    (`count(r_filtered.return_quantity)` under `union join quantity =
    r_filtered.return_quantity`). It never enters `aggregate_input_grain`
    — an argument contributes its own grain, not itself — but a measure
    the relation pairs on is an axis column like any other: the aggregate
    reads it per coalesced axis row, so the axis has to be in the grain or
    the merge above loses that leg of the pairing. Only the FUNCTION's
    arguments: the wrapper's `by` grain is already the output grain, and
    feeding those back through here re-adds them as axis members and
    splits the answer per joined row (union_reproject direct-RHS).

    ...but never the ANCHOR-side member the aggregate itself reads. A
    rowset handle read per axis row is a presence measure — it is NULL on
    axis rows the boundary never matched, so `count(handle)` per axis is a
    meaningful 0/1 (q17). The anchor-side key is the axis, so grouping by
    the very key being counted is degenerate: `count(cust_id)` beside
    `region` becomes 1 per customer instead of the customers per region
    (q35 `store AND (web OR catalog)`).

    Nor a member the aggregate names inside a counted `grain(...)` tuple —
    that is row identity, not an axis (`_row_identity_components`)."""
    candidates = set(aggregate_input_grain)
    if isinstance(concept.lineage, BuildAggregateWrapper):
        candidates |= {
            arg.address
            for arg in concept.lineage.function.concept_arguments
            if isinstance(arg, BuildConcept)
        }
    own_anchor_args = {
        arg.address
        for arg in (
            concept.lineage.function.concept_arguments
            if isinstance(concept.lineage, BuildAggregateWrapper)
            else ()
        )
        if isinstance(arg, BuildConcept)
        and not isinstance(arg.lineage, BuildRowsetItem)
    }
    row_identity = _row_identity_components(concept, environment)
    return frozenset(
        addr
        for addr in candidates & _statement_scoped_relation_members(environment)
        if _relation_crosses_rowset_boundary(addr, environment)
        and addr not in own_anchor_args
        and addr not in row_identity
    )


def _grouping_pass_sibling_axis_members(
    concept: BuildConcept, environment: BuildEnvironment
) -> frozenset[str]:
    """The axis widening a grouping()/grouping_id() identity inherits: the
    union of `_aggregate_axis_members` over the (non-identity) aggregates
    sharing its grouping spec. The identity is part of the pass's row identity
    and MUST land in the same bucket-shape as its pass siblings; computing its
    widening from its own arg lineage instead either drags orthogonal axis
    columns into the pass's GROUP BY shape (BinderException) or strands the
    flag on a node its pass pairs with via the literal-0 grain-match stamp
    (subtotal rows drop)."""
    spec = nonstandard_grouping_spec(concept.lineage)
    if spec is None:
        return frozenset()
    members: set[str] = set()
    seen: set[str] = set()
    for other in environment.concepts.values():
        if (
            other.address == concept.address
            or other.address in seen
            or is_grouping_identity(other)
            or nonstandard_grouping_spec(other.lineage) != spec
        ):
            continue
        seen.add(other.address)
        other_grain = frozenset(other.grain.components) if other.grain else frozenset()
        if environment.scoped_join_key_groups:
            other_grain = _aggregate_authored_grain(other, other_grain, environment)
        other_input = _aggregate_input_grain(other, environment, other_grain)
        other_dimension_grain = {
            addr for addr in other_grain if not addr.endswith(f".{ALL_ROWS_CONCEPT}")
        }
        if other_input and other_dimension_grain:
            members |= _aggregate_axis_members(other, environment, other_input)
    return frozenset(members)


def _upstream_aggregate(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """AGGREGATE: lineage args plus row-identity concepts of each function
    arg (property keys, rowset grain). Stops at inner aggregate boundaries
    via `_walk_aggregate_grain_inputs`.

    An arg that is itself an inner aggregate (`avg(daily_rides) by
    start_station.id`, daily_rides = `count(...) by ride_date`) contributes its
    OUTPUT grain (ride_date) as a row-level input: that grain key is the join
    bridge between the outer aggregate's grouping dimension (start_station.id)
    and the inner aggregate's value. Without it the dimension is row-sourced
    alone and the input merge cross-joins ON 1=1. Mirrors `_aggregate_input_grain`
    (which already includes it) so the graph edges and the computed input grain
    agree."""
    # A grand-total (`by *`) aggregate's grouping key is the abstract
    # `__preql_internal.all_rows` marker. It is a single-row cross-join marker,
    # never a real sourced column -- demanding it forces the input scan to
    # project `1 as __preql_internal.all_rows` and the consumer to INNER JOIN on
    # it instead of cross-joining ON 1=1.
    base = [
        c for c in _lineage_args(concept, environment) if c.name != ALL_ROWS_CONCEPT
    ]
    if isinstance(concept.lineage, BuildAggregateWrapper):
        for arg in concept.lineage.function.arguments:
            if isinstance(arg, BuildConcept):
                grain_inputs = _walk_scoped_aggregate_grain_inputs(
                    concept, arg, environment
                )
                if grain_inputs:
                    base.extend(grain_inputs)
                elif arg.derivation == Derivation.AGGREGATE and arg.grain:
                    # The arg is itself an inner aggregate; its output grain is the
                    # join bridge (see docstring). A non-aggregate arg's grain is
                    # NOT added — its own row identity already flows via the walk.
                    base.extend(
                        environment.concepts[g]
                        for g in arg.grain.components
                        if g in environment.concepts
                    )
    return base


def _upstream_filter(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """FILTER: lineage args plus property keys of the filtered concept
    (matches `resolve_filter_parent_concepts`). A filter over a property
    needs the property's keys to keep the row stream identifiable.

    Existence-only args (semijoin RHS) are dropped from the row lineage — they
    get a side-channel `existence` edge instead (see `build_concept_graph`)."""
    existence_only = lineage_existence_only(concept)
    base = [
        c
        for c in _lineage_args(concept, environment)
        if c.address not in existence_only
    ]
    if isinstance(concept.lineage, BuildFilterItem):
        direct_parent = concept.lineage.content
        if (
            isinstance(direct_parent, BuildConcept)
            and direct_parent.purpose in (Purpose.PROPERTY, Purpose.METRIC)
            and direct_parent.keys
        ):
            base += [
                environment.concepts[k]
                for k in direct_parent.keys
                if k in environment.concepts
            ]
    return base


def _grain_and_keys(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    extras: list[BuildConcept] = []
    if concept.grain:
        for g in concept.grain.components:
            if g in environment.concepts:
                extras.append(environment.concepts[g])
    if concept.keys:
        for k in concept.keys:
            if k in environment.concepts:
                extras.append(environment.concepts[k])
    return extras


def _window_aggregate_grain_keys(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """Grain keys of every aggregate in the window's argument closure.

    A window preserves its argument's grain row-for-row. When an argument is
    (or rides, through BASIC expressions, on top of) an aggregate at its own
    group grain, every grain key of that aggregate must be a window parent —
    otherwise a dropped key forces a join-back on (kept_key, aggregate_value),
    which is non-unique and NULL-bearing for ROLLUP subtotal/total rows
    (q36/q59). Walks
    transitively through BASIC args and stops at each aggregate boundary (a
    nested aggregate already collapsed its own upstream)."""
    extras: list[BuildConcept] = []
    seen: set[str] = set()
    stack = list(_lineage_args(concept, environment))
    while stack:
        arg = stack.pop()
        if arg.address in seen:
            continue
        seen.add(arg.address)
        if arg.derivation == Derivation.AGGREGATE:
            for gkey in arg.grain.components:
                if gkey in environment.concepts:
                    extras.append(environment.concepts[gkey])
            continue  # stop at the aggregate boundary
        if arg.derivation == Derivation.BASIC and arg.lineage is not None:
            stack.extend(
                environment.concepts.get(p.address, p) or p
                for p in arg.lineage.concept_arguments
            )
    return extras


def _upstream_window(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """WINDOW: lineage args, the grain keys of any aggregate in the argument
    closure, plus the window's own grain components and keys (matches
    `resolve_window_parent_concepts`)."""
    return (
        list(_lineage_args(concept, environment))
        + _window_aggregate_grain_keys(concept, environment)
        + _grain_and_keys(concept, environment)
    )


def _upstream_subselect(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """SUBSELECT: lineage args plus grain components (matches
    `resolve_subselect_parent_concepts`)."""
    base = list(_lineage_args(concept, environment))
    if concept.grain:
        for g in concept.grain.components:
            if g in environment.concepts:
                base.append(environment.concepts[g])
    return base


_UPSTREAM: dict[Derivation, UpstreamFetcher] = {
    Derivation.AGGREGATE: _upstream_aggregate,
    Derivation.FILTER: _upstream_filter,
    Derivation.WINDOW: _upstream_window,
    Derivation.SUBSELECT: _upstream_subselect,
}


def node_id(label: str, address: str) -> str:
    """Compose a concept-graph node key from (label, address).

    For the default outer-query label (``""``), the key is just the bare
    address so existing code that reads addresses as keys keeps working.
    For a labeled sub-graph (a rowset's inner walk, label = rowset name),
    the key is prefixed: ``"[q5_results]local.channel_label"``. The
    bracketed prefix is what keeps the inner and outer copies of the
    same concept distinct when both appear in the graph."""
    return f"[{label}]{address}" if label else address


def _statement_scoped_relation_members(environment: BuildEnvironment) -> frozenset[str]:
    """All addresses of scoped-join relations declared at STATEMENT scope
    (query-level `union/left/full/subset join a = b`). Global `merge`
    identities are excluded — they pair INNER and never redefine row
    identity."""
    from trilogy.core.domain_graph import EdgeScope

    if not environment.scoped_join_key_groups:
        return frozenset()
    statement_addrs = {
        addr
        for e in environment.domain_graph.edges
        if e.scope is EdgeScope.STATEMENT
        for addr in (e.source, e.target)
    }
    out: set[str] = set()
    for canonical, members in environment.scoped_join_key_groups.items():
        relation = {canonical, *members}
        if relation & statement_addrs:
            out |= relation
    return frozenset(out)


def _unsourced_relation_mates(
    mandatory_list: list[BuildConcept],
    conditions: list[BuildWhereClause],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Rowset-handle key-group mates the statement never references, requested
    so their scope enters the plan. Without this the mate's rowset is
    absent entirely: a coalescing axis silently collapses to the demanded
    side's own domain, and a subset-declared RAW member stays partial and trips
    the final no-complete-source guard.

    - COALESCING (`union`/`full`) member demanded: projecting one member of the
      group yields the unified axis, so its unauthored rowset mates are
      requested (bare-member projection cell).
    - SUBSET side demanded as a RAW (datasource-bound) member: its only binding
      is partial under the declaration and only the anchor rowset carries the
      axis domain (rowset-anchor `subset join cust = members.mid` cells). A
      demanded ROWSET member never requests its anchor — complete at its own
      opaque boundary, the declaration is pure domain metadata and the plan
      collapses to that side alone (union-reproject rowset-LHS cells). A
      COMPOSITE relation onto one anchor rowset is withheld too: the composite
      raw-LHS shape is a pinned clean error (union-reproject clean-error).

    AUTHORED mates are never requested here: a member the author references
    (projects, null-tests) sources per-side values and presence from its own
    scope via the probe machinery (q35/q44), and the walk already carries it.
    """
    if not environment.scoped_join_key_groups:
        return []
    demanded = {c.address for c in mandatory_list} | {
        c.address for clause in conditions for c in clause.concept_arguments
    }
    authored = environment.statement_authored_addresses
    coalescing = environment.domain_graph.coalescing_relation_members()
    subset_map = environment.domain_graph.subset_join_map()

    def _rowset_mate(address: str) -> BuildConcept | None:
        mate = environment.concepts.get(address)
        if (
            mate is None
            or mate.address != address
            or not isinstance(mate.lineage, BuildRowsetItem)
            or address in demanded
            or (authored is not None and address in authored)
        ):
            return None
        return mate

    def _anchor_rowset_name(address: str) -> str | None:
        concept = environment.concepts.get(address)
        if concept is None or not isinstance(concept.lineage, BuildRowsetItem):
            return None
        return concept.lineage.rowset.name

    out: dict[str, BuildConcept] = {}
    anchor_rowset_counts: dict[str, int] = {}
    for anchor in set(subset_map.values()):
        name = _anchor_rowset_name(anchor)
        if name is not None:
            anchor_rowset_counts[name] = anchor_rowset_counts.get(name, 0) + 1
    for canonical, members in environment.scoped_join_key_groups.items():
        for member in sorted({canonical, *members} & demanded):
            member_concept = environment.concepts.get(member)
            if member_concept is None or member_concept.address != member:
                continue
            if member in coalescing:
                for other in sorted({canonical, *members} - {member}):
                    mate = _rowset_mate(other)
                    if mate is not None:
                        out[mate.address] = mate
            member_anchor = subset_map.get(member)
            if (
                member_anchor is not None
                and not isinstance(member_concept.lineage, BuildRowsetItem)
                and anchor_rowset_counts.get(
                    _anchor_rowset_name(member_anchor) or "", 0
                )
                == 1
            ):
                mate = _rowset_mate(member_anchor)
                if mate is not None:
                    out[mate.address] = mate
    return [out[addr] for addr in sorted(out)]


def computed_origin_relation_members(environment: BuildEnvironment) -> frozenset[str]:
    """Members of relations — ANY scope — whose collapse left a ROW-SHAPE
    computed origin in `alias_origin_lookup` (`merge recursive_parent into
    root_parent.id`, the origin a RECURSIVE).

    Such a relation is an equality between two DIFFERENT lineages, exactly like
    a statement-scoped computed join key: the collapsed side's computation IS
    the join, the completion merge null-extends, and the axis must surface as
    graph structure. Spelling-identity merges (both sides bare keys) stay
    excluded — they pair INNER and never redefine row identity. So does a
    scalar-derived origin (`merge ka into kb` with `ka <- a.l_key + 1`): each
    side renders its own variant inline on its scan through the derived
    merge-key rail (`_datasource_renders_derived`), and routing it through the
    relation axis instead loses the preserving FULL's null-extended rows
    (join_matrix derived/union/merge nullable cell)."""
    if not environment.scoped_join_key_groups:
        return frozenset()
    out: set[str] = set()
    for canonical, members in environment.scoped_join_key_groups.items():
        relation = {canonical, *members}
        for member in relation:
            resolved = environment.concepts.get(member)
            origin = environment.alias_origin_lookup.get(member)
            if (
                resolved is not None
                and resolved.address != member
                and origin is not None
                and origin.lineage is not None
                and origin.derivation in ROW_SHAPE_BARRIER_DERIVATIONS
            ):
                out |= relation
                break
    return frozenset(out)


def _scoped_group_sides(
    members: set[str], environment: BuildEnvironment
) -> frozenset[str]:
    """Identity of the endpoints a scoped join-key group pairs: the rowset (or
    namespace) each member belongs to. Two key groups with the same sides are
    legs of ONE composite relation (`union join a.x = b.x and a.y = b.y`, or
    two clauses over the same pair — both render as one FULL JOIN on all
    legs)."""
    sides: set[str] = set()
    for addr in members:
        member = environment.concepts.get(addr)
        if member is not None and isinstance(member.lineage, BuildRowsetItem):
            sides.add(f"rowset:{member.lineage.rowset.name}")
        else:
            sides.add(f"ns:{addr.rsplit('.', 1)[0]}")
    return frozenset(sides)


def _composite_relation_sibling_axes(
    addresses: frozenset[str], environment: BuildEnvironment
) -> frozenset[str]:
    """Members of sibling key groups completing a composite scoped relation.

    A FULL/union join on a composite key pairs rows on ALL its legs at once,
    so the joined row identity is every leg's axis. An aggregate whose inputs
    ride one leg (a presence probe over `customer_id`) still consumes rows at
    the FULL composite grain — deduping its input to just the touched leg
    collapses distinct `(customer, item)` pairs into one row per customer and
    undercounts (coalescing-presence composite)."""
    groups = environment.scoped_join_key_groups
    if not groups:
        return frozenset()
    group_sets = {
        canonical: {canonical, *members} for canonical, members in groups.items()
    }
    sides_of = {
        canonical: _scoped_group_sides(g, environment)
        for canonical, g in group_sets.items()
    }
    out: set[str] = set()
    for canonical, group in group_sets.items():
        if not group & addresses:
            continue
        for other, other_group in group_sets.items():
            if other == canonical or sides_of[other] != sides_of[canonical]:
                continue
            out |= other_group
    return frozenset(out - addresses)


def _aggregate_input_grain(
    concept: BuildConcept, environment: BuildEnvironment, out_grain: frozenset[str]
) -> frozenset[str]:
    """The row grain an aggregate's inputs must have before aggregation.

    Every aggregate has one: the aggregate's output grouping grain plus the
    natural grain of each aggregate argument. Aggregates sharing this grain can
    share one input stream; aggregates with different input grains need
    separate streams.
    """
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return frozenset()
    input_grain: set[str] = set(out_grain)
    for arg in concept.lineage.function.arguments:
        # Descend into inline expressions: `sum(case when ... then
        # web_sales.price else 0)` arrives as a Function, not a BuildConcept, so
        # walking only top-level concept args would miss its fact inputs. Two
        # aggregates over different facts at the same output grain would then
        # look identical and co-source into one raw fact-to-fact join before
        # aggregating (q2.1/q2.2 fan-out). A referenced concept that is itself a
        # row-shape barrier (inner aggregate / rowset) has already collapsed to
        # its own grain and is consumed opaquely — pulling its grain here would
        # force a spurious regroup of the outer aggregate's input (q97: a
        # grand-total sum over rowset outputs would dedup the rowset rows).
        if isinstance(arg, BuildConcept):
            sub_args = [arg]
        elif isinstance(arg, BuildConceptArgs):
            sub_args = [
                c
                for c in arg.concept_arguments
                if isinstance(c, BuildConcept)
                and c.derivation not in ROW_SHAPE_BARRIER_DERIVATIONS
            ]
        else:
            continue
        for sub in sub_args:
            grain_inputs = _walk_scoped_aggregate_grain_inputs(
                concept, sub, environment
            )
            if grain_inputs:
                input_grain.update(c.address for c in grain_inputs)
            elif (
                sub.purpose == Purpose.KEY
                and sub.derivation != Derivation.ROOT
                and sub.keys
            ):
                # Aggregating a DERIVED key ranges over the key's distinct
                # domain, not its host scan's rows: its authored grain is the
                # row grain of the scan that computes it, but its identity is
                # its defining keys (gcat array_agg(launch_filter): one entry
                # per distinct _launch_code).
                input_grain.update(sub.keys)
            elif sub.grain:
                input_grain.update(sub.grain.components)
    # GLOBAL aggregates only: their whole population IS the joined axis, so a
    # composite relation's row identity must include every leg (the presence
    # sums over `union join a.k = b.k and a.e = b.e` dedup per (k, e) pair).
    # A dimension-grained aggregate reading one side's member keeps its own
    # input stream — widening it re-shapes the isolated two-pass aggregate
    # CTEs of the q17 family (composite_union_join stddev cells).
    if input_grain and not out_grain and environment.scoped_join_key_groups:
        input_grain |= _composite_relation_sibling_axes(
            frozenset(input_grain), environment
        )
    return minimize_build_grain(environment, input_grain)


def _aggregate_distinct_rewritable(
    concept: BuildConcept,
    environment: BuildEnvironment,
    input_grain: frozenset[str],
    out_grain: frozenset[str],
) -> bool:
    """True for a COUNT whose argument's VALUE is the key that carries the
    aggregate's residual input grain: `count(order_id) by item` or
    `count(order_id ? cond) by item`. Counting a key means counting at the
    key's grain, so the dedup its coarser input stream would perform is
    exactly DISTINCT on the counted value — the bucket may instead share a
    finer-grain sibling stream and render COUNT(DISTINCT ...).

    A key with a HOME datasource (a table whose grain is exactly that key) is
    never rewritable: its count population is that table's full key set, while
    a sibling fact stream only carries the key values present in the fact
    (`count(user_id)` beside post-fact sums must still count post-less
    users)."""
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return False
    function = concept.lineage.function
    if function.operator != FunctionType.COUNT:
        return False
    if len(function.arguments) != 1:
        return False
    arg = function.arguments[0]
    if not isinstance(arg, BuildConcept):
        return False
    content = arg
    if arg.derivation == Derivation.FILTER and isinstance(arg.lineage, BuildFilterItem):
        inner = arg.lineage.content
        if not isinstance(inner, BuildConcept):
            return False
        content = inner
    if content.purpose != Purpose.KEY:
        return False
    if input_grain - out_grain != frozenset({content.address}):
        return False
    content_identities = {content.address, *content.pseudonyms}
    return not any(
        set(datasource.grain.components) <= content_identities
        and datasource.grain.components
        for datasource in environment.datasources.values()
    )


def _derivable_pseudonym_origins(
    concept: BuildConcept,
    environment: BuildEnvironment,
    datasource_addresses: frozenset[str],
) -> list[BuildConcept]:
    """All derivable pseudonym origins of an unsourceable bare key (ROOT, no
    lineage, bound by no datasource), deterministically ordered by address.

    The motivating case is a struct field reached through an unnest:
    `unnest_array.a` parses to the bare key `local.a`, which no datasource binds
    directly — it is only reachable as `attr_access(unnest(array_struct), a)`.
    Swap the bare key for that attr-access origin so the graph walks
    attr_access -> unnest -> datasource instead of dead-ending on a ROOT leaf
    with no source.

    A field name can resolve to MORE THAN ONE origin — two struct arrays both
    exposing `a` leave `local.a` with pseudonyms `{x.a, y.a}`, each its own
    attr-access origin. They are equivalent columns but live over different
    sources, so the caller must pick a *satisfiable* one rather than commit to
    an arbitrary (hash-ordered) pseudonym."""
    if concept.derivation != Derivation.ROOT or concept.lineage is not None:
        return []
    if concept.address in datasource_addresses:
        return []
    # A merge can demote a derived concept to a bare ROOT key while its real
    # lineage survives in `alias_origin_lookup` under the SAME address (e.g.
    # `merge first_parent into parent.id` leaves `local.first_parent` ROOT but its
    # origin is the RECURSIVE `recurse_edge(...)`). Check the concept's own
    # address alongside its pseudonyms. Recursion is bounded: each origin has a
    # lineage, so re-entry on it returns [].
    origins: dict[str, BuildConcept] = {}
    for pseudonym in (concept.address, *concept.pseudonyms):
        origin = environment.alias_origin_lookup.get(pseudonym)
        if origin is not None and origin.lineage is not None and origin is not concept:
            origins[origin.address] = origin
    return [origins[a] for a in sorted(origins)]


def _resolve_pseudonym_origin(
    concept: BuildConcept,
    environment: BuildEnvironment,
    datasource_addresses: frozenset[str],
) -> BuildConcept | None:
    """Pick the origin the graph should substitute for an unsourceable bare key.

    Among the candidate origins, prefer one whose lineage actually bottoms out
    at a datasource (`concept_satisfiable` against the bound addresses); the
    alternatives are equivalent columns over sources that may not exist in this
    environment. Falling back to the first candidate when none is satisfiable
    preserves the original loud-failure behavior (a downstream
    `NoDatasourceException` rather than a silent drop). Selection is
    deterministic — origins are address-sorted — so a multi-origin key plans the
    same way regardless of set iteration order."""
    candidates = _derivable_pseudonym_origins(
        concept, environment, datasource_addresses
    )
    if not candidates:
        return None
    for origin in candidates:
        if concept_satisfiable(origin, set(datasource_addresses)):
            return origin
    return candidates[0]


def _alternative_origins(
    concept: BuildConcept,
    environment: BuildEnvironment,
    datasource_addresses: frozenset[str],
) -> list[BuildConcept]:
    """Derivable origins at a DIFFERENT address than the bare key — the genuine
    alternatives that warrant a hub (`local.a` via `uA.a` OR `uB.a`).

    A same-address origin (the brief-02 recursive-merge demotion, where a merge
    leaves `local.first_parent` a bare ROOT key whose origin is the RECURSIVE
    concept at the same address) is a *promotion*, not an alternative: it is
    handled by in-place substitution, never a hub, so it is excluded here."""
    return [
        o
        for o in _derivable_pseudonym_origins(
            concept, environment, datasource_addresses
        )
        if o.address != concept.address
    ]


def _add_concept(
    concept: BuildConcept,
    environment: BuildEnvironment,
    graph: nx.DiGraph,
    edges: EdgeMap,
    attrs: dict[str, ConceptAttrs],
    label: str = "",
    materialized_roots: frozenset[str] = frozenset(),
    datasource_addresses: frozenset[str] = frozenset(),
    pinned_probes: frozenset[str] = frozenset(),
) -> None:
    """Walk lineage from a concept toward its roots, under a fixed label.

    The label encodes (scope, phase) — scope is "" for the outer query / the
    rowset name for rowset internals; phase is "blank" by default, or
    "condition" via the ``@condition`` suffix. The same concept reached from
    the SELECT walk and from the WHERE walk thus lands in two separate nodes
    (the WHERE one is d1, the SELECT one keeps its derivation-driven label).
    No second-pass promotion is needed — the depth falls out of the label.

    A concept in `materialized_roots` is treated as a ROOT leaf: its lineage is
    not walked (a datasource materializes it directly), and its node carries
    `derivation=ROOT` so the group graph buckets it into a datasource scan.

    A concept in `pinned_probes` (a presence probe over a datasource-bound
    key-group member) also carries `derivation=ROOT` — it must be computed on
    its member's own scan, so it belongs in the ROOT bucket where `plan_source`
    pins it per side — but its lineage IS walked: the probe's argument is the
    group's axis key, which the plan still needs as the join spine."""
    alternatives = _alternative_origins(concept, environment, datasource_addresses)
    use_hub = len(alternatives) >= 2
    if not use_hub:
        # 0 or 1 genuine alternative: substitute the (satisfiable) origin in place
        # exactly as before. A same-address origin (brief-02 recursive merge) and a
        # single struct-field arm both take this path — no hub, no resolution pass.
        origin = _resolve_pseudonym_origin(concept, environment, datasource_addresses)
        if origin is not None:
            concept = origin
    root_like = materialized_roots | pinned_probes
    is_materialized_root = concept.address in materialized_roots
    is_pinned_probe = concept.address in pinned_probes
    eff_label = _effective_label(concept, label, root_like)
    nid = node_id(eff_label, concept.address)
    if nid in graph:
        return
    # Surface the aggregate's grouping mode (STANDARD / ROLLUP / CUBE /
    # GROUPING_SETS) so downstream group-partitioning can split distinct
    # modes into their own buckets — two AGGREGATEs sharing grain but
    # using different grouping modes need separate CTEs (one emits GROUP
    # BY, the other GROUP BY ROLLUP).
    grouping_mode: AggregateGroupingMode | None = None
    if not is_materialized_root and isinstance(concept.lineage, BuildAggregateWrapper):
        grouping_mode = concept.lineage.grouping
    # Rowset identity: every handle of one rowset shares a row population (the
    # rowset is one sub-query, planned in full by `gen_rowset`), so the rowset
    # grouping rule buckets them into a single boundary group by name. This
    # holds for multiselect (merge/align) rowsets too now that the inner is
    # planned recursively rather than walked into this graph — the arms and any
    # cross-arm HAVING are resolved inside `resolve_rowset`, so the outer
    # boundary must NOT fragment per-grain (q64: per-grain split left each
    # boundary exposing only a subset of handles, so the FINAL merge couldn't
    # source the rest).
    rowset_name = None
    if isinstance(concept.lineage, BuildRowsetItem):
        rowset_name = concept.lineage.rowset.name
    elif concept.derivation in (Derivation.TVF_UNION, Derivation.MULTISELECT):
        # A demanded multiselect/union OUTPUT itself (`local._combined_sort_k`
        # — e.g. the ORDER-BY carry of a union column the select groups away)
        # has no independent source: only the boundary wrapping the multiselect
        # can produce it. Tag it with that rowset so it buckets into the
        # boundary and `resolve_rowset` exposes it as a demanded content.
        for handle_concept in environment.concepts.values():
            hlineage = handle_concept.lineage
            if (
                isinstance(hlineage, BuildRowsetItem)
                and hlineage.content.address == concept.address
            ):
                rowset_name = hlineage.rowset.name
                break
    elif is_presence_probe(concept.address):
        # A presence probe over a ROWSET member has no datasource to pin to —
        # its value must be computed INSIDE the member's rowset boundary,
        # pre-merge (post-merge the member reads as the fused group coalesce,
        # never NULL). Tag it with the member's rowset so the rowset grouping
        # rule buckets it into that boundary and `resolve_rowset` discharges it
        # as an obligation output.
        member = probe_member_address(concept.address, environment)
        member_concept = environment.concepts.get(member) if member else None
        if member_concept is not None and isinstance(
            member_concept.lineage, BuildRowsetItem
        ):
            rowset_name = member_concept.lineage.rowset.name
    is_rename = (
        isinstance(concept.lineage, BuildFunction)
        and concept.lineage.operator == FunctionType.ALIAS
    )
    out_grain = frozenset(concept.grain.components) if concept.grain else frozenset()
    if (
        not is_materialized_root
        and concept.derivation == Derivation.AGGREGATE
        and environment.scoped_join_key_groups
    ):
        out_grain = _aggregate_authored_grain(concept, out_grain, environment)
    aggregate_input_grain = (
        frozenset()
        if is_materialized_root
        else _aggregate_input_grain(concept, environment, out_grain)
    )
    # Under a STATEMENT-scoped preserving join to a ROWSET (`union join ticket
    # = r_filtered.r_ticket`), row identity is the coalesced relation axis: an
    # aggregate whose inputs ride the relation computes per axis row, not per
    # its authored dimension grain — it renders at the joined relation's grain
    # via the grain-match formulas, and the outer select then dedups.
    # Widen the grouping grain by the relation members its inputs carry.
    # Global `merge` identities pair INNER 1:1 and are excluded, as are
    # GLOBAL aggregates (empty/all_rows grain — the q97 presence counts stay
    # one total row over the joined relation, never per-axis).
    dimension_grain = {
        addr for addr in out_grain if not addr.endswith(f".{ALL_ROWS_CONCEPT}")
    }
    if (
        not is_materialized_root
        and concept.derivation == Derivation.AGGREGATE
        and dimension_grain
    ):
        if is_grouping_identity(concept):
            # A grouping()/grouping_id() identity is a KEY of its pass, not a
            # row reader — widening it from its OWN arg lineage (its arg IS a
            # grouping key, whose lineage rides the axis even when the pass's
            # measures don't) puts it in a bucket whose grain names columns the
            # rendered GROUP BY (the by-list verbatim) never groups: a bare
            # ungrouped projection (BinderException). But it must still bucket
            # WITH its pass — a separately-bucketed flag pairs with the
            # aggregate via the literal-0 grain-match stamp and the join drops
            # subtotal rows. So it inherits exactly the axis widening of the
            # aggregates sharing its grouping spec.
            out_grain |= _grouping_pass_sibling_axis_members(concept, environment)
        elif aggregate_input_grain:
            out_grain |= _aggregate_axis_members(
                concept, environment, aggregate_input_grain
            )
    graph.add_node(nid)
    attrs[nid] = ConceptAttrs(
        address=concept.address,
        label=eff_label,
        derivation=(
            Derivation.ROOT
            if (is_materialized_root or is_pinned_probe)
            else concept.derivation
        ),
        purpose=concept.purpose,
        granularity=concept.granularity,
        depth_label=classify_depth(concept, eff_label, root_like),
        grain_components=out_grain,
        grouping_mode=grouping_mode,
        rowset_name=rowset_name,
        aggregate_input_grain=aggregate_input_grain,
        aggregate_distinct_rewritable=(
            bool(aggregate_input_grain)
            and _aggregate_distinct_rewritable(
                concept, environment, aggregate_input_grain, out_grain
            )
        ),
        keys=frozenset(concept.keys or set()),
        pseudonyms=frozenset(concept.pseudonyms),
        is_rename=is_rename,
    )

    # Materialized root: a datasource provides this concept directly (a
    # precomputed/summary table), so we stop here exactly like a ROOT leaf —
    # walking its lineage would re-derive it from base instead.
    if is_materialized_root:
        return

    # Rowset boundary: a ROWSET concept is the outer's "handle" on a
    # sub-query. From the outer graph's perspective it's a leaf — the
    # actual lineage lives inside the rowset's inner select, which we
    # walk separately under `label=rowset.name`. Stopping here is what
    # prevents the outer BASIC group (e.g. q05's `local.sales`) from
    # absorbing the rowset's internal BASIC computations (q05's
    # `q5_results.sales_metric`) and forming a group-level cycle.
    if concept.derivation == Derivation.ROWSET:
        return

    # Multiple distinct derivable origins: emit each as a mutually-exclusive
    # ALTERNATIVE parent of this bare-key hub (`local.a` reachable via `uA.a` OR
    # `uB.a`). `resolve_alternatives` — run before the group graph — keeps the
    # cheapest satisfiable arm and contracts the hub away, so every downstream
    # pass sees a single ordinary lineage parent.
    if use_hub:
        for origin in alternatives:
            _add_concept(
                origin,
                environment,
                graph,
                edges,
                attrs,
                label,
                materialized_roots,
                datasource_addresses,
                pinned_probes,
            )
            origin_nid = node_id(
                _effective_label(origin, label, root_like), origin.address
            )
            add_edge(
                graph,
                edges,
                origin_nid,
                nid,
                EdgeKind.LINEAGE,
                alt_group=concept.address,
            )
        return

    # Per-derivation upstream fetcher (see `_UPSTREAM`): everything the
    # fetcher returns is a real lineage dependency — the concept's input
    # CTE has to contain it for this node to render correctly. An
    # aggregate's grain keys aren't optional metadata; they're what keeps
    # row identity intact through the SUM. Same story for window
    # partition keys and filter property keys. So every fetcher result
    # gets a lineage edge, not just `concept_arguments`.
    fetcher = _UPSTREAM.get(concept.derivation, _upstream_default)
    upstreams = list(fetcher(concept, environment))
    # A BASIC whose grain is the coalesced axis of a rowset-crossing preserving
    # relation reads the COMPLETED axis row: a null-sensitive scalar
    # (`coalesce(web.qty, 0) + ...`) computed on only the sides it reads gets
    # NULL-padded by the merge above instead of evaluating on the padded row
    # (multi_partial_anchor: store-only customers came back NULL, not 0). Wire
    # the axis member itself as an upstream so the axis-owning boundary parents
    # this group and the completion merge sits below the computation.
    #
    # A pure rename needs the same upstream for a different reason: it projects
    # to its alias alone, so the axis its source binds never reaches the FINAL
    # merge and that join goes keyless (`select rs.k, dim.attr as a subset join
    # rs.k = dim.key`).
    if (
        not is_materialized_root
        and concept.derivation == Derivation.BASIC
        and environment.scoped_join_key_groups
    ):
        upstream_addrs = {u.address for u in upstreams}
        scoped = _statement_scoped_relation_members(environment)
        for addr in sorted(out_grain & scoped):
            axis = environment.concepts.get(addr)
            if (
                axis is not None
                and addr not in upstream_addrs
                and _relation_crosses_rowset_boundary(addr, environment)
            ):
                upstreams.append(axis)
    for upstream in upstreams:
        # Substitute here too so the edge wires to the origin's node (the
        # recursive call below adds the origin, not the bare key) — otherwise
        # the bare key gets an implicit graph node with no attrs entry. A
        # genuine multi-alternative upstream is left as the bare key: its
        # recursion builds the hub, and the edge below wires to that hub.
        if len(_alternative_origins(upstream, environment, datasource_addresses)) < 2:
            upstream = (
                _resolve_pseudonym_origin(upstream, environment, datasource_addresses)
                or upstream
            )
        _add_concept(
            upstream,
            environment,
            graph,
            edges,
            attrs,
            label,
            materialized_roots,
            datasource_addresses,
            pinned_probes,
        )
        upstream_label = _effective_label(upstream, label, root_like)
        add_edge(
            graph,
            edges,
            node_id(upstream_label, upstream.address),
            nid,
            EdgeKind.LINEAGE,
        )


# ---------------------------------------------------------------------------
# Alternative (pseudonym-hub) resolution
#
# A bare key with ≥2 distinct derivable origins is added as a HUB with one
# ALTERNATIVE-tagged lineage edge per arm (`_add_concept`). These functions
# collapse every hub to a single arm BEFORE the group graph is built, so the
# AND-only downstream never sees an OR. Selection is cost-aware (reuse scans
# already in the query) and deterministic, and correlated hubs converge on a
# shared source because each pick folds its scan footprint into `committed`.
# ---------------------------------------------------------------------------


def _is_datasource_node(
    attrs: dict[str, ConceptAttrs], nid: str, datasource_addresses: frozenset[str]
) -> bool:
    a = attrs.get(nid)
    return (
        a is not None
        and a.derivation == Derivation.ROOT
        and a.address in datasource_addresses
    )


def _lineage_ancestors(
    graph: nx.DiGraph, edges: EdgeMap, node: str, *, follow_alt: bool = True
) -> set[str]:
    """Ancestors of `node` reachable purely through LINEAGE edges (optionally
    excluding ALTERNATIVE-tagged ones, to walk only the committed backbone)."""
    seen: set[str] = set()
    stack = [node]
    while stack:
        n = stack.pop()
        for p in graph.predecessors(n):
            if p in seen:
                continue
            ea = edges.get((p, n))
            if ea is None or ea.kind != EdgeKind.LINEAGE:
                continue
            if not follow_alt and ea.alt_group is not None:
                continue
            seen.add(p)
            stack.append(p)
    return seen


def _datasource_footprint(
    graph: nx.DiGraph,
    edges: EdgeMap,
    attrs: dict[str, ConceptAttrs],
    node: str,
    datasource_addresses: frozenset[str],
) -> set[str]:
    """The datasource scans an arm pulls in — its datasource-bound ROOT lineage
    ancestors (inclusive). This is read off the already-built graph, not a
    re-derivation of source-resolution logic."""
    return {
        n
        for n in (_lineage_ancestors(graph, edges, node) | {node})
        if _is_datasource_node(attrs, n, datasource_addresses)
    }


def _remove_node(graph: nx.DiGraph, edges: EdgeMap, n: str) -> None:
    for p in list(graph.predecessors(n)):
        edges.pop((p, n), None)
    for s in list(graph.successors(n)):
        edges.pop((n, s), None)
    graph.remove_node(n)


def _backbone_datasource_nodes(
    graph: nx.DiGraph,
    edges: EdgeMap,
    attrs: dict[str, ConceptAttrs],
    datasource_addresses: frozenset[str],
    sink_ids: set[str],
) -> set[str]:
    """Datasource scans the query already performs along NON-alternative lineage
    — the context an arm's cost is measured against. An arm reusing one of these
    adds no new scan."""
    roots: set[str] = set()
    for sink in sink_ids:
        if sink not in graph:
            continue
        for anc in _lineage_ancestors(graph, edges, sink, follow_alt=False) | {sink}:
            if _is_datasource_node(attrs, anc, datasource_addresses):
                roots.add(anc)
    return roots


def _pick_alternative(
    alts: list[str],
    graph: nx.DiGraph,
    edges: EdgeMap,
    attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    datasource_addresses: frozenset[str],
    committed: set[str],
) -> str:
    """Score each arm and keep the best: satisfiable first, then maximal reuse of
    already-committed scans, then fewest new scans, then shallowest, then lowest
    address (deterministic). `max` returns the first of equal-key elements, so
    iterating address-sorted makes ties resolve to the lowest address."""

    def key(nid: str) -> tuple[int, int, int, int]:
        origin = environment.alias_origin_lookup.get(attrs[nid].address)
        satisfiable = origin is not None and concept_satisfiable(
            origin, set(datasource_addresses)
        )
        fp = _datasource_footprint(graph, edges, attrs, nid, datasource_addresses)
        return (
            1 if satisfiable else 0,
            len(fp & committed),
            -len(fp - committed),
            -len(_lineage_ancestors(graph, edges, nid)),
        )

    return max(sorted(alts), key=key)


def _contract_hub(
    graph: nx.DiGraph, edges: EdgeMap, hub: str, winner: str, sinks: set[str]
) -> None:
    """Redirect the hub's successors onto the chosen arm, then drop the hub. A
    hub that was itself a sink hands its sink role to the winner."""
    for succ in list(graph.successors(hub)):
        ea = edges.get((hub, succ))
        kind = ea.kind if ea is not None else EdgeKind.LINEAGE
        if succ != winner and not graph.has_edge(winner, succ):
            add_edge(graph, edges, winner, succ, kind)
    hub_is_sink = hub in sinks
    _remove_node(graph, edges, hub)
    if hub_is_sink:
        sinks.discard(hub)
        sinks.add(winner)


def _prune_orphan_branch(
    graph: nx.DiGraph, edges: EdgeMap, start: str, sinks: set[str]
) -> None:
    """Drop a losing arm: the start node and any lineage ancestor that, having
    lost its only consumer, now feeds nothing. Cascades up but stops at nodes
    still shared with a surviving arm (they keep another successor) and never
    removes a sink."""
    if start not in graph:
        return
    candidates = {start} | _lineage_ancestors(graph, edges, start)
    changed = True
    while changed:
        changed = False
        for n in list(candidates):
            if n not in graph or n in sinks:
                continue
            if graph.out_degree(n) == 0:
                _remove_node(graph, edges, n)
                candidates.discard(n)
                changed = True


def resolve_alternatives(
    graph: nx.DiGraph,
    edges: EdgeMap,
    attrs: dict[str, ConceptAttrs],
    environment: BuildEnvironment,
    datasource_addresses: frozenset[str],
    sink_ids: set[str],
) -> None:
    """Collapse every pseudonym hub to its cheapest satisfiable arm.

    Hubs are processed in a stable order; each pick folds its scan footprint into
    `committed`, so correlated hubs (two struct fields drawn from the same pair
    of arrays) converge on one array rather than scanning both. Afterward no
    ALTERNATIVE-tagged edge remains and the graph is AND-only again."""
    hubs = sorted({v for (_, v), a in edges.items() if a.alt_group is not None})
    if not hubs:
        return
    sinks = set(sink_ids)
    committed = _backbone_datasource_nodes(
        graph, edges, attrs, datasource_addresses, sinks
    )
    for hub in hubs:
        if hub not in graph:
            continue
        alts = sorted(
            p
            for p in graph.predecessors(hub)
            if (ea := edges.get((p, hub))) is not None and ea.alt_group is not None
        )
        if not alts:
            continue
        winner = _pick_alternative(
            alts, graph, edges, attrs, environment, datasource_addresses, committed
        )
        committed |= _datasource_footprint(
            graph, edges, attrs, winner, datasource_addresses
        )
        _contract_hub(graph, edges, hub, winner, sinks)
        for loser in alts:
            if loser != winner:
                _prune_orphan_branch(graph, edges, loser, sinks)


def _staged_condition_labels(
    conditions: list[BuildWhereClause],
    staged_conditions: list[BuildWhereClause] | None,
) -> dict[str, str]:
    """Map cross-row condition-arg addresses to stage-qualified condition
    labels, when this search's conditions span 2+ cross-row-hosting stages.

    The first cross-row-hosting stage present keeps the plain condition label
    — its graph is bit-identical to the unstaged one — and each later
    cross-row-hosting stage plans under its own label. Presence is judged
    against THIS search's conditions, not the statement's stage list: a
    sub-search re-sourcing one stage's gate carries only the earlier stages'
    atoms, and its single population needs no split.

    The map is keyed by ADDRESS, so it relies on one computation never gating
    two stages — `_validate_staged_where` (parsing/v2/select_finalize.py) owns
    that invariant; without it the later stage's label would silently answer
    for the earlier stage's gate too."""
    if not staged_conditions or len(staged_conditions) < 2:
        return {}
    present = {arg.address for clause in conditions for arg in clause.row_arguments}
    stage_args: list[tuple[int, list[BuildConcept]]] = []
    for index, clause in enumerate(staged_conditions):
        args = [a for a in cross_row_stage_args(clause) if a.address in present]
        if args:
            stage_args.append((index, args))
    if len(stage_args) < 2:
        return {}
    labels: dict[str, str] = {}
    for index, args in stage_args[1:]:
        for arg in args:
            labels[arg.address] = stage_condition_label("", index)
    return labels


def build_concept_graph(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
    materialized_roots: frozenset[str] = frozenset(),
    staged_conditions: list[BuildWhereClause] | None = None,
) -> tuple[nx.DiGraph, dict[str, ConceptAttrs], EdgeMap]:
    """Build the concept-level DAG. Constraint edges (d1→d0) record the
    invariant that filter inputs must be available above any row-shape barrier
    that consumes their filtered output.

    Rowset handling: a ROWSET concept in the outer mandatory list is
    walked as a leaf (no lineage edges) by `_add_concept`, and after the
    outer walk completes we discover every ROWSET node and build its
    inner sub-graph under `label=rowset.name`. The labeled sub-graph's
    nodes use keys like ``"[q5_results]local.channel_label"`` so they
    can't collide with an outer-namespace copy of the same address. This
    is what keeps the outer query's BASIC groups independent of any
    rowset's internal BASICs (which would otherwise get bucketed
    together by `partition_basics_by_subset_grain` and form a group-
    level cycle through the rowset)."""
    graph: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    attrs: dict[str, ConceptAttrs] = {}
    datasource_addresses = frozenset(
        c.address for ds in environment.datasources.values() for c in ds.output_concepts
    )
    pinned_probes = pinned_probe_addresses(environment)
    root_like = materialized_roots | pinned_probes
    # Outer SELECT: blank-phase label "".
    for concept in mandatory_list:
        _add_concept(
            concept,
            environment,
            graph,
            edges,
            attrs,
            materialized_roots=materialized_roots,
            datasource_addresses=datasource_addresses,
            pinned_probes=pinned_probes,
        )
    # Outer WHERE: condition-phase label "@condition". The same concept that
    # also appears in the SELECT gets a separate node here, so we never
    # have to retro-promote depth labels. A later `then where` stage's
    # cross-row computation walks under a stage-qualified condition label
    # instead: its input population differs per stage, so its lineage subtree
    # (and its root_d1 feeder, downstream) must not be shared across stages.
    staged_labels = _staged_condition_labels(conditions, staged_conditions)
    for clause in conditions:
        for concept in clause.concept_arguments:
            resolved = environment.concepts.get(concept.address, concept) or concept
            _add_concept(
                resolved,
                environment,
                graph,
                edges,
                attrs,
                label=staged_labels.get(resolved.address, _condition_label("")),
                materialized_roots=materialized_roots,
                datasource_addresses=datasource_addresses,
                pinned_probes=pinned_probes,
            )

    # Unreferenced rowset-handle key-group mates of demanded members (see
    # `_unsourced_relation_mates`): without a node here the mate's rowset never
    # enters the plan and the relation axis collapses to one side's domain.
    for mate in _unsourced_relation_mates(mandatory_list, conditions, environment):
        _add_concept(
            mate,
            environment,
            graph,
            edges,
            attrs,
            materialized_roots=materialized_roots,
            datasource_addresses=datasource_addresses,
            pinned_probes=pinned_probes,
        )

    # A statement-scoped join key authored as a computed expression (`union
    # join rank orders.oid order by orders.amt desc = customers.rnk`) is
    # canonicalized onto the relation's other side by the build-scope merge
    # collapse, leaving the member address a bare redirect whose computed
    # lineage survives only in `alias_origin_lookup`. The axis is an EQUALITY
    # between two different lineages, not two spellings of one lineage — so
    # the collapsed side's computation is still load-bearing: without it the
    # relation has no computable member on its side and the plan degrades to
    # an axis-less cross join. Re-inject every collapsed member's origin as a
    # first-class node; the relation itself (scoped_join_key_groups) supplies
    # the equivalence downstream. Rowset members are untouched — they are
    # carved out of the collapse and their computation lives behind the
    # rowset boundary already.
    #
    # GLOBAL merge members with a computed origin ride the same rail: `merge
    # recursive_parent into root_parent.id` is likewise an equality between two
    # different lineages (the collapsed side's RECURSIVE computation IS the
    # join), so its origin must be re-injected too or the plan degrades to the
    # same cross join (hackernews adhoc03). Gated on the relation being
    # DEMANDED (a member is an output or condition arg): a query that merely
    # filters on a side's property (`where parent.label = 'A' select
    # count(id)`) is served by the condition-feeder fallback joining below
    # the consumer, and restructuring its graph into relation-partitioned
    # sides breaks that path (recursive-enrichment over-count). Statement
    # relations stay unconditional — declared in the query, always in play.
    if environment.scoped_join_key_groups:
        demanded_addresses = {c.address for c in mandatory_list} | {
            c.address for clause in conditions for c in clause.concept_arguments
        }
        relation_members = set(_statement_scoped_relation_members(environment))
        computed_members = computed_origin_relation_members(environment)
        for canonical_addr, group in environment.scoped_join_key_groups.items():
            relation = {canonical_addr, *group}
            if relation & computed_members and relation & demanded_addresses:
                relation_members |= relation
        for member in sorted(relation_members):
            if member in datasource_addresses:
                continue
            resolved_member = environment.concepts.get(member)
            if resolved_member is None or resolved_member.address == member:
                continue
            origin = environment.alias_origin_lookup.get(member)
            if origin is None or origin.lineage is None:
                continue
            # A collapsed ROWSET handle (a composite leg paired through the
            # anonymous canonical) is not re-injected: its computation lives
            # behind its rowset boundary, and demanding the handle here
            # re-enters the rowset's own build with the scoped join still in
            # scope (the self-weld recursion, join_matrix composite subset).
            # Same for a computed key OVER rowset handles (`fut.period + 2 =
            # agg.period + 1`): walking its lineage re-enters the rowsets one
            # level down (scoped_derived_rowset deriv_both cells), and the
            # rowset-boundary + inline-render machinery already own those.
            if isinstance(origin.lineage, BuildRowsetItem) or any(
                isinstance(source.lineage, BuildRowsetItem) for source in origin.sources
            ):
                continue
            canonical = resolved_member
            _add_concept(
                origin,
                environment,
                graph,
                edges,
                attrs,
                materialized_roots=materialized_roots,
                datasource_addresses=datasource_addresses,
                pinned_probes=pinned_probes,
            )
            _add_concept(
                canonical,
                environment,
                graph,
                edges,
                attrs,
                materialized_roots=materialized_roots,
                datasource_addresses=datasource_addresses,
                pinned_probes=pinned_probes,
            )
            origin_nid = node_id(
                _effective_label(origin, "", root_like), origin.address
            )
            canonical_nid = node_id(
                _effective_label(canonical, "", root_like), canonical.address
            )
            if (
                origin_nid in graph
                and canonical_nid in graph
                and origin_nid != canonical_nid
                and not graph.has_edge(origin_nid, canonical_nid)
            ):
                add_edge(graph, edges, origin_nid, canonical_nid, EdgeKind.RELATION)

    # A ROWSET concept stays a leaf in the outer graph (see `_add_concept`):
    # its inner select is a self-contained sub-query that the native
    # `gen_rowset` generator plans recursively through v4's own
    # `search_concepts`, so the inner
    # lineage never enters the outer concept/group graph. Walking it in here
    # only ever produced a partial picture — it captured the inner outputs and
    # WHERE but not the inner HAVING or multiselect arms — so the boundary
    # node is built from the recursively-planned inner instead.

    # Filter-nested existence: a semijoin inside a derived FILTER concept
    # (q08 `final_zips <- substring(zips ? zips in substring(p_cust_zip,1,5),
    # 1, 2)`) needs its existence source built as a side-channel subselect, not
    # merged into the filter's row stream. `_upstream_filter` already dropped
    # the existence-only args from the filter's lineage; here we walk each
    # source under the filter node's label and wire an `existence` edge to the
    # filter so it lands in its own group and renders as `... IN (SELECT src
    # FROM <cte>)`.
    for nid in list(graph.nodes):
        fconcept = environment.concepts.get(attrs[nid].address)
        if fconcept is None:
            continue
        existence_only = lineage_existence_only(fconcept)
        if not existence_only:
            continue
        flabel = attrs[nid].label
        for addr in existence_only:
            source = environment.concepts.get(addr)
            if source is None:
                continue
            _add_concept(source, environment, graph, edges, attrs, label=flabel)
            src_nid = node_id(_effective_label(source, flabel), source.address)
            if src_nid in graph and src_nid != nid and not graph.has_edge(src_nid, nid):
                add_edge(graph, edges, src_nid, nid, EdgeKind.EXISTENCE)

    # Collapse pseudonym hubs to a single arm now — after every `_add_concept`
    # call (so all hubs exist) but before the constraint/existence EDGE passes
    # below, which must see the winning origin node exactly as substitution would
    # have left it. Sinks anchor hub-sink remapping and protect demanded nodes
    # from the losing-arm prune.
    sink_ids: set[str] = set()
    for c in mandatory_list:
        sid = node_id(_effective_label(c, "", root_like), c.address)
        if sid in graph:
            sink_ids.add(sid)
    for clause in conditions:
        for cc in clause.concept_arguments:
            resolved = environment.concepts.get(cc.address, cc) or cc
            sid = node_id(
                _effective_label(
                    resolved,
                    staged_labels.get(resolved.address, _condition_label("")),
                    root_like,
                ),
                resolved.address,
            )
            if sid in graph:
                sink_ids.add(sid)
    resolve_alternatives(
        graph, edges, attrs, environment, datasource_addresses, sink_ids
    )

    # Classify how each atom uses its concept arguments. A row-argument
    # gets joined into the consumer's row stream; an existence-argument
    # is consumed via a side-channel subselect (IN/EXISTS) and must never
    # be modeled as JOIN partner. We collect both sets so the constraint
    # and existence edge passes below can flow strictly along the right
    # channel for each address.
    row_arg_addresses: set[str] = set()
    existence_arg_addresses: set[str] = set()
    existence_arg_pairs: list[tuple[str, str]] = []  # (existence_addr, row_addr)
    for clause in conditions:
        for atom in decompose_condition(clause.conditional):
            atom_row_addrs = [c.address for c in atom.row_arguments]
            for c in atom.row_arguments:
                row_arg_addresses.add(c.address)
            for arg_group in atom.existence_arguments or ():
                for ec in arg_group:
                    existence_arg_addresses.add(ec.address)
                    for row_addr in atom_row_addrs:
                        existence_arg_pairs.append((ec.address, row_addr))
    # Tag nodes that appear only as existence args (not as row args anywhere)
    # so partition_roots can place them in their own scan buckets — they're
    # side-channel subselect sources, not part of the main row stream
    # (q16: `cr.order_number` from `cs.order_number not in cr.order_number`).
    existence_only_addresses = existence_arg_addresses - row_arg_addresses
    for n in graph.nodes:
        if attrs[n].address in existence_only_addresses:
            attrs[n].existence_only = True

    # Lineage-level existence sources: the filter-nested pass above wired an
    # EXISTENCE edge from a derived membership's RHS (`auto f <- a in b` — b)
    # to its consumer, but b appears in no statement-WHERE atom, so the
    # address sweep can't see it. The graph itself carries the classification:
    # an address consumed ONLY through EXISTENCE edges and demanded by no sink
    # feeds side-channel subselects exclusively.
    feeds_existence: set[str] = set()
    row_demanded: set[str] = set(row_arg_addresses)
    for n in graph.nodes:
        addr = attrs[n].address
        if n in sink_ids:
            row_demanded.add(addr)
        for succ in graph.successors(n):
            if edge_kind(edges, n, succ) == EdgeKind.EXISTENCE:
                feeds_existence.add(addr)
            else:
                row_demanded.add(addr)
    for n in graph.nodes:
        if attrs[n].address in feeds_existence - row_demanded:
            attrs[n].existence_only = True

    # Group nodes by scope-and-phase. Condition-phase nodes are d1 by
    # construction; the only d0 candidates live in the matching blank-phase
    # scope. Constraint edges flow strictly from condition-phase ROW-ARG
    # nodes to blank-phase row-shape barriers in the same scope — these
    # are the only d1s that will JOIN into the d0's row stream. A condition
    # concept that only ever appears as an existence-arg gets an explicit
    # `existence` edge instead (below), so the dataflow distinction is
    # carried in the graph rather than recovered later via heuristics.
    # `then where` stage qualifiers collapse into the one condition phase here
    # on purpose: the constraint is "filter inputs sit above the barrier",
    # which every stage's gate owes the barrier alike.
    nodes_by_scope_phase: dict[tuple[str, str], list[str]] = {}
    for n in graph.nodes:
        scope, phase = _scope_and_phase(attrs[n].label)
        nodes_by_scope_phase.setdefault((scope, phase), []).append(n)
    scopes_present = {scope for scope, _ in nodes_by_scope_phase}
    for scope in scopes_present:
        condition_nodes = nodes_by_scope_phase.get((scope, "condition"), [])
        d0_blank_nodes = [
            n
            for n in nodes_by_scope_phase.get((scope, "blank"), [])
            if attrs[n].depth_label == DepthLabel.D0
        ]
        for src in condition_nodes:
            src_address = attrs[src].address
            if src_address not in row_arg_addresses:
                continue
            # A condition concept derived from a ROWSET (e.g. a WINDOW `eldest`
            # computed over a rowset, then filtered `eldest = 1`) sits above that
            # rowset already — its value can't exist until the rowset's rows do.
            # A rowset is one indivisible group, so constraining the condition
            # back onto ANY of the rowset's handles forms a cycle (rowset→window
            # lineage, window→rowset constraint). Skip those: deriving from one
            # handle means `src` is above the whole rowset, including the handles
            # it doesn't read directly (the window reads `id`/`last_name`/`age`
            # but its filter sits above `name`/`survived` from the same rowset).
            src_lineage_ancestor_rowsets = {
                attrs[a].rowset_name
                for a in nx.ancestors(graph, src)
                if attrs[a].rowset_name
            }
            for dst in d0_blank_nodes:
                if (
                    attrs[dst].rowset_name
                    and attrs[dst].rowset_name in src_lineage_ancestor_rowsets
                ):
                    continue
                # A rowset-scoped condition value is computed inside ITS own
                # rowset's boundary and its test only applies above the
                # completion merge (FINAL). Constraining it onto ANOTHER
                # rowset's boundary would force that independent scope to consume
                # a value it cannot see, polluting its output contract (the
                # b-side boundary "output" the a-side's value and lost its own
                # handles). Two forms: a rowset-member presence probe, and a
                # plain rowset handle used in a post-merge filter (`where a.amt
                # is not null and b.amt is not null` over two independent rowsets
                # — each null test lands at FINAL, never inside the sibling's
                # scan; a mutual constraint would 2-cycle the two rowset groups).
                if (
                    attrs[src].rowset_name
                    and attrs[src].rowset_name != attrs[dst].rowset_name
                    and attrs[dst].derivation == Derivation.ROWSET
                ):
                    continue
                # A lineage edge already present src→dst is left as-is; the
                # constraint ordering it would carry is implied by the lineage.
                if not graph.has_edge(src, dst):
                    add_edge(graph, edges, src, dst, EdgeKind.CONSTRAINT)

    # Existence edges: for each `... IN <subselect>` atom, the existence
    # source must be built and topologically ordered before the host
    # consumer, but its rows never JOIN into the host's row stream — the
    # renderer reads them via a subselect against the source CTE. Mark
    # this with a distinct edge kind so downstream passes (group-edge
    # propagation, JOIN-key projection, strategy parent selection) can
    # treat existence siblings as side-channel, not JOIN partners.
    for existence_addr, row_addr in existence_arg_pairs:
        existence_nid = node_id(_condition_label(""), existence_addr)
        # The row arg may be in either the blank or @condition phase
        # depending on whether it's also a SELECT output — or in a
        # stage-qualified condition phase when it's a later stage's cross-row
        # computation. Connect to whichever exists; the atom's host bucket
        # consumes from there.
        row_candidates = ["", _condition_label("")]
        if row_addr in staged_labels:
            row_candidates.append(staged_labels[row_addr])
        for candidate_label in row_candidates:
            row_nid = node_id(candidate_label, row_addr)
            if (
                existence_nid in graph
                and row_nid in graph
                and existence_nid != row_nid
                and not graph.has_edge(existence_nid, row_nid)
            ):
                add_edge(graph, edges, existence_nid, row_nid, EdgeKind.EXISTENCE)

    # Backfill: if a condition-phase node has no successor (no d0 barrier
    # consumed it), wire a constraint from it to the matching blank-phase
    # mandatory outputs so the condition has somewhere to land. q04's
    # `store_first_year > 0` over customer-grain rows is the motivating
    # case — purely scalar SELECT, no d0 to absorb the WHERE.
    #
    # Limits, mirroring the original logic:
    #   - skip ROOT-derivation condition nodes (those represent in-scan
    #     attributes that the mandatory walk already produces);
    #   - require the node's address to actually appear as a row argument
    #     (existence args don't need row-stream consumers);
    #   - skip nodes that already have any outgoing edge.
    mandatory_blank_ids = {node_id("", c.address) for c in mandatory_list}
    outer_condition_nodes = nodes_by_scope_phase.get(("", "condition"), [])
    for src in outer_condition_nodes:
        if attrs[src].derivation == Derivation.ROOT:
            continue
        src_address = attrs[src].address
        if src_address not in row_arg_addresses:
            continue
        if any(True for _ in graph.successors(src)):
            continue
        # Same rowset cycle guard as the main constraint pass above: a
        # condition concept DERIVED from a rowset (a bare aggregate over a
        # rowset handle, co-grained to the select grain) already sits above
        # that rowset; constraining it back onto a mandatory output owned by
        # the same rowset forms a rowset→condition→rowset cycle that kills
        # the topological concept-set pass.
        src_lineage_ancestor_rowsets = {
            attrs[a].rowset_name
            for a in nx.ancestors(graph, src)
            if attrs[a].rowset_name
        }
        for dst in mandatory_blank_ids:
            if dst not in graph.nodes or graph.has_edge(src, dst):
                continue
            if (
                attrs[dst].rowset_name
                and attrs[dst].rowset_name in src_lineage_ancestor_rowsets
            ):
                continue
            # Same cross-rowset guard as the main constraint pass: a
            # rowset-scoped value's test lands at FINAL, never inside a sibling
            # rowset's independent scope.
            if (
                attrs[src].rowset_name
                and attrs[src].rowset_name != attrs[dst].rowset_name
                and attrs[dst].derivation == Derivation.ROWSET
            ):
                continue
            add_edge(graph, edges, src, dst, EdgeKind.CONSTRAINT)

    # Stamp ROOT-leaf nodes with the datasources binding them, so
    # `partition_roots` can relate two roots that share a physical row stream
    # even when the demanded lineage graph never connects them (a fact FK
    # column beside a fact property, each consumed only by its own rename).
    binding_map: dict[str, set[str]] = defaultdict(set)
    for ds in environment.datasources.values():
        for out in ds.output_concepts:
            binding_map[out.address].add(ds.identifier)
    for n in graph.nodes:
        node_attrs = attrs[n]
        bound_concept = environment.concepts.get(node_attrs.address)
        if bound_concept is not None and bound_concept.lineage is not None:
            continue
        bindings = set(binding_map.get(node_attrs.address, set()))
        for pseudonym in node_attrs.pseudonyms:
            bindings |= binding_map.get(pseudonym, set())
        if bindings:
            node_attrs.datasource_bindings = frozenset(bindings)
    return graph, attrs, edges
