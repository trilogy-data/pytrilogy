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

This mirrors the per-derivation `resolve_*_parent_concepts` helpers used
by the v3 generators (`gen_group_node`, `gen_filter_node`,
`gen_window_node`, `gen_subselect_node`).
"""

from typing import Callable

from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_CONCEPT
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
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.node_generators.common import (
    _walk_aggregate_grain_inputs,
)
from trilogy.core.processing.node_generators.presence_probe import (
    is_presence_probe,
    member_binding_datasources,
    probe_member_address,
)

from .constants import (
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import EdgeMap, add_edge
from .functional_dependency import minimize_build_grain
from .models import ConceptAttrs
from .projection import concept_satisfiable

UpstreamFetcher = Callable[[BuildConcept, BuildEnvironment], list[BuildConcept]]


# Phase suffix appended to a label when a concept is reached via the WHERE
# recursion. The same concept can appear once in the SELECT (blank) sub-graph
# and once in the WHERE (condition) sub-graph; the suffix is what keeps them
# distinct so we don't try to promote/demote a single shared node.
PHASE_CONDITION_SUFFIX = "@condition"


def _scope_and_phase(label: str) -> tuple[str, str]:
    """Split a label into its (scope, phase) parts. scope is "" for the outer
    query and the rowset name for rowset internals; phase is "blank" or
    "condition"."""
    if label.endswith(PHASE_CONDITION_SUFFIX):
        return label[: -len(PHASE_CONDITION_SUFFIX)], "condition"
    return label, "blank"


def _condition_label(scope_label: str) -> str:
    """Build the condition-phase label from a blank-phase label."""
    return f"{scope_label}{PHASE_CONDITION_SUFFIX}"


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
    own scan — mirroring v3's `gen_presence_probe_node`. Rowset-member probes
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
    existence_only = _lineage_existence_only(concept)
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


def _walk_scoped_aggregate_grain_inputs(
    aggregate: BuildConcept, concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """`_walk_aggregate_grain_inputs`, with a rowset handle's grain keys
    redirected to its own rowset members (see `_rowset_local_grain_identity`)."""
    inputs = _walk_aggregate_grain_inputs(concept, environment)
    lineage = concept.lineage
    if not inputs or not isinstance(lineage, BuildRowsetItem):
        return inputs
    if not environment.scoped_join_key_groups:
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
    # it instead of cross-joining ON 1=1 (v3 strips it in `gen_group_node`).
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


def _lineage_existence_only(concept: BuildConcept) -> set[str]:
    """Addresses that appear ONLY as existence args in the concept's lineage (a
    semijoin RHS like `zips in substring(p_cust_zip,1,5)`). These feed a
    side-channel subselect, not the concept's row stream. Two shapes: a FILTER's
    where, and a membership comparison authored as a SELECT output
    (`(20, 1) in (pairs.val, pairs.cat) as present`) whose lineage IS (or
    propagates from) the SubselectComparison."""
    args: BuildConceptArgs
    if isinstance(concept.lineage, BuildFilterItem):
        args = concept.lineage.where
    elif isinstance(concept.lineage, BuildConceptArgs):
        args = concept.lineage
    else:
        return set()
    existence = {ec.address for grp in (args.existence_arguments or []) for ec in grp}
    return existence - {r.address for r in args.row_arguments}


def _upstream_filter(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """FILTER: lineage args plus property keys of the filtered concept
    (matches `resolve_filter_parent_concepts`). A filter over a property
    needs the property's keys to keep the row stream identifiable.

    Existence-only args (semijoin RHS) are dropped from the row lineage — they
    get a side-channel `existence` edge instead (see `build_concept_graph`)."""
    existence_only = _lineage_existence_only(concept)
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
    (q36/q59). Mirrors v3's `resolve_window_parent_concepts`, but walks
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
            elif sub.grain:
                input_grain.update(sub.grain.components)
    return minimize_build_grain(environment, input_grain)


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
    v3's synonym node swaps the bare key for that attr-access origin; mirror it
    here so the graph walks attr_access -> unnest -> datasource instead of
    dead-ending on a ROOT leaf with no source.

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
    elif is_presence_probe(concept.address):
        # A presence probe over a ROWSET member has no datasource to pin to —
        # its value must be computed INSIDE the member's rowset boundary,
        # pre-merge (post-merge the member reads as the fused group coalesce,
        # never NULL). Tag it with the member's rowset so the rowset grouping
        # rule buckets it into that boundary and `resolve_rowset` discharges it
        # as an obligation output (v3's `_local_exposure_obligations`).
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
    # Under a STATEMENT-scoped preserving join (`union join ticket =
    # r_filtered.r_ticket`), row identity is the coalesced relation axis: an
    # aggregate whose inputs ride the relation computes per axis row, not per
    # its authored dimension grain (v3 renders it at the joined relation's
    # grain via the grain-match formulas; the outer select then dedups).
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
        and aggregate_input_grain
        and dimension_grain
    ):
        axis_members = aggregate_input_grain & _statement_scoped_relation_members(
            environment
        )
        if axis_members:
            out_grain |= axis_members
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
    for upstream in fetcher(concept, environment):
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


def build_concept_graph(
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
    materialized_roots: frozenset[str] = frozenset(),
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
    # have to retro-promote depth labels.
    for clause in conditions:
        for concept in clause.concept_arguments:
            resolved = environment.concepts.get(concept.address, concept) or concept
            _add_concept(
                resolved,
                environment,
                graph,
                edges,
                attrs,
                label=_condition_label(""),
                materialized_roots=materialized_roots,
                datasource_addresses=datasource_addresses,
                pinned_probes=pinned_probes,
            )

    # A ROWSET concept stays a leaf in the outer graph (see `_add_concept`):
    # its inner select is a self-contained sub-query that the native
    # `gen_rowset` generator plans recursively through v4's own
    # `search_concepts` (mirroring v3's `get_query_node`), so the inner
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
        existence_only = _lineage_existence_only(fconcept)
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
                _effective_label(resolved, _condition_label(""), root_like),
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

    # Group nodes by scope-and-phase. Condition-phase nodes are d1 by
    # construction; the only d0 candidates live in the matching blank-phase
    # scope. Constraint edges flow strictly from condition-phase ROW-ARG
    # nodes to blank-phase row-shape barriers in the same scope — these
    # are the only d1s that will JOIN into the d0's row stream. A condition
    # concept that only ever appears as an existence-arg gets an explicit
    # `existence` edge instead (below), so the dataflow distinction is
    # carried in the graph rather than recovered later via heuristics.
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
        # depending on whether it's also a SELECT output. Connect to
        # whichever exists; the atom's host bucket consumes from there.
        for candidate_label in ("", _condition_label("")):
            row_nid = node_id(candidate_label, row_addr)
            if existence_nid in graph and row_nid in graph and existence_nid != row_nid:
                if not graph.has_edge(existence_nid, row_nid):
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
    return graph, attrs, edges
