"""The FINAL merge's join plan.

Which keys identify each contributor's rows, which keys a ROOT contributor
must carry to pair with its siblings, and which pairs of contributors pair on
which keys. Computed once over the elected cover from stage-2 facts (group
attrs and the group graph), never from built nodes: assembly widens, wraps and
joins to satisfy it instead of re-deriving it from whatever the built parents
happen to project. See `docs/join_axis_audit_2026_10.md`.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from itertools import combinations

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
    get_grouped_aggregate_wrapper,
)
from trilogy.core.models.build_environment import BuildEnvironment

from .concept_graph import _statement_scoped_relation_members
from .constants import FINAL_NODE_ID, GROUPING_DERIVATIONS
from .functional_dependency import build_fd_closure, build_fd_determines_all
from .models import GroupAttrs


class PairingKind(Enum):
    # the keys identify one side's rows: the pair cannot multiply
    LOOKUP = "lookup"
    # an authored statement relation (`subset join a.k = b.k`) is the pairing
    AUTHORED = "authored"
    # one side is a single row: the legitimate cross product
    SCALAR = "scalar"
    # shared keys that identify neither side
    BLEND = "blend"


@dataclass(frozen=True)
class Pairing:
    left: str
    right: str
    keys: frozenset[str]
    kind: PairingKind
    # shared columns the keys determine: equating them too changes nothing,
    # so resolve may (a side's physical grain is never pruned)
    implied: frozenset[str] = frozenset()


@dataclass(frozen=True)
class JoinPlan:
    contributors: tuple[str, ...]
    # the keys each contributor fixes the merge's grain at (`row_identity`)
    pins: dict[str, frozenset[str]]
    # the keys each contributor's rows are unique at, as a pairing reads them
    identity: dict[str, frozenset[str]]
    # keys a ROOT contributor must be widened to carry
    carry_demands: dict[str, frozenset[str]]
    pairings: tuple[Pairing, ...]
    # contributors of one row, which pair with every row
    scalars: frozenset[str] = frozenset()

    @property
    def merge_grain(self) -> frozenset[str]:
        return frozenset().union(*self.pins.values())

    @property
    def join_keys(self) -> frozenset[str]:
        return frozenset().union(*(p.keys for p in self.pairings))

    @property
    def permitted_keys(self) -> frozenset[str]:
        """Everything resolve may equate: the keys and what they imply."""
        return self.join_keys | frozenset().union(*(p.implied for p in self.pairings))

    def islands(self) -> list[set[str]]:
        """Contributors grouped by what pairs them, single-row ones aside."""
        islands: list[set[str]] = [
            {gid} for gid in self.contributors if gid not in self.scalars
        ]
        for p in self.pairings:
            if p.kind == PairingKind.SCALAR:
                continue
            joined = [i for i in islands if p.left in i or p.right in i]
            if len(joined) > 1:
                for island in joined:
                    islands.remove(island)
                islands.append(set().union(*joined))
        return islands


def lineage_pinned_grain(
    addresses: Iterable[str], environment: BuildEnvironment
) -> frozenset[str]:
    """Grain components pinned by fixed-grain barriers in the lineage of
    ``addresses``. A BASIC that renames or derives from an aggregate
    (`cust_state_amt as total`) is a fixed-grain barrier exactly like the
    aggregate itself: its rows are unique at the aggregate's grain and cannot
    be regenerated finer. Same for a rename of a rowset member
    (`buyers_b.cust_id as b_cust`): its join axis is the handle the body value
    is keyed by (`buyers_b.id`). The walk stops at each barrier; grains BELOW
    it are pre-aggregation / body row grains, not join axes. A row-level ROOT
    reached BESIDE a barrier is part of the pin: a derivation over an
    aggregate and a row key (`coalesce(min(x) by cell, tree_id)`) varies per
    row, so its rows are unique at the row grain. With no barrier anywhere
    nothing is pinned: a plain BASIC over row columns regenerates at any
    grain. Members of an AUTHORED scoped-join relation are skipped: the
    authored keys are the axis there, and pairing on internal grain too would
    silently narrow the authored fan-out."""
    scoped_members = environment.all_scoped_join_group_members()
    stack: list[BuildConcept] = []
    for addr in addresses:
        concept = environment.concepts.get(addr) or environment.alias_origin_lookup.get(
            addr
        )
        if concept is not None:
            stack.append(concept)
    grain: set[str] = set()
    row_grain: set[str] = set()
    seen: set[str] = set()
    while stack:
        concept = stack.pop()
        if concept.address in seen:
            continue
        seen.add(concept.address)
        # A ROLLUP/CUBE/GROUPING SETS aggregate NULLs its grouping keys on the
        # subtotal rows, so those keys are no row identity: pinning them drops
        # every subtotal row from the merge (a rolled-up NULL never pairs).
        wrapper = get_grouped_aggregate_wrapper(concept)
        if wrapper is not None and wrapper.grouping.nulls_grouping_keys:
            continue
        if concept.derivation in GROUPING_DERIVATIONS:
            if concept.grain:
                grain |= set(concept.grain.components)
            continue
        if concept.derivation == Derivation.ROWSET:
            axis = set(concept.grain.components) if concept.grain else set()
            if not ({concept.address} | axis) & scoped_members:
                grain |= axis
            continue
        if concept.lineage is not None:
            stack.extend(concept.lineage.concept_arguments)
        elif concept.derivation == Derivation.ROOT and concept.grain:
            row_grain |= set(concept.grain.components)
    return frozenset(grain | row_grain) if grain else frozenset()


def authored_axes(
    cover: Sequence[str],
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    relation_edge_members: frozenset[str],
) -> frozenset[str]:
    """Members of an authored relation carried on the concept graph's RELATION
    edges and hosted by different contributors: the sides recombine only
    through the authored equality (`union join rank orders.oid order by
    orders.amt desc = customers.rnk`). An unhosted third member of a chained
    relation is not this merge's to demand."""
    if len(cover) < 2 or not relation_edge_members:
        return frozenset()
    out: set[str] = set()
    for relation in environment.scoped_join_relations():
        if not relation & relation_edge_members:
            continue
        hosts: set[str] = set()
        hosted: set[str] = set()
        for gid in cover:
            have = relation & set(attrs[gid].members)
            if have:
                hosts.add(gid)
                hosted |= have
        if len(hosts) > 1 and len(hosted) > 1:
            out |= hosted
    return frozenset(out)


def merge_axes(
    cover: Sequence[str],
    attrs: dict[str, GroupAttrs],
    mandatory_list: Sequence[BuildConcept],
    environment: BuildEnvironment,
    relation_edge_members: frozenset[str],
) -> frozenset[str]:
    """Every key some contributor's rows are identified by: a candidate join
    axis. A BASIC over aggregates (`customer_status <- case ... min(x) by
    user`) sits at user grain and emits that key; a region domain's rows are
    keyed by its spans. A grouping or rowset output's grain is the result's
    row grain (a rowset's `keys` are not: they are the FK path that determines
    it). Authored relations hosted on both sides are axes by declaration."""
    axes: set[str] = set()
    for gid in cover:
        axes |= attrs[gid].emitted_grain | attrs[gid].extent_spans
    for concept in mandatory_list:
        if (
            concept.derivation in GROUPING_DERIVATIONS
            or concept.derivation == Derivation.ROWSET
        ) and concept.grain:
            axes |= set(concept.grain.components)
    if environment.scoped_join_key_groups:
        axes |= authored_axes(cover, attrs, environment, relation_edge_members)
    return frozenset(axes)


def row_identity(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    cover: Sequence[str],
    axes: frozenset[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """The keys `gid`'s rows are unique at, as the FINAL merge pairs them.

    A grouping group's GROUP BY; a region domain's spans; a rowset boundary's
    the merge axes. A row-preserving group is identified by what its rows
    carry of: an authored statement relation (the merge's axis whether or not
    it is the group's grain: `subset join best.rank = worst.rank` projecting
    only the two names); the grain a fixed-grain barrier in its lineage pins
    (`cust_state_amt as total` rides its aggregate's grain); its own grain when
    its grouped siblings' grain does not determine it (`ret + 1` per line
    beside `sum(qty) by order` would collapse onto `order` and fan back out);
    and the spans of a region domain it reads (folded into it, the domain no
    longer advertises them itself). A pin's row lineage the rows never hold
    (`event_date <- date(event_time)` read off the barrier) is no identity."""
    group = attrs[gid]
    if group.derivation in GROUPING_DERIVATIONS:
        return group.grain_components
    if group.extent_spans:
        return group.extent_spans
    if group.derivation == Derivation.ROWSET:
        return axes
    available = group.row_columns
    keys = set(_statement_scoped_relation_members(environment) & axes & available)
    pinned = group.output_concepts or group.members
    keys |= lineage_pinned_grain(pinned, environment) & available
    grouped = frozenset().union(
        *(
            attrs[other].grain_components
            for other in cover
            if attrs[other].derivation in GROUPING_DERIVATIONS
        )
    )
    if (
        grouped
        and group.derivation == Derivation.BASIC
        and not build_fd_determines_all(environment, grouped, group.primary_members)
    ):
        keys |= group.emitted_grain & axes
    held = frozenset().union(
        *(
            attrs[parent].extent_spans
            for parent in group_graph.predecessors(gid)
            if parent in attrs
        )
    )
    return frozenset(keys) | (held & available)


def _fd_determined(
    environment: BuildEnvironment, keys: Iterable[str], concept: BuildConcept
) -> bool:
    return concept.address in build_fd_closure(
        environment, keys, include_empty_grain=False
    )


def _relevant_carry_keys(
    environment: BuildEnvironment,
    concepts: Sequence[BuildConcept],
    candidates: frozenset[str],
    members: frozenset[str],
) -> frozenset[str]:
    """Of `candidates`, the keys a ROOT delivering `concepts` pairs on: one it
    delivers, an authored statement-relation member (the declared axis, which
    an FD test cannot vouch for when a `unique` business key determines the
    outputs), a member it holds beside the outputs (a bridge: `customer_id`
    beside `product_name`, connected through the fact's order), or a key that
    determines something it delivers. A single-row output pairs with every
    row, and keeping a key for it would re-source a scalar at that grain."""
    concepts = [c for c in concepts if c.granularity != Granularity.SINGLE_ROW]
    if not candidates or not concepts:
        return frozenset()
    delivered = {c.address for c in concepts}
    statement_members = _statement_scoped_relation_members(environment)
    relevant: set[str] = set()
    for key in candidates:
        if (
            key in delivered
            or (key in statement_members and environment.concepts.get(key) is not None)
            or key in members
            or any(_fd_determined(environment, {key}, c) for c in concepts)
        ):
            relevant.add(key)
    # A composite grain reaches a dimension through a foreign key OFF it
    # (`(item, ticket)` -> customer -> region): jointly, and by no key alone.
    if any(
        c.address not in candidates
        and not _fd_determined(environment, relevant, c)
        and _fd_determined(environment, candidates, c)
        for c in concepts
    ):
        return candidates
    return frozenset(relevant)


def root_carry_demand(
    attrs: dict[str, GroupAttrs],
    gid: str,
    cover: Sequence[str],
    concepts: Sequence[BuildConcept],
    pins: dict[str, frozenset[str]],
    axes: frozenset[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """The keys a ROOT contributor must carry to pair with its siblings.

    Candidates are the axes some sibling's rows are identified by (its
    identity, or the grain keys it emits: a group at user grain emits the user
    key whether or not it advertises it). A region domain pairs on its spans
    alone; widening it to anything else re-sources it through the fact.

    A ROOT already carrying a candidate among its own concepts pairs on that
    alone, when those keys determine what it delivers: preserving the others
    would drag in whatever fact carries them, and the merge would stitch every
    key null-safely, pairing join-manufactured NULLs under `~` partials. A
    span carrying `order_id` beside a `product_id` the composite `(order_id,
    line_no)` determines repeats per line, so there the others stay."""
    group = attrs[gid]
    base = group.extent_spans or axes
    merge_grain = frozenset().union(*pins.values())
    sibling_keys = frozenset().union(
        *(attrs[other].emitted_grain for other in cover if other != gid)
    )
    demand = _relevant_carry_keys(
        environment,
        concepts,
        base & (merge_grain | sibling_keys),
        frozenset(group.members),
    )
    own = demand & ({c.address for c in concepts} | set(group.output_concepts))
    if own and all(
        c.address in demand or _fd_determined(environment, own, c) for c in concepts
    ):
        return own
    return demand


def _boundary_columns(
    group: GroupAttrs, environment: BuildEnvironment
) -> frozenset[str]:
    """Every column of the rowsets a rowset group belongs to: its boundary
    carries them all, projected or not (an axis-only side projects none)."""
    names = {
        c.lineage.rowset.name
        for addr in {*group.output_concepts, *group.members, *group.grain_components}
        if (c := environment.concepts.get(addr)) is not None
        and isinstance(c.lineage, BuildRowsetItem)
    }
    if not names:
        return frozenset()
    return frozenset(
        c.address
        for c in environment.concepts.values()
        if isinstance(c.lineage, BuildRowsetItem) and c.lineage.rowset.name in names
    )


def _over_own_rows(group: GroupAttrs) -> frozenset[str]:
    """The other row arguments of an atom `group` hosts that reads its own
    outputs (`date = max_date` on the `max(date)` group): it is decided after
    the value exists, against rows that still hold those arguments. An atom
    over inputs alone (`id > 2`) filters before and leaves nothing behind."""
    outputs = set(group.output_concepts)
    held: set[str] = set()
    for atom in group.condition_atoms:
        args = {c.address for c in atom.row_arguments}
        if args & outputs:
            held |= args - outputs
    return frozenset(held)


def _carries(
    group: GroupAttrs,
    identity: frozenset[str],
    merge_grain: frozenset[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """The columns a contributor's rows hold or can be widened to: a grouping
    group's outputs; a rowset's boundary; a row stream's inputs and members,
    its grain when it reads source rows (a scan holds its row keys; a BASIC
    over aggregates is computed off their rows and does not), and any merge
    key its identity determines (a filter at `item.sk` is widened to
    `item.manufact` to pair with an aggregate by it). Any group also holds
    what an atom over its own outputs is decided against."""
    if group.derivation in GROUPING_DERIVATIONS:
        return frozenset(group.output_concepts) | _over_own_rows(group)
    if group.derivation == Derivation.ROWSET:
        return frozenset(group.output_concepts) | _boundary_columns(group, environment)
    held = (
        frozenset(group.output_concepts)
        | group.row_columns
        | set(group.members)
        | _over_own_rows(group)
    )
    if not lineage_pinned_grain(group.output_concepts or group.members, environment):
        held |= group.grain_components
    # a ROOT carries exactly its carry demand beyond what it holds
    if not identity or group.derivation == Derivation.ROOT:
        return held
    closure = build_fd_closure(environment, identity, include_empty_grain=False)
    return held | (merge_grain & closure)


def _renderable_members(
    carried: frozenset[str], environment: BuildEnvironment
) -> frozenset[str]:
    """Authored relation members a side renders off what it carries: an
    expression member (`subset join b.oid + 1 = a.oid`) is no group's column,
    and is computed on the side holding its arguments."""
    out: set[str] = set()
    for relation in environment.scoped_join_relations():
        for address in relation - carried:
            concept = environment.concepts.get(
                address
            ) or environment.alias_origin_lookup.get(address)
            if (
                concept is not None
                and concept.lineage is not None
                and concept.lineage.concept_arguments
                and {a.address for a in concept.lineage.concept_arguments} <= carried
            ):
                out.add(address)
    return frozenset(out)


def _single_row(group: GroupAttrs, environment: BuildEnvironment) -> bool:
    """Every output is one row's worth, and no atom keeps rows beside them."""
    if _over_own_rows(group):
        return False
    concepts = [
        c
        for addr in group.output_concepts
        if (c := environment.concepts.get(addr)) is not None
    ]
    return bool(concepts) and all(
        c.granularity == Granularity.SINGLE_ROW for c in concepts
    )


def _axis_of(
    address: str, roots: dict[str, str], environment: BuildEnvironment
) -> frozenset[str]:
    """`address` with every name of the same axis: its value class root, and
    the members of an authored relation it belongs to (`union join a.k = b.k`
    keys one axis under two names)."""
    names = {address, roots.get(address, address)}
    for relation in environment.scoped_join_relations():
        if address in relation:
            names |= relation
    return frozenset(names)


def _pair(
    left: str,
    right: str,
    identity: dict[str, frozenset[str]],
    carries: dict[str, frozenset[str]],
    scalars: frozenset[str],
    environment: BuildEnvironment,
) -> Pairing:
    shared = carries[left] & carries[right]
    # scoped: an unscoped lookup caches on the statement's class baseline
    roots = environment.address_roots(shared | identity[left] | identity[right])
    axes = frozenset().union(
        *(_axis_of(a, roots, environment) for a in identity[left] | identity[right])
    )
    keys = {a for a in shared if _axis_of(a, roots, environment) & axes}
    authored: set[str] = set()
    for relation in environment.scoped_join_relations():
        lhs, rhs = relation & carries[left], relation & carries[right]
        if lhs and rhs and lhs | rhs != lhs & rhs:
            authored |= lhs | rhs
    keys |= authored
    implied = (
        shared & build_fd_closure(environment, keys, include_empty_grain=False)
        if keys
        else frozenset()
    ) - keys
    if authored:
        kind = PairingKind.AUTHORED
    elif {left, right} & scalars:
        kind = PairingKind.SCALAR
    elif any(
        identity[gid] and build_fd_determines_all(environment, keys, identity[gid])
        for gid in (left, right)
    ):
        kind = PairingKind.LOOKUP
    else:
        kind = PairingKind.BLEND
    return Pairing(left, right, frozenset(keys), kind, frozenset(implied))


def plan_final_join(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    cover: dict[str, list[BuildConcept]],
    mandatory_list: Sequence[BuildConcept],
    environment: BuildEnvironment,
    relation_edge_members: frozenset[str] = frozenset(),
) -> JoinPlan:
    """The plan for merging `cover` (contributor -> the outputs it supplies)."""
    contributors = tuple(cover)
    axes = merge_axes(
        contributors, attrs, mandatory_list, environment, relation_edge_members
    )
    pins = {
        gid: row_identity(group_graph, attrs, gid, contributors, axes, environment)
        for gid in contributors
    }
    merge_grain = frozenset().union(*pins.values())
    identity = {
        gid: pins[gid]
        | attrs[gid].emitted_grain
        | attrs[gid].anchor_keys
        | attrs[gid].extent_spans
        for gid in contributors
    }
    carry_demands = {
        gid: root_carry_demand(
            attrs, gid, contributors, cover[gid], pins, axes, environment
        )
        for gid in contributors
        if attrs[gid].derivation == Derivation.ROOT
    }
    carries = {
        gid: _carries(attrs[gid], identity[gid], merge_grain, environment)
        | carry_demands.get(gid, frozenset())
        for gid in contributors
    }
    if environment.scoped_join_key_groups:
        carries = {
            gid: held | _renderable_members(held, environment)
            for gid, held in carries.items()
        }
    scalars = frozenset(
        gid for gid in contributors if _single_row(attrs[gid], environment)
    )
    pairings = tuple(
        pairing
        for left, right in combinations(contributors, 2)
        if (pairing := _pair(left, right, identity, carries, scalars, environment)).keys
        or pairing.kind == PairingKind.SCALAR
    )
    return JoinPlan(contributors, pins, identity, carry_demands, pairings, scalars)


def final_predecessors(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs]
) -> list[str]:
    return sorted(
        gid for gid in group_graph.predecessors(FINAL_NODE_ID) if gid in attrs
    )
