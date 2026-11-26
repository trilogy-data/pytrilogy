"""The FINAL merge's join plan.

Which keys identify each contributor's rows, which keys a ROOT contributor
must carry to pair with its siblings, and which pairs of contributors pair on
which keys. Computed once over the elected cover from stage-2 facts (group
attrs and the group graph), never from built nodes: assembly widens, wraps and
joins to satisfy it instead of re-deriving it from whatever the built parents
happen to project. See `docs/join_axis_audit_2026_10.md`.
"""

from collections.abc import Callable, Collection, Iterable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from functools import partial
from itertools import combinations

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
    get_concept_row_arguments,
)
from trilogy.core.models.build_environment import BuildEnvironment

from .concept_graph import _scope_and_phase, _statement_scoped_relation_members
from .constants import FINAL_NODE_ID, GROUPING_DERIVATIONS
from .functional_dependency import build_fd_determines_all, strict_fd_closure
from .models import GroupAttrs, InputChannel, pairing_keys


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
    # what the keys determine: equating it too changes nothing, so resolve
    # may (a side's physical grain is never pruned, a re-sourced side carries
    # its raw key)
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
    # the columns each contributor's rows hold or can be widened to
    carries: dict[str, frozenset[str]] = field(default_factory=dict)
    # members of the statement's coalescing (`union`/`full`) relations
    coalescing: frozenset[str] = frozenset()
    # the contributors supplying outputs; the rest join to pair or filter them
    suppliers: frozenset[str] = frozenset()
    # bridges elected to key a pairing, not to join islands
    keying: frozenset[str] = frozenset()
    # contributors holding a declared `subset` member whose anchor another
    # contributor holds
    subset_sides: frozenset[str] = frozenset()
    # BASIC contributors computed off grouped rows alone: a group's rows
    over_groups: frozenset[str] = frozenset()

    @property
    def merge_grain(self) -> frozenset[str]:
        """The grain the suppliers fix: a side joined only to pair or filter
        them (a bridge, a filter argument's producer) fixes none."""
        return frozenset().union(
            *(pins for gid, pins in self.pins.items() if gid in self.suppliers)
        )

    def over(self, sides: Sequence[str]) -> "JoinPlan":
        """The plan for a merge joining `sides`: a contributor folded into
        another side pairs with nothing there. A side joined more than once is
        split: its projections pair on anything they both hold."""
        return self.across([frozenset({gid}) for gid in sides])

    def across(self, sides: Sequence[frozenset[str]]) -> "JoinPlan":
        """The plan for a merge joining `sides`, each the contributors whose
        rows it holds (a merge assembled below the consumer holds several):
        only pairings between two sides are joined here, the rest were joined
        below. A contributor on more than one side, or split already, pairs
        with itself."""
        index: dict[str, set[int]] = {}
        for i, side in enumerate(sides):
            for gid in side:
                index.setdefault(gid, set()).add(i)
        return replace(
            self,
            pairings=tuple(
                p
                for p in self.pairings
                if p.left == p.right
                and p.left in index
                or any(
                    i != j
                    for i in index.get(p.left, ())
                    for j in index.get(p.right, ())
                )
            ),
        ).with_split(frozenset(gid for gid, at in index.items() if len(at) > 1))

    def with_split(self, split: frozenset[str]) -> "JoinPlan":
        """The plan with each `split` contributor joined as several projections
        of its own rows, which pair on anything they both hold."""
        return replace(
            self,
            pairings=self.pairings
            + tuple(
                Pairing(gid, gid, frozenset(), PairingKind.LOOKUP, self.carries[gid])
                for gid in self.contributors
                if gid in split
            ),
        )

    @property
    def extras(self) -> list[str]:
        """The sides joined to pair or filter the suppliers."""
        return [gid for gid in self.contributors if gid not in self.suppliers]

    @property
    def precedence(self) -> tuple[str, ...]:
        """The order resolve joins the contributors in when nothing else
        decides: a declared `subset` member after the side holding its
        anchor, a row stream before the lookups its pairings identify (a
        bridge holding the rows included), the suppliers before the sides
        joined to pair or filter them, and finer rows first."""
        looked_up = self.looked_up
        return tuple(
            sorted(
                self.contributors,
                key=lambda gid: (
                    gid in self.subset_sides,
                    gid in looked_up,
                    gid not in self.suppliers,
                    -len(self.identity.get(gid, ())),
                ),
            )
        )

    @property
    def looked_up(self) -> frozenset[str]:
        """Contributors some pairing's keys identify the rows of."""
        return frozenset(
            gid
            for p in self.pairings
            if p.left != p.right
            for gid in (p.left, p.right)
            if self.identity.get(gid) and self.identity[gid] <= p.keys | p.implied
        )

    @property
    def join_keys(self) -> frozenset[str]:
        return frozenset().union(*(p.keys for p in self.pairings))

    @property
    def permitted_keys(self) -> frozenset[str]:
        """Everything resolve may equate: the keys and what they imply."""
        return self.join_keys | frozenset().union(*(p.implied for p in self.pairings))

    def pair_keys(self) -> dict[frozenset[str], frozenset[str]]:
        """What resolve may equate between each two contributors (one, for a
        contributor split into projections that pair with each other)."""
        out: dict[frozenset[str], frozenset[str]] = {}
        for p in self.pairings:
            pair = frozenset({p.left, p.right})
            out[pair] = out.get(pair, frozenset()) | p.keys | p.implied
        return out

    def keys_for(self, gid: str) -> frozenset[str]:
        """The keys `gid`'s own side of its pairings holds. A side of a
        `subset` relation carries its member, never its mate's (two rowsets
        over one table pair on `a.id = b.id`, not on the base `id`); a
        coalescing (`union`) relation is one axis every side presents under
        all of its names."""
        held = self.carries.get(gid, frozenset()) | self.coalescing
        return frozenset().union(
            *(p.keys & held for p in self.pairings if gid in (p.left, p.right))
        )

    def owed_by(self, gid: str) -> frozenset[str]:
        """What `gid`'s side is widened to: its pairing keys and the keys
        the consumer's contract asks it to carry."""
        return self.keys_for(gid) | self.carry_demands.get(gid, frozenset())

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
        concept = _concept_or_origin(addr, environment)
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


def _concept_or_origin(
    address: str, environment: BuildEnvironment
) -> BuildConcept | None:
    return environment.concepts.get(address) or environment.alias_origin_lookup.get(
        address
    )


def _group_parents(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], gid: str
) -> list[str]:
    return [p for p in group_graph.predecessors(gid) if p in attrs]


def authored_axes(
    groups: Sequence[str],
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    relation_edge_members: frozenset[str],
) -> frozenset[str]:
    """Members of an authored relation carried on the concept graph's RELATION
    edges and hosted by different groups: the statement's sides recombine only
    through the authored equality (`union join rank orders.oid order by
    orders.amt desc = customers.rnk`), whichever groups the cover elects. An
    unhosted third member of a chained relation is not the statement's to
    demand."""
    if len(groups) < 2 or not relation_edge_members:
        return frozenset()
    out: set[str] = set()
    for relation in environment.scoped_join_relations():
        if not relation & relation_edge_members:
            continue
        hosts: set[str] = set()
        hosted: set[str] = set()
        for gid in groups:
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
    authored: frozenset[str],
    row_columns: Callable[[str], frozenset[str]] | None = None,
) -> frozenset[str]:
    """Every key some contributor's rows are identified by: a candidate join
    axis. A BASIC over aggregates (`customer_status <- case ... min(x) by
    user`) sits at user grain and emits that key; a region domain's rows are
    keyed by its spans. A grouping or rowset output's grain is the result's
    row grain (a rowset's `keys` are not: they are the FK path that determines
    it). Authored relations hosted on both sides are axes by declaration.
    `row_columns` reads a group's columns off a statement in place of its
    attrs."""
    axes: set[str] = set()
    for gid in cover:
        group = attrs[gid]
        emitted = (
            group.grain_emitted_over(row_columns(gid))
            if row_columns
            else group.emitted_grain
        )
        axes |= emitted | group.extent_spans
    for concept in mandatory_list:
        if (
            concept.derivation in GROUPING_DERIVATIONS
            or concept.derivation == Derivation.ROWSET
        ) and concept.grain:
            axes |= set(concept.grain.components)
    return frozenset(axes) | authored


def row_identity(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    cover: Sequence[str],
    axes: frozenset[str],
    environment: BuildEnvironment,
    delivered: Collection[str] | None = None,
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
    if group.derivation == Derivation.ROOT and delivered is not None:
        # a stored aggregate a scan carries for another reader is a lookup
        # beside its rows, no pin on them
        pinned = tuple(
            a for a in pinned if a in delivered or not _grouped(a, environment)
        )
    keys |= lineage_pinned_grain(pinned, environment) & available
    # a pinned value is evaluated on the select's rows, keyed by its anchors
    keys |= group.grain_pin_anchors & available
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
        *(attrs[p].extent_spans for p in _group_parents(group_graph, attrs, gid))
    )
    # a WHERE gate over a row expression with no grain of its own
    # (`upper(species) != 'X'`) is its own row stream, identified by the keys
    # the expression is a function of
    if (
        not keys
        and group.derivation == Derivation.BASIC
        and not group.grain_components
        and _scope_and_phase(group.label)[1] == "condition"
    ):
        keys |= _member_keys(group.primary_members, environment) & available
    return frozenset(keys) | (held & available)


def _grouped(address: str, environment: BuildEnvironment) -> bool:
    concept = environment.concepts.get(address)
    return concept is not None and concept.derivation in GROUPING_DERIVATIONS


def _member_keys(members: Iterable[str], environment: BuildEnvironment) -> set[str]:
    out: set[str] = set()
    for address in members:
        concept = environment.concepts.get(address)
        if concept is not None and concept.keys:
            out |= set(concept.keys)
    return out


def _fd_determined(
    environment: BuildEnvironment, keys: Iterable[str], concept: BuildConcept
) -> bool:
    return concept.address in strict_fd_closure(environment, keys)


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
    # A key the delivered rows determine is a lookup onto a coarser sibling
    # (`tree_id` -> `genus` beside a value by genus): widening to it keeps them.
    # An authored relation's member pairs as authored, never as a lookup.
    relevant |= (
        pairing_keys(candidates & strict_fd_closure(environment, delivered))
        - environment.all_scoped_join_group_members()
    )
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


def _paired_grain(group: GroupAttrs) -> frozenset[str]:
    """The grain keys a FINAL contributor pairs on: those it emits, and those
    its rows are stitched on (a value by genus read off rows projected to
    genus holds genus, though it neither reads nor outputs it)."""
    return group.emitted_grain | (group.grain_components & _joined_on(group))


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
        *(_paired_grain(attrs[other]) for other in cover if other != gid)
    )
    candidates = base & (merge_grain | sibling_keys)
    members = frozenset(group.members)
    demand = _relevant_carry_keys(
        environment,
        concepts,
        candidates,
        members
        | _renderable(members, candidates, environment, frozenset({Derivation.BASIC})),
    )
    own = demand & ({c.address for c in concepts} | set(group.output_concepts))
    if own and all(
        c.address in demand or _fd_determined(environment, own, c) for c in concepts
    ):
        return own
    return demand


# a filtered column's stream holds its parents' rows, one for one
_ROW_STREAMS = (Derivation.ROOT, Derivation.FILTER)


def _root_lookups(
    group: GroupAttrs,
    identities: list[frozenset[str]],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """The stored keys of a supplier it shares none with that an extra ROOT
    scan's own outputs determine: widening it to them looks each row's value
    up and adds none (the species scan beside a window by species, widened to
    `tree_form` to pair a value by form). A supplier it shares a key with
    pairs on that key."""
    if group.extent_spans:
        return frozenset()
    own = frozenset(group.output_concepts)
    # a filtered stream shares the select's keys with every supplier: it is
    # widened to the rest of a supplier's grain too (`city` held, `tree_form`
    # looked up, to pair a value by both)
    shares_none = group.derivation != Derivation.FILTER
    keys = pairing_keys(
        frozenset().union(
            *(i for i in identities if not (shares_none and pairing_keys(i) & own))
        )
    )
    return frozenset(
        key
        for key in keys & strict_fd_closure(environment, own)
        if key not in own
        and key not in environment.all_scoped_join_group_members()
        and (concept := environment.concepts.get(key)) is not None
        and concept.derivation == Derivation.ROOT
    )


def _boundary_columns(
    group: GroupAttrs, environment: BuildEnvironment
) -> frozenset[str]:
    """Every column of the rowsets a rowset group belongs to: its boundary
    carries them all, projected or not (an axis-only side projects none)."""
    names = {
        c.lineage.rowset.name
        for addr in {
            *group.output_concepts,
            *group.input_concepts,
            *group.members,
            *group.grain_components,
        }
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
    over inputs alone (`id > 2`, `city = 'B' or ...` on a group by city) filters
    before and leaves nothing behind, as does one over a value the group only
    passes through (a `by city` count its rows carry beside `max(x) by
    species`)."""
    outputs = set(group.primary_members) - set(group.grain_components)
    held: set[str] = set()
    for atom in group.condition_atoms:
        args = {c.address for c in atom.row_arguments}
        if args & outputs:
            held |= args - outputs
    return frozenset(held)


def _from_row_parents(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    environment: BuildEnvironment,
) -> frozenset[str]:
    """What a row stream's ROOT or row-preserving parents output: it passes
    their rows through, columns and all (`item_name as p_name` off the sale
    lines holds the store each line was sold at). Beside a grouped parent the
    row parents are projected to the stream's grain before they pair with it
    (`_satisfy_parent_projection_contract`): `sale_recency` at customer and
    sale date, beside a by-customer aggregate, holds no sale line's item."""
    group = attrs[gid]
    if group.derivation in GROUPING_DERIVATIONS or group.derivation in (
        Derivation.ROOT,
        Derivation.ROWSET,
    ):
        return frozenset()
    parents = _group_parents(group_graph, attrs, gid)
    passed = frozenset().union(
        *(
            attrs[parent].output_concepts
            for parent in parents
            if attrs[parent].derivation
            in (Derivation.ROOT, Derivation.BASIC, Derivation.FILTER)
        )
    )
    if (
        not group.grain_components
        or _read_only_by_final(group_graph, attrs, gid)
        or not any(
            attrs[parent].derivation in (*GROUPING_DERIVATIONS, Derivation.WINDOW)
            for parent in parents
        )
    ):
        return passed
    return passed & _determined(group.grain_components, environment)


def _read_only_by_final(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], gid: str
) -> bool:
    """A filter only FINAL reads passes its row parents' rows through whole:
    its claimed grain is the select's, not a projection of them."""
    return attrs[gid].derivation == Derivation.FILTER and set(
        group_graph.successors(gid)
    ) == {FINAL_NODE_ID}


def _held(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    carry_demands: dict[str, frozenset[str]],
    environment: BuildEnvironment,
    identity: frozenset[str] = frozenset(),
    sibling_keys: frozenset[str] = frozenset(),
) -> frozenset[str]:
    return (
        _from_row_parents(group_graph, attrs, gid, environment)
        | carries(
            attrs[gid],
            identity,
            sibling_keys,
            environment,
            _domain_spans(group_graph, attrs, gid),
        )
        | carry_demands.get(gid, frozenset())
    )


def _over_groups_only(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], gid: str
) -> bool:
    """A BASIC computed off grouped rows alone (`count(id) + 1`): it holds what
    they output, and no row of it reaches a key they determine but lack. A
    group at its input rows' own grain (`sum(x)` by `tree_id`) is those rows."""
    if attrs[gid].derivation != Derivation.BASIC:
        return False
    parents = _group_parents(group_graph, attrs, gid)
    return bool(parents) and all(
        _groups_its_rows(attrs[p]) or _over_groups_only(group_graph, attrs, p)
        for p in parents
    )


def _groups_its_rows(group: GroupAttrs) -> bool:
    return group.derivation in GROUPING_DERIVATIONS and not (
        group.aggregate_input_grain
        and group.aggregate_input_grain <= group.grain_components
    )


def _widenable(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    environment: BuildEnvironment,
) -> frozenset[str]:
    """What a consumer's preserve key can be carried on `gid` as: a grouped
    side holds only its outputs; a row side what its rows determine. A
    dimension peel is never widened to its sibling dimension's key."""
    held = _held(group_graph, attrs, gid, {}, environment)
    if attrs[gid].derivation in (*GROUPING_DERIVATIONS, Derivation.ROWSET):
        return held
    return _determined(held, environment)


def carryable(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    keys: frozenset[str],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """Of `keys`, what `gid` can be asked to carry: what it can be widened to
    (`_widenable`) and what it computes off those columns (`subset join
    b.oid + 1 = a.oid` on the side holding `b.oid`). Each side is asked only
    this, so the merge plan and `renders` read the ask as stated."""
    widenable = _widenable(group_graph, attrs, gid, environment)
    return keys & (widenable | _renderable(widenable, keys, environment))


def _domain_spans(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], gid: str
) -> frozenset[str] | None:
    """The spans a row stream reading only region domains holds (`s.sk` off
    the item domain holds the item, never the sale it is grained at); None
    when it reads anything else."""
    parents = _group_parents(group_graph, attrs, gid)
    if not parents or not all(attrs[p].extent_spans for p in parents):
        return None
    return frozenset().union(*(attrs[p].extent_spans for p in parents))


def _joined_on(group: GroupAttrs) -> frozenset[str]:
    """The keys a row stream's inputs are stitched on, its input grain: its
    rows hold them (`sale_recency` over a by-customer aggregate and a sale's
    date holds `customer.sk` and `sale_date.sk`)."""
    rows = any(c.channel == InputChannel.ROW_STREAM for c in group.input_contracts)
    return group.input_grain if rows else frozenset()


def carries(
    group: GroupAttrs,
    identity: frozenset[str],
    sibling_keys: frozenset[str],
    environment: BuildEnvironment,
    domain_spans: frozenset[str] | None = None,
) -> frozenset[str]:
    """`_held_columns`, less the all-rows marker: any row can emit it, and no
    side is paired on it (`pairing_keys`)."""
    return pairing_keys(
        _held_columns(group, identity, sibling_keys, environment, domain_spans)
    )


def _held_columns(
    group: GroupAttrs,
    identity: frozenset[str],
    sibling_keys: frozenset[str],
    environment: BuildEnvironment,
    domain_spans: frozenset[str] | None = None,
) -> frozenset[str]:
    """The columns a contributor's rows hold or can be widened to: a grouping
    group's outputs; a rowset's boundary; a row stream's inputs and members,
    its grain when it reads source rows (a scan holds its row keys; a BASIC
    over aggregates is computed off their rows and does not), and any merge
    key a sibling is identified by, or authored relation member, that its own
    identity determines (a filter at `item.sk` is widened to `item.manufact`
    to pair with an aggregate by it). Any group also holds
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
    extra = _joined_on(group)
    if not lineage_pinned_grain(group.output_concepts or group.members, environment):
        extra |= group.grain_components
    if domain_spans is not None:
        extra &= domain_spans
    # a ROOT carries exactly its carry demand beyond what it holds
    if identity and group.derivation != Derivation.ROOT:
        closure = strict_fd_closure(environment, identity)
        extra |= sibling_keys & closure
    # Off a rowset boundary the rows hold the boundary's columns under its
    # handles, and nothing else: a base key there (the `id` a body selects)
    # welds two rowsets of one table on that key.
    if any(_is_handle(address, environment) for address in group.input_concepts):
        return (
            held
            | _boundary_columns(group, environment)
            | frozenset(a for a in extra if _is_handle(a, environment))
        )
    return (held | extra) - _peel_atom_columns(group)


def _peel_atom_columns(group: GroupAttrs) -> frozenset[str]:
    """A dimension peel groups to its keys, so a column only its WHERE atom
    reads is filtered on below the grouping and is no column of its rows."""
    if group.derivation != Derivation.ROOT or not group.dim_keys:
        return frozenset()
    return frozenset(
        c.address
        for atom in group.condition_atoms
        for c in get_concept_row_arguments(atom)
    ) - frozenset(group.output_concepts)


def _is_handle(address: str, environment: BuildEnvironment) -> bool:
    concept = environment.concepts.get(address)
    return concept is not None and concept.derivation == Derivation.ROWSET


def _renderable(
    carried: frozenset[str],
    candidates: Iterable[str],
    environment: BuildEnvironment,
    derivations: frozenset[Derivation] | None = None,
) -> frozenset[str]:
    """Of `candidates`, what a side renders off the columns it carries: an
    expression relation member (`subset join b.oid + 1 = a.oid`) or a row
    derivation a sibling is keyed by (`case sort_k ... end -> label`) is no
    group's column of this side, and is computed where its arguments are."""
    out: set[str] = set()
    for address in set(candidates) - carried:
        concept = _concept_or_origin(address, environment)
        # a rowset handle is a column of its own boundary: computed off
        # another relation it is a different value under the same name
        if (
            concept is not None
            and concept.lineage is not None
            and concept.derivation != Derivation.ROWSET
            and concept.lineage.concept_arguments
            and (derivations is None or concept.derivation in derivations)
            and {a.address for a in concept.lineage.concept_arguments} <= carried
        ):
            out.add(address)
    return frozenset(out)


def _single_row(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    environment: BuildEnvironment,
) -> bool:
    """Every output is one row's worth, no atom keeps rows beside them, and no
    row-preserving parent hands the group a grain: a rename of a gated global
    aggregate (`avg(x) by * ? yr = 2001` read off a filter at brand grain)
    is one value per row of that filter, whatever its outputs declare."""
    group = attrs[gid]
    if _over_own_rows(group):
        return False
    if group.derivation not in GROUPING_DERIVATIONS and any(
        attrs[parent].grain_components
        for parent in _group_parents(group_graph, attrs, gid)
        if attrs[parent].derivation not in GROUPING_DERIVATIONS
    ):
        return False
    concepts = [
        c
        for addr in group.output_concepts
        if (c := environment.concepts.get(addr)) is not None
    ]
    return bool(concepts) and all(
        c.granularity == Granularity.SINGLE_ROW for c in concepts
    )


def _determined(keys: Iterable[str], environment: BuildEnvironment) -> frozenset[str]:
    """What `keys` determine, a `unique` property determining its key too
    (`unique property sk.id`: equal ids are one customer)."""
    closure = frozenset(keys)
    while True:
        seed = closure | {
            key
            for address in closure
            if (c := environment.concepts.get(address)) is not None
            and c.purpose == Purpose.UNIQUE_PROPERTY
            for key in c.keys or ()
        }
        grown = strict_fd_closure(environment, seed)
        if grown == closure:
            return closure
        closure = grown


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
    reach: dict[str, frozenset[str]],
    scalars: frozenset[str],
    environment: BuildEnvironment,
    merge_keys: frozenset[str] = frozenset(),
) -> Pairing:
    # scoped: an unscoped lookup caches on the statement's class baseline
    # one row pairs with every row: no key, whatever the sides share
    if {left, right} & scalars:
        return Pairing(left, right, frozenset(), PairingKind.SCALAR)
    roots = environment.address_roots(
        carries[left] | carries[right] | identity[left] | identity[right]
    )
    # one value under two names (`ws` beside `week_seq`) is shared
    right_values = {roots.get(a, a) for a in carries[right]}
    left_values = {roots.get(a, a) for a in carries[left]}
    shared = frozenset(
        a
        for side, other in (
            (carries[left], right_values),
            (carries[right], left_values),
        )
        for a in side
        if roots.get(a, a) in other
    )
    # two scans identified by nothing pair on the keys the merge is (the trees
    # and their WHERE-filtered copy, both holding `species`)
    named = identity[left] | identity[right] or (
        merge_keys if not (identity[left] or identity[right]) else frozenset()
    )
    axes = frozenset().union(*(_axis_of(a, roots, environment) for a in named))
    keys = {a for a in shared if _axis_of(a, roots, environment) & axes}
    # a relation both sides carry a member of is an axis by declaration;
    # authored when the sides hold different members of it
    authored: set[str] = set()
    reached: set[str] = set()
    for relation in environment.scoped_join_relations():
        lhs, rhs = relation & reach[left], relation & reach[right]
        if lhs and rhs:
            reached |= lhs | rhs
            keys |= (lhs & carries[left]) | (rhs & carries[right])
            if lhs | rhs != lhs & rhs:
                authored |= lhs | rhs
    implied = (
        (shared & _determined(keys, environment) if keys else frozenset())
        | _determined(reached, environment)
    ) - keys
    if authored:
        kind = PairingKind.AUTHORED
    elif merge_keys and keys and not (identity[left] or identity[right]):
        # two scans paired on the merge's keys hold the same rows there
        kind = PairingKind.LOOKUP
    elif any(
        (named_by := pairing_keys(identity[gid]))
        and build_fd_determines_all(environment, keys, named_by)
        for gid in (left, right)
    ):
        kind = PairingKind.LOOKUP
    else:
        kind = PairingKind.BLEND
    return Pairing(left, right, frozenset(keys), kind, frozenset(implied))


def _filtered_row_keys(
    attrs: dict[str, GroupAttrs],
    gid: str,
    contracted: dict[str, frozenset[str]] | None,
) -> frozenset[str]:
    """A filtered column's grain is the select's, its rows the ones it filters:
    the keys a consumer asks it to carry name them (`Rows.coarse_pair_squares`)."""
    if attrs[gid].derivation != Derivation.FILTER or not contracted:
        return frozenset()
    return contracted.get(gid, frozenset())


def _filter_output_keys(
    group: GroupAttrs, environment: BuildEnvironment
) -> frozenset[str]:
    """A filter's rows are one per value of each key it outputs, its grain's
    or not: `city` beside `tree_form ? species in <set>` at `species` keeps
    one row per city (`Filters.filter_rows_keyed_by_outputs`)."""
    if group.derivation != Derivation.FILTER:
        return frozenset()
    return frozenset(
        a
        for a in group.output_concepts
        if (c := environment.concepts.get(a)) is not None
        and c.purpose == Purpose.KEY
        and c.derivation == Derivation.ROOT
    )


def _looked_up_keys(
    keys: frozenset[str],
    identity: frozenset[str],
    others: Iterable[frozenset[str]],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """Of `keys`, the ROOT properties whose own keys `identity` holds: a value
    over groups alone reaches no column its groups lack, save one a scan of
    the property beside its keys looks up per row (`tree_form` by `species`).
    A side holding both already pairs it through them: no lookup."""
    others = list(others)
    return frozenset(
        a
        for a in keys
        if (c := environment.concepts.get(a)) is not None
        and c.derivation == Derivation.ROOT
        and c.purpose == Purpose.PROPERTY
        and c.keys
        and set(c.keys) <= identity
        and not any({a, *c.keys} <= held for held in others)
    )


def _delivered_keys(
    group: GroupAttrs, concepts: Sequence[BuildConcept]
) -> frozenset[str]:
    """The keys a ROOT contributor delivers: the FINAL wraps it to their grain,
    so its rows are unique on them, and a row expansion of it (an unnest
    carrying them) pairs with it there."""
    if group.derivation != Derivation.ROOT:
        return frozenset()
    return frozenset(c.address for c in concepts if c.purpose == Purpose.KEY)


def plan_final_join(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    cover: dict[str, list[BuildConcept]],
    mandatory_list: Sequence[BuildConcept],
    environment: BuildEnvironment,
    authored: frozenset[str] = frozenset(),
    split: frozenset[str] = frozenset(),
    contracted: dict[str, frozenset[str]] | None = None,
    extras: Sequence[str] = (),
) -> JoinPlan:
    """The plan for merging `cover` (contributor -> the outputs it supplies).
    A `split` contributor is joined as several projections of its own rows,
    which pair on anything they both hold: that is the one relation, never a
    key to carry. `contracted` are the keys a consumer's input contracts ask
    each contributor to carry (a per-group merge's `preserve_keys`), each
    already narrowed to what that contributor can carry (`carryable`). `extras`
    join beside the cover supplying nothing (bridges, filter-argument
    producers): they pair, but fix no grain and ask no ROOT to carry a key."""
    supplying = tuple(cover)
    contributors = supplying + tuple(g for g in extras if g not in cover)
    cover = {**cover, **{gid: [] for gid in contributors if gid not in cover}}
    axes = merge_axes(supplying, attrs, mandatory_list, authored) | frozenset().union(
        *(_paired_grain(attrs[gid]) for gid in supplying)
    )
    pins = {
        gid: row_identity(
            group_graph,
            attrs,
            gid,
            supplying,
            axes,
            environment,
            {c.address for c in cover[gid]},
        )
        for gid in contributors
    }
    identity = {
        gid: pins[gid]
        | attrs[gid].emitted_grain
        | attrs[gid].anchor_keys
        | attrs[gid].extent_spans
        | _delivered_keys(attrs[gid], cover[gid])
        for gid in contributors
    }
    carry_demands = {
        gid: root_carry_demand(
            attrs,
            gid,
            supplying,
            cover[gid],
            {g: pins[g] for g in supplying},
            axes,
            environment,
        )
        for gid in contributors
        if attrs[gid].derivation == Derivation.ROOT
    }
    for gid, keys in (contracted or {}).items():
        carry_demands[gid] = carry_demands.get(gid, frozenset()) | keys
    for gid in contributors:
        if gid not in supplying and attrs[gid].derivation in _ROW_STREAMS:
            carry_demands[gid] = carry_demands.get(gid, frozenset()) | _root_lookups(
                attrs[gid], [identity[g] for g in supplying], environment
            )
    # A row stream is identified by the grain keys its rows turn out to hold
    # (`sale_recency` at customer and sale date holds both off its inputs);
    # settled before what each side is widened to is read off the identities.
    identity = {
        gid: identity[gid]
        | (
            (attrs[gid].grain_components | _filtered_row_keys(attrs, gid, contracted))
            & _held(group_graph, attrs, gid, carry_demands, environment)
            if attrs[gid].derivation not in GROUPING_DERIVATIONS
            else frozenset()
        )
        | _filter_output_keys(attrs[gid], environment)
        for gid in contributors
    }
    members = environment.all_scoped_join_group_members()
    carries: dict[str, frozenset[str]] = {}
    for gid in contributors:
        # an extra side is joined as built, never widened to a sibling's key
        sibling_keys = frozenset().union(
            *(identity[o] for o in contributors if o != gid)
        )
        if gid not in supplying:
            siblings: frozenset[str] = frozenset()
        elif _over_groups_only(group_graph, attrs, gid):
            siblings = _looked_up_keys(
                sibling_keys,
                identity[gid],
                # a side already planned holds what it looks up
                (
                    carries.get(o)
                    or _held(
                        group_graph, attrs, o, carry_demands, environment, identity[o]
                    )
                    for o in contributors
                    if o != gid
                ),
                environment,
            )
        else:
            siblings = sibling_keys
        held = _held(
            group_graph, attrs, gid, carry_demands, environment, identity[gid], siblings
        )
        # an extra row stream is joined as built: its input rows' columns are
        # no column of it (a filter at `tree_id` over rows holding `city`)
        if gid not in supplying and attrs[gid].derivation == Derivation.FILTER:
            held &= (
                frozenset(attrs[gid].output_concepts)
                | identity[gid]
                | carry_demands.get(gid, frozenset())
            )
        carries[gid] = (
            held
            | _renderable(held, members, environment)
            | _renderable(held, siblings, environment, frozenset({Derivation.BASIC}))
        )
    # authored members a row stream's identity determines: it can be widened
    # to them, so the relation pairs it, but they are no key to carry
    reach = {
        gid: carries[gid]
        | (
            members & strict_fd_closure(environment, identity[gid])
            if identity[gid]
            and gid in supplying
            and attrs[gid].derivation not in (*GROUPING_DERIVATIONS, Derivation.ROOT)
            else frozenset()
        )
        for gid in contributors
    }
    scalars = frozenset(
        gid for gid in contributors if _single_row(group_graph, attrs, gid, environment)
    )
    pairings = tuple(
        pairing
        for left, right in combinations(contributors, 2)
        if (
            pairing := _pair(
                left,
                right,
                identity,
                carries,
                reach,
                scalars,
                environment,
                (
                    frozenset().union(*(identity[g] for g in supplying))
                    if attrs[left].derivation == Derivation.ROOT
                    and attrs[right].derivation == Derivation.ROOT
                    else frozenset()
                ),
            )
        ).keys
        or pairing.implied
        or pairing.kind == PairingKind.SCALAR
    )
    return JoinPlan(
        contributors,
        pins,
        identity,
        carry_demands,
        pairings,
        scalars,
        carries,
        frozenset(environment.domain_graph.coalescing_relation_members()),
        frozenset(supplying),
        subset_sides=subset_sides(carries, environment),
        over_groups=frozenset(
            gid for gid in supplying if _over_groups_only(group_graph, attrs, gid)
        ),
    ).with_split(split)


def subset_sides(
    carries: dict[str, frozenset[str]], environment: BuildEnvironment
) -> frozenset[str]:
    """The sides holding a declared `subset` member whose anchor another side
    holds: the anchor's rows are the relation's, and each member hangs off
    them (`subset join a = b` is a LEFT anchored on `b`)."""
    graph = environment.domain_graph
    return frozenset(
        gid
        for member, anchor in graph.declared_subset_pairs()
        for gid, held in carries.items()
        if member in held
        and any(
            {anchor, graph.canonical(anchor)} & other
            for o, other in carries.items()
            if o != gid and member not in other
        )
    )


def final_predecessors(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs]
) -> list[str]:
    return sorted(
        gid for gid in group_graph.predecessors(FINAL_NODE_ID) if gid in attrs
    )


def elect_bridges(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    cover: dict[str, list[BuildConcept]],
    candidates: Sequence[str],
    mandatory_list: Sequence[BuildConcept],
    environment: BuildEnvironment,
    authored: frozenset[str] = frozenset(),
    split: frozenset[str] = frozenset(),
    contracted: dict[str, frozenset[str]] | None = None,
    reader_grain: frozenset[str] = frozenset(),
) -> JoinPlan:
    """The plan over `cover` with the groups outside it that join its islands
    as extras, in the order they join: each, planned beside the cover as a
    side supplying nothing, leaves fewer islands (`select id, min(x) by id,
    min(y) by cell` is bridged by the row stream holding both keys).
    `candidates` in preference order."""

    plan = partial(
        plan_final_join,
        group_graph,
        attrs,
        cover,
        mandatory_list,
        environment,
        authored,
        split,
        contracted,
    )
    bridges: list[str] = []
    current = plan(extras=bridges)
    while True:
        islands = len(current.islands())
        blends = _underkeyed_blends(current, attrs, environment, reader_grain)
        if islands <= 1 and not blends:
            return current
        # a bridge every pairing of which is keyed: shared columns that
        # identify neither side multiply both, so none is elected and the
        # keyless-join guard names the gap
        free = [gid for gid in candidates if gid not in cover and gid not in bridges]
        elected = next(
            (
                (added, trial)
                for added in [*((gid,) for gid in free), *_bridge_pairs(free)]
                if (
                    len((trial := plan(extras=[*bridges, *added])).islands()),
                    _underkeyed_blends(trial, attrs, environment, reader_grain),
                )
                < (islands, blends)
                and all(
                    p.kind != PairingKind.BLEND
                    for p in trial.pairings
                    if {p.left, p.right} & set(added)
                )
            ),
            None,
        )
        if elected is None:
            return current
        added, trial = elected
        keying = current.keying | (
            set(added) if len(trial.islands()) == islands else frozenset()
        )
        current = replace(trial, keying=frozenset(keying))
        bridges.extend(added)


def _bridge_pairs(free: Sequence[str]) -> Iterator[tuple[str, str]]:
    """Two groups that bridge only together (the row stream holding `city,
    species` and a value by species holding `fc`, between groups by `city`
    and by `fc`): tried once no single group joins an island."""
    for i, left in enumerate(free):
        for right in free[i + 1 :]:
            yield left, right


def _underkeyed_blends(
    plan: JoinPlan,
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    reader_grain: frozenset[str] = frozenset(),
) -> int:
    """Pairings of two grouped suppliers on fewer keys than one's grain
    determines of the other's (`count(id)` by city, species beside `max(x) by
    city, tree_form`, paired on city alone): the merge needs a side holding
    the determined key too (`SelectGrain.by_property_without_its_key_misjoins`)."""
    count = 0
    for p in plan.pairings:
        if p.kind != PairingKind.BLEND or not {p.left, p.right} <= plan.suppliers:
            continue
        # a dimension peel's rows are identified by its keys, as a group's
        # are by its grain (a stored `sum(height) by city, species`)
        if not all(
            attrs[g].derivation in GROUPING_DERIVATIONS
            or attrs[g].dim_keys
            or g in plan.over_groups
            for g in (p.left, p.right)
        ):
            continue
        left, right = plan.identity[p.left], plan.identity[p.right]
        if not any(
            not b <= p.keys and b <= _determined(a, environment)
            for a, b in ((left, right), (right, left))
        ) and not (
            _pinned_finer(plan, p, environment)
            or _reader_pinned(plan, p, reader_grain, environment)
        ):
            continue
        if not any(
            _bridges_on_identity(plan, extra, p.left, environment)
            and _bridges_on_identity(plan, extra, p.right, environment)
            for extra in plan.extras
        ):
            count += 1
    return count


def _pinned_finer(plan: JoinPlan, p: Pairing, environment: BuildEnvironment) -> bool:
    """Another supplier's rows determine both sides of blend `p` (`tree_id`
    beside a value by species and one by city, tree_form): each merged row has
    one partner on each side, and the shared keys alone pair it with every
    side row that shares them."""
    both = plan.identity[p.left] | plan.identity[p.right]
    return not both <= p.keys and any(
        gid not in (p.left, p.right)
        and plan.identity[gid]
        and both <= _determined(plan.identity[gid], environment)
        for gid in plan.suppliers
    )


def _reader_pinned(
    plan: JoinPlan,
    p: Pairing,
    reader_grain: frozenset[str],
    environment: BuildEnvironment,
) -> bool:
    """The reader's own grain determines both sides of blend `p` (a value at
    city, species over one by species and one by city, tree_form): each of
    its rows has one partner on each side, and the shared keys pair every
    species of a form with every city of it
    (`SelectGrain.blend_under_a_finer_reader_invents_pairs`)."""
    both = plan.identity[p.left] | plan.identity[p.right]
    return (
        bool(reader_grain)
        and not both <= p.keys
        and both <= _determined(reader_grain, environment)
    )


def _bridges_on_identity(
    plan: JoinPlan, bridge: str, side: str, environment: BuildEnvironment
) -> bool:
    """`bridge` pairs with `side` on keys that determine its identity (a value
    the side is also grained by, `v1` beside `tree_id`, is one of them)."""
    return any(
        {q.left, q.right} == {bridge, side}
        and q.kind != PairingKind.BLEND
        and pairing_keys(plan.identity[side])
        <= _determined(q.keys | q.implied, environment)
        for q in plan.pairings
    )
