"""Why a source's NULLs are NULL, decided once per source.

A NULL on a column is a VALUE (a `?` leaf, a nullable derivation, a ROLLUP
grouping key), EXTENT padding (a `?`-driven outer join found no partner),
GUEST padding (a `~?` guest's own columns), ROLLUP padding or SPAN padding (a
`~`-preserving join carrying a span's extension members). Each is a walk of
the source's parent chain; `ProvenanceMemo` answers every question once per
source, for one `get_node_joins` call or, under `plan_scope`, for the whole
plan. The scope is safe only while planning: a resolved `QueryDatasource` is
not changed until the optimizer rewrites its joins, so the optimizer's
readers (`value_set_join_upgrade`) walk uncached."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial

from trilogy.core.enums import AggregateGroupingMode
from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    get_grouped_aggregate_wrapper,
)
from trilogy.core.models.execute import (
    Join,
    QueryDatasource,
    SourceJoin,
)
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    PADS_RIGHT_JOIN_TYPES,
    left_deep_joins,
)

DataSource = QueryDatasource | BuildDatasource


class ProvenanceMemo:
    """The walks' memos, keyed by source identity; the sources are held so an
    id names one source for the memo's life."""

    def __init__(self) -> None:
        self.sources: dict[int, DataSource] = {}
        self.extent: dict[int, frozenset[str]] = {}
        self.guest: dict[int, frozenset[str]] = {}
        self.driven: dict[int, bool] = {}
        self.span: dict[frozenset[str], dict[int, frozenset[str]]] = {}
        self.rollup: dict[int, frozenset[str]] = {}
        self.value: dict[tuple[int, str], bool] = {}

    def of(self, source: DataSource) -> NullProvenance:
        self.sources.setdefault(id(source), source)
        return NullProvenance(self, source)


class NullProvenance:
    """One source's answers, read off the memo."""

    def __init__(self, memo: ProvenanceMemo, source: DataSource) -> None:
        self.memo = memo
        self.source = source

    @property
    def extent(self) -> frozenset[str]:
        return extent_null_addresses(self.source, self.memo)

    @property
    def guest(self) -> frozenset[str]:
        return guest_padded_addresses(self.source, self.memo)

    @property
    def rollup(self) -> frozenset[str]:
        key = id(self.source)
        found = self.memo.rollup.get(key)
        if found is None:
            found = self.memo.rollup[key] = frozenset(
                rollup_padded_addresses(self.source)
            )
        return found

    def padded_by(self, spans: frozenset[str]) -> frozenset[str]:
        return span_padded_addresses(self.source, spans, self.memo)

    def values(self, concept: BuildConcept) -> bool:
        """Whether the NULLs this source carries for `concept` are values."""
        key = (id(self.source), concept.address)
        found = self.memo.value.get(key)
        if found is None:
            found = self.memo.value[key] = nulls_are_values(concept, self.source)
        return found


_ACTIVE: ContextVar[ProvenanceMemo | None] = ContextVar("null_provenance", default=None)


@contextmanager
def plan_scope() -> Iterator[ProvenanceMemo]:
    """Share one memo across every merge a plan resolves."""
    memo = ProvenanceMemo()
    token = _ACTIVE.set(memo)
    try:
        yield memo
    finally:
        _ACTIVE.reset(token)


def active_memo() -> ProvenanceMemo:
    """The plan's memo, or a fresh one for a caller outside any plan."""
    return _ACTIVE.get() or ProvenanceMemo()


def rollup_padded_addresses(datasource: DataSource) -> set[str]:
    """Grouping-key addresses this source NULL-pads because it renders
    `GROUP BY ROLLUP/CUBE/GROUPING SETS` itself. A wrapper already computed
    upstream is a passthrough: it re-emits the padded rows, it does not
    create them."""
    if not isinstance(datasource, QueryDatasource):
        return set()
    upstream = {
        c.address for parent in datasource.datasources for c in parent.output_concepts
    }
    padded: set[str] = set()
    for concept in datasource.output_concepts:
        wrapper = get_grouped_aggregate_wrapper(concept)
        if (
            wrapper is None
            or wrapper.grouping == AggregateGroupingMode.STANDARD
            or concept.address in upstream
        ):
            continue
        padded.update(b.address for b in wrapper.by)
    return padded


def _leaf_null_addresses(datasource: BuildDatasource) -> set[str]:
    out: set[str] = set()
    for concept in datasource.nullable_concepts:
        out.add(concept.address)
        out.update(concept.pseudonyms)
    return out


def _no_leaf_addresses(datasource: BuildDatasource) -> set[str]:
    return set()


def _value_null_driven(join: SourceJoin, memo: ProvenanceMemo) -> bool:
    driven = memo.driven.get(id(join))
    if driven is None:
        driven = memo.driven[id(join)] = any(
            memo.of(pair.node).values(pair.left)
            or memo.of(join.right).values(pair.right)
            for pair in join.pairs or []
        )
    return driven


def _span_keyed(join: SourceJoin, spans: frozenset[str]) -> bool:
    return any(
        pair.left.address in spans or pair.right.address in spans
        for pair in join.pairs or []
    ) or any(concept.address in spans for concept in join.concepts or [])


def _padded_addresses(
    datasource: DataSource,
    leaf_addresses: Callable[[BuildDatasource], set[str]],
    join_extends: Callable[[SourceJoin], bool],
    memo: dict[int, frozenset[str]] | None = None,
    chain: bool = False,
) -> frozenset[str]:
    """Addresses this source emits NULL for because an outer join `join_extends`
    licenses padded them, or because a leaf declared them nullable.

    Left-deep accumulation as in find_nullable_concepts: a RIGHT/FULL join
    null-extends the whole accumulated left input, not just one operand. An
    address counts only when EVERY provider of it was extended (or was already
    padded inside that provider)."""
    memo = {} if memo is None else memo
    cached = memo.get(id(datasource))
    if cached is not None:
        return cached
    memo[id(datasource)] = frozenset()
    if isinstance(datasource, BuildDatasource):
        memo[id(datasource)] = frozenset(leaf_addresses(datasource))
        return memo[id(datasource)]
    child_padded = {
        child.identifier: _padded_addresses(
            child, leaf_addresses, join_extends, memo, chain
        )
        for child in datasource.datasources
    }
    right_ids = {j.right.identifier for j in datasource.joins if isinstance(j, Join)}
    extended: set[str] = set()
    out: set[str] = set()
    base_ids = [i for i in child_padded if i not in right_ids]
    for join, accumulated in left_deep_joins(datasource.joins, base_ids):
        right_id = join.right.identifier
        pairs = join.pairs or []
        extends = join_extends(join)
        # a lookup keyed on a column already padded on its preserved side pads
        # for the same rows (a guest order's customer, then that customer's
        # address)
        left_padded = chain and any(
            pair.node.identifier in extended
            or pair.left.address in child_padded.get(pair.node.identifier, frozenset())
            for pair in pairs
        )
        right_padded = chain and any(
            pair.right.address in child_padded.get(right_id, frozenset())
            for pair in pairs
        )
        if join.join_type in PADS_RIGHT_JOIN_TYPES and (extends or left_padded):
            extended.add(right_id)
        if join.join_type in PADS_LEFT_JOIN_TYPES and (extends or right_padded):
            extended |= accumulated
    for address, providers in datasource.source_map.items():
        idents = {
            p.identifier
            for p in providers
            if isinstance(p, (BuildDatasource, QueryDatasource))
        }
        if idents and all(
            ident in extended or address in child_padded.get(ident, frozenset())
            for ident in idents
        ):
            out.add(address)
    for concept in datasource.output_concepts:
        if concept.address in out:
            out.update(concept.pseudonyms)
    memo[id(datasource)] = frozenset(out)
    return memo[id(datasource)]


def extent_null_addresses(
    datasource: DataSource, memo: ProvenanceMemo | None = None
) -> frozenset[str]:
    """Addresses this source can genuinely emit NULL for or omit a member of
    BECAUSE of a `?` declaration: a `?` binding at a leaf, or a key every
    provider of which is null-extended by a VALUE-NULL-DRIVEN outer join in
    this source's own tree (or already extent-null within that provider).
    Narrower than ``nullable_concepts`` twice over: a side merely JOINED on a
    nullable condition gets no mark (an INNER join introduces no NULLs), and
    padding from partial-driven (`~`) preserving joins gets none either, since
    extension families ride the host machinery and claiming their padding here
    would re-preserve rows that machinery already keeps exactly once. ROLLUP
    padding is likewise excluded; ``rollup_padded_addresses`` owns it."""
    memo = memo or ProvenanceMemo()
    return _padded_addresses(
        datasource,
        _leaf_null_addresses,
        partial(_value_null_driven, memo=memo),
        memo.extent,
        chain=True,
    )


def guest_padded_addresses(
    datasource: DataSource, memo: ProvenanceMemo | None = None
) -> frozenset[str]:
    """Addresses this source emits NULL for because a VALUE-NULL key found no
    partner: a `~?` guest sale's item columns, and whatever is chained off
    them. ``extent_null_addresses`` without the `?` leaves: a leaf's NULL is a
    member some holder of the key has a row for, and a guest's NULL is not."""
    memo = memo or ProvenanceMemo()
    return _padded_addresses(
        datasource,
        _no_leaf_addresses,
        partial(_value_null_driven, memo=memo),
        memo.guest,
        chain=True,
    )


def span_padded_addresses(
    datasource: DataSource, spans: frozenset[str], memo: ProvenanceMemo | None = None
) -> frozenset[str]:
    """Addresses this source only emits NULL for on the rows that carry the
    extension members of one of ``spans``: a ``~``-preserving join keyed on
    the span padded them, or a lookup chained off a key it padded (`users
    LEFT orders` on the span, then `LEFT lines` on `order_id`).

    A merge extent-free for those spans reads them as absence, not content:
    another branch owns those rows. An ordinary outer lookup's nullability
    stands."""
    memo = memo or ProvenanceMemo()
    return _padded_addresses(
        datasource,
        _no_leaf_addresses,
        partial(_span_keyed, spans=spans),
        memo.span.setdefault(spans, {}),
        chain=True,
    )


def side_nullable(concept: BuildConcept, side: DataSource | None) -> bool:
    if side is None:
        return False
    # Intrinsic nullability: the concept's own definition can yield NULL (a
    # `?` column, a filtered value or aggregate, a no-else CASE) on any side
    # that carries it, regardless of that side's join structure.
    if concept.is_nullable:
        return True
    equivalent = concept.equivalent_addresses
    if any(equivalent & nc.equivalent_addresses for nc in side.nullable_concepts):
        return True
    # a side that COMPUTES the join key from nullable inputs yields NULL keys
    # too (`l_key + 1` is NULL wherever `l_key` is) even when the derived key
    # itself never got flagged
    if not propagates_argument_nulls(concept):
        return False
    args = {a.address for a in concept.concept_arguments}
    if not args:
        return False
    nullable_addrs: set[str] = set()
    for nc in side.nullable_concepts:
        nullable_addrs |= nc.equivalent_addresses
    return bool(args & nullable_addrs)


def _side_outputs(concept: BuildConcept, side: DataSource) -> bool:
    equivalent = concept.equivalent_addresses
    return any(equivalent & c.equivalent_addresses for c in side.output_concepts)


def nulls_are_values(
    concept: BuildConcept,
    side: DataSource,
    _seen: frozenset[tuple[str, int]] = frozenset(),
) -> bool:
    """Whether the NULLs this side carries for ``concept`` are VALUES (a `?`
    column, a nullable derivation, a nullable input to a null-propagating
    expression, a ROLLUP grouping key) rather than pure outer-join extension.

    Outer-join extension means absent: there is no row on that side, so no
    key. Pairing that against a real NULL group cross-joins the two."""
    if concept.is_nullable:
        return True
    # Argument chains can be mutually recursive; a repeat visit of the same
    # concept on the same source contributes nothing new.
    visit = (concept.address, id(side))
    if visit in _seen:
        return False
    seen = _seen | {visit}
    equivalent = concept.equivalent_addresses
    if isinstance(side, BuildDatasource):
        # Column-level `?` is the only value-NULL source on a physical table.
        return any(
            equivalent & nc.equivalent_addresses for nc in side.nullable_concepts
        )
    # A grouping-set NULL is padding too, but a twin-rollup partner pads the
    # same key, so it stays pairable here; get_join_type handles the mismatch.
    if equivalent & rollup_padded_addresses(side):
        return True
    carriers = [p for p in side.datasources if _side_outputs(concept, p)]
    if not carriers:
        # Nothing upstream to attribute the NULL to; stay conservative rather
        # than call an unexplained nullability extension.
        return True
    if any(nulls_are_values(concept, p, seen) for p in carriers):
        return True
    if not propagates_argument_nulls(concept):
        return False
    args = {a.address for a in concept.concept_arguments}
    if not args:
        return False
    return any(
        (args & nc.equivalent_addresses) and nulls_are_values(nc, side, seen)
        for nc in side.nullable_concepts
    )


def padding_sources(
    side: DataSource, keys: set[str], canon: Callable[[str], str]
) -> set[str]:
    """Identifiers of the sources at or above `side` whose own rows carry the
    key as join-analysis padding. A leaf datasource never pads: a NULL in its
    column is a value, which the caller has already exempted."""
    found: set[str] = set()
    if not isinstance(side, QueryDatasource):
        return found
    if keys & {canon(c.address) for c in side.nullable_concepts}:
        found.add(side.identifier)
    for parent in side.datasources:
        found |= padding_sources(parent, keys, canon)
    return found
