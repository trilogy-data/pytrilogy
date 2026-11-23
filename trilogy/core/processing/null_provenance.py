"""Why a source's NULLs are NULL, owned by the source.

A NULL on a column is a VALUE (a `?` leaf, a nullable derivation, a ROLLUP
grouping key), EXTENT padding (a `?`-driven outer join found no partner),
GUEST padding (a `~?` guest's own columns), ROLLUP padding or SPAN padding (a
`~`-preserving join carrying a span's extension members). A source answers
each from its own joins and its parents' answers (`provenance`), once: a
`QueryDatasource` holds its answers until one of its fields is written, so
the planner and the optimizer read the same stamp, and an optimizer rewrite
of a source's joins or parents restamps it."""

from __future__ import annotations

from collections.abc import Callable
from weakref import ReferenceType, ref

from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
)
from trilogy.core.models.execute import (
    QueryDatasource,
    SourceJoin,
)
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    PADS_RIGHT_JOIN_TYPES,
    joined_right_ids,
    left_deep_joins,
    nullable_addresses_with_pseudonyms,
)

DataSource = QueryDatasource | BuildDatasource

_STAMP = "_provenance_cache"


def provenance(source: DataSource) -> NullProvenance:
    """The source's own answers. A leaf's are a read of its bindings, so only
    a `QueryDatasource` keeps them."""
    if not isinstance(source, QueryDatasource):
        return NullProvenance(source)
    found = source.__dict__.get(_STAMP)
    if found is None:
        found = source.__dict__[_STAMP] = NullProvenance(source, held=True)
    return found


class NullProvenance:
    """One source's answers, each computed on first read from its parents'."""

    def __init__(self, source: DataSource, held: bool = False) -> None:
        # A source that holds its provenance is referenced back weakly: the
        # pair would otherwise keep a whole plan alive until the collector ran.
        self._source: DataSource | ReferenceType[DataSource] = (
            ref(source) if held else source
        )
        self._extent: frozenset[str] | None = None
        self._guest: frozenset[str] | None = None
        self._rollup: frozenset[str] | None = None
        self._spans: dict[frozenset[str], frozenset[str]] = {}
        self._values: dict[str, bool] = {}
        self._driven: dict[int, bool] = {}
        self._padders: dict[str, frozenset[str]] = {}

    @property
    def source(self) -> DataSource:
        held = self._source
        if isinstance(held, ReferenceType):
            source = held()
            assert source is not None
            return source
        return held

    @property
    def extent(self) -> frozenset[str]:
        """Addresses this source can genuinely emit NULL for or omit a member
        of BECAUSE of a `?` declaration: a `?` binding at a leaf, or a key
        every provider of which is null-extended by a VALUE-NULL-DRIVEN outer
        join in this source's own tree (or already extent-null within that
        provider). Narrower than ``nullable_concepts`` twice over: a side
        merely JOINED on a nullable condition gets no mark (an INNER join
        introduces no NULLs), and padding from partial-driven (`~`) preserving
        joins gets none either, since extension families ride the host
        machinery and claiming their padding here would re-preserve rows that
        machinery already keeps exactly once. ROLLUP padding is likewise
        excluded; ``rollup`` owns it."""
        if self._extent is None:
            self._extent = frozenset()
            self._extent = _padded_addresses(
                self.source,
                nullable_addresses_with_pseudonyms,
                self.driven,
                lambda p: p.extent,
            )
        return self._extent

    @property
    def guest(self) -> frozenset[str]:
        """Addresses this source emits NULL for because a VALUE-NULL key found
        no partner: a `~?` guest sale's item columns, and whatever is chained
        off them. ``extent`` without the `?` leaves: a leaf's NULL is a member
        some holder of the key has a row for, and a guest's NULL is not."""
        if self._guest is None:
            self._guest = frozenset()
            self._guest = _padded_addresses(
                self.source, _no_leaf_addresses, self.driven, lambda p: p.guest
            )
        return self._guest

    @property
    def rollup(self) -> frozenset[str]:
        if self._rollup is None:
            self._rollup = frozenset(rollup_padded_addresses(self.source))
        return self._rollup

    def padded_by(self, spans: frozenset[str]) -> frozenset[str]:
        """Addresses this source only emits NULL for on the rows that carry
        the extension members of one of ``spans``: a ``~``-preserving join
        keyed on the span padded them, or a lookup chained off a key it padded
        (`users LEFT orders` on the span, then `LEFT lines` on `order_id`).

        A merge extent-free for those spans reads them as absence, not
        content: another branch owns those rows. An ordinary outer lookup's
        nullability stands."""
        found = self._spans.get(spans)
        if found is None:
            self._spans[spans] = frozenset()
            found = self._spans[spans] = _padded_addresses(
                self.source,
                _no_leaf_addresses,
                lambda join: _span_keyed(join, spans),
                lambda p: p.padded_by(spans),
            )
        return found

    def values(self, concept: BuildConcept) -> bool:
        """Whether the NULLs this source carries for `concept` are values."""
        found = self._values.get(concept.address)
        if found is None:
            found = self._values[concept.address] = nulls_are_values(
                concept, self.source
            )
        return found

    def padders(self, address: str) -> frozenset[str]:
        """Identifiers of this source and those above it whose own rows carry
        `address` nullable."""
        found = self._padders.get(address)
        if found is None:
            found = frozenset()
            if isinstance(self.source, QueryDatasource):
                own = {c.address for c in self.source.nullable_concepts}
                found = frozenset(
                    ({self.source.identifier} if address in own else set()).union(
                        *(
                            provenance(p).padders(address)
                            for p in self.source.datasources
                        )
                    )
                )
            self._padders[address] = found
        return found

    def driven(self, join: SourceJoin) -> bool:
        """Whether one of the join's keys is NULL as a value on its side."""
        found = self._driven.get(id(join))
        if found is None:
            found = self._driven[id(join)] = any(
                provenance(pair.node).values(pair.left)
                or provenance(join.right).values(pair.right)
                for pair in join.pairs or []
            )
        return found


def rollup_padded_addresses(datasource: DataSource) -> set[str]:
    """Grouping-key addresses this source NULL-pads because it renders
    `GROUP BY ROLLUP/CUBE/GROUPING SETS` itself. A source that passes the
    pass's rows through re-emits the padding, it does not create it."""
    if not isinstance(datasource, QueryDatasource) or datasource.grouping_sets is None:
        return set()
    return set(datasource.grouping_sets.keys)


def _no_leaf_addresses(datasource: BuildDatasource) -> set[str]:
    return set()


def _span_keyed(join: SourceJoin, spans: frozenset[str]) -> bool:
    return any(
        pair.left.address in spans or pair.right.address in spans
        for pair in join.pairs or []
    ) or any(concept.address in spans for concept in join.concepts or [])


def _padded_addresses(
    datasource: DataSource,
    leaf_addresses: Callable[[BuildDatasource], set[str]],
    join_extends: Callable[[SourceJoin], bool],
    parent_padded: Callable[[NullProvenance], frozenset[str]],
) -> frozenset[str]:
    """Addresses this source emits NULL for because an outer join `join_extends`
    licenses padded them, or because a leaf declared them nullable; a parent's
    own padding is its answer (`parent_padded`).

    Left-deep accumulation as in find_nullable_concepts: a RIGHT/FULL join
    null-extends the whole accumulated left input, not just one operand. An
    address counts only when EVERY provider of it was extended (or was already
    padded inside that provider)."""
    if isinstance(datasource, BuildDatasource):
        return frozenset(leaf_addresses(datasource))
    child_padded = {
        child.identifier: parent_padded(provenance(child))
        for child in datasource.datasources
    }
    right_ids = joined_right_ids(datasource.joins)
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
        left_padded = any(
            pair.node.identifier in extended
            or pair.left.address in child_padded.get(pair.node.identifier, frozenset())
            for pair in pairs
        )
        right_padded = any(
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
    return frozenset(out)


def side_nullable(concept: BuildConcept, side: DataSource | None) -> bool:
    """Whether `side` can emit NULL for `concept`: its stamp, which already
    folds in intrinsic nullability (`QueryDatasource.__post_init__`) and the
    side's own WHERE proofs."""
    if side is None:
        return False
    equivalent = concept.equivalent_addresses
    return any(equivalent & nc.equivalent_addresses for nc in side.nullable_concepts)


def _side_outputs(concept: BuildConcept, side: DataSource) -> bool:
    equivalent = concept.equivalent_addresses
    return any(equivalent & c.equivalent_addresses for c in side.output_concepts)


def nulls_are_values(
    concept: BuildConcept,
    side: DataSource,
    _seen: frozenset[str] = frozenset(),
) -> bool:
    """Whether the NULLs this side carries for ``concept`` are VALUES (a `?`
    column, a nullable derivation, a nullable input to a null-propagating
    expression, a ROLLUP grouping key) rather than pure outer-join extension.

    Outer-join extension means absent: there is no row on that side, so no
    key. Pairing that against a real NULL group cross-joins the two."""
    if concept.is_nullable:
        return True
    # Argument chains on one source can be mutually recursive; a repeat visit
    # contributes nothing new. A parent answers for itself.
    if concept.address in _seen:
        return False
    seen = _seen | {concept.address}
    equivalent = concept.equivalent_addresses
    if isinstance(side, BuildDatasource):
        # Column-level `?` is the only value-NULL source on a physical table.
        return side_nullable(concept, side)
    # A grouping-set NULL is padding too, but a twin-rollup partner pads the
    # same key, so it stays pairable here; get_join_type handles the mismatch.
    if equivalent & provenance(side).rollup:
        return True
    carriers = [p for p in side.datasources if _side_outputs(concept, p)]
    if not carriers:
        # Nothing upstream to attribute the NULL to; stay conservative rather
        # than call an unexplained nullability extension.
        return True
    if any(provenance(p).values(concept) for p in carriers):
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


def padding_sources(side: DataSource, keys: set[str]) -> set[str]:
    """Identifiers of the sources at or above `side` whose own rows carry the
    key as join-analysis padding. A leaf datasource never pads: a NULL in its
    column is a value, which the caller has already exempted."""
    stamp = provenance(side)
    return set().union(*(stamp.padders(key) for key in keys))
