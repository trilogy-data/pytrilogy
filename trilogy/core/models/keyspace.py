"""A plan's row universe, as a value: built by
``processing.v4_helper.keyspace.build_keyspace`` and read by grouping, join
typing and pin-heal. Model-level so ``BuildEnvironment.span_scope`` can carry
it into every merge built under a plan."""

from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import cached_property


@dataclass(frozen=True)
class Completion:
    """A source holding only SOME rows of a region, beside sources holding the
    rest (``returns`` beside ``lines``; ``web_orders`` beside ``store_orders``).
    ``spans`` are the ``~`` keys that say so. ``emptied_by`` are the concepts
    the WHERE null-rejects that only this source supplies: no entity is absent
    on the rows it lacks, but none of them survives the statement."""

    source: str
    spans: frozenset[str]
    emptied_by: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Region:
    """One kind of row a plan can return: the entity keys PRESENT on it.

    ``spans`` are the ``~`` bindings that keep this kind of row from being
    absorbed into a larger one (a customer no order references); empty for the
    base region. ``completions`` are the sources the plan needs that hold only
    some of this region's rows (``returns`` beside ``lines``): no entity is
    absent, but the rest still have to come from somewhere. ``emptied_by`` are
    the concepts the plan's WHERE null-rejects that are ABSENT here, so no row
    of this kind survives the statement. ``reach`` is what a keyed lookup from
    every span TOGETHER arrives at: a composite-key dimension (`grain (name,
    variant)`) is entered only with its whole grain, so no span alone reaches
    its properties."""

    present: frozenset[str]
    spans: frozenset[str] = frozenset()
    # some complete source's rows ARE this kind of row and no larger source
    # holds them all: the unmatched members of a dimension. A region kept only
    # by a completion (a partial aggregate table beside its fact) has none:
    # every row of it is a row of the larger source, where nothing is absent
    has_own_rows: bool = False
    emptied_by: frozenset[str] = frozenset()
    # every source whose rows ARE this kind of row
    witnesses: frozenset[str] = frozenset()
    completions: tuple[Completion, ...] = ()
    reach: frozenset[str] = frozenset()

    @property
    def is_empty(self) -> bool:
        return bool(self.emptied_by)

    @property
    def completes(self) -> frozenset[str]:
        return frozenset().union(*(c.spans for c in self.completions))

    @property
    def live_completes(self) -> frozenset[str]:
        """``completes`` less the keys every completion of which the WHERE
        empties: a partial source whose rows are all gone demands nothing."""
        return frozenset().union(
            *(c.spans for c in self.completions if not c.emptied_by)
        )

    def describe(self) -> str:
        body = "{" + ", ".join(sorted(self.present)) + "}"
        if self.spans:
            body += f" ~{sorted(self.spans)}"
        if self.completes:
            body += f" completes {sorted(self.completes)}"
        if self.emptied_by:
            body += f" EMPTY by {sorted(self.emptied_by)}"
        return body


def spans_in_play(regions: Iterable[Region]) -> frozenset[str]:
    """Every span a join over `regions` can pad for."""
    return frozenset().union(*(r.spans | r.completes for r in regions))


@dataclass(frozen=True)
class Keyspace:
    """A plan's row universe: disjoint regions over the requested entity keys,
    the base region first. A concept is DEFINED on a region when every one of
    its keys is present there; elsewhere it is absent, which renders as NULL
    but is not a NULL value."""

    regions: tuple[Region, ...] = ()
    # requested concept address -> the entity keys it is a function of
    keys_by_address: dict[str, frozenset[str]] = field(default_factory=dict)
    outputs: tuple[str, ...] = ()
    # span -> the entities a keyed lookup from that span alone arrives at
    span_reach: dict[str, frozenset[str]] = field(default_factory=dict)
    # a spelling a join below this plan (a rowset body) pads a span under ->
    # the span in this plan's spelling (`RowsetWitness.spellings`)
    witnessed: dict[str, str] = field(default_factory=dict)
    # the spans of a rowset body's regions no requested entity is present on:
    # the body pads them, and they are not rows of this plan
    unread_spans: frozenset[str] = frozenset()
    # spans some source binds `?`: a NULL key is a member of its own there,
    # one no dimension row holds, so a fact row keyed on it pairs with nothing
    value_null_spans: frozenset[str] = frozenset()
    # (address, region, id(environment)) -> `extent_ownership.null_on_padding`
    padding_nulls: dict[tuple[str, Region, int], bool] = field(
        default_factory=dict, compare=False, repr=False
    )

    @cached_property
    def live_regions(self) -> tuple[Region, ...]:
        return tuple(r for r in self.regions if not r.is_empty)

    @cached_property
    def families(self) -> tuple[frozenset[str], ...]:
        """Each live extension region's spans. A node holding two regions
        unions their spans, so only this partition says how many families a
        merge has: counting spans reads one composite-key region as two."""
        return tuple(r.spans for r in self.live_regions if r.spans)

    def row_absent(self, keys: frozenset[str]) -> Region:
        """A row of the base region with `keys` absent."""
        return Region(present=self.regions[0].present - keys)

    def live_regions_within(self, spans: frozenset[str]) -> list[Region]:
        """The live extension regions `spans` covers."""
        return [r for r in self.live_regions if r.spans and r.spans <= spans]

    @cached_property
    def in_play_spans(self) -> frozenset[str]:
        """Every span a join of this plan can pad for. An emptied region still
        counts: its rows are gone once the WHERE has run, and a merge below
        that point still sees their padding."""
        return spans_in_play(self.regions)

    @cached_property
    def output_demanded_spans(self) -> frozenset[str]:
        """What the extent election asks: the spans whose unmatched members
        carry an OUTPUT, one that is a function of what the span alone reaches,
        or of what a region's spans reach together (`vclass` of `grain (name,
        variant)`), and then every span of that region is."""
        in_play: frozenset[str] = frozenset().union(
            *(r.spans | r.live_completes for r in self.live_regions)
        )
        demanded = {
            span
            for span in in_play
            if self._output_within(self.span_reach.get(span, frozenset()))
        }
        for region in self.live_regions:
            if self._output_within(region.reach):
                demanded |= region.spans
        return frozenset(demanded)

    def _output_within(self, reach: frozenset[str]) -> bool:
        return any(
            self.keys_by_address.get(o) and self.keys_by_address[o] <= reach
            for o in self.outputs
        )

    def binding_is_complete(self, source: str, span: str) -> bool:
        """Does ``source``'s ``~`` on ``span`` cost this plan nothing? The
        binding says the source lacks some of the key's members. Those live on
        the regions other sources witness, and on the rows of its own region it
        holds no match for; when the WHERE empties every one of them, the
        source is complete for this plan. False when the span is not in play."""
        if span not in self.in_play_spans:
            return False
        for region in self.live_regions:
            if span in region.spans and source not in region.witnesses:
                return False
            if any(
                c.source == source and span in c.spans and not c.emptied_by
                for c in region.completions
            ):
                return False
        return True

    def defined_on(self, address: str, region: Region) -> bool:
        return self.keys_by_address.get(address, frozenset()) <= region.present

    def region_of(self, spans: frozenset[str]) -> Region | None:
        return next((r for r in self.regions if r.spans == spans), None)

    def keys_of(self, address: str) -> frozenset[str]:
        return self.keys_by_address.get(address, frozenset())

    def carried_on(self, address: str, region: Region) -> bool:
        """Does an extension row of ``region`` hold a value for ``address``: it
        is keyed on what a lookup from the region's spans reaches. An entity
        merely cross-joined onto the region is present, but not carried."""
        keys = self.keys_of(address)
        return bool(keys) and keys <= region.reach

    def describe(self) -> str:
        return " | ".join(r.describe() for r in self.regions)
