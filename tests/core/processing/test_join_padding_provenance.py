"""``_padding_sources`` names the sources whose own rows carry a join key as
join-analysis padding; the optimizer's value-set upgrade reads it to tell
shared padding (one source's rows arriving twice) from unrelated NULLs."""

from dataclasses import fields

from trilogy.core.enums import JoinType, Modifier, Purpose, SourceType
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import BaseJoin, ConceptPair, QueryDatasource
from trilogy.core.processing.join_resolution import (
    JoinFacts,
    SideFacts,
    _padding_sources,
    _pads_for_different_members,
    _span_padding_matrix,
    _span_spellings,
    complete_key_domain,
    get_join_type,
    guest_padded_addresses,
)

KEY = "local.padded"


def _concept(address: str) -> BuildConcept:
    namespace, name = address.split(".")
    return BuildConcept(
        name=name,
        canonical_name=name,
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(components=set()),
    )


def _qds(
    outputs: list[str],
    nullable: list[str],
    parents: list[QueryDatasource] | None = None,
    grain: set[str] | None = None,
) -> QueryDatasource:
    concepts = [_concept(a) for a in outputs]
    return QueryDatasource(
        input_concepts=[],
        output_concepts=concepts,
        datasources=list(parents or []),
        source_map={a: set() for a in outputs},
        grain=BuildGrain(components=grain or set()),
        joins=[],
        source_type=SourceType.SELECT,
        nullable_concepts=[_concept(a) for a in nullable],
    )


def _identity(address: str) -> str:
    return address


def test_padding_sources_ignores_leaf_and_unpadded():
    assert _padding_sources(_qds([KEY], []), {KEY}, _identity) == set()


def test_padding_sources_reports_own_identifier():
    padded = _qds([KEY], [KEY])
    assert _padding_sources(padded, {KEY}, _identity) == {padded.identifier}


def test_padding_sources_walks_parents():
    padded = _qds([KEY], [KEY])
    consumer = _qds([KEY], [], parents=[padded])
    assert padded.identifier in _padding_sources(consumer, {KEY}, _identity)


def test_padding_sources_scoped_to_the_requested_key():
    padded = _qds([KEY, "local.other"], ["local.other"])
    assert _padding_sources(padded, {KEY}, _identity) == set()


_LEFT, _RIGHT, _AXIS = "ds~left", "ds~right", "c~local.order_id"
_BOTH_NULLABLE = {_LEFT: {_AXIS}, _RIGHT: {_AXIS}}


def _sides(**maps) -> dict[str, SideFacts]:
    """`{SideFacts field: {side: value}}`, the shape the merge collects its
    facts in, folded into one ``SideFacts`` per side."""
    nodes = {node for per_side in maps.values() for node in per_side}
    return {
        node: SideFacts(
            **{
                field: value
                for field, per_side in maps.items()
                if (value := per_side.get(node)) is not None
            }
        )
        for node in nodes
    }


def _typed(padding: dict[str, dict[str, frozenset[str]]]) -> JoinType:
    return _join({_AXIS}, nullables=_BOTH_NULLABLE, span_padding=padding)


_MERGE_FIELDS = {f.name for f in fields(JoinFacts)} - {"sides"}


def _join(keys: set[str], **facts) -> JoinType:
    """Merge-wide `JoinFacts` fields by name, everything else a per-side map."""
    merge = {k: v for k, v in facts.items() if k in _MERGE_FIELDS}
    sides = _sides(**{k: v for k, v in facts.items() if k not in _MERGE_FIELDS})
    return get_join_type(_LEFT, _RIGHT, keys, JoinFacts(sides=sides, **merge))


def test_padding_for_different_spans_never_pairs():
    padding = {
        _LEFT: {_AXIS: frozenset({"local.product_id"})},
        _RIGHT: {_AXIS: frozenset({"local.user_id"})},
    }
    sides = _sides(span_padding=padding)
    assert _pads_for_different_members(sides[_LEFT], sides[_RIGHT], {_AXIS})
    assert _typed(padding) == JoinType.FULL


def test_padding_for_a_shared_span_still_pairs():
    padding = {
        _LEFT: {_AXIS: frozenset({"local.user_id"})},
        _RIGHT: {_AXIS: frozenset({"local.user_id", "local.product_id"})},
    }
    sides = _sides(span_padding=padding)
    assert not _pads_for_different_members(sides[_LEFT], sides[_RIGHT], {_AXIS})
    assert _typed(padding) == JoinType.INNER


def test_unattributed_padding_keeps_its_typing():
    padding = {_LEFT: {_AXIS: frozenset({"local.user_id"})}, _RIGHT: {}}
    sides = _sides(span_padding=padding)
    assert not _pads_for_different_members(sides[_LEFT], sides[_RIGHT], {_AXIS})
    assert _typed(padding) == JoinType.INNER


def test_an_authored_axis_keeps_its_typing_over_an_extent_null():
    """An extent NULL one side carries on the merge axis normally preserves
    that side (its NULL names a row, the other side's padding names nothing).
    On an AUTHORED axis (a `subset join`'s declared key) the relation owns the
    pairing, so the rule stands down like every other direction inference --
    q64's `subset join catalog_item_agg.item_id = ss.item.id`, where the fact
    stream's own padding once flipped the declared join to LEFT_OUTER and made
    the FINAL restate every pushed atom over five extra projected columns."""
    asymmetric = {
        "nullables": {_LEFT: {_AXIS}, _RIGHT: {_AXIS}},
        "extent_nullables": {_LEFT: {_AXIS}},
    }
    assert _join({_AXIS}, **asymmetric) == JoinType.LEFT_OUTER
    for owner in ("scoped_keys", "anchor_keys", "authored_join_keys"):
        assert (
            _join({_AXIS}, **{owner: frozenset({_AXIS})}, **asymmetric)
            == JoinType.INNER
        ), owner


USER, ORDER, CANON = "local.user_id", "local.order_id", "other.user_id"


def _extended_for(span: str) -> QueryDatasource:
    """`users LEFT JOIN orders` keyed on `span`: `order_id` is padding."""
    users = _qds([span], [], grain={span})
    orders = _qds([span, ORDER], [], grain={ORDER})
    merged = _qds([span, ORDER], [ORDER], parents=[users, orders])
    merged.source_map = {span: {users}, ORDER: {orders}}
    merged.joins = [
        BaseJoin(
            left_datasource=users,
            right_datasource=orders,
            join_type=JoinType.LEFT_OUTER,
            concept_pairs=[
                ConceptPair(
                    left=_concept(span), right=_concept(span), existing_datasource=users
                )
            ],
        )
    ]
    return merged


def _matrix(span: str, spellings: dict[str, str]) -> dict[str, frozenset[str]]:
    return _span_padding_matrix(
        {"ds~merged": _extended_for(span)},
        {"ds~merged": frozenset({ORDER})},
        spellings,
        _identity,
    )["ds~merged"]


def test_padding_is_attributed_to_the_span_in_play():
    assert _matrix(USER, {USER: USER}) == {ORDER: frozenset({USER})}


def test_a_span_out_of_play_attributes_nothing():
    assert _matrix(USER, {}) == {}


def test_scoped_join_spelling_names_the_same_span():
    environment = BuildEnvironment()
    environment.scoped_join_key_groups = {CANON: {USER}}
    spellings = _span_spellings(frozenset({USER}), environment)
    assert spellings == {USER: CANON, CANON: CANON}
    assert _matrix(CANON, spellings) == {ORDER: frozenset({CANON})}


def test_unrelated_scoped_join_adds_no_spelling():
    environment = BuildEnvironment()
    environment.scoped_join_key_groups = {CANON: {"local.elsewhere"}}
    assert _span_spellings(frozenset({USER}), environment) == {USER: USER}


HANDLE = "local.s.user"


def test_rowset_body_padding_is_named_by_the_readers_handle():
    """The body pads under `USER`; the plan reading the handle attributes it
    to the handle, and to the handle's scoped canonical when it has one. A
    spelling the plan uses for a span of its own keeps that name."""
    environment = BuildEnvironment()
    witnessed = {USER: HANDLE}
    assert _span_spellings(frozenset({HANDLE}), environment, witnessed) == {
        HANDLE: HANDLE,
        USER: HANDLE,
    }
    assert _matrix(USER, {USER: HANDLE}) == {ORDER: frozenset({HANDLE})}
    environment.scoped_join_key_groups = {CANON: {HANDLE}}
    assert _span_spellings(frozenset({HANDLE}), environment, witnessed) == {
        HANDLE: CANON,
        CANON: CANON,
        USER: CANON,
    }
    assert _span_spellings(frozenset({USER}), BuildEnvironment(), witnessed) == {
        USER: USER
    }


_SPAN, _ATTR = "c~local.item_sk", "c~local.item_desc"


def test_extent_free_span_pairs_inner_with_its_complete_domain():
    """A `~` binding joined to the span's whole domain has a partner for every
    row, so it is INNER at plan time; a domain that is not the whole one, or a
    binder carrying a NULL on the span, keeps the binder anchored."""
    typed = {"partials": {_RIGHT: {_SPAN}}, "extent_free_keys": frozenset({_SPAN})}
    complete = {_LEFT: {_SPAN}}
    assert _join({_SPAN}, complete_spans=complete, **typed) == JoinType.INNER
    assert (
        _join({_SPAN}, complete_spans={_LEFT: set()}, **typed) == JoinType.RIGHT_OUTER
    )
    assert (
        _join({_SPAN}, nullables={_RIGHT: {_SPAN}}, complete_spans=complete, **typed)
        == JoinType.RIGHT_OUTER
    )


def test_region_holder_preserves_over_a_feeder_whose_value_null_pairs():
    """A value NULL the feeder carries on an attribute the holder carries
    nullable too pairs null-safely: the holder is preserved, not both. One the
    holder cannot pair keeps FULL. On the region's own key the same holds for
    a VALUE null (a region spelled by a nullable stand-in has a member whose
    key is NULL, on both sides); an EXTENT null there is a guest order naming
    no member, and keeps FULL whatever the holder carries."""
    keys = {_SPAN, _ATTR}
    typed = {
        "nullables": {_LEFT: {_ATTR}, _RIGHT: {_ATTR}},
        "held_spans": {_LEFT: {_SPAN}},
    }
    both = {_LEFT: {_ATTR}, _RIGHT: {_ATTR}}
    assert _join(keys, value_nullables=both, **typed) == JoinType.LEFT_OUTER
    assert _join(keys, value_nullables={_RIGHT: {_ATTR}}, **typed) == JoinType.FULL
    on_key = {_LEFT: {_ATTR, _SPAN}, _RIGHT: {_ATTR, _SPAN}}
    assert _join(keys, value_nullables=on_key, **typed) == JoinType.LEFT_OUTER
    assert (
        _join(keys, value_nullables={_RIGHT: {_ATTR, _SPAN}}, **typed) == JoinType.FULL
    )
    assert (
        _join(
            keys,
            value_nullables=on_key,
            extent_nullables={_RIGHT: {_SPAN}},
            **typed,
        )
        == JoinType.FULL
    )


_OTHER, _OTHER_SPAN = "ds~other", "c~local.user_id"


def test_region_join_escalates_only_over_another_familys_rows():
    """FULL keeps another family's extension rows, NULL on this key, and they
    exist only in the stream joined against the holder: the side being added
    when the holder is already joined, everything joined when the holder is
    the one being added."""
    partition = (frozenset({_SPAN}), frozenset({_OTHER_SPAN}))

    def typed(held: dict[str, set[str]], joined: set[str]) -> JoinType:
        facts = JoinFacts(sides=_sides(held_spans=held), region_partition=partition)
        return get_join_type(_LEFT, _RIGHT, {_SPAN}, facts, joined)

    other = {_OTHER: {_OTHER_SPAN}}
    assert typed({_LEFT: {_SPAN}, **other}, {_LEFT}) == JoinType.LEFT_OUTER
    assert typed({_LEFT: {_SPAN}, **other}, {_LEFT, _OTHER}) == JoinType.LEFT_OUTER
    assert typed({_LEFT: {_SPAN}, _RIGHT: {_OTHER_SPAN}}, {_LEFT}) == JoinType.FULL
    assert typed({_RIGHT: {_SPAN}, **other}, {_LEFT}) == JoinType.RIGHT_OUTER
    assert typed({_RIGHT: {_SPAN}, **other}, {_LEFT, _OTHER}) == JoinType.FULL
    assert typed({_RIGHT: {_SPAN}, _LEFT: {_OTHER_SPAN}}, {_LEFT}) == JoinType.FULL


def _scan(name: str, outputs: list[str], partial: list[str] | None = None):
    return BuildDatasource(
        name=name,
        address=name,
        columns=[
            BuildColumnAssignment(
                alias=a,
                concept=_concept(a),
                modifiers=[Modifier.PARTIAL] if a in (partial or []) else [],
            )
            for a in outputs
        ],
        grain=BuildGrain(),
    )


def test_complete_key_domain_is_an_unfiltered_scan_kept_whole():
    items = _scan("items", [USER])
    assert complete_key_domain(items, USER)
    assert not complete_key_domain(_scan("orders", [USER], partial=[USER]), USER)
    read = _qds([USER], [], parents=[items])
    assert complete_key_domain(read, USER)
    read.limit = 5
    assert not complete_key_domain(read, USER)
    # the scan on the dropped side of a LEFT join is not kept whole
    orders = _scan("orders", [USER, ORDER], partial=[USER])
    merged = _qds([USER, ORDER], [], parents=[orders, items])
    merged.joins = [
        BaseJoin(
            left_datasource=orders,
            right_datasource=items,
            join_type=JoinType.LEFT_OUTER,
            concept_pairs=[
                ConceptPair(
                    left=_concept(USER),
                    right=_concept(USER),
                    existing_datasource=orders,
                )
            ],
        )
    ]
    assert not complete_key_domain(merged, USER)
    merged.joins[0].join_type = JoinType.RIGHT_OUTER
    assert complete_key_domain(merged, USER)


def test_host_preserves_over_a_partial_feeder_whose_value_null_pairs():
    typed = {
        "partials": {_RIGHT: {_ATTR}},
        "nullables": {_LEFT: {_ATTR}, _RIGHT: {_ATTR}},
        "hosts": {_LEFT: True},
    }
    both = {_LEFT: {_ATTR}, _RIGHT: {_ATTR}}
    assert _join({_ATTR}, value_nullables=both, **typed) == JoinType.LEFT_OUTER
    assert _join({_ATTR}, value_nullables={_RIGHT: {_ATTR}}, **typed) == JoinType.FULL


def test_guest_padding_on_the_feeder_is_unpaired_by_the_host_value_null():
    """The feeder NULLs the attribute for a `~?` guest (its NULL key found no
    row of the host's): the host's `?` member is not that guest, so the two
    value NULLs do not pair and the feeder keeps FULL. A host padded for the
    same guests pairs it again."""
    typed = {
        "partials": {_RIGHT: {_ATTR}},
        "nullables": {_LEFT: {_ATTR}, _RIGHT: {_ATTR}},
        "hosts": {_LEFT: True},
        "value_nullables": {_LEFT: {_ATTR}, _RIGHT: {_ATTR}},
    }
    assert _join({_ATTR}, guest_padded={_RIGHT: {_ATTR}}, **typed) == JoinType.FULL
    assert (
        _join({_ATTR}, guest_padded={_LEFT: {_ATTR}, _RIGHT: {_ATTR}}, **typed)
        == JoinType.LEFT_OUTER
    )
    held = {**typed, "held_spans": {_LEFT: {_SPAN}}}
    assert _join({_ATTR}, guest_padded={_RIGHT: {_ATTR}}, **held) == JoinType.FULL


def test_guest_padded_addresses_walks_a_value_null_join_without_leaves():
    """`items` LEFT-joined onto a `~?` fact: the item's columns are NULL on the
    guest sale, so they are guest padding downstream (through an aggregate
    over the merge too), while the `?` leaf itself is not."""
    attr = _ATTR.removeprefix("c~")
    sales = _scan("sales", [USER, ORDER], partial=[USER])
    sales.columns[0].modifiers.append(Modifier.NULLABLE)
    items = _scan("items", [USER, attr])
    merged = _qds([USER, ORDER, attr], [USER, attr], parents=[sales, items])
    merged.source_map = {USER: {sales}, ORDER: {sales}, attr: {items}}
    merged.joins = [
        BaseJoin(
            left_datasource=sales,
            right_datasource=items,
            join_type=JoinType.LEFT_OUTER,
            concept_pairs=[
                ConceptPair(
                    left=_concept(USER),
                    right=_concept(USER),
                    existing_datasource=sales,
                )
            ],
        )
    ]
    assert guest_padded_addresses(merged) == {attr}
    grouped = _qds([attr], [attr], parents=[merged])
    grouped.source_map = {attr: {merged}}
    assert guest_padded_addresses(grouped) == {attr}
    assert guest_padded_addresses(items) == set()


def test_a_right_join_keyed_on_guest_padding_pads_no_left_input():
    """`lookup` RIGHT-joined on the item column a guest sale NULLs: the guest
    row finds no lookup row and is dropped, and a lookup row with no sale pads
    the sale's columns for its own reason, not the guest's."""
    attr = _ATTR.removeprefix("c~")
    sales = _scan("sales", [USER, ORDER], partial=[USER])
    sales.columns[0].modifiers.append(Modifier.NULLABLE)
    items = _scan("items", [USER, attr])
    lookup = _scan("lookup", [attr])
    merged = _qds(
        [USER, ORDER, attr], [USER, ORDER, attr], parents=[sales, items, lookup]
    )
    merged.source_map = {USER: {sales}, ORDER: {sales}, attr: {items, lookup}}
    merged.joins = [
        BaseJoin(
            left_datasource=sales,
            right_datasource=items,
            join_type=JoinType.LEFT_OUTER,
            concept_pairs=[
                ConceptPair(
                    left=_concept(USER), right=_concept(USER), existing_datasource=sales
                )
            ],
        ),
        BaseJoin(
            left_datasource=items,
            right_datasource=lookup,
            join_type=JoinType.RIGHT_OUTER,
            concept_pairs=[
                ConceptPair(
                    left=_concept(attr), right=_concept(attr), existing_datasource=items
                )
            ],
        ),
    ]
    assert guest_padded_addresses(merged) == set()
