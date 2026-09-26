"""``_padding_sources`` names the sources whose own rows carry a join key as
join-analysis padding; the optimizer's value-set upgrade reads it to tell
shared padding (one source's rows arriving twice) from unrelated NULLs."""

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
    _padding_sources,
    _pads_for_different_members,
    _span_padding_matrix,
    _span_spellings,
    complete_key_domain,
    get_join_type,
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
_BOTH_NULLABLE = {_LEFT: [_AXIS], _RIGHT: [_AXIS]}


def _typed(padding: dict[str, dict[str, frozenset[str]]]) -> JoinType:
    return get_join_type(
        _LEFT,
        _RIGHT,
        partials={},
        nullables=_BOTH_NULLABLE,
        all_connecting_keys={_AXIS},
        span_padding=padding,
    )


def test_padding_for_different_spans_never_pairs():
    padding = {
        _LEFT: {_AXIS: frozenset({"local.product_id"})},
        _RIGHT: {_AXIS: frozenset({"local.user_id"})},
    }
    assert _pads_for_different_members(_LEFT, _RIGHT, {_AXIS}, padding)
    assert _typed(padding) == JoinType.FULL


def test_padding_for_a_shared_span_still_pairs():
    padding = {
        _LEFT: {_AXIS: frozenset({"local.user_id"})},
        _RIGHT: {_AXIS: frozenset({"local.user_id", "local.product_id"})},
    }
    assert not _pads_for_different_members(_LEFT, _RIGHT, {_AXIS}, padding)
    assert _typed(padding) == JoinType.INNER


def test_unattributed_padding_keeps_its_typing():
    padding = {_LEFT: {_AXIS: frozenset({"local.user_id"})}, _RIGHT: {}}
    assert not _pads_for_different_members(_LEFT, _RIGHT, {_AXIS}, padding)
    assert _typed(padding) == JoinType.INNER


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
        {"ds~merged": _extended_for(span)}, {"ds~merged": [ORDER]}, spellings, _identity
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
    typed = {
        "partials": {_RIGHT: [_SPAN]},
        "all_connecting_keys": {_SPAN},
        "extent_free_keys": {_SPAN},
    }
    complete = {_LEFT: {_SPAN}}
    assert (
        get_join_type(_LEFT, _RIGHT, nullables={}, complete_spans=complete, **typed)
        == JoinType.INNER
    )
    assert (
        get_join_type(
            _LEFT, _RIGHT, nullables={}, complete_spans={_LEFT: set()}, **typed
        )
        == JoinType.RIGHT_OUTER
    )
    assert (
        get_join_type(
            _LEFT,
            _RIGHT,
            nullables={_RIGHT: [_SPAN]},
            complete_spans=complete,
            **typed,
        )
        == JoinType.RIGHT_OUTER
    )


def test_region_holder_preserves_over_a_feeder_whose_value_null_pairs():
    """A value NULL the feeder carries on an attribute the holder carries
    nullable too pairs null-safely: the holder is preserved, not both. One on
    the region's own key, or one the holder cannot pair, keeps FULL."""
    typed = {
        "partials": {},
        "nullables": {_LEFT: [_ATTR], _RIGHT: [_ATTR]},
        "all_connecting_keys": {_SPAN, _ATTR},
        "region_holders": {_LEFT: {_SPAN}},
    }
    both = {_LEFT: [_ATTR], _RIGHT: [_ATTR]}
    assert get_join_type(_LEFT, _RIGHT, value_nullables=both, **typed) == (
        JoinType.LEFT_OUTER
    )
    assert get_join_type(_LEFT, _RIGHT, value_nullables={_RIGHT: [_ATTR]}, **typed) == (
        JoinType.FULL
    )
    on_key = {_LEFT: [_ATTR, _SPAN], _RIGHT: [_ATTR, _SPAN]}
    assert get_join_type(_LEFT, _RIGHT, value_nullables=on_key, **typed) == (
        JoinType.FULL
    )


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
        "partials": {_RIGHT: [_ATTR]},
        "nullables": {_LEFT: [_ATTR], _RIGHT: [_ATTR]},
        "all_connecting_keys": {_ATTR},
        "host_nodes": {_LEFT},
    }
    both = {_LEFT: [_ATTR], _RIGHT: [_ATTR]}
    assert get_join_type(_LEFT, _RIGHT, value_nullables=both, **typed) == (
        JoinType.LEFT_OUTER
    )
    assert get_join_type(_LEFT, _RIGHT, value_nullables={_RIGHT: [_ATTR]}, **typed) == (
        JoinType.FULL
    )
