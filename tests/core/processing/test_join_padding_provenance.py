"""``_padding_sources`` names the sources whose own rows carry a join key as
join-analysis padding; the optimizer's value-set upgrade reads it to tell
shared padding (one source's rows arriving twice) from unrelated NULLs."""

from trilogy.core.enums import JoinType, Purpose, SourceType
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import BaseJoin, ConceptPair, QueryDatasource
from trilogy.core.processing.join_resolution import (
    _padding_sources,
    _pads_for_different_members,
    _span_padding_matrix,
    _span_spellings,
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
