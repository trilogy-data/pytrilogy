"""Discovery dead-ends where the requested concepts span unconnected models
should raise a `DisconnectedConceptsException` naming the disconnected subgraphs
and asking whether a join/merge is missing — not the opaque internal
"No remaining priority concepts" candidate-set dump.

Covers the pure subgraph partition helper plus an end-to-end query that two
unrelated models (no merge) cannot resolve.
"""

import pytest

from trilogy import Dialects
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_utility import (
    format_disconnected_subgraphs_error,
    identify_disconnected_subgraphs,
)


def _concept(
    name: str,
    grain: set[str] | None = None,
    pseudonyms: set[str] | None = None,
    derivation: Derivation = Derivation.ROOT,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        derivation=derivation,
        granularity=Granularity.MULTI_ROW,
        grain=BuildGrain(components=set(grain or set())),
        pseudonyms=set(pseudonyms or set()),
    )


def _addrs(subgraphs):
    return [sorted(c.address for c in group) for group in subgraphs]


def test_two_disconnected_models_partition():
    a_id = _concept("a_id", grain={"local.a_id"})
    sa = _concept("sa", grain={"local.a_id"}, derivation=Derivation.AGGREGATE)
    sb = _concept("sb", grain={"local.b_id"}, derivation=Derivation.AGGREGATE)

    subgraphs = identify_disconnected_subgraphs([a_id, sa, sb])

    assert _addrs(subgraphs) == [["local.a_id", "local.sa"], ["local.sb"]]


def test_single_connected_component():
    a_id = _concept("a_id", grain={"local.a_id"})
    sa = _concept("sa", grain={"local.a_id"}, derivation=Derivation.AGGREGATE)

    subgraphs = identify_disconnected_subgraphs([a_id, sa])

    assert _addrs(subgraphs) == [["local.a_id", "local.sa"]]


def test_pseudonym_bridges_subgraphs():
    # a merge surfaces as a pseudonym link; the two keys must collapse to one
    # connected subgraph (not reported as a missing join/merge).
    a_id = _concept("a_id", grain={"local.a_id"}, pseudonyms={"local.b_id"})
    sa = _concept("sa", grain={"local.a_id"}, derivation=Derivation.AGGREGATE)
    sb = _concept("sb", grain={"local.b_id"}, derivation=Derivation.AGGREGATE)

    subgraphs = identify_disconnected_subgraphs([a_id, sa, sb])

    assert len(subgraphs) == 1


def test_three_way_disconnect():
    a = _concept("a", grain={"local.a"})
    b = _concept("b", grain={"local.b"})
    c = _concept("c", grain={"local.c"})

    subgraphs = identify_disconnected_subgraphs([a, b, c])

    assert _addrs(subgraphs) == [["local.a"], ["local.b"], ["local.c"]]


def test_format_message_lists_subgraphs_and_hint():
    a_id = _concept("a_id", grain={"local.a_id"})
    sb = _concept("sb", grain={"local.b_id"})

    message = format_disconnected_subgraphs_error([[a_id], [sb]])

    assert "2 disconnected" in message
    assert "{local.a_id}" in message
    assert "{local.sb}" in message
    assert "join or merge" in message


_DISCONNECTED_MODEL = """
key a_id int;
property a_id.av int;
datasource a (id: a_id, v: av) grain (a_id)
query '''select 1 id, 10 v union all select 2 id, 20 v''';

key b_id int;
property b_id.bv int;
datasource b (id: b_id, v: bv) grain (b_id)
query '''select 1 id, 100 v union all select 2 id, 200 v''';

auto sa <- sum(av) by a_id;
auto sb <- sum(bv) by b_id;
select a_id, sa, sb;
"""


def test_disconnected_query_raises_typed_exception():
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_DISCONNECTED_MODEL)
    message = str(exc.value)
    assert "Are you missing a join or merge" in message
    # the two unrelated models each form their own subgraph
    groups = {frozenset(g) for g in exc.value.subgraphs}
    assert frozenset({"local.a_id", "local.sa"}) in groups
    assert frozenset({"local.sb"}) in groups
