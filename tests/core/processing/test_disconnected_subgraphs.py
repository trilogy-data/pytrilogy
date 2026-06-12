"""Discovery dead-ends where the requested concepts span unconnected models
should raise a `DisconnectedConceptsException` naming the disconnected subgraphs
and asking whether a join/merge is missing — not the opaque internal
"No remaining priority concepts" candidate-set dump.

Covers the message formatter plus end-to-end queries that two unrelated models
(no merge) cannot resolve. The partition logic itself (`disconnected_components`,
graph-reachability based) is exercised end-to-end here and in
`test_disconnected_components_e2e.py`.
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


_DISCONNECTED_ROOT_PROPERTIES = """
key cust int;
property cust.cname string;
key prod int;
property prod.pname string;
datasource customers (cust: cust, cname: cname) grain (cust)
query '''select 1 as cust, 'a' as cname''';
datasource products (prod: prod, pname: pname) grain (prod)
query '''select 1 as prod, 'b' as pname''';
select cname, pname;
"""


def test_disconnected_root_properties_raise_typed_exception():
    # Distinct surface from the aggregate case above: a plain ROOT-property select
    # dead-ends in `source_query_concepts` (no priority exhaustion in
    # get_priority_concept), which previously emitted a bare "Could not resolve
    # connections" message. It must now name the disconnected subgraphs too.
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        eng.generate_sql(_DISCONNECTED_ROOT_PROPERTIES)
    assert "Are you missing a join or merge" in str(exc.value)
    groups = {frozenset(g) for g in exc.value.subgraphs}
    assert groups == {frozenset({"local.cname"}), frozenset({"local.pname"})}
