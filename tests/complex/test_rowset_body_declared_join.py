"""A rowset read inside another select's body pairs with that body's base
concepts only through a join declared IN that body: a rowset's body
(`with b as select rs.k, cat subset join rs.k = oid`) or a membership
subquery (`oid in (select rs.k subset join rs.k = oid where cat = 'a')`).
Without it the body refuses, and the refusal says where the join goes."""

import pytest

from tests.helpers.rows import executor_for, sorted_rows
from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.environment import Environment

_MODEL = """
key oid int;
property oid.cat string;
property oid.amt float;
datasource orders (o: oid, c: cat, a: amt) grain (oid)
query '''select 1 o, 'a' c, 10.0 a union all select 2 o, 'a' c, 30.0 a
    union all select 3 o, 'b' c, 5.0 a''';

with rs as where amt > 6 select oid as k, amt as v;
"""


def _rows(query: str) -> list[tuple]:
    return sorted_rows(executor_for(_MODEL), query)


def _refusal(query: str) -> str:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    with pytest.raises(DisconnectedConceptsException) as exc:
        executor.generate_sql(_MODEL + query)
    return str(exc.value)


def test_body_declared_join_pairs_a_handle_with_a_base_concept():
    rows = _rows(
        "with b as select rs.k, cat subset join rs.k = oid; select b.rs.k, b.cat;"
    )
    assert rows == [(1, "a"), (2, "a")]


def test_body_without_the_join_refuses_naming_the_body():
    message = _refusal("with b as select rs.k, cat; select b.rs.k, b.cat;")
    assert "inside the body of `b`" in message, message
    assert "`subset join rs.k = oid`" in message, message


def test_membership_subquery_declares_its_own_join():
    rows = _rows(
        "select oid where oid in (select rs.k subset join rs.k = oid where cat = 'a');"
    )
    assert rows == [(1,), (2,)]


def test_membership_subquery_without_the_join_refuses_naming_the_subquery():
    message = _refusal("select oid where oid in (select rs.k where cat = 'a');")
    assert "inside the subquery" in message, message
    assert "`subset join rs.k = oid`" in message, message
