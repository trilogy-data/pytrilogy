"""A bare aggregate function (the `<concept>.<aggregate>` shorthand) resolves to
the statement grain at build, exactly like the authored aggregate."""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import FunctionType, Purpose
from trilogy.core.models.author import Function
from trilogy.core.models.build import BuildAggregateWrapper
from trilogy.core.models.core import DataType
from trilogy.executor import Executor
from trilogy.parsing.common import function_to_concept

_SINGLE = """
key nation_id int;
key supplier_id int;
property supplier_id.balance int;

datasource supplier (
    s_suppkey: supplier_id,
    s_nationkey: nation_id,
    s_balance: balance,
)
grain (supplier_id)
query '''
select * from (values (1, 10, 5), (2, 10, 7), (3, 10, 1), (4, 20, 2), (5, 20, 9), (6, 30, 4))
    t(s_suppkey, s_nationkey, s_balance)
''';
"""

_JOINED = """
key nation_id int;
key customer_id int;
key order_id int;

datasource customer (
    c_custkey: customer_id,
    c_nationkey: nation_id,
)
grain (customer_id)
query '''
select * from (values (1, 10), (2, 10), (3, 20)) t(c_custkey, c_nationkey)
''';

datasource orders (
    o_orderkey: order_id,
    o_custkey: customer_id,
)
grain (order_id)
query '''
select * from (values (100, 1), (101, 1), (102, 2), (103, 3), (104, 3))
    t(o_orderkey, o_custkey)
''';
"""


def _executor(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(model)
    return executor


def _rows(model: str, query: str) -> list[tuple]:
    return [tuple(r) for r in _executor(model).execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize(
    "model,shorthand,inline,expected",
    [
        (
            _SINGLE,
            "select supplier_id.count, nation_id.count;",
            "select count(supplier_id) as a, count(nation_id) as b;",
            [(6, 3)],
        ),
        (
            _JOINED,
            "select order_id.count, nation_id.count;",
            "select count(order_id) as a, count(nation_id) as b;",
            [(5, 2)],
        ),
        (
            _SINGLE,
            "select balance.sum, nation_id.count, balance.max;",
            "select sum(balance) as a, count(nation_id) as b, max(balance) as c;",
            [(28, 3, 9)],
        ),
        (
            _SINGLE,
            "select nation_id, supplier_id.count, balance.sum order by nation_id asc;",
            "select nation_id, count(supplier_id) as a, sum(balance) as b order by nation_id asc;",
            [(10, 3, 13), (20, 2, 11), (30, 1, 4)],
        ),
    ],
)
def test_aggregate_shorthand_matches_inline(
    model: str, shorthand: str, inline: str, expected: list[tuple]
):
    assert _rows(model, inline) == expected
    assert _rows(model, shorthand) == expected


def test_aggregate_shorthand_builds_like_the_authored_aggregate():
    executor = _executor(_SINGLE)
    executor.parse_text(
        "auto authored <- count(supplier_id); select supplier_id.count;"
    )
    built = executor.environment.materialize_for_select().concepts
    shorthand, authored = built["supplier_id.count"], built["authored"]
    assert isinstance(shorthand.lineage, BuildAggregateWrapper)
    assert shorthand.lineage == authored.lineage
    assert shorthand.canonical_address == authored.canonical_address


def test_bare_aggregate_function_concepts_resolve_at_build():
    """Any producer of a bare aggregate function, not only the shorthand."""
    executor = _executor(_SINGLE)
    environment = executor.environment
    for name, key in (("suppliers", "supplier_id"), ("nations", "nation_id")):
        environment.add_concept(
            function_to_concept(
                parent=Function(
                    operator=FunctionType.COUNT,
                    arguments=[environment.concepts[key].reference],
                    output_datatype=DataType.INTEGER,
                    output_purpose=Purpose.METRIC,
                ),
                name=name,
                environment=environment,
            )
        )
    assert [
        tuple(r) for r in executor.execute_text("select suppliers, nations;")[-1]
    ] == [(6, 3)]
    by_nation = "select nation_id, suppliers order by nation_id asc;"
    assert [tuple(r) for r in executor.execute_text(by_nation)[-1]] == [
        (10, 3),
        (20, 2),
        (30, 1),
    ]
