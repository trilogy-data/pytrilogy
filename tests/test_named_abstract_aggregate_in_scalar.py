"""A named abstract aggregate reads like the inline aggregate it abbreviates.

`count(order_id) / len(region)` is a METRIC: the inline aggregate marks the
expression, its grain is the row inputs it names (`region`), and the select
groups the count by them. Spelled through a name (`order_count / len(region)`)
the reference carried no such mark. The expression came out a PROPERTY keyed on
`region`'s key, landed in the select grain, and the count -- resolved at that
grain -- grouped by the very expression being built: a RecursionError in the
build, not a plan. A CASE hid an inline aggregate the same way, its arms never
being searched.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import Purpose
from trilogy.parser import parse

_MODEL = """
key order_id int;
key customer_id int;
key order_date int;
property customer_id.region string;
auto order_count <- count(order_id);
auto half_count <- order_count / 2;
auto count_per_region_char <- order_count / len(region);

datasource orders (order_id, customer_id, order_date)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id, 1 as order_date
union all select 2, 101, 1
union all select 3, 102, 2
union all select 4, 103, 2
''';

datasource customers (customer_id, region)
grain (customer_id)
query '''
select 101 as customer_id, 'east' as region
union all select 102, 'east'
union all select 103, 'we'
''';
"""

_SHAPES = [
    pytest.param(
        "order_count / len(region)",
        "count(order_id) / len(region)",
        [(1, 0.5), (2, 0.25), (2, 0.5)],
        id="function",
    ),
    pytest.param(
        "case when order_count > len(region) then 1 else 0 end",
        "case when count(order_id) > len(region) then 1 else 0 end",
        [(1, 0), (2, 0)],
        id="case",
    ),
    pytest.param(
        "half_count / len(region)",
        "(count(order_id) / 2) / len(region)",
        [(1, 0.25), (2, 0.125), (2, 0.25)],
        id="through_a_named_scalar",
    ),
    pytest.param(
        "count_per_region_char",
        "count(order_id) / len(region)",
        [(1, 0.5), (2, 0.25), (2, 0.5)],
        id="declared",
    ),
    pytest.param(
        "order_count + order_date",
        "count(order_id) + order_date",
        [(1, 3), (2, 4)],
        id="grain_key_operand",
    ),
]


def _rows(expression: str) -> list[tuple]:
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MODEL)
    query = f"select order_date, {expression} -> x order by order_date asc, x asc;"
    return [tuple(r) for r in engine.execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize("named,inline,expected", _SHAPES)
def test_named_aggregate_matches_inline_rows(
    named: str, inline: str, expected: list[tuple]
):
    assert _rows(inline) == expected
    assert _rows(named) == expected


@pytest.mark.parametrize("named,inline,expected", _SHAPES)
def test_named_aggregate_matches_inline_shape(
    named: str, inline: str, expected: list[tuple]
):
    shapes = []
    for expression in (named, inline):
        env = Environment()
        parse(f"{_MODEL}\nselect order_date, {expression} -> x;", env)
        x = env.concepts["local.x"]
        shapes.append((x.purpose, x.grain, x.keys, x.granularity))
    assert shapes[0] == shapes[1]
    assert shapes[0][0] == Purpose.METRIC


# A declared `property` over an aggregate expression is rejected; wrapping the
# aggregates in a CASE must not smuggle one past that check.
@pytest.mark.parametrize(
    "expression",
    [
        "count(order_id) / len(region)",
        "case when count(order_id) > 1 then 1 else 0 end",
        "case when order_count > 1 then 1 else 0 end",
    ],
)
def test_declared_property_over_aggregate_is_rejected(expression: str):
    with pytest.raises(Exception, match="does not match declared purpose"):
        parse(f"{_MODEL}\nproperty customer_id.flag <- {expression};", Environment())
