"""Lock: a dimension reached through a foreign key OFF a composite fact grain
re-attaches to an aggregate at that grain.

`sales` is keyed `(item_id, ticket)` and binds `customer_id` beside it, so the
pair determines `region` jointly while neither key does alone.

An aggregate over `sales` is already at that grain, so it hosts `region` on the
fact read it makes anyway. One over the finer `sale_lines` truly reduces and
peels `region` onto its own `sales` scan; that scan kept a merge key only when
a single key determined an output, so it carried none and the FINAL merge was
a keyless join.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key item_id int;
property item_id.brand string;
key ticket int;
key customer_id int;
property customer_id.region string;
properties <ticket, item_id> (amount float);
key line int;
properties <ticket, item_id, line> (qty int);

datasource items (item_id, brand) grain (item_id)
query '''select 1 as item_id, 'acme' as brand union all select 2, 'zed' ''';

datasource customers (customer_id, region) grain (customer_id)
query '''
select 101 as customer_id, 'east' as region
union all select 102, 'west'
union all select 103, 'north'
''';

datasource sales (ticket, item_id, ?customer_id, amount) grain (item_id, ticket)
query '''
select 1 as ticket, 1 as item_id, 101 as customer_id, 6.0 as amount
union all select 1, 2, 101, 4.0
union all select 2, 1, 102, 4.0
union all select 3, 2, null, 1.0
''';

datasource sale_lines (ticket, item_id, line, qty) grain (item_id, ticket, line)
query '''
select 1 as ticket, 1 as item_id, 1 as line, 2 as qty
union all select 1, 1, 2, 3
union all select 1, 2, 1, 1
union all select 2, 1, 1, 5
union all select 3, 2, 1, 7
''';
"""

_ORDER = "order by ticket asc, item_id asc"


def _engine():
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MODEL)
    return engine


def _rows(query: str) -> list[tuple]:
    return [tuple(r) for r in _engine().execute_text(query)[-1].fetchall()]


def _sales_reads(query: str) -> int:
    return _engine().generate_sql(query)[-1].count("102, 4.0")


@pytest.mark.parametrize("total", ["sum(amount)", "sum(amount) by item_id, ticket"])
def test_dimension_through_off_grain_foreign_key(total: str):
    assert _rows(f"select item_id, ticket, region, {total} as total {_ORDER};") == [
        (1, 1, "east", 6.0),
        (2, 1, "east", 4.0),
        (1, 2, "west", 4.0),
        (2, 3, None, 1.0),
    ]


def test_both_dimensions_beside_by_key_aggregate():
    query = (
        "select item_id, ticket, brand, region,"
        f" sum(amount) by item_id, ticket as total {_ORDER};"
    )
    assert _rows(query) == [
        (1, 1, "acme", "east", 6.0),
        (2, 1, "zed", "east", 4.0),
        (1, 2, "acme", "west", 4.0),
        (2, 3, "zed", None, 1.0),
    ]


@pytest.mark.parametrize("dims", ["region", "brand, region", "amount, region"])
def test_fact_grain_aggregate_hosts_its_dimensions(dims: str):
    query = f"select item_id, ticket, {dims}, sum(amount) as total;"
    assert _sales_reads(query) == 1


def test_reducing_aggregate_peels_onto_the_grain_scan():
    query = f"select item_id, ticket, region, sum(qty) as units {_ORDER};"
    assert _rows(query) == [
        (1, 1, "east", 5),
        (2, 1, "east", 1),
        (1, 2, "west", 5),
        (2, 3, None, 7),
    ]


# A scalar over the hosted dimension is a grouping key of the fact-grain
# aggregate; one over a PEELED dimension carries the peel key to the merge.
@pytest.mark.parametrize("total", ["sum(amount)", "sum(amount) by item_id, ticket"])
def test_scalar_over_hosted_dimension(total: str):
    query = (
        f"select item_id, ticket, upper(region) as shout, {total} as total {_ORDER};"
    )
    assert _sales_reads(query) == 1
    assert _rows(query) == [
        (1, 1, "EAST", 6.0),
        (2, 1, "EAST", 4.0),
        (1, 2, "WEST", 4.0),
        (2, 3, None, 1.0),
    ]


def test_scalar_over_peeled_dimension_keeps_the_peel_key():
    query = (
        "select customer_id, upper(region) as shout, sum(amount) as total"
        " order by customer_id asc nulls last;"
    )
    assert _rows(query) == [
        (101, "EAST", 10.0),
        (102, "WEST", 4.0),
        (103, "NORTH", None),
        (None, None, 1.0),
    ]
