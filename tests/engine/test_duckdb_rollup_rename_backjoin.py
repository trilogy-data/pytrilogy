"""A pure rename must never be read by joining back onto a ROLLUP output.

`item_id as item_code` is grain-neutral, but when the rename landed in its own
contributor group (anchored on `item_sk`) while the ROLLUP landed in another
(anchored on `addr_sk`), the alias-promotion pass only accepted a target group
whose *group* derivation was AGGREGATE/WINDOW/GROUP_TO. A mixed group -- the
ROLLUP plus its own renamed dimensions -- classifies as BASIC, so promotion was
skipped and the FINAL joined the rollup output back to the item dimension on
`item_id` just to read the alias. `item_id` is not unique in that table (an
SCD), so every rollup row multiplied by its duplicate count (tpc-ds q18: 110,615
rows for 55,447 groups).

The gate is now the only thing that actually matters -- does some other
contributor already emit the aliased source -- so the rename rides that
contributor and no join is emitted.
"""

import pytest

from trilogy import Dialects

FIXTURE = """
key order_number int;
key item_sk int;
property item_sk.item_id string;
key demo_sk int;
property demo_sk.dependent_count int;
key customer_sk int;
key addr_sk int;
property addr_sk.country string;
property addr_sk.state string;
property addr_sk.county string;
property customer_sk.current_addr_sk int;

datasource lines (
    order_number: order_number,
    item_sk: item_sk,
    demo_sk: demo_sk,
    customer_sk: customer_sk)
grain (order_number, item_sk)
query '''
select 1 as order_number, 10 as item_sk, 100 as demo_sk, 900 as customer_sk union all
select 1, 11, 100, 900 union all
select 2, 10, 101, 901 union all
select 2, 11, 101, 901 union all
select 3, 10, 100, 902 union all
select 4, 11, 101, 900
''';

datasource customers (customer_sk: customer_sk, current_addr_sk: addr_sk)
grain (customer_sk)
query '''
select 900 as customer_sk, 200 as current_addr_sk union all
select 901, 201 union all
select 902, 202
''';

# i_item_id is an SCD business key: two surrogate rows share each item_id.
datasource items (item_sk: item_sk, item_id: item_id)
grain (item_sk)
query '''
select 10 as item_sk, 'I10' as item_id union all
select 11, 'I11' union all
select 12, 'I10' union all
select 13, 'I11'
''';

datasource demos (demo_sk: demo_sk, dependent_count: dependent_count)
grain (demo_sk)
query '''
select 100 as demo_sk, 2 as dependent_count union all
select 101, 5
''';

datasource addrs (addr_sk: addr_sk, country: country, state: state, county: county)
grain (addr_sk)
query '''
select 200 as addr_sk, 'US' as country, 'CA' as state, 'Alameda' as county union all
select 201, 'US', 'CA', 'Marin' union all
select 202, 'US', 'NY', 'Kings'
''';
"""

QUERY = """
auto row_dependent_count <- group(dependent_count) by order_number, item_sk;
select
    item_id as item_code,
    country as ctry,
    state as st,
    county as cty,
    avg(row_dependent_count) as avg_dc
by rollup (item_id, country, state, county);
"""

REFERENCE_SQL = """
WITH lines AS (
select 1 as order_number, 10 as item_sk, 100 as demo_sk, 900 as customer_sk union all
select 1, 11, 100, 900 union all
select 2, 10, 101, 901 union all
select 2, 11, 101, 901 union all
select 3, 10, 100, 902 union all
select 4, 11, 101, 900
), customers AS (
select 900 as customer_sk, 200 as current_addr_sk union all
select 901, 201 union all
select 902, 202
), items AS (
select 10 as item_sk, 'I10' as item_id union all
select 11, 'I11' union all
select 12, 'I10' union all
select 13, 'I11'
), demos AS (
select 100 as demo_sk, 2 as dependent_count union all
select 101, 5
), addrs AS (
select 200 as addr_sk, 'US' as country, 'CA' as state, 'Alameda' as county union all
select 201, 'US', 'CA', 'Marin' union all
select 202, 'US', 'NY', 'Kings'
), joined AS (
select i.item_id, a.country, a.state, a.county, d.dependent_count
from lines l
join items i on l.item_sk = i.item_sk
join demos d on l.demo_sk = d.demo_sk
join customers c on l.customer_sk = c.customer_sk
join addrs a on c.current_addr_sk = a.addr_sk
)
select item_id, country, state, county, avg(dependent_count)
from joined
group by rollup (item_id, country, state, county)
"""


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def _norm(rows):
    return sorted(
        (
            tuple(round(float(v), 6) if isinstance(v, float) else v for v in row)
            for row in rows
        ),
        key=lambda t: tuple((x is not None, str(x)) for x in t),
    )


def test_rollup_rename_does_not_join_back_to_dimension(executor):
    sql = executor.generate_sql(QUERY)[-1]
    final = sql[sql.rindex("\nSELECT") :]
    assert "JOIN" not in final.upper(), sql


def test_rollup_rename_matches_single_statement_oracle(executor):
    got = _norm(executor.execute_text(QUERY)[0].fetchall())
    expected = _norm(executor.execute_raw_sql(REFERENCE_SQL).fetchall())
    assert got == expected
    assert len(got) == len(set(got))


_PRELUDE = "auto rdc <- group(dependent_count) by order_number, item_sk;\n"
_DIMS = "item_id, country, state, county"
_RENAMED = "item_id as item_code, country as ctry, state as st, county as cty"
# Every ORDER BY is total: a tie under LIMIT would pick rows arbitrarily and
# make the comparison flap rather than report a defect.
_SHAPES = [
    "select {X}, avg(rdc) as m by rollup ({D});",
    "select {X}, avg(rdc) as m by cube ({D});",
    "select {X}, avg(rdc) as m;",
    "select {X}, sum(rdc) as m;",
    "select {X}, avg(rdc) as m, rank() over (order by avg(rdc) desc) as k;",
    (
        "select {X}, avg(rdc) as m,"
        " rank() over (partition by country order by avg(rdc) desc) as k;"
    ),
    (
        "select {X}, avg(rdc) as m, rank() over (order by avg(rdc) desc) as k"
        " by rollup ({D});"
    ),
    (
        "select {X}, grouping(item_id) as g1, grouping(county) as g2,"
        " avg(rdc) as m by rollup ({D});"
    ),
    "select {X}, avg(rdc) as m, sum(dependent_count) as t;",
    "select {X}, avg(rdc) as m having m > 0;",
    (
        "select {X}, avg(rdc) as m by rollup ({D}) order by m desc nulls first,"
        " item_id asc nulls first, country asc nulls first, state asc nulls first,"
        " county asc nulls first limit 6;"
    ),
]


@pytest.mark.parametrize("shape", _SHAPES)
def test_rename_never_changes_the_answer(executor, shape):
    """A pure rename is grain-neutral, so every shape must return exactly what
    its rename-free twin returns. This is the property the back-join broke."""
    renamed = executor.execute_text(_PRELUDE + shape.format(X=_RENAMED, D=_DIMS))[
        0
    ].fetchall()
    bare = executor.execute_text(_PRELUDE + shape.format(X=_DIMS, D=_DIMS))[
        0
    ].fetchall()
    assert _norm(renamed) == _norm(bare)
