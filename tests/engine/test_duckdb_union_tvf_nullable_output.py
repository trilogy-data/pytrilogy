"""A `union(...)` TVF output inherits its arms' nullability.

The arms of a union are rows of one column, not sides of a join, so a stacked
column carries NULL wherever any arm's does. Read as non-nullable, sibling
aggregates over the union rejoin on their shared group keys with plain `=` and
silently drop the NULL group (q66: the warehouse with no name vanished).

Fixture: three warehouses, one with a NULL name, summarised per month out of
two fact tables.
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key wh_sk int;
property wh_sk.wh_name string?;

datasource warehouses (s: wh_sk, n: wh_name)
grain (wh_sk)
query '''select 1 as s, 'alpha' as n
union all select 2, 'beta'
union all select 5, cast(null as varchar)''';

key ws_id int;
property ws_id.ws_month int;
property ws_id.ws_qty float;
property ws_id.ws_paid float;

datasource web_sales (i: ws_id, w: wh_sk, m: ws_month, q: ws_qty, p: ws_paid)
grain (ws_id)
query '''select 1 as i, 1 as w, 1 as m, 10.0 as q, 3.0 as p
union all select 2, 2, 1, 20.0, 4.0
union all select 3, 5, 1, 30.0, 5.0
union all select 4, 5, 2, 40.0, 6.0''';

key cs_id int;
property cs_id.cs_month int;
property cs_id.cs_qty float;
property cs_id.cs_paid float;

datasource catalog_sales (i: cs_id, w: wh_sk, m: cs_month, q: cs_qty, p: cs_paid)
grain (cs_id)
query '''select 10 as i, 1 as w, 1 as m, 1.0 as q, 1.0 as p
union all select 11, 5, 1, 2.0, 2.0''';
"""

UNION = """
with combined as union(
    (select wh_sk as u_sk, wh_name as u_name, ws_month as u_mon,
            sum(ws_qty) as u_sales, sum(ws_paid) as u_net),
    (select wh_sk as u_sk, wh_name as u_name, cs_month as u_mon,
            sum(cs_qty) as u_sales, sum(cs_paid) as u_net)
) -> (u_sk, u_name, u_mon, u_sales, u_net);
"""


def _select(*measures: str) -> str:
    return (
        "select combined.u_sk, combined.u_name, "
        + ", ".join(f"{m} as m{i}" for i, m in enumerate(measures))
        + " order by combined.u_sk asc;"
    )


JAN_SALES = "sum(combined.u_sales ? combined.u_mon = 1)"
FEB_SALES = "sum(combined.u_sales ? combined.u_mon = 2)"
ALL_SALES = "sum(combined.u_sales)"
JAN_NET = "sum(combined.u_net ? combined.u_mon = 1)"

SELECTS = {
    "one_aggregate": (
        _select(JAN_SALES),
        [(1, "alpha", 11.0), (2, "beta", 20.0), (5, None, 32.0)],
    ),
    "two_filters_one_measure": (
        _select(JAN_SALES, FEB_SALES),
        [(1, "alpha", 11.0, None), (2, "beta", 20.0, None), (5, None, 32.0, 40.0)],
    ),
    "filtered_and_unfiltered": (
        _select(JAN_SALES, ALL_SALES),
        [(1, "alpha", 11.0, 11.0), (2, "beta", 20.0, 20.0), (5, None, 32.0, 72.0)],
    ),
    "two_measures": (
        _select(JAN_SALES, JAN_NET),
        [(1, "alpha", 11.0, 4.0), (2, "beta", 20.0, 4.0), (5, None, 32.0, 7.0)],
    ),
}


@pytest.mark.parametrize("shape", sorted(SELECTS))
@pytest.mark.parametrize("declare_nullable", [False, True])
def test_null_named_warehouse_survives_sibling_aggregates(shape, declare_nullable):
    body, expected = SELECTS[shape]
    signature = "(u_sk, u_name, u_mon, u_sales, u_net)"
    union = (
        UNION.replace(signature, signature.replace("u_name", "u_name string?"))
        if declare_nullable
        else UNION
    )
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = [
        tuple(
            float(v) if isinstance(v, (int, float)) and i > 1 else v
            for i, v in enumerate(row)
        )
        for row in executor.execute_query(union + body).fetchall()
    ]
    assert rows == expected
