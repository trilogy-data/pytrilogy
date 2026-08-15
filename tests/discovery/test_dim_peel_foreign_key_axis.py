"""Lock: a dimension peeled onto an entity key sources through that key's axis.

`user_center` binds `center_id` at user grain (one centre per user);
`centers` is center_id's own root table. Selecting an aggregate-derived
concept grouped by user beside `center_id` (or a centre attribute) peels the
centre columns into a `dim:user_id` bucket — but the bucket never demanded its
entity key, so the source search tie-broke to `centers`, which has no user
axis, and the FINAL merge degraded to FULL JOIN ON 1=1: every status row
paired with every centre, silently (trilogy-cloud
bug-pytrilogy-persist-grain-cross-join-2026-08).

Executed row assertions, not just SQL shape: the cross join produced valid SQL.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key user_id int;
key center_id int;
property center_id.center_name string;
property user_id.distance_km float;
key item_id int;
property item_id.order_date date;

datasource user_center (
    user_id: user_id,
    center_id: center_id,
    km: distance_km
)
grain (user_id)
query '''
select 10 user_id, 1 center_id, 5.0 km
union all select 11 user_id, 3 center_id, 7.0 km
''';

datasource centers (
    id: center_id,
    cname: center_name
)
grain (center_id)
query '''
select 1 id, 'a' cname
union all select 2 id, 'b' cname
union all select 3 id, 'c' cname
''';

datasource items (
    item_id: item_id,
    user_id: user_id,
    order_date: order_date
)
grain (item_id)
query '''
select 100 item_id, 10 user_id, date '2024-02-01' order_date
union all select 101 item_id, 10 user_id, date '2024-03-01' order_date
union all select 102 item_id, 11 user_id, date '2024-01-15' order_date
''';

auto first_order <- min(order_date) by user_id;
auto last_order <- max(order_date) by user_id;

auto customer_status <- case
    when first_order is null then 'Prospect'
    when first_order = last_order then 'New'
    else 'Returning'
    end;
"""

# user 10 has two distinct order dates (Returning) and centre 1; user 11 has
# one (New) and centre 3. Centre 2 belongs to no user and must not appear —
# under the bug every status paired with every centre (2 users x 3 centres).
_CASES = [
    pytest.param(
        "SELECT customer_status, center_id ORDER BY center_id asc;",
        [("Returning", 1), ("New", 3)],
        id="foreign_key",
    ),
    pytest.param(
        "SELECT customer_status, center_name ORDER BY center_name asc;",
        [("Returning", "a"), ("New", "c")],
        id="foreign_property",
    ),
    pytest.param(
        "SELECT user_id, customer_status, center_id ORDER BY user_id asc;",
        [(10, "Returning", 1), (11, "New", 3)],
        id="foreign_key_with_axis",
    ),
]


def _engine():
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MODEL)
    return engine


@pytest.mark.parametrize("query,expected", _CASES)
def test_dim_peel_foreign_key_rows(query: str, expected: list[tuple]):
    rows = _engine().execute_text(query)[-1].fetchall()
    assert sorted((tuple(r) for r in rows), key=str) == sorted(expected, key=str)


@pytest.mark.parametrize("query,expected", _CASES)
def test_dim_peel_foreign_key_sql_shape(query: str, expected: list[tuple]):
    sql = _engine().generate_sql(query)[-1]
    assert "1=1" not in sql, sql
