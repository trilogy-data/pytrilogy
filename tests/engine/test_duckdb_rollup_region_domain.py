"""A `by rollup` over keys from dimensions a fact reaches through a partial `~?`
foreign key. A region domain joined back after the rollup pairs on the rolled-up
key, which is NULL on every subtotal row, so the padded rows must enter below
the rollup instead; a window over the rollup must carry the pass's full row
identity (every key plus the grouping flags) to rejoin its siblings."""

import pytest

from trilogy import Dialects

FIXTURE = """
key item_id int;
property item_id.category string?;
property item_id.cls string?;
key date_id int;
property date_id.year int;
key sale_id int;
property sale_id.qty int;

datasource items (item_id: item_id, category: category, cls: cls) grain (item_id)
query '''select 1 item_id, 'a' category, 'x' cls union all select 2, 'b', 'y'
union all select 3, 'b', 'z' union all select 4, null, null''';
datasource dates (date_id: date_id, year: year) grain (date_id)
query '''select 1 date_id, 2000 "year" union all select 2, 2000 union all select 3, 2001
union all select 4, 2002''';
datasource sales (sale_id: sale_id, item_id: item_id, date_id: ~?date_id, qty: qty)
grain (sale_id)
query '''select 1 sale_id, 1 item_id, 1 date_id, 5 qty union all select 2, 2, 2, 7
union all select 3, 3, 3, 11 union all select 4, 1, 3, 2 union all select 5, 2, null, 4
union all select 6, 4, 1, 3''';

auto total <- coalesce(sum(qty), 0);
auto rnk <- rank(category) over (partition by category order by total desc);
"""

JOINED = """
from (select 1 sale_id, 1 item_id, 1 date_id, 5 qty union all select 2, 2, 2, 7
union all select 3, 3, 3, 11 union all select 4, 1, 3, 2 union all select 5, 2, null, 4
union all select 6, 4, 1, 3) s
join (select 1 item_id, 'a' category, 'x' cls union all select 2, 'b', 'y'
union all select 3, 'b', 'z' union all select 4, null, null) i on s.item_id = i.item_id
join (select 1 date_id, 2000 "year" union all select 2, 2000 union all select 3, 2001
union all select 4, 2002) d on s.date_id = d.date_id
"""

CASES = [
    pytest.param(
        "select category as cat, year as yr, total where year = 2000"
        " by rollup (category, year);",
        f"""select category, "year", coalesce(sum(qty), 0) {JOINED}
        where "year" = 2000 group by rollup (category, "year")""",
        id="filtered_rollup",
    ),
    pytest.param(
        "select category as cat, year as yr, total, rnk where year = 2000"
        " by rollup (category, year);",
        f"""select category, "year", coalesce(sum(qty), 0) t,
        rank() over (partition by category order by coalesce(sum(qty), 0) desc)
        {JOINED} where "year" = 2000 group by rollup (category, "year")""",
        id="filtered_rollup_rank",
    ),
    pytest.param(
        "where year in (2000, 2001) select category as cat, year as yr, total, rnk"
        " by rollup (category, year);",
        f"""select category, "year", coalesce(sum(qty), 0) t,
        rank() over (partition by category order by coalesce(sum(qty), 0) desc)
        {JOINED} where "year" in (2000, 2001) group by rollup (category, "year")""",
        id="two_dimension_rollup_rank",
    ),
    pytest.param(
        "where year in (2000, 2001) select category as cat, cls as c, year as yr,"
        " total, rnk by rollup (category, cls, year);",
        f"""select category, cls, "year", coalesce(sum(qty), 0) t,
        rank() over (partition by category order by coalesce(sum(qty), 0) desc)
        {JOINED} where "year" in (2000, 2001) group by rollup (category, cls, "year")""",
        id="window_over_scalar_of_measure_aliased_keys",
    ),
]


def _norm(rows):
    return sorted(
        (tuple(row) for row in rows),
        key=lambda t: tuple((x is not None, str(x)) for x in t),
    )


@pytest.mark.parametrize("query,oracle", CASES)
def test_rollup_over_partial_dimension_keeps_subtotals(query, oracle):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(FIXTURE)
    sql = executor.generate_sql(query)[-1]
    got = _norm(executor.execute_raw_sql(sql).fetchall())
    assert got == _norm(executor.execute_raw_sql(oracle).fetchall()), sql
