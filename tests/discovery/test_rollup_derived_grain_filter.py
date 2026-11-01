"""A filter on a summary's derived grain column (`cell_lat <- round(latitude, 2)`)
is a predicate on the stored column: it must not pull the column's lineage roots
into the search, which joins the tree rows back and fans the count out (#829)."""

import pytest

from trilogy import Dialects

TREES = """
select 1 as tree_id, 'A' as city, 'Acer rubrum' as species, 37.771 as latitude
union all select 2, 'A', 'Acer rubrum', 37.772
union all select 3, 'B', 'Acer rubrum', 37.772
"""

MODEL = f"""
key city string;
key tree_id int;
key species string;
property tree_id.latitude float;

partial datasource trees_a (tree_id, city, species, latitude) grain (tree_id)
complete where city = 'A'
query '''select * from ({TREES}) where city = 'A' ''';

datasource trees_all (tree_id, city, species, latitude) grain (tree_id)
query '''{TREES}''';

auto rollup_count <- count(tree_id);
auto cell_lat <- round(latitude, 2);
datasource summary_cell (city, species, cell_lat, rollup_count)
grain (city, species, cell_lat)
query '''select city, species, round(latitude, 2) as cell_lat, count(*) as rollup_count
from ({TREES}) group by all''';
"""

CASES = {
    "unfiltered": ("select cell_lat, count(tree_id) as n;", [(37.77, 3)], True),
    "not_null": (
        "select cell_lat, count(tree_id) as n where cell_lat is not null;",
        [(37.77, 3)],
        True,
    ),
    "city_not_null": (
        "select cell_lat, count(tree_id) as n where city = 'A' and cell_lat is not null;",
        [(37.77, 2)],
        True,
    ),
    "filter_only": (
        "select count(tree_id) as n where cell_lat is not null;",
        [(3,)],
        True,
    ),
    "tree_rows": (
        "select tree_id where cell_lat is not null order by tree_id;",
        [(1,), (2,), (3,)],
        False,
    ),
}


@pytest.mark.parametrize("name", list(CASES))
def test_derived_grain_filter_reads_summary(name: str) -> None:
    query, expected, summary_only = CASES[name]
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql(query)[-1]
    rows = [
        tuple(float(v) if not isinstance(v, int) else v for v in row)
        for row in executor.execute_raw_sql(sql).fetchall()
    ]
    assert rows == expected, sql
    if summary_only:
        assert "trees_all" not in sql and "trees_a" not in sql, sql
