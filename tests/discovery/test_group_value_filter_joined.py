"""`Rollup.group_value_filter_joined_exact`: a WHERE over a stored group value,
grouped by a dimension's column keyed on the summary's grain, filters the
summary rows, joins the dimension, then re-sums."""

from trilogy import Dialects

TREES = """
select 1 as tree_id, 'A' as city, 'Acer' as species, 3 as height
union all select 2, 'A', 'Quercus', 5
union all select 3, 'B', 'Acer', 7
union all select 4, 'B', 'Tilia', 2
"""

MODEL = f"""
key tree_id int;
key city string;
key species string;
property tree_id.height int;
property species.form string;
auto total_height <- sum(height);

datasource trees (tree_id, city, species, height) grain (tree_id)
query '''{TREES}''';

datasource species_dim (species, form) grain (species)
query '''select 'Acer' as species, 'broad' as form union all select 'Quercus', 'broad'
union all select 'Tilia', 'linden' ''';

datasource summary (city, species, h: total_height) grain (city, species)
query '''select city, species, sum(height) as h from ({TREES}) group by 1, 2''';
"""


def _run(query: str) -> tuple[str, list[tuple]]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql(query)[-1]
    return sql, [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]


def test_form_total_filters_summary_rows_before_resum():
    sql, rows = _run(
        "select form, sum(height) as v where sum(height) by city, species > 3"
        " order by form;"
    )
    assert "summary" in sql and "trees" not in sql, sql
    assert rows == [("broad", 12)]


def test_city_form_sum_reads_summary():
    sql, rows = _run(
        "select city, form, sum(height) as v where sum(height) by city, species < 5"
        " order by city, form;"
    )
    assert "trees" not in sql, sql
    assert rows == [("A", "broad", 3), ("B", "linden", 2)]
