"""A second named concept with a summary grain's lineage must not change how a
query on the summary's own concept plans (trilogy-data/pytrilogy#826)."""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key city string;
key tree_id int;
key species string;
property tree_id.plant_date date;
{before}
datasource trees_all (tree_id, city, species, plant_date) grain (tree_id)
query '''select 1 as tree_id, 'A' as city, 'x' as species, date '1990-05-01' as plant_date
union all select 2, 'B', 'y', date '2010-05-01'
union all select 3, 'B', 'y', date '2010-07-01' ''';
auto rollup_count <- count(tree_id);
auto plant_year <- year(plant_date);
datasource summary_year (city, species, plant_year, rollup_count) grain (city, species, plant_year)
query '''select * from (values ('A','x',1990,1),('B','y',2010,2)) t(city, species, plant_year, rollup_count)''';
{after}
"""

ALIAS = "auto planted_year <- year(plant_date);"
QUERY = "SELECT plant_year, count(tree_id) as n ORDER BY plant_year asc;"
EXPECTED = [(1990, 1), (2010, 2)]


@pytest.mark.parametrize(
    "before,after",
    [("", ""), (ALIAS, ""), ("", ALIAS)],
    ids=["no_alias", "alias_first", "alias_last"],
)
def test_declared_alias_does_not_break_summary(before: str, after: str):
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL.format(before=before, after=after))
    sql = executor.generate_sql(QUERY)[-1]
    assert "planted_year" not in sql
    assert "summary_year" in sql
    assert executor.execute_text(QUERY)[-1].fetchall() == EXPECTED


def test_select_alias_does_not_break_later_query():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL.format(before="", after=""))
    executor.generate_sql("SELECT year(plant_date) as planted_year WHERE city = 'A';")
    assert executor.execute_text(QUERY)[-1].fetchall() == EXPECTED
