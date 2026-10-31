"""A summary whose grain holds a derived column (`rollup_plant_year <-
year(plant_date)`) serves a count at that column: the stored year is read
beside the rolled count rather than rebuilt from the fact rows, which would
join them back onto the summary and fan it out (issue #803 E)."""

import pytest

from trilogy import Dialects

TREES = """
select 'a1' as tree_id, 'A' as city, 'Acer rubrum' as species, date '2001-05-01' as plant_date
union all select 'a2', 'A', 'Acer rubrum', date '2001-06-01'
union all select 'a3', 'A', 'Quercus alba', null
union all select 'b1', 'B', 'Acer rubrum', date '1999-01-01'
"""

BASE = f"""
key tree_id string;
key species string;
key city string;
property tree_id.plant_date date;

partial datasource trees_a (tree_id: tree_id, city: city, species: species, plant_date: ?plant_date)
grain (tree_id) complete where city = 'A'
query '''select * from ({TREES}) where city = 'A' ''';

partial datasource trees_b (tree_id: tree_id, city: city, species: species, plant_date: ?plant_date)
grain (tree_id) complete where city = 'B'
query '''select * from ({TREES}) where city = 'B' ''';

root datasource trees (tree_id: tree_id, city: city, species: species, plant_date: ?plant_date)
grain (tree_id)
query '''{TREES}''';

auto rollup_plant_year <- year(plant_date);
"""

ROLLUP = f"""
auto rollup_tree_count <- count(tree_id);
root datasource tree_rollup (city: city, species: species, plant_year: ?rollup_plant_year, n: rollup_tree_count)
grain (city, species, rollup_plant_year)
query '''select city, species, year(plant_date) as plant_year, count(tree_id) as n
from ({TREES}) group by all''';
"""

CASES = {
    "by_year": (
        (
            "select rollup_plant_year, count(tree_id) as n where city = 'A' "
            "order by rollup_plant_year;"
        ),
        [(2001, 2), (None, 1)],
    ),
    "by_year_expression": (
        "select year(plant_date) as y, count(tree_id) as n where city = 'A' order by y;",
        [(2001, 2), (None, 1)],
    ),
    "by_species_year": (
        (
            "select species, rollup_plant_year, count(tree_id) as n where city = 'A' "
            "order by species, rollup_plant_year;"
        ),
        [("Acer rubrum", 2001, 2), ("Quercus alba", None, 1)],
    ),
    "by_year_unfiltered": (
        "select rollup_plant_year, count(tree_id) as n order by rollup_plant_year;",
        [(1999, 1), (2001, 2), (None, 1)],
    ),
    "by_year_species_filter": (
        (
            "select rollup_plant_year, count(tree_id) as n where species = 'Acer rubrum' "
            "order by rollup_plant_year;"
        ),
        [(1999, 1), (2001, 2)],
    ),
    "finer_date_filter": (
        (
            "select rollup_plant_year, count(tree_id) as n "
            "where plant_date > '2001-05-15'::date order by rollup_plant_year;"
        ),
        [(2001, 1)],
    ),
}

ROLLUP_SERVED = {
    "by_year",
    "by_year_expression",
    "by_species_year",
    "by_year_unfiltered",
    "by_year_species_filter",
}


def _run(model: str, query: str) -> tuple[str, list[tuple]]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    sql = executor.generate_sql(query)[-1]
    return sql, [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]


@pytest.mark.parametrize("name", sorted(CASES))
def test_derived_grain_summary_answers(name: str):
    query, expected = CASES[name]
    _, without = _run(BASE, query)
    sql, with_rollup = _run(BASE + ROLLUP, query)
    assert without == expected
    assert with_rollup == expected, sql
    assert ("tree_rollup" in sql) == (name in ROLLUP_SERVED), sql
