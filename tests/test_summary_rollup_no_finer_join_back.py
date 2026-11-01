from trilogy import Dialects, Environment

# A count summary at (city, species, plant_year) must not be joined back to
# the tree rows to supply a key derived off them (`plant_year::int`): each
# summary row then repeats once per tree and the re-sum counts n*n.
MODEL = """
import std.date;
key tree_id int;
key city string;
key species string;
property tree_id.plant_date date;
auto plant_year <- year(plant_date);
datasource trees (tree_id, city, species, plant_date) grain (tree_id)
query '''select 1 tree_id, 'A' city, 'Acer' species, date '1991-01-01' plant_date
union all select 2, 'A', 'Acer', date '1991-06-01'
union all select 3, 'A', 'Quercus', date '1995-01-01' ''';
"""

SUMMARY = """
auto rollup_tree_count <- count(tree_id);
datasource tree_rollup_year (city, species, plant_year, rollup_tree_count)
grain (city, species, plant_year)
query '''select 'A' city, 'Acer' species, 1991 plant_year, 2 rollup_tree_count
union all select 'A', 'Quercus', 1995, 1''';
"""

QUERY = """
SELECT plant_year::int::year as planted_year, count(tree_id) as tree_count
WHERE city = 'A'
ORDER BY planted_year ASC;
"""


def answer(model: str) -> list[tuple]:
    env = Environment()
    env.parse(model)
    return (
        Dialects.DUCK_DB.default_executor(environment=env)
        .execute_text(QUERY)[-1]
        .fetchall()
    )


def test_year_summary_is_not_re_summed_per_tree():
    assert answer(MODEL + SUMMARY) == answer(MODEL) == [(1991, 2), (1995, 1)]
