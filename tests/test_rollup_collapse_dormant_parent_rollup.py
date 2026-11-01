from trilogy import Dialects, Environment

# #834: a summary at (city, species), pinned to one city, already holds the
# per-species count; the outer aggregate folding into that scan must not
# re-sum it inside its own aggregate.
MODEL = """
key city string;
key tree_id int;
key species string;
property species.common_names list<string>;
datasource enrich (species, common_names) grain (species)
query '''select 'Acer rubrum' as species, ['red maple'] as common_names
union all select 'Acer saccharum', ['sugar maple']
union all select 'Opuntia ficus', []''';
datasource trees (tree_id, city, species) grain (tree_id)
query '''select 1 as tree_id, 'A' as city, 'Acer rubrum' as species
union all select 2, 'A', 'Acer rubrum'
union all select 3, 'A', 'Acer saccharum'
union all select 4, 'A', 'Opuntia ficus'
union all select 5, 'B', 'Opuntia ficus' ''';
property species.zone <- CASE WHEN len(common_names) > 0 THEN 'named' ELSE 'unnamed' END::string;
"""

SUMMARY = """
auto rollup_count <- count(tree_id);
datasource summary_species (city, species, rollup_count) grain (city, species)
query '''select city, species, count(*) as rollup_count from (
 select 'A' as city, 'Acer rubrum' as species union all select 'A', 'Acer rubrum'
 union all select 'A', 'Acer saccharum' union all select 'A', 'Opuntia ficus'
 union all select 'B', 'Opuntia ficus') group by 1, 2''';
"""

QUERY = (
    "SELECT count(species ? count(tree_id) by species > 0) as u "
    "WHERE city = 'A' and zone = 'named';"
)


def answer(model: str) -> tuple[str, list]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(QUERY)[-1]
    return sql, executor.execute_raw_sql(sql).fetchall()


def test_summary_count_under_city_and_derived_property_does_not_nest():
    sql, rows = answer(MODEL + SUMMARY)
    assert "summary_species" in sql
    assert rows == answer(MODEL)[1] == [(2,)]
