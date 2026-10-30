"""A summary at grain (city, species) holding count(tree_id), read for a count
grouped by a CASE bucket over species properties (issue #794). The answer is
one row per bucket, re-summed from the summary rows, whether or not a scalar
of the bucket rides beside it or a WHERE on the bucket splits off its own
condition group. A `merge ... into species` makes species a join key
group's canonical; the summary still serves a count by it."""

import pytest

from trilogy import Dialects

TREES = """
select * from (values
  ('a1','A','Acer rubrum'),('a2','A','Acer rubrum'),('a3','A','Quercus alba'),
  ('a4','A','Tilia cordata'),('a5','A','Ulmus americana'),('b1','B','Acer rubrum')
) t(tree_id, city, species)
"""

MODEL = f"""
key tree_id string;
key species string;
key city string;
property species.tree_form string;
property species.lifespan int;

partial datasource trees_a (tree_id: tree_id, city: city, species: species)
grain (tree_id) complete where city = 'A'
query '''select * from ({TREES}) where city = 'A' ''';
partial datasource trees_b (tree_id: tree_id, city: city, species: species)
grain (tree_id) complete where city = 'B'
query '''select * from ({TREES}) where city = 'B' ''';
datasource all_trees (tree_id: tree_id, city: city, species: species)
grain (tree_id)
query '''{TREES}''';
root datasource enrichment (species: species, tree_form: ?tree_form, lifespan: ?lifespan)
grain (species)
query '''select * from (values
  ('Acer rubrum','broadleaf',10),('Quercus alba','broadleaf',200),
  ('Tilia cordata','broadleaf',null),('Ulmus americana','broadleaf',300)
) t(species, tree_form, lifespan)''';

auto lifespan_bucket <- CASE when lifespan is null then 'Unknown' when lifespan < 50 then 'Short' else 'Long' end::string;
"""

SUMMARY = f"""
auto rollup_tree_count <- count(tree_id);
root datasource tree_summary (city: city, species: species, n: rollup_tree_count)
grain (city, species)
query '''select city, species, count(tree_id) as n from ({TREES}) group by all''';
"""

COLOUR = "case lifespan_bucket when 'Long' then '#1' else '#2' end::string as colour"

BUCKETS = [("Long", 2), ("Short", 2), ("Unknown", 1)]
COLOURS = {"Long": "#1", "Short": "#2", "Unknown": "#2"}

CASES = [
    pytest.param(
        "select lifespan_bucket, count(tree_id) as tree_count "
        "where city = 'A' order by lifespan_bucket;",
        BUCKETS,
        id="bucket",
    ),
    pytest.param(
        "where city = 'A' and lifespan_bucket is not null "
        "select lifespan_bucket, count(tree_id) as tree_count order by lifespan_bucket;",
        BUCKETS,
        id="bucket_notnull",
    ),
    pytest.param(
        f"select lifespan_bucket, count(tree_id) as tree_count, {COLOUR} "
        "where city = 'A' order by lifespan_bucket;",
        [(b, n, COLOURS[b]) for b, n in BUCKETS],
        id="bucket_colour",
    ),
    pytest.param(
        "where city = 'A' and lifespan_bucket is not null "
        f"select lifespan_bucket, count(tree_id) as tree_count, {COLOUR} "
        "order by lifespan_bucket;",
        [(b, n, COLOURS[b]) for b, n in BUCKETS],
        id="bucket_colour_notnull",
    ),
]


def _sql(model: str, query: str) -> str:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    return executor.generate_sql(query)[-1]


def _rows(sql: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    return [tuple(row) for row in executor.execute_raw_sql(sql).fetchall()]


@pytest.mark.parametrize("query,expected", CASES)
def test_bucket_answer_without_summary(query, expected):
    assert _rows(_sql(MODEL, query)) == expected


@pytest.mark.parametrize("query,expected", CASES)
def test_bucket_answer_with_summary(query, expected):
    sql = _sql(MODEL + SUMMARY, query)
    assert _rows(sql) == expected, sql


@pytest.mark.parametrize("query,expected", CASES)
def test_bucket_reads_summary(query, expected):
    sql = _sql(MODEL + SUMMARY, query)
    assert '"tree_summary"' in sql, sql
    assert "sum(" in sql, sql


MERGED_SPECIES = """
key cluster_id string;
property tree_id.raw_species string;
property cluster_id.cluster_species string;
auto merged_species <- coalesce(raw_species, cluster_species);
merge merged_species into species;
"""

SPECIES_COUNTS = [
    ("Acer rubrum", 3),
    ("Quercus alba", 1),
    ("Tilia cordata", 1),
    ("Ulmus americana", 1),
]

MERGED_CASES = [
    pytest.param(
        "select species, count(tree_id) as tree_count order by species;",
        SPECIES_COUNTS,
        id="count",
    ),
    pytest.param(
        "select species, tree_form, count(tree_id) by species as tree_count "
        "order by tree_count desc, species asc limit 100;",
        [(s, "broadleaf", n) for s, n in SPECIES_COUNTS],
        id="spotlight",
    ),
]


@pytest.mark.parametrize("query,expected", MERGED_CASES)
def test_merged_key_reads_summary(query, expected):
    assert _rows(_sql(MODEL + MERGED_SPECIES, query)) == expected
    sql = _sql(MODEL + MERGED_SPECIES + SUMMARY, query)
    assert '"tree_summary"' in sql, sql
    assert _rows(sql) == expected, sql
