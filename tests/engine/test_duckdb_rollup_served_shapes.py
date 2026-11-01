"""Shapes a (city, species) count summary answers, read from it and re-summed
(issue #803): a ratio over the grand total beside the per-species count, a
running share over a species-grain rank, and a count of the members that have
rows. Each must give the answer the per-city tree files give."""

import pytest

from trilogy import Dialects, Environment

TREES = """
select * from (values
  ('a1','A','Acer rubrum'), ('a2','A','Acer rubrum'), ('a3','A','Quercus alba'),
  ('a4','A','Unknown'), ('a5','A','Acer saccharum'),
  ('b1','B','Acer rubrum'), ('b2','B','Tilia cordata')
) t(tree_id, city, species)
"""

MODEL = f"""
key tree_id string;
key species string;
key city string;
property species.tree_form string;

partial datasource trees_a (tree_id: tree_id, city: city, species: species)
grain (tree_id) complete where city = 'A'
query '''select * from ({TREES}) where city = 'A' ''';
partial datasource trees_b (tree_id: tree_id, city: city, species: species)
grain (tree_id) complete where city = 'B'
query '''select * from ({TREES}) where city = 'B' ''';
root datasource enrichment (species: species, tree_form: ?tree_form)
grain (species)
query '''select * from (values ('Acer rubrum','broadleaf'),('Quercus alba','broadleaf'),
  ('Tilia cordata','broadleaf'),('Acer saccharum','broadleaf')) t(species, tree_form)''';

auto dominance_rank <- rank(species) over (order by count(tree_id) by species desc, species asc);
auto cumulative_share <- (sum count(tree_id) by species order by dominance_rank asc) / (count(tree_id) by *);
"""

SUMMARY = f"""
auto rollup_tree_count <- count(tree_id);
root datasource tree_summary (city: city, species: species, n: rollup_tree_count)
grain (city, species)
query '''select city, species, count(tree_id) as n from ({TREES}) group by all''';
"""

CITY_A = [
    ("Acer rubrum", 2, 0.4),
    ("Acer saccharum", 1, 0.2),
    ("Quercus alba", 1, 0.2),
    ("Unknown", 1, 0.2),
]

FILTERED = [
    pytest.param(
        "select species, count(tree_id) as tree_count, "
        "count(tree_id) / (count(tree_id) by *) as pct "
        "where city = 'A' order by tree_count desc, species asc;",
        CITY_A,
        id="species_pct",
    ),
    pytest.param(
        "select species, count(tree_id) as tree_count, "
        "(count(tree_id) by *) + 1 as total "
        "where city = 'A' order by tree_count desc, species asc;",
        [(s, n, 6) for s, n, _ in CITY_A],
        id="total_expression",
    ),
    pytest.param(
        "select cumulative_share as top_share where city = 'A' "
        "having dominance_rank = 2;",
        [(0.6,)],
        id="share_city",
    ),
    pytest.param(
        "select dominance_rank, cumulative_share as share where city = 'A' "
        "order by dominance_rank;",
        [(1, 0.4), (2, 0.6), (3, 0.8), (4, 1.0)],
        id="dominance_curve",
    ),
    pytest.param(
        "select count(species ? count(tree_id) by species > 0) as unique_species "
        "where city = 'A';",
        [(4,)],
        id="unique_species",
    ),
]


def _sql(model: str, query: str) -> str:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(model)
    return executor.generate_sql(query)[-1]


def _rows(sql: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    return [tuple(row) for row in executor.execute_raw_sql(sql).fetchall()]


@pytest.mark.parametrize("query,expected", FILTERED)
def test_answer_without_summary(query, expected):
    assert _rows(_sql(MODEL, query)) == expected


@pytest.mark.parametrize("query,expected", FILTERED)
def test_summary_serves(query, expected):
    sql = _sql(MODEL + SUMMARY, query)
    assert '"tree_summary"' in sql, sql
    assert "trees_a" not in sql, sql
    assert _rows(sql) == expected, sql


def test_unfiltered_share_reads_summary():
    sql = _sql(
        MODEL + SUMMARY,
        "select cumulative_share as top_share having dominance_rank = 2;",
    )
    assert '"tree_summary"' in sql, sql
    assert _rows(sql) == [(4 / 7,)], sql
