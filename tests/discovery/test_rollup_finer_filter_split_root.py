"""A summary at (city, species) serves `count(tree_id)` under `where city = ...`
only when the filter lands on the summary's own scan. When bucketing splits the
rolled-up root away from the filter column (an empty target grain, or a window
pulling the fact rows into the root stream), the root must not be taken from
the summary: the filter would never reach it (issue #782)."""

import pytest

from trilogy import Dialects

TREES = """
select 'a1' as tree_id, 'A' as city, 'Acer rubrum' as species
union all select 'a2', 'A', 'Acer rubrum'
union all select 'a3', 'A', 'Quercus alba'
union all select 'b1', 'B', 'Acer rubrum'
union all select 'b2', 'B', 'Tilia cordata'
"""

BASE = f"""
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
query '''select 'Acer rubrum' as species, 'broadleaf' as tree_form
union all select 'Quercus alba', 'broadleaf'
union all select 'Tilia cordata', 'broadleaf' ''';

auto dominance_rank <- rank(species) over (order by count(tree_id) by species desc, species asc);
"""

ROLLUP = f"""
auto rollup_tree_count <- count(tree_id);
root datasource tree_rollup (city: city, species: species, n: rollup_tree_count)
grain (city, species)
query '''select city, species, count(tree_id) as n from ({TREES}) group by all''';
"""

CASES = {
    "total": (
        "select count(tree_id) as total_trees where city = 'A';",
        [(3,)],
    ),
    "dominance": (
        (
            "select dominance_rank, species, count(tree_id) as tree_count "
            "where city = 'A' having dominance_rank <= 50 order by dominance_rank asc;"
        ),
        [(1, "Acer rubrum", 2), (2, "Quercus alba", 1)],
    ),
    "total_two_aggs": (
        "select count(tree_id) as c, count(species) as s where city = 'A';",
        [(3, 2)],
    ),
    "unrolled_sibling_agg": (
        (
            "select species, count(tree_id) as c, count(city) as cc "
            "where city = 'A' order by species;"
        ),
        [("Acer rubrum", 2, 1), ("Quercus alba", 1, 1)],
    ),
    "window_over_count": (
        (
            "select species, count(tree_id) as c, "
            "rank(species) over (order by count(tree_id) desc, species asc) as r "
            "where city = 'A' order by species;"
        ),
        [("Acer rubrum", 2, 1), ("Quercus alba", 1, 2)],
    ),
    "rank_beside_property": (
        (
            "select species, tree_form, dominance_rank, count(tree_id) as c "
            "where city = 'A' order by species;"
        ),
        [("Acer rubrum", "broadleaf", 1, 2), ("Quercus alba", "broadleaf", 2, 1)],
    ),
    "by_form": (
        (
            "select tree_form, count(tree_id) as tree_count "
            "where city = 'A' and tree_form is not null order by tree_count desc;"
        ),
        [("broadleaf", 3)],
    ),
    "by_species": (
        "select species, count(tree_id) as tree_count where city = 'A' order by species;",
        [("Acer rubrum", 2), ("Quercus alba", 1)],
    ),
}

ROLLUP_SERVED = {"by_form", "by_species"}


def _run(model: str, query: str) -> tuple[str, list[tuple]]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    sql = executor.generate_sql(query)[-1]
    return sql, [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]


@pytest.mark.parametrize("name", sorted(CASES))
def test_rollup_in_scope_never_changes_answer(name: str):
    query, expected = CASES[name]
    _, without = _run(BASE, query)
    sql, with_rollup = _run(BASE + ROLLUP, query)
    assert without == expected
    assert with_rollup == expected, sql
    assert ("tree_rollup" in sql) == (name in ROLLUP_SERVED), sql


def test_unfiltered_species_count_served_by_rollup():
    sql, rows = _run(
        BASE + ROLLUP,
        "select species, tree_form, count(tree_id) by species as tree_count "
        "order by tree_count desc, species asc limit 100;",
    )
    assert "tree_rollup" in sql
    assert rows == [
        ("Acer rubrum", "broadleaf", 3),
        ("Quercus alba", "broadleaf", 1),
        ("Tilia cordata", "broadleaf", 1),
    ]
