"""Regressions for `Cost.filtered_dominated_le` and the shapes its law cases
found: a coarser summary applying the WHERE beats a `complete where` table,
a literal `in` list reads the summary, a species property in the WHERE keeps
the summary, and a `partial datasource` never bridges two other sources."""

from trilogy import Dialects

TREES = """
select 1 as tree_id, 'A' as city, 'Acer' as species
union all select 2, 'A', 'Acer'
union all select 3, 'A', 'Quercus'
union all select 4, 'B', 'Tilia'
"""

BASE = f"""
key tree_id int;
key city string;
key species string;
property species.form string;
auto n_trees <- count(tree_id);
property species.form_bucket <- case when form = 'broad' then 'B' else 'O' end;

datasource trees (tree_id, city, species) grain (tree_id)
query '''{TREES}''';

datasource species_dim (species, form) grain (species)
query '''select 'Acer' as species, 'broad' as form union all select 'Quercus', 'broad'
union all select 'Tilia', 'linden' union all select 'Ulmus', 'elm' ''';
"""

PARTITION = f"""
partial datasource trees_a (tree_id, city, species) grain (tree_id)
complete where city = 'A'
query '''select * from ({TREES}) where city = 'A' ''';
"""

SUMMARY = f"""
datasource summary (city, species, n: n_trees) grain (city, species)
query '''select city, species, count(*) as n from ({TREES}) group by 1, 2''';
"""

CITY_SUMMARY = f"""
datasource city_summary (city, n: n_trees) grain (city)
query '''select city, count(*) as n from ({TREES}) group by 1''';
"""

PARTIAL_SUMMARY = f"""
partial datasource summary_a (city, species, n: n_trees) grain (city, species)
complete where city = 'A'
query '''select city, species, count(*) as n from ({TREES}) where city = 'A' group by 1, 2''';
"""


def _run(model: str, query: str) -> tuple[str, list[tuple]]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    sql = executor.generate_sql(query)[-1]
    return sql, sorted(
        (tuple(r) for r in executor.execute_raw_sql(sql).fetchall()), key=repr
    )


def _reads(model: str, query: str) -> str:
    _, expected = _run(BASE, query)
    sql, rows = _run(model, query)
    assert rows == expected, sql
    return sql


def test_summary_beats_city_partition_for_a_lookup():
    sql = _reads(BASE + PARTITION + SUMMARY, "select species, form where city = 'A';")
    assert '"summary"' in sql and '"trees_a"' not in sql, sql


def test_in_list_reads_the_summary():
    sql = _reads(BASE + SUMMARY, "select species where city in ('A', 'B');")
    assert '"summary"' in sql and '"trees"' not in sql, sql


def test_species_property_filter_keeps_the_summary():
    sql = _reads(
        BASE + PARTITION + SUMMARY,
        "select species where city = 'A' and form_bucket = 'B';",
    )
    assert '"summary"' in sql and '"trees' not in sql, sql


def test_partial_summary_never_bridges():
    for where in ("city in ('A', 'B')", "city = 'B'"):
        _reads(
            BASE + CITY_SUMMARY + PARTIAL_SUMMARY,
            f"select form where {where};",
        )
