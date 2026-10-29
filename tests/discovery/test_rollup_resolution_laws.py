"""`formal/Trilogy/Rollup.lean`: a summary answers a rollable aggregate
(`sum`, `count`, `min`, `max`, `bool_and`, `bool_or`) whenever
every filter and target column is a function of its grain, and the answer is
the one the base rows give. Each case runs with and without the summary in
scope; the summary must be read exactly when the rule says it can be."""

import itertools

import pytest

from trilogy import Dialects

TREES = """
select 'a1' as tree_id, 'A' as city, 'Acer' as species, 3 as height, 'x' as tag,
  false as tall
union all select 'a2', 'A', 'Acer', null, 'y', null
union all select 'a3', 'A', 'Quercus', 5, 'x', true
union all select 'a4', 'A', 'Ulmus', null, 'x', null
union all select 'b1', 'B', 'Acer', 7, 'y', true
union all select 'b2', 'B', 'Tilia', 2, 'x', false
"""

BASE = f"""
key tree_id string;
key species string;
key city string;
property tree_id.height int;
property tree_id.tag string;
property tree_id.tall bool;
property species.tree_form string;

datasource trees (tree_id: tree_id, city: city, species: species, height: ?height, tag: tag,
    tall: ?tall)
grain (tree_id)
query '''{TREES}''';

datasource enrichment (species: species, tree_form: ?tree_form)
grain (species)
query '''select 'Acer' as species, 'broadleaf' as tree_form
union all select 'Quercus', 'broadleaf'
union all select 'Ulmus', null
union all select 'Tilia', 'linden' ''';
"""

SUMMARY = f"""
auto summary_count <- count(tree_id);
auto summary_height <- sum(height);
auto summary_min <- min(height);
auto summary_max <- max(height);
auto summary_any_tall <- bool_or(tall);
auto summary_all_tall <- bool_and(tall);
datasource tree_summary (
    city: city,
    species: species,
    n: summary_count,
    h: ?summary_height,
    lo: ?summary_min,
    hi: ?summary_max,
    any_tall: ?summary_any_tall,
    all_tall: ?summary_all_tall
)
grain (city, species)
query '''select city, species, count(tree_id) as n, sum(height) as h,
min(height) as lo, max(height) as hi, bool_or(tall) as any_tall,
bool_and(tall) as all_tall
from ({TREES}) group by all''';
"""

GRAINS = {
    "total": [],
    "city": ["city"],
    "species": ["species"],
    "form": ["tree_form"],
    "form_case": ["case when tree_form = 'broadleaf' then 'b' else 'o' end as fc"],
    "city_species": ["city", "species"],
}

FILTERS = {
    "none": None,
    "city": "city = 'A'",
    "species": "species = 'Acer'",
    "form": "tree_form = 'broadleaf'",
    "form_null": "tree_form is null",
    "empty": "city = 'Z'",
    "city_and_species": "city = 'A' and species != 'Tilia'",
    "city_or_form": "city = 'B' or tree_form = 'linden'",
}

AGGREGATES = {
    "count": "count(tree_id) as v",
    "sum": "sum(height) as v",
    "min": "min(height) as v",
    "max": "max(height) as v",
    "bool_or": "bool_or(tall) as v",
    "bool_and": "bool_and(tall) as v",
}

# Shapes the summary cannot serve: a filter or aggregate input below its grain,
# an aggregate that does not re-aggregate.
UNSERVABLE = {
    "row_filter": "select species, count(tree_id) as v where tag = 'x' order by species;",
    "row_filter_total": "select count(tree_id) as v where height > 2;",
    "avg": "select species, avg(height) as v where city = 'A' order by species;",
    "count_distinct": "select count_distinct(height) as v where city = 'A';",
    "max_row_filter": "select city, max(height) as v where tag = 'x' order by city;",
    "uncarried_sibling": (
        "select species, count(tree_id) as v, count(tag) as t where city = 'A' "
        "order by species;"
    ),
}

# An aggregate inside a larger demand rolls up at its own grain.
NESTED = {
    "window": (
        "select species, rank(species) over (order by count(tree_id) desc, species asc)"
        " as r where city = 'A' order by species;"
    ),
    "window_beside_count": (
        "select species, count(tree_id) as v,"
        " rank(species) over (order by count(tree_id) by species desc, species asc)"
        " as r where city = 'A' order by species;"
    ),
    "sum_of_counts": "select sum(count(tree_id) by species) as v where city = 'A';",
    "expression": "select city, sum(height) + count(tree_id) as v order by city;",
    "ratio_total": "select sum(height) / count(tree_id) as v where species = 'Acer';",
    "range": "select tree_form, max(height) - min(height) as v order by tree_form;",
    "window_over_max": (
        "select species, rank(species) over (order by max(height) desc, species asc)"
        " as r where city = 'A' order by species;"
    ),
    "max_of_mins": "select max(min(height) by species) as v where city = 'A';",
    "mixed": (
        "select city, count(tree_id) as n, min(height) as lo, bool_or(tall) as t"
        " where tree_form is not null order by city;"
    ),
    "max_beside_avg": "select city, max(height) as v, avg(height) as a order by city;",
}


def _select(
    grain: list[str], aggregate: str, where: str | None, having: str = ""
) -> str:
    columns = ", ".join([*grain, aggregate])
    clauses = f" where {where}" if where else ""
    clauses += f" having {having}" if having else ""
    order = (
        " order by " + ", ".join("fc" if " as fc" in c else c for c in grain)
        if grain
        else ""
    )
    return f"select {columns}{clauses}{order};"


def _run(model: str, query: str) -> tuple[str, list[tuple]]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    sql = executor.generate_sql(query)[-1]
    return sql, [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]


def _assert_same_answer(query: str, served: bool) -> None:
    _, without = _run(BASE, query)
    sql, with_summary = _run(BASE + SUMMARY, query)
    assert with_summary == without, sql
    assert ("tree_summary" in sql) == served, sql


@pytest.mark.parametrize(
    "grain,where,aggregate",
    list(itertools.product(GRAINS, FILTERS, AGGREGATES)),
)
def test_summary_serves_every_satisfiable_query(grain: str, where: str, aggregate: str):
    _assert_same_answer(
        _select(GRAINS[grain], AGGREGATES[aggregate], FILTERS[where]), served=True
    )


@pytest.mark.parametrize("grain,where", list(itertools.product(GRAINS, FILTERS)))
def test_having_reads_rolled_up_value(grain: str, where: str):
    _assert_same_answer(
        _select(
            GRAINS[grain],
            "max(height) as v, count(tree_id) as n",
            FILTERS[where],
            having="n > 1",
        ),
        served=True,
    )


@pytest.mark.parametrize("name", sorted(NESTED))
def test_nested_aggregate_rolls_up(name: str):
    _assert_same_answer(NESTED[name], served=True)


@pytest.mark.parametrize("name", sorted(UNSERVABLE))
def test_unservable_query_reads_base_rows(name: str):
    _assert_same_answer(UNSERVABLE[name], served=False)


@pytest.mark.parametrize("aggregate", sorted(AGGREGATES))
def test_empty_summary_grand_total_is_no_row(aggregate: str):
    model = BASE.replace(TREES, TREES + " limit 0") + SUMMARY.replace(
        "group by all", "group by all limit 0"
    )
    sql, rows = _run(model, f"select {AGGREGATES[aggregate]};")
    assert "tree_summary" in sql, sql
    assert rows == []
