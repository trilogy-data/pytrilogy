"""`formal/Trilogy/Rollup.lean`: a summary answers a rollable aggregate
(`sum`, `count`, `min`, `max`, `bool_and`, `bool_or`) whenever
every filter and target column is a function of its grain, and the answer is
the one the base rows give. Each case runs with and without the summary in
scope; the summary must be read exactly when the rule says it can be."""

import itertools
import re

import pytest

from trilogy import Dialects, parse
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.snowflake import SnowflakeDialect

TREES = """
select 'a1' as tree_id, 'A' as city, 'Acer' as species, 3 as height, 'x' as tag,
  false as tall
union all select 'a2', 'A', 'Acer', null, 'y', null
union all select 'a3', 'A', 'Quercus', 5, 'x', true
union all select 'a4', 'A', 'Ulmus', null, 'x', null
union all select 'b1', 'B', 'Acer', 7, 'y', true
union all select 'b2', 'B', 'Tilia', 2, 'x', false
"""

ENRICHMENT = """select 'Acer' as species, 'broadleaf' as tree_form
union all select 'Quercus', 'broadleaf'
union all select 'Ulmus', null
union all select 'Tilia', 'linden' """

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
query '''{ENRICHMENT}''';
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
    "city_form": ["city", "tree_form"],
    "species_form": ["species", "tree_form"],
}

GRAINS_BY = {
    "total": "*",
    "city": "city",
    "species": "species",
    "form": "tree_form",
    "city_species": "city, species",
    "city_form": "city, tree_form",
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
    "nested_beside_total_gate": (
        "select sum(sum(height) by city, species) as v where sum(height) by * > 10;"
    ),
    "nested_beside_failed_gate": (
        "select city, sum(sum(height) by city, species) as v"
        " where count(tree_id) by * > 100 order by city;"
    ),
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


def _assert_same_answer(query: str, served: bool, summary: str = SUMMARY) -> None:
    _, without = _run(BASE, query)
    sql, with_summary = _run(BASE + summary, query)
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


# The same aggregates read into an expression: the summary rows the scan keeps
# for a derived grain's inputs are re-summed before the expression reads them,
# and at the summary's own grain the expression reads its stored column.
EXPRESSIONS = {
    "count": "count(tree_id) + 1 as v",
    "sum": "sum(height) * 2 as v",
    "min": "min(height) - 1 as v",
    "max": "coalesce(max(height), 0) as v",
    "bool_or": "not bool_or(tall) as v",
    "bool_and": "bool_and(tall) = true as v",
}


@pytest.mark.parametrize(
    "grain,where,aggregate",
    list(itertools.product(GRAINS, FILTERS, EXPRESSIONS)),
)
def test_expression_over_rolled_up_aggregate(grain: str, where: str, aggregate: str):
    _assert_same_answer(
        _select(GRAINS[grain], EXPRESSIONS[aggregate], FILTERS[where]), served=True
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


# A HAVING aggregate an output already computes (`sum(height)` pinned to the
# select grain, beside `having sum(height) by <grain>`) reads that output: built
# inline, it read rows the summary scan never carries.
@pytest.mark.parametrize(
    "grain,where,aggregate",
    list(itertools.product(GRAINS_BY, FILTERS, ["sum", "count", "max"])),
)
def test_having_on_own_grain_aggregate_reads_output(
    grain: str, where: str, aggregate: str
):
    function = AGGREGATES[aggregate].removesuffix(" as v")
    _assert_same_answer(
        _select(
            GRAINS[grain],
            AGGREGATES[aggregate],
            FILTERS[where],
            having=f"{function} by {GRAINS_BY[grain]} > 1",
        ),
        served=True,
    )


@pytest.mark.parametrize("name", sorted(NESTED))
def test_nested_aggregate_rolls_up(name: str):
    _assert_same_answer(NESTED[name], served=True)


@pytest.mark.parametrize("name", sorted(UNSERVABLE))
def test_unservable_query_reads_base_rows(name: str):
    _assert_same_answer(UNSERVABLE[name], served=False)


# `Rollup.group_value_filter_exact`: a WHERE over the aggregate the summary
# stores at its own grain filters the summary rows, then they are re-summed.
# Dropped (`Rollup.dropped_group_filter_overcounts`) or re-summed at a grain
# that does not determine it (`Rollup.resummed_condition_misfilters`), the
# answer was wrong. A dimension's column keyed on the grain (`tree_form`) is
# joined onto the filtered summary rows before the re-sum
# (`Rollup.group_value_filter_joined_exact`); a CASE over it, and a grand
# total under the OR that joins `tree_form`, read the base rows.
GROUP_VALUE_GRAINS = {
    "total",
    "city",
    "species",
    "city_species",
    "form",
    "city_form",
    "species_form",
}
GROUP_VALUE_FILTERS = {
    "sum": "sum(height) by city, species > 3",
    "count": "count(tree_id) by city, species = 1",
    "max_and_city": "max(height) by city, species >= 5 and city = 'A'",
    "min_null": "min(height) by city, species is null",
    "or_form": "sum(height) by city, species > 3 or tree_form = 'linden'",
}


@pytest.mark.parametrize(
    "grain,where,aggregate",
    list(itertools.product(GRAINS, GROUP_VALUE_FILTERS, ["count", "sum", "max"])),
)
def test_group_value_filter_applies_before_rollup(
    grain: str, where: str, aggregate: str
):
    _assert_same_answer(
        _select(GRAINS[grain], AGGREGATES[aggregate], GROUP_VALUE_FILTERS[where]),
        served=grain in GROUP_VALUE_GRAINS and (grain, where) != ("total", "or_form"),
    )


# `Rollup.coarse_group_value_filter_exact`: a WHERE over an aggregate at a grain
# inside the summary's (`sum(height) by city`) is the re-sum of the summary rows
# sharing that key, joined back onto each summary row before the re-sum, at the
# summary's own grain too, where the re-sum is an identity. Read as each row's
# stored partial, it misfilters (`Rollup.coarse_gate_on_partial_misfilters`).
# A gate over a re-sum of the stored column (`nested`) is the same value. A gate
# grouped by a dimension's column (`tree_form`) joins that column onto the
# summary rows and the gate back on its whole grain
# (`Rollup.dimension_gate_exact`). An IN-list member is read at its own `by`
# (`SelectGrain.membership_pin_misreads`). A CASE over `tree_form`, a row
# column beside the gate the target does not group by, and an OR joining
# `tree_form` outside the target still read the base rows.
COARSE_GATE_GRAINS = {
    "total",
    "city",
    "species",
    "form",
    "city_form",
    "city_species",
    "species_form",
}
COARSE_GATES = {
    "sum": ("sum(height) by city > 6", COARSE_GATE_GRAINS),
    "count_species": ("count(tree_id) by species > 1", COARSE_GATE_GRAINS),
    "min_null": ("min(height) by city is null", COARSE_GATE_GRAINS),
    "avg": ("avg(height) by city > 4", COARSE_GATE_GRAINS),
    "count_distinct": ("count_distinct(tag) by city = 2", COARSE_GATE_GRAINS),
    "not": ("not sum(height) by city > 9", COARSE_GATE_GRAINS),
    "beside_exact_gate": (
        "sum(height) by city > 6 and sum(height) by city, species > 3",
        COARSE_GATE_GRAINS,
    ),
    "or_species_gate": (
        "sum(height) by city > 9 or count(tree_id) by species > 1",
        COARSE_GATE_GRAINS,
    ),
    "nested": ("sum(sum(height) by city, species) by city > 6", COARSE_GATE_GRAINS),
    "nested_max": ("max(sum(height) by city, species) by city > 6", COARSE_GATE_GRAINS),
    "in_list": ("sum(height) by city in (8, 9)", COARSE_GATE_GRAINS),
    "form": ("sum(height) by tree_form > 6", COARSE_GATE_GRAINS),
    "form_count": ("count(tree_id) by tree_form >= 3", COARSE_GATE_GRAINS),
    "form_min_null": ("min(height) by tree_form is null", COARSE_GATE_GRAINS),
    "form_in_list": ("max(height) by tree_form in (7, 2)", COARSE_GATE_GRAINS),
    "city_form": ("sum(height) by city, tree_form > 4", COARSE_GATE_GRAINS),
    "form_nested": (
        "sum(sum(height) by city, species) by tree_form > 6",
        COARSE_GATE_GRAINS - {"total"},
    ),
    "form_beside_city": (
        "sum(height) by tree_form > 6 and sum(height) by city > 8",
        COARSE_GATE_GRAINS,
    ),
    "form_vs_city": (
        "sum(height) by tree_form > sum(height) by city",
        COARSE_GATE_GRAINS,
    ),
    "and_species": (
        "max(height) by city >= 7 and species != 'Tilia'",
        {"species", "city_species", "species_form"},
    ),
    "or_form": (
        "sum(height) by city > 9 or tree_form = 'linden'",
        {"form", "city_form"},
    ),
}


@pytest.mark.parametrize(
    "grain,gate,aggregate",
    list(itertools.product(GRAINS, COARSE_GATES, ["count", "sum", "max", "bool_or"])),
)
def test_coarse_group_value_filter_reads_summary(grain: str, gate: str, aggregate: str):
    where, served = COARSE_GATES[gate]
    _assert_same_answer(
        _select(GRAINS[grain], AGGREGATES[aggregate], where), served=grain in served
    )


# `Rollup.partial_is_not_a_key`: a re-sum of the summary's partials beside a
# value at another grain (`sum(sum(height) by city) by *`) joined the node
# holding each partial to the node holding its re-sum on the value itself, so a
# group whose partials differ from their total (`broadleaf` in both cities) or
# are NULL lost its row. They join on the group key alone
# (`Rollup.resum_lookup_exact`). A count beside a bare total reads the base
# rows, as in `MIXED_GRAINS`.
BESIDE_SELECTS = {
    "form": "tree_form",
    "species": "species",
    "city": "city",
    "city_form": "city, tree_form",
}
BESIDE_VALUES = {
    "nested_city_total": "sum(sum(height) by city) by *",
    "nested_species_total": "sum(sum(height) by species) by *",
    "count_city_total": "sum(count(tree_id) by city) by *",
    "max_city_total": "max(sum(height) by city) by *",
    "total": "sum(height) by *",
}


@pytest.mark.parametrize(
    "select,value,aggregate,where",
    list(itertools.product(BESIDE_SELECTS, BESIDE_VALUES, ["count", "sum"], FILTERS)),
)
def test_resum_beside_other_grain_keeps_every_group(
    select: str, value: str, aggregate: str, where: str
):
    query = _select(
        [BESIDE_SELECTS[select]],
        f"{AGGREGATES[aggregate]}, {BESIDE_VALUES[value]} as w",
        FILTERS[where],
    )
    _assert_same_answer(query, served=(value, aggregate) != ("total", "count"))


# A membership whose key the summary's grain holds widens the scan to that key;
# the partial each summary row holds is still re-summed before the expression
# reads it (`Rollup.unsummed_partial_splits_a_group`).
MEMBERSHIPS = {
    "subquery": ("", "species in (select species where tag = 'x')"),
    "rowset": (
        "rowset xs <- select species, count(tree_id) as k where tag = 'x';\n",
        "species in xs.species",
    ),
}


@pytest.mark.parametrize(
    "grain,membership,aggregate",
    list(itertools.product(GRAINS, MEMBERSHIPS, EXPRESSIONS)),
)
def test_membership_beside_rolled_up_expression(
    grain: str, membership: str, aggregate: str
):
    preamble, where = MEMBERSHIPS[membership]
    _assert_same_answer(
        preamble + _select(GRAINS[grain], EXPRESSIONS[aggregate], where), served=True
    )


# `Rollup.count_distinct_grain_column_exact`: the distinct values of a column
# of the summary's grain are the ones its rows list. A filter over `tree_form`,
# a property of the grain's `species`, selects whole groups and reads the
# summary joined to its dimension (`Cover.joined_case_exact`); a total of
# `species` alone, or filtered on itself or on `tree_form`, is read from its
# dimension or the base rows.
DISTINCT_GRAIN_COLUMNS = {
    "species": ["total", "city"],
    "city": ["total", "species"],
}


@pytest.mark.parametrize(
    "column,grain,where",
    [
        (column, grain, where)
        for column, grains in DISTINCT_GRAIN_COLUMNS.items()
        for grain in grains
        for where in FILTERS
    ],
)
def test_count_distinct_of_grain_column(column: str, grain: str, where: str):
    _assert_same_answer(
        _select(GRAINS[grain], f"count_distinct({column}) as v", FILTERS[where]),
        served=(column, grain, where)
        not in {
            ("species", "total", "none"),
            ("species", "total", "species"),
            ("species", "total", "form"),
            ("species", "total", "form_null"),
        },
    )


# `Rollup.stored_column_answers_its_own_argument`: a stored `sum(height)` holds
# nothing a `sum(height * 2)` can be rolled up from.
EXPRESSION_ARGUMENTS = {
    "sum": "select city, sum(height * 2) as v order by city;",
    "min": "select city, min(0 - height) as v order by city;",
    "max": "select city, max(height + 100) as v order by city;",
    "total": "select sum(height * 2) as v where species = 'Acer';",
}

DOUBLED_SUMMARY = (
    SUMMARY.replace(
        "auto summary_min", "auto summary_doubled <- sum(height * 2);\nauto summary_min"
    )
    .replace("n: summary_count,", "n: summary_count,\n    d: ?summary_doubled,")
    .replace("count(tree_id) as n,", "count(tree_id) as n, sum(height * 2) as d,")
)


@pytest.mark.parametrize("name", sorted(EXPRESSION_ARGUMENTS))
def test_aggregate_of_expression_is_not_its_argument(name: str):
    _assert_same_answer(EXPRESSION_ARGUMENTS[name], served=False)


def test_summary_serves_its_own_expression_argument():
    _assert_same_answer(
        EXPRESSION_ARGUMENTS["sum"], served=True, summary=DOUBLED_SUMMARY
    )


def test_partial_of_argument_does_not_answer_its_expression():
    _, rows = _run(
        BASE,
        "select city, species, sum(height) as v0, sum(height * 2) by city as v1"
        " order by city, species;",
    )
    assert rows == [
        ("A", "Acer", 3, 16),
        ("A", "Quercus", 5, 16),
        ("A", "Ulmus", None, 16),
        ("B", "Acer", 7, 18),
        ("B", "Tilia", 2, 18),
    ]


@pytest.mark.parametrize("aggregate", sorted(AGGREGATES))
def test_empty_summary_grand_total_is_no_row(aggregate: str):
    model = BASE.replace(TREES, TREES + " limit 0") + SUMMARY.replace(
        "group by all", "group by all limit 0"
    )
    sql, rows = _run(model, f"select {AGGREGATES[aggregate]};")
    assert "tree_summary" in sql, sql
    assert rows == []


# `Rollup.avg_of_parts_exact`: a summary storing `sum(height)` and
# `count(height)` answers `avg(height)` as their rolled-up quotient.
AVG_SUMMARY = (
    SUMMARY.replace(
        "auto summary_min", "auto summary_nonnull <- count(height);\nauto summary_min"
    )
    .replace("n: summary_count,", "n: summary_count,\n    nh: summary_nonnull,")
    .replace("count(tree_id) as n,", "count(tree_id) as n, count(height) as nh,")
)

AVG_SERVED = {
    "having": "select species, avg(height) as v where city = 'A' having v > 2 order by species;",
    "beside_max": "select city, max(height) as m, avg(height) as v order by city;",
    "window": (
        "select species, rank(species) over (order by avg(height) desc, species asc)"
        " as r where city = 'A' order by species;"
    ),
    "two_grains": (
        "select city, avg(height) as v, avg(height) by * as t where tree_form is not null"
        " order by city;"
    ),
}

# Correct from the base rows: a filter below the summary's grain, an argument
# with no stored parts, and a shape that declines for `sum` and `count` too.
AVG_UNSERVABLE = {
    "row_filter": "select species, avg(height) as v where tag = 'x' order by species;",
    "no_parts": "select city, avg(height * 2) as v order by city;",
    "max_of_avgs": "select max(avg(height) by species) as v where city = 'A';",
}


@pytest.mark.parametrize("grain,where", list(itertools.product(GRAINS, FILTERS)))
def test_summary_serves_avg_from_sum_and_count(grain: str, where: str):
    _assert_same_answer(
        _select(GRAINS[grain], "avg(height) as v", FILTERS[where]),
        served=True,
        summary=AVG_SUMMARY,
    )


@pytest.mark.parametrize("name", sorted(AVG_SERVED))
def test_avg_from_parts_in_larger_demand(name: str):
    _assert_same_answer(AVG_SERVED[name], served=True, summary=AVG_SUMMARY)


@pytest.mark.parametrize("name", sorted(AVG_UNSERVABLE))
def test_avg_without_servable_parts_reads_base_rows(name: str):
    _assert_same_answer(AVG_UNSERVABLE[name], served=False, summary=AVG_SUMMARY)


@pytest.mark.parametrize("dialect", [BigqueryDialect, SnowflakeDialect])
def test_avg_of_parts_is_null_without_input(dialect: type[BaseDialect]):
    environment, statements = parse(
        BASE + AVG_SUMMARY + "select species, avg(height) as v;"
    )
    renderer = dialect()
    query = renderer.generate_queries(environment, [statements[-1]])[-1]
    sql = renderer.compile_statement(query)
    assert "tree_summary" in sql and "= 0 THEN null" in sql, sql


# `Rollup.reaggregated_*_is_partial`: one aggregate at a fine grain and coarser
# grains it determines groups the rows once; each coarser grain re-aggregates
# the next finer one.
PARTIAL_GRAINS = [
    "city_species>city",
    "city_species>species",
    "city_species>form",
    "city_species>form_case",
    "city_species>total",
    "city>total",
    "species>form",
    "species>form_case",
    "form>total",
    "city_form>city",
    "city_form>form",
    "city_form>total",
]

RAW_AGGREGATE = re.compile(
    r'\b(count|sum|min|max|bool_or|bool_and)\("[^"]+"\."(tree_id|height|tall)"\)'
)


def _oracle_rows(
    grains: list[list[str]], aggregate: str, where: str | None
) -> list[tuple]:
    """Each grain grouped from the base rows on its own, joined on the coarser
    grain's columns."""
    expression = aggregate.removesuffix(" as v")
    base = (
        f"with base as (select * from ({TREES}) t left join ({ENRICHMENT}) e"
        f" using (species){f' where {where}' if where else ''})"
    )
    carried = [c for grain in grains[1:] for c in grain if c not in grains[0]]
    tables = []
    for index, grain in enumerate(grains):
        keys = [*grain, *carried] if index == 0 else grain
        columns = ", ".join([*dict.fromkeys(keys), f"{expression} as v{index}"])
        group = " group by all" if grain else " having count(*) > 0"
        tables.append(f"g{index} as (select {columns} from base{group})")
    names = [[c.split(" as ")[-1] for c in grain] for grain in grains]
    select = ", ".join(
        [f"g0.{n}" for n in names[0]] + [f"g{i}.v{i}" for i in range(len(grains))]
    )
    joins = "".join(
        f" join g{i} on "
        + (
            " and ".join(f"g0.{n} is not distinct from g{i}.{n}" for n in names[i])
            or "true"
        )
        for i in range(1, len(grains))
    )
    sql = f"{base}, {', '.join(tables)} select {select} from g0{joins}"
    executor = Dialects.DUCK_DB.default_executor()
    return sorted(
        (tuple(r) for r in executor.execute_raw_sql(sql).fetchall()), key=repr
    )


def _partial_select(grains: list[list[str]], aggregate: str, where: str | None) -> str:
    expression = aggregate.removesuffix(" as v")
    outputs = [f"{expression} as v0"]
    for index, grain in enumerate(grains[1:], start=1):
        by = ", ".join(c.split(" as ")[-1] for c in grain) or "*"
        outputs.append(f"{expression} by {by} as v{index}")
    hidden = [c for grain in grains[1:] for c in grain if " as " in c]
    columns = ", ".join(
        [*grains[0], *(f"--{c}" for c in hidden if c not in grains[0]), *outputs]
    )
    return f"select {columns}{f' where {where}' if where else ''};"


def _assert_partials_reused(
    grains: list[list[str]], aggregate: str, where: str | None
) -> None:
    query = _partial_select(grains, aggregate, where)
    expected = _oracle_rows(grains, aggregate, where)
    for model, raw in ((BASE, 1), (BASE + SUMMARY, 0)):
        sql, rows = _run(model, query)
        assert sorted(rows, key=repr) == expected, sql
        assert len(RAW_AGGREGATE.findall(sql)) == raw, sql


@pytest.mark.parametrize(
    "grains,where,aggregate",
    list(itertools.product(PARTIAL_GRAINS, FILTERS, AGGREGATES)),
)
def test_coarser_grain_reaggregates_finer_partial(
    grains: str, where: str, aggregate: str
):
    _assert_partials_reused(
        [GRAINS[g] for g in grains.split(">")], AGGREGATES[aggregate], FILTERS[where]
    )


@pytest.mark.parametrize("aggregate", sorted(AGGREGATES))
def test_grain_chain_reads_rows_once(aggregate: str):
    _assert_partials_reused(
        [GRAINS["city_species"], GRAINS["city"], GRAINS["total"]],
        AGGREGATES[aggregate],
        "tree_form is not null",
    )


# An aggregate at a grain other than the select's beside a summary-served one:
# it is a separate value the rows are joined to, never one the select's grain
# re-aggregates. Each was a wrong answer when the summary was in scope.
MIXED_GRAINS = {
    "total_count_by_form": (
        "select tree_form, count(tree_id) by * as v1 order by tree_form;"
    ),
    "total_count_beside_form_sum": (
        "select tree_form, sum(height) as v0, count(tree_id) by * as v1"
        " where city = 'A' order by tree_form;"
    ),
    "total_count_beside_city_sum": (
        "select city, sum(height) as v0, count(tree_id) by * as v1"
        " where tree_form = 'broadleaf' order by city;"
    ),
    "nested_total_beside_inner": (
        "select city, sum(height) as v0, sum(sum(height) by city) by * as v1"
        " where tree_form = 'broadleaf' order by city;"
    ),
    "city_count_beside_species_sum": (
        "select species, sum(height) as v0, count(tree_id) by city as v1"
        " where city = 'A' order by species;"
    ),
    "share_of_city": (
        "select city, species, count(tree_id) as n, count(tree_id) by city as cn,"
        " n / cn as share order by city, species;"
    ),
}
MIXED_GRAINS_SERVED = {"nested_total_beside_inner", "share_of_city"}


@pytest.mark.parametrize("name", sorted(MIXED_GRAINS))
def test_aggregate_keeps_its_own_grain_beside_summary(name: str):
    _assert_same_answer(MIXED_GRAINS[name], served=name in MIXED_GRAINS_SERVED)


# A window is evaluated once per tuple of its grain (content, partition and the
# ordering aggregate's grain) and joined to the select's rows. The aggregate it
# orders by is a value at its own grain: never repeated once per base row, never
# read off a finer summary row without re-aggregating.
WINDOW_SELECTS = {"city_species": ["city", "species"], "species": ["species"]}
WINDOW_BYS = {
    "unscoped": None,
    "species": ["species"],
    "city": ["city"],
    "form": ["tree_form"],
    "total": [],
}
WINDOW_PARTITIONS = {"none": [], "city": ["city"]}
WINDOW_FILTERS = ["none", "city", "form", "empty"]


def _window_cases() -> list[tuple[str, ...]]:
    return [
        (window, select, by, partition, where, aggregate)
        for window, select, by, partition, where, aggregate in itertools.product(
            ["rank", "lag"],
            WINDOW_SELECTS,
            WINDOW_BYS,
            WINDOW_PARTITIONS,
            WINDOW_FILTERS,
            AGGREGATES,
        )
        if {*WINDOW_PARTITIONS[partition], *(WINDOW_BYS[by] or [])}
        <= {*WINDOW_SELECTS[select], "tree_form"}
    ]


def _window_grain(partition: list[str], by: list[str]) -> list[str]:
    return list(dict.fromkeys(["species", *partition, *by]))


def _window_call(
    window: str, content: str, partition: list[str], order: str, ties: list[str]
) -> str:
    over = f"partition by {', '.join(partition)} " if partition else ""
    if window == "rank":
        return f"rank({content}) over ({over}order by {order} desc)"
    tiebreak = ", ".join(f"{c} asc" for c in ties if not c.endswith("tree_form"))
    return f"lag({content}) over ({over}order by {order} desc, {tiebreak})"


def _window_oracle(
    window: str,
    keys: list[str],
    by: list[str],
    partition: list[str],
    expression: str,
    where: str | None,
) -> list[tuple]:
    grain = _window_grain(partition, by)
    call = _window_call(
        window,
        "" if window == "rank" else "r.species",
        [f"r.{c}" for c in partition],
        "a.a",
        [f"r.{c}" for c in grain],
    )
    by_match = " and ".join(f"r.{c} is not distinct from a.{c}" for c in by)
    key_match = " and ".join(f"s.{c} = w.{c}" for c in keys if c in grain)
    sql = (
        f"with base as (select * from ({TREES}) t left join ({ENRICHMENT}) e"
        f" using (species){f' where {where}' if where else ''}),"
        f" r as (select distinct {', '.join(grain)} from base),"
        f" a as (select {', '.join([*by, f'{expression} as a'])} from base"
        f"{' group by all' if by else ''}),"
        f" w as (select r.*, {call} as w from r join a on {by_match or 'true'}),"
        f" s as (select distinct {', '.join(keys)} from base)"
        f" select {', '.join(f's.{k}' for k in keys)}, w.w from s join w on {key_match}"
    )
    executor = Dialects.DUCK_DB.default_executor()
    return sorted(
        (tuple(r) for r in executor.execute_raw_sql(sql).fetchall()), key=repr
    )


@pytest.mark.parametrize("window,select,by,partition,where,aggregate", _window_cases())
def test_window_orders_by_aggregate_at_its_own_grain(
    window: str, select: str, by: str, partition: str, where: str, aggregate: str
):
    keys = WINDOW_SELECTS[select]
    scoped = WINDOW_BYS[by]
    over = WINDOW_PARTITIONS[partition]
    condition = FILTERS[where]
    expression = AGGREGATES[aggregate].removesuffix(" as v")
    inner = (
        expression if scoped is None else f"{expression} by {', '.join(scoped) or '*'}"
    )
    # an unscoped aggregate under a window is at the select grain: the
    # selected keys and the window's (`SelectGrain.bare_is_select_grain`)
    by_keys = (
        list(dict.fromkeys([*keys, "species", *over])) if scoped is None else scoped
    )
    call = _window_call(window, "species", over, inner, _window_grain(over, by_keys))
    query = (
        f"select {', '.join(keys)}, {call} as w"
        f"{f' where {condition}' if condition else ''};"
    )
    expected = _window_oracle(window, keys, by_keys, over, expression, condition)
    for model in (BASE, BASE + SUMMARY):
        sql, rows = _run(model, query)
        assert sorted(rows, key=repr) == expected, sql
