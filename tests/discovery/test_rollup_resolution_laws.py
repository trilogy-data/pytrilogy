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
# for a derived grain's inputs are re-summed before the expression reads them.
# At the summary's own grain an expression's aggregate is still computed from
# the base rows.
EXACT_GRAIN = {"city_species"}
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
        _select(GRAINS[grain], EXPRESSIONS[aggregate], FILTERS[where]),
        served=grain not in EXACT_GRAIN,
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
}

# Correct from the base rows: a filter below the summary's grain, an argument
# with no stored parts, and shapes that decline for `sum` and `count` too.
AVG_UNSERVABLE = {
    "row_filter": "select species, avg(height) as v where tag = 'x' order by species;",
    "no_parts": "select city, avg(height * 2) as v order by city;",
    "max_of_avgs": "select max(avg(height) by species) as v where city = 'A';",
    "two_grains": (
        "select city, avg(height) as v, avg(height) by * as t where tree_form is not null"
        " order by city;"
    ),
}


@pytest.mark.parametrize("grain,where", list(itertools.product(GRAINS, FILTERS)))
def test_summary_serves_avg_from_sum_and_count(grain: str, where: str):
    _assert_same_answer(
        _select(GRAINS[grain], "avg(height) as v", FILTERS[where]),
        served=grain not in EXACT_GRAIN,
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
    # an unscoped aggregate under a window is at the window content's grain
    by_keys = ["species"] if scoped is None else scoped
    call = _window_call(window, "species", over, inner, _window_grain(over, by_keys))
    query = (
        f"select {', '.join(keys)}, {call} as w"
        f"{f' where {condition}' if condition else ''};"
    )
    expected = _window_oracle(window, keys, by_keys, over, expression, condition)
    for model in (BASE, BASE + SUMMARY):
        sql, rows = _run(model, query)
        assert sorted(rows, key=repr) == expected, sql
