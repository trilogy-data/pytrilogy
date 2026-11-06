"""`formal/Trilogy/Cover.lean` rule 2 at any grain: `agg(x ? c)` is the
re-aggregate of `agg(x) by G, cols(c)` over the groups `c` keeps
(`Cover.filtered_sum_exact`, `filtered_min_exact`, `filtered_max_exact`,
`case_count_exact`). Over seeded random trees with NULLs, every filtered
aggregate must answer as the Python reference does and read the summary."""

import itertools
import random

import pytest

from trilogy import Dialects

SEEDS = (3, 11)
SPECIES = {
    "Acer rubrum": "Native",
    "Acer saccharum": "Native",
    "Opuntia ficus": "Introduced",
    "Tilia cordata": None,
    "Quercus alba": "Native",
}
CITIES = ("A", "B", "C")


def _trees(seed: int) -> list[tuple]:
    rng = random.Random(seed)
    return [
        (
            tree_id,
            rng.choice(CITIES),
            rng.choice(list(SPECIES)),
            rng.choice([None, *range(1, 40)]),
            rng.choice([None, *range(1990, 2021)]),
        )
        for tree_id in range(1, 41)
    ]


def _sql_literal(value) -> str:
    if value is None:
        return "null"
    return f"'{value}'" if isinstance(value, str) else str(value)


def _values(rows: list[tuple], columns: str) -> str:
    body = ", ".join(f"({', '.join(map(_sql_literal, r))})" for r in rows)
    return f"select * from (values {body}) as t({columns})"


def _model(trees: list[tuple]) -> str:
    tree_sql = _values(trees, "tree_id, city, species, dbh, plant_year")
    enrichment_sql = _values(list(SPECIES.items()), "species, bucket")
    return f"""
key tree_id int;
key city string;
key species string;
property tree_id.dbh int;
property tree_id.plant_year int;
property species.bucket string;

datasource trees (tree_id, city, species, dbh: ?dbh, plant_year: ?plant_year)
grain (tree_id)
query '''{tree_sql}''';

datasource enrichment (species, bucket: ?bucket)
grain (species)
query '''{enrichment_sql}''';

auto n_trees <- count(tree_id);
auto n_dbh <- count(dbh);
auto total_dbh <- sum(dbh);
auto least_dbh <- min(dbh);
auto most_dbh <- max(dbh);
datasource summary (city, species, plant_year: ?plant_year, n_trees, n_dbh: ?n_dbh,
    total_dbh: ?total_dbh, least_dbh: ?least_dbh, most_dbh: ?most_dbh)
grain (city, species, plant_year)
query '''select city, species, plant_year, count(*) as n_trees, count(dbh) as n_dbh,
    sum(dbh) as total_dbh, min(dbh) as least_dbh, max(dbh) as most_dbh
from ({tree_sql}) group by 1, 2, 3''';
"""


def _count(xs):
    return sum(1 for x in xs if x is not None)


def _sum(xs):
    present = [x for x in xs if x is not None]
    return sum(present) if present else None


def _min(xs):
    present = [x for x in xs if x is not None]
    return min(present) if present else None


def _max(xs):
    present = [x for x in xs if x is not None]
    return max(present) if present else None


AGGREGATES = {
    "count_key": ("count", "tree_id", _count),
    "count_value": ("count", "dbh", _count),
    "distinct_key": ("count_distinct", "tree_id", _count),
    "sum": ("sum", "dbh", _sum),
    "min": ("min", "dbh", _min),
    "max": ("max", "dbh", _max),
}

# `x ? c` and `case when c then x end` are one value per row (`Cover.filtered`)
SPELLINGS = {
    "filter": "{fn}({x} ? {c})",
    "case": "{fn}(case when {c} then {x} end)",
}

CONDITIONS = {
    "key": ("species = 'Acer rubrum'", lambda r: r["species"] == "Acer rubrum"),
    "property": ("bucket = 'Native'", lambda r: r["bucket"] == "Native"),
    "stored": ("plant_year > 2005", lambda r: _gt(r["plant_year"], 2005)),
    "null": ("plant_year is null", lambda r: r["plant_year"] is None),
    "or": (
        "(bucket = 'Introduced' or plant_year < 2000)",
        lambda r: r["bucket"] == "Introduced" or _gt(2000, r["plant_year"]),
    ),
    "membership": (
        "city in ('A', 'C')",
        lambda r: r["city"] in ("A", "C"),
    ),
}

TARGETS = {
    "total": (),
    "city": ("city",),
    "species": ("species",),
    "bucket": ("bucket",),
    "city_year": ("city", "plant_year"),
}

WHERES = {
    "none": ("", lambda r: True),
    "city": ("city != 'B'", lambda r: r["city"] != "B"),
    "property": ("bucket = 'Native'", lambda r: r["bucket"] == "Native"),
    "year": ("plant_year >= 2000", lambda r: _gt(r["plant_year"], 1999)),
    "finer": ("dbh > 10", lambda r: _gt(r["dbh"], 10)),
}
SERVABLE_WHERES = [w for w in WHERES if w != "finer"]


def _gt(a, b) -> bool:
    return a is not None and b is not None and a > b


def _rows(trees: list[tuple]) -> list[dict]:
    names = ("tree_id", "city", "species", "dbh", "plant_year")
    return [dict(zip(names, t), bucket=SPECIES[t[2]]) for t in trees]


def _reference(rows, aggregate, condition, target, where) -> list[tuple]:
    _, column, reduce = AGGREGATES[aggregate]
    keep = CONDITIONS[condition][1]
    kept = [r for r in rows if WHERES[where][1](r)]
    columns = TARGETS[target]
    groups: dict[tuple, list] = {}
    for r in kept:
        groups.setdefault(tuple(r[c] for c in columns), []).append(
            r[column] if keep(r) else None
        )
    return sorted(((*k, reduce(v)) for k, v in groups.items()), key=repr)


def _query(aggregate, condition, target, where, spelling="filter") -> str:
    fn, column, _ = AGGREGATES[aggregate]
    expression = SPELLINGS[spelling].format(fn=fn, x=column, c=CONDITIONS[condition][0])
    select = ", ".join([*TARGETS[target], f"{expression} as v"])
    clause = WHERES[where][0]
    return f"select {select}{' where ' + clause if clause else ''};"


def _executor(seed: int):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_model(_trees(seed)))
    return executor


CASES = list(
    itertools.product(
        SEEDS, SPELLINGS, AGGREGATES, CONDITIONS, TARGETS, SERVABLE_WHERES
    )
)


@pytest.fixture(scope="module")
def executors():
    return {seed: _executor(seed) for seed in SEEDS}


def _ids(cases: list[tuple]) -> list[str]:
    return ["-".join(map(str, case)) for case in cases]


def _sql_and_rows(executor, query: str) -> tuple[str, list[tuple]]:
    sql = executor.generate_sql(query)[-1]
    rows = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    return sql, sorted(rows, key=repr)


@pytest.mark.parametrize(
    "seed,spelling,aggregate,condition,target,where", CASES, ids=_ids(CASES)
)
def test_filtered_aggregate_answers(
    executors, seed, spelling, aggregate, condition, target, where
):
    sql, rows = _sql_and_rows(
        executors[seed], _query(aggregate, condition, target, where, spelling)
    )
    assert rows == _reference(
        _rows(_trees(seed)), aggregate, condition, target, where
    ), sql


@pytest.mark.parametrize(
    "seed,spelling,aggregate,condition,target,where", CASES, ids=_ids(CASES)
)
def test_filtered_aggregate_reads_the_summary(
    executors, seed, spelling, aggregate, condition, target, where
):
    sql, _ = _sql_and_rows(
        executors[seed], _query(aggregate, condition, target, where, spelling)
    )
    assert '"summary"' in sql and '"trees"' not in sql, sql


FINER_CASES = list(itertools.product(AGGREGATES, CONDITIONS, ("total", "species")))


@pytest.mark.parametrize(
    "aggregate,condition,target",
    FINER_CASES,
    ids=["-".join(case) for case in FINER_CASES],
)
def test_finer_where_scans_the_rows_once(executors, aggregate, condition, target):
    """`Rollup.finer_filter_unservable`: no summary answers under `dbh > 10`,
    so the filtered aggregate stays one pass over the trees."""
    seed = SEEDS[0]
    sql = executors[seed].generate_sql(_query(aggregate, condition, target, "finer"))[
        -1
    ]
    expected = _reference(_rows(_trees(seed)), aggregate, condition, target, "finer")
    rows = sorted(
        (tuple(r) for r in executors[seed].execute_raw_sql(sql).fetchall()), key=repr
    )
    assert rows == expected, sql
    assert sql.count(' as "trees"') == 1, sql


NATIVE = "species in ('Acer rubrum', 'Acer saccharum', 'Quercus alba')"
STORED_FILTERED = {
    "count_key": ("native_trees", "count", "1"),
    "sum": ("native_dbh", "sum", "dbh"),
    "min": ("least_native_dbh", "min", "dbh"),
    "max": ("most_native_dbh", "max", "dbh"),
}


def _stored_filtered_model(trees: list[tuple], spelling: str = "filter") -> str:
    tree_sql = _values(trees, "tree_id, city, species, dbh, plant_year")
    autos = "\n".join(
        f"auto {name} <- "
        + SPELLINGS[spelling].format(fn=fn, x=column, c="bucket = 'Native'")
        + ";"
        for name, fn, column in (
            (name, AGGREGATES[key][0], AGGREGATES[key][1])
            for key, (name, *_) in STORED_FILTERED.items()
        )
    )
    columns = ", ".join(f"{name}: ?{name}" for name, *_ in STORED_FILTERED.values())
    stored = ", ".join(
        f"{fn}(case when {NATIVE} then {value} end) as {name}"
        for name, fn, value in STORED_FILTERED.values()
    )
    return f"""{_model(trees)}
{autos}
datasource stored_native (city, species, plant_year: ?plant_year, {columns})
grain (city, species, plant_year)
query '''select city, species, plant_year, {stored}
from ({tree_sql}) group by 1, 2, 3''';
"""


STORED_CASES = list(
    itertools.product(SPELLINGS, SPELLINGS, STORED_FILTERED, TARGETS, SERVABLE_WHERES)
)


@pytest.mark.parametrize(
    "stored,spelling,aggregate,target,where", STORED_CASES, ids=_ids(STORED_CASES)
)
def test_stored_filtered_aggregate_is_read_as_stored(
    stored, spelling, aggregate, target, where
):
    """A summary storing `agg(x ? c)` itself holds filtered values: it answers
    a condition no summary column reaches (`bucket`), in either spelling of
    the stored or the asked aggregate, and its column is never filtered a
    second time."""
    seed = SEEDS[0]
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_stored_filtered_model(_trees(seed), stored))
    sql, rows = _sql_and_rows(
        executor, _query(aggregate, "property", target, where, spelling)
    )
    assert rows == _reference(
        _rows(_trees(seed)), aggregate, "property", target, where
    ), sql
    assert '"stored_native"' in sql and '"trees"' not in sql, sql
