"""`formal/Trilogy/Cost.lean`: every datasource that answers a query gives its
rows, and the planner never reads one that another answering source in scope
dominates. A source is an arm (`complete where`, every row, or no row a
WHERE can complete for a `partial datasource` without one) stored at a
grain. It answers when the WHERE implies its arm and every filter and target
column is a function of its grain; it dominates another when its arm implies
the other's and its grain is coarser (`Cost.dominated_le`). Each case runs one
query against the base table plus a set of filtered tables and summaries,
checks the rows against the base table alone, and checks which table was
read."""

import itertools
import re
from dataclasses import dataclass

import pytest

from trilogy import Dialects, Executor, parse
from trilogy.dialect.duckdb import DuckDBDialect

ROWS = """('a1', 'A', 'Acer', 3), ('a2', 'A', 'Acer', null), ('a3', 'A', 'Quercus', 5),
('b1', 'B', 'Acer', 7), ('b2', 'B', 'Tilia', 2), ('c1', 'C', 'Tilia', 4)"""

TABLES = f"""
create table trees_t as select * from (values {ROWS}) t(tree_id, city, species, height);
create table trees_a_t as select * from trees_t where city = 'A';
create table trees_ab_t as select * from trees_t where city in ('A', 'B');
create table sum_cs_t as
  select city, species, count(tree_id) n, sum(height) h from trees_t group by all;
create table sum_cs_a_t as select * from sum_cs_t where city = 'A';
create table sum_c_t as select city, count(tree_id) n, sum(height) h from trees_t group by all;
create table sum_c_a_t as select * from sum_c_t where city = 'A';
create table sum_cs_sub_t as select * from sum_cs_t where city = 'A';
create table sum_s_t as
  select species, count(tree_id) n, sum(height) h from trees_t group by all;
"""

MODEL = """
key tree_id string;
key city string;
key species string;
property tree_id.height int;
auto n_trees <- count(tree_id);
auto total_height <- sum(height);
"""

ALL_CITIES = frozenset("ABC")
ROW_KEY = frozenset({"tree_id"})


@dataclass(frozen=True)
class Source:
    declaration: str
    arm: frozenset[str]
    grain: frozenset[str]


SOURCES = {
    "trees": Source(
        "datasource trees (tree_id, city, species, height: ?height) grain (tree_id)",
        ALL_CITIES,
        ROW_KEY,
    ),
    "trees_a": Source(
        "partial datasource trees_a (tree_id, city, species, height: ?height)"
        " grain (tree_id) complete where city = 'A'",
        frozenset("A"),
        ROW_KEY,
    ),
    "trees_ab": Source(
        "partial datasource trees_ab (tree_id, city, species, height: ?height)"
        " grain (tree_id) complete where city in ('A', 'B')",
        frozenset("AB"),
        ROW_KEY,
    ),
    "sum_cs": Source(
        "datasource sum_cs (city, species, n: n_trees, h: ?total_height)"
        " grain (city, species)",
        ALL_CITIES,
        frozenset({"city", "species"}),
    ),
    "sum_cs_a": Source(
        "partial datasource sum_cs_a (city, species, n: n_trees, h: ?total_height)"
        " grain (city, species) complete where city = 'A'",
        frozenset("A"),
        frozenset({"city", "species"}),
    ),
    "sum_c": Source(
        "datasource sum_c (city, n: n_trees, h: ?total_height) grain (city)",
        ALL_CITIES,
        frozenset({"city"}),
    ),
    "sum_c_a": Source(
        "partial datasource sum_c_a (city, n: n_trees, h: ?total_height) grain (city)"
        " complete where city = 'A'",
        frozenset("A"),
        frozenset({"city"}),
    ),
    "sum_s": Source(
        "datasource sum_s (species, n: n_trees, h: ?total_height) grain (species)",
        ALL_CITIES,
        frozenset({"species"}),
    ),
    "sum_cs_sub": Source(
        "partial datasource sum_cs_sub (city, species, n: n_trees, h: ?total_height)"
        " grain (city, species)",
        frozenset(),
        frozenset({"city", "species"}),
    ),
}

# name -> (WHERE, the cities it keeps, the columns it reads)
FILTERS = {
    "none": (None, ALL_CITIES, frozenset()),
    "a": ("city = 'A'", frozenset("A"), frozenset({"city"})),
    "b": ("city = 'B'", frozenset("B"), frozenset({"city"})),
    "ab_in": ("city in ('A', 'B')", frozenset("AB"), frozenset({"city"})),
    "ab_or": ("(city = 'A' or city = 'B')", frozenset("AB"), frozenset({"city"})),
    "acer": ("species = 'Acer'", ALL_CITIES, frozenset({"species"})),
    "a_acer": (
        "city = 'A' and species = 'Acer'",
        frozenset("A"),
        frozenset({"city", "species"}),
    ),
}

GRAINS = {
    "total": [],
    "city": ["city"],
    "species": ["species"],
    "city_species": ["city", "species"],
}

AGGREGATES = {"count": "count(tree_id)", "sum": "sum(height)"}


@dataclass(frozen=True)
class Query:
    text: str
    cities: frozenset[str]
    columns: frozenset[str]
    row_level: bool


def _queries() -> dict[str, Query]:
    out: dict[str, Query] = {}
    for name, (where, cities, filtered) in FILTERS.items():
        clause = f" where {where}" if where else ""
        out[f"rows-{name}"] = Query(
            f"select tree_id, height{clause};", cities, filtered | ROW_KEY, True
        )
        for grain, keys in GRAINS.items():
            for aggregate, expression in AGGREGATES.items():
                columns = ", ".join([*keys, f"{expression} as v"])
                out[f"{aggregate}-{grain}-{name}"] = Query(
                    f"select {columns}{clause};", cities, filtered | set(keys), False
                )
    return out


QUERIES = _queries()

_OPTIONAL = [name for name in SOURCES if name != "trees"]
SCOPES = {
    "+".join(scope) or "base": ["trees", *scope]
    for scope in [
        (),
        *((name,) for name in _OPTIONAL),
        *itertools.combinations(_OPTIONAL, 2),
        tuple(_OPTIONAL),
    ]
}


def _closure(grain: frozenset[str]) -> frozenset[str]:
    return grain | {"city", "species", "height"} if "tree_id" in grain else grain


def answers(source: Source, query: Query) -> bool:
    if not query.cities <= source.arm:
        return False
    if query.row_level:
        return "tree_id" in source.grain
    return query.columns <= _closure(source.grain)


def dominates(a: Source, b: Source) -> bool:
    narrower = a.arm <= b.arm
    coarser = _closure(a.grain) <= _closure(b.grain)
    return (
        narrower
        and coarser
        and (a.arm < b.arm or _closure(a.grain) < _closure(b.grain))
    )


@pytest.fixture(scope="module")
def engine() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    for statement in TABLES.split(";"):
        if statement.strip():
            executor.execute_raw_sql(statement)
    return executor


def _sql(scope: list[str], query: str) -> str:
    declarations = "\n".join(
        f"{SOURCES[name].declaration} address {name}_t;" for name in scope
    )
    environment, statements = parse(MODEL + declarations + "\n" + query)
    renderer = DuckDBDialect()
    built = renderer.generate_queries(environment, [statements[-1]])[-1]
    return renderer.compile_statement(built)


def _rows(engine: Executor, sql: str) -> list[tuple]:
    return sorted((tuple(r) for r in engine.execute_raw_sql(sql).fetchall()), key=repr)


@pytest.mark.parametrize(
    "scope,query", list(itertools.product(SCOPES, QUERIES)), ids=lambda x: x
)
def test_reads_an_undominated_answering_source(
    engine: Executor, scope: str, query: str
):
    case = QUERIES[query]
    names = SCOPES[scope]
    sql = _sql(names, case.text)
    assert _rows(engine, sql) == _rows(engine, _sql(["trees"], case.text)), sql
    read = set(re.findall(r"\b(\w+)_t\b", sql)) & set(SOURCES)
    assert len(read) == 1, sql
    (source,) = read
    assert answers(SOURCES[source], case), sql
    cheaper = [
        name
        for name in names
        if answers(SOURCES[name], case) and dominates(SOURCES[name], SOURCES[source])
    ]
    assert not cheaper, sql
