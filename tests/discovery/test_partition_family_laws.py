"""`formal/Trilogy/Stack.lean`: a family of `complete where` arms stacks into
one source. Its arms may mix `x = a` and `x in (...)`, since `x in S` is the
disjunction of `x = a` over `S` (`Stack.in_is_eq_disjunction`), and the
stack answers at the arms' grain only when that grain determines each arm
(`Stack.stacked_summary_sum_exact`). Otherwise a grain value repeats across
arms (`Stack.unbound_arm_repeats_grain`). Each case runs one query against a
scope of arms and checks the rows against the base table. A scope that holds
the base table or a complete row family must answer; any other may decline."""

import itertools
from dataclasses import dataclass

import pytest

from trilogy import Dialects, Executor, parse
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.dialect.duckdb import DuckDBDialect

ROWS = """('a1', 'A', 'Acer', 3), ('a2', 'A', 'Acer', null), ('a3', 'A', 'Quercus', 5),
('b1', 'B', 'Acer', 7), ('b2', 'B', 'Tilia', 2), ('c1', 'C', 'Tilia', 4)"""

ARMS = {
    "a": "city = 'A'",
    "b": "city = 'B'",
    "c": "city = 'C'",
    "ain": "city in ('A')",
    "bc": "city in ('B', 'C')",
    "ab": "city in ('A', 'B')",
}

MODEL = """
key tree_id string;
key city enum<string>['A', 'B', 'C'];
key species string;
property tree_id.height int;
"""

ROW_COLUMNS = "(tree_id, city, species, height: ?height) grain (tree_id)"
CITY_SPECIES = "(city, species, n: n, h: ?h) grain (city, species)"
CITY = "(city, n: n, h: ?h) grain (city)"
SPECIES = "(species, n: n, h: ?h) grain (species)"
SPECIES_CITY = "(species, city, n: n, h: ?h) grain (species)"

SUMMARY_MODEL = """
auto n <- count(tree_id);
auto h <- sum(height);
"""


def _arm(name: str, columns: str, arm: str) -> str:
    return (
        f"partial datasource {name} {columns} complete where {ARMS[arm]}"
        f" address {name}_t;"
    )


SOURCES = {
    "trees": f"datasource trees {ROW_COLUMNS} address trees_t;",
    # States that each tree has one city and one species, but as a partial
    # table answers nothing: a summary's rollup rests on these FDs alone.
    "tree_fds": "partial datasource tree_fds (tree_id, city, species) grain (tree_id)"
    " address trees_t;",
    **{
        f"trees_{a}": _arm(f"trees_{a}", ROW_COLUMNS, a)
        for a in ("a", "b", "c", "ain", "bc")
    },
    **{
        f"cs_{a}": _arm(f"cs_{a}", CITY_SPECIES, a) for a in ("a", "b", "c", "bc", "ab")
    },
    **{f"c_{a}": _arm(f"c_{a}", CITY, a) for a in ("a", "bc")},
    **{f"s_{a}": _arm(f"s_{a}", SPECIES, a) for a in ("a", "b", "c")},
    **{f"sc_{a}": _arm(f"sc_{a}", SPECIES_CITY, a) for a in ("a", "b", "c")},
}


def _tables() -> list[str]:
    out = [
        f"create table trees_t as select * from (values {ROWS}) t(tree_id, city, species, height)"
    ]
    grains = {
        "trees": "*",
        "cs": "city, species",
        "c": "city",
        "s": "species",
        "sc": "species, city",
    }
    for name in SOURCES:
        if name in ("trees", "tree_fds"):
            continue
        kind, arm = name.rsplit("_", 1)
        keys = grains[kind]
        body = (
            f"select * from trees_t where {ARMS[arm]}"
            if keys == "*"
            else f"select {keys}, count(tree_id) n, sum(height) h from trees_t"
            f" where {ARMS[arm]} group by all"
        )
        out.append(f"create table {name}_t as {body}")
    return out


@dataclass(frozen=True)
class Scope:
    sources: tuple[str, ...]
    rows: bool
    rollups: bool

    def must_answer(self, query: str) -> bool:
        return self.rows if query.startswith("rows-") else self.rollups


SCOPES = {
    "rows_points": Scope(("trees_a", "trees_b", "trees_c"), True, True),
    "rows_value_set": Scope(("trees_a", "trees_bc"), True, True),
    "rows_parenthesized": Scope(("trees_ain", "trees_bc"), True, True),
    "rows_value_set+summaries": Scope(
        ("trees_a", "trees_bc", "cs_a", "cs_bc"), True, True
    ),
    "rows_value_set+city": Scope(("trees_a", "trees_bc", "c_a", "c_bc"), True, True),
    "base+value_set_summary": Scope(("trees", "cs_bc"), True, True),
    "base+summary_family": Scope(("trees", "cs_a", "cs_bc"), True, True),
    "base+city_family": Scope(("trees", "c_a", "c_bc", "cs_a"), True, True),
    "base+overlap": Scope(("trees", "cs_ab", "cs_bc"), True, True),
    "base+unbound_arms": Scope(("trees", "s_a", "s_b", "s_c"), True, True),
    "base+bound_arms": Scope(("trees", "sc_a", "sc_b", "sc_c"), True, True),
    "summary_family": Scope(("tree_fds", "cs_a", "cs_b", "cs_c"), False, True),
    "summary_value_set_family": Scope(("tree_fds", "cs_a", "cs_bc"), False, True),
    "bound_arms": Scope(("tree_fds", "sc_a", "sc_b", "sc_c"), False, True),
    "summary_family_no_fds": Scope(("cs_a", "cs_b", "cs_c"), False, False),
    "bound_arms_no_fds": Scope(("sc_a", "sc_b", "sc_c"), False, False),
    "overlap": Scope(("cs_ab", "cs_bc", "c_a"), False, False),
    "unbound_arms": Scope(("s_a", "s_b", "s_c"), False, False),
}

FILTERS = {
    "none": "",
    "a": " where city = 'A'",
    "b": " where city = 'B'",
    "ab": " where city in ('A', 'B')",
    "acer": " where species = 'Acer'",
}

GRAINS = {
    "total": [],
    "city": ["city"],
    "species": ["species"],
    "both": ["city", "species"],
}


def _queries() -> dict[str, str]:
    out: dict[str, str] = {}
    for name, clause in FILTERS.items():
        out[f"rows-{name}"] = f"select tree_id, height{clause};"
        for grain, keys in GRAINS.items():
            for agg in ("count(tree_id)", "sum(height)"):
                columns = ", ".join([*keys, f"{agg} as v"])
                out[f"{agg.split('(')[0]}-{grain}-{name}"] = (
                    f"select {columns}{clause};"
                )
    return out


QUERIES = _queries()


@pytest.fixture(scope="module")
def engine() -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    for statement in _tables():
        executor.execute_raw_sql(statement)
    executor.connection.commit()
    return executor


def _rows(engine: Executor, scope: tuple[str, ...], query: str) -> list[tuple]:
    declarations = "\n".join(SOURCES[name] for name in scope)
    environment, statements = parse(MODEL + SUMMARY_MODEL + declarations + "\n" + query)
    renderer = DuckDBDialect()
    built = renderer.generate_queries(environment, [statements[-1]])[-1]
    sql = renderer.compile_statement(built)
    return sorted((tuple(r) for r in engine.execute_raw_sql(sql).fetchall()), key=repr)


@pytest.mark.parametrize(
    "scope,query", list(itertools.product(SCOPES, QUERIES)), ids=lambda x: x
)
def test_partition_family_answers_like_the_table(
    engine: Executor, scope: str, query: str
):
    case = SCOPES[scope]
    expected = _rows(engine, ("trees",), QUERIES[query])
    if case.must_answer(query):
        assert _rows(engine, case.sources, QUERIES[query]) == expected
        return
    try:
        got = _rows(engine, case.sources, QUERIES[query])
    except (UnresolvableQueryException, ValueError):
        return
    assert got == expected
