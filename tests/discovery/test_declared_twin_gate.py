"""A WHERE aggregate a model concept also spells (`auto xx <- max(max(height)
by city, species)`) is read from the summary under the WHERE's own name. The
build environment's canonical concept for the expression was the model's
spelling, so the summary scan output that name, the WHERE-scoped twin was
re-derived from lineage over a table without `height`, and the query raised
"Missing source reference". A select defines its outputs in the environment,
so a batch reached it with no declaration: an earlier `select ... as v0` left
`v0` behind. Rows checked by hand against `test_oracle_sweep_laws.ROWS`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import normalize
from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
from trilogy import Dialects, Executor

CASES = [
    (
        "auto xx <- max(max(height) by city, species);",
        "select city where max(height) by city, species is not null;",
        [("A",), ("B",)],
    ),
    (
        "auto xx <- max(max(height) by city, species);",
        "select tree_form where max(height) by city, species is not null;",
        [("broadleaf",), ("linden",)],
    ),
    (
        "auto xx <- count(max(height) by city, species);",
        "select tree_form where max(height) by city, species > 4;",
        [("broadleaf",)],
    ),
    (
        "auto xx <- max(sum(height) by city, species) by city;",
        "select tree_form where sum(height) by city, species > 4;",
        [("broadleaf",)],
    ),
]


def _executor(model: str, extra: str = "") -> Executor:
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(BASE + (SUMMARY if model == "summary" else "") + "\n" + extra)
    return exec


def _rows(exec: Executor, query: str) -> list:
    return normalize(exec.execute_raw_sql(exec.generate_sql(query)[-1]).fetchall())


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("declared,query,expected", CASES)
def test_declared_twin_of_where_aggregate(
    declared: str, query: str, expected: list, model: str
):
    assert _rows(_executor(model, declared), query) == normalize(expected)


@pytest.mark.parametrize("model", ["base", "summary"])
def test_earlier_select_output_twins_where_aggregate(model: str):
    exec = _executor(model)
    exec.generate_sql("select sum(height ? max(height) by city, species = 1) as v0;")
    query = (
        "select tree_form where max(height) by city, species is not null"
        " and tree_form = 'broadleaf';"
    )
    assert _rows(exec, query) == [("broadleaf",)]
