"""`formal/Trilogy/ValueSet.lean`: a table that declares `grain (cls)` and binds
`cls` completely answers every repeat-blind aggregate of `cls`, and every
aggregate of a column it stores at that grain, from its own rows. Each query
runs with the row table in scope and again with only the summary visible; the
answers must agree. `count(cls)` counts rows, which one row per class does
not hold, so the summary alone refuses it."""

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.core.exceptions import NoDatasourceException
from trilogy.execution.assertions import DatasourceAssertionError
from trilogy.execution.state.isolation import hidden_datasources

ROWS = """select 1 as id, 'a' as species union all select 2, 'b'
union all select 3, 'b' union all select 4, 'c' union all select 5, null"""

CLASS = "case when species = 'a' then 'x' when species = 'b' then 'y' end"

FITS = """select 'x' as fit_class, 1 as n union all select 'y', 2
union all select null, 2"""

DEFINITIONS = {
    "derived": (
        f"auto cls <- {CLASS};",
        "id: id, species: species",
        ROWS,
    ),
    "property": (
        "property id.cls string;",
        "id: id, species: species, cls: cls",
        f"select id, species, {CLASS} as cls from ({ROWS})",
    ),
}


def _model(definition: str) -> str:
    cls, columns, rows = DEFINITIONS[definition]
    return f"""
key id int;
property id.species string;
{cls}
auto n_by_cls <- count(id) by cls;
datasource raw ({columns}) grain (id) query '''{rows}''';
datasource fits (fit_class: cls, n: n_by_cls) grain (cls) query '''{FITS}''';
"""


ANSWERED = {
    "count_distinct": "select count_distinct(cls) as v;",
    "min": "select min(cls) as v;",
    "max": "select max(cls) as v;",
    "bool_or": "select bool_or(cls = 'x') as v;",
    "sum_stored": "select sum(n_by_cls) as v;",
    "count_stored": "select count(n_by_cls) as v;",
    "max_stored": "select max(n_by_cls) as v;",
    "count_distinct_filtered": "select count_distinct(cls) as v where cls = 'x';",
    "row_count": "select count(id) as v;",
    "stored_rows": "select cls, n_by_cls order by cls asc nulls first;",
}


def _run(definition: str, query: str, hide: list[str]) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_model(definition))
    with hidden_datasources(executor.environment, hide):
        return [tuple(row) for row in executor.execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize("definition", sorted(DEFINITIONS))
@pytest.mark.parametrize("name", sorted(ANSWERED))
def test_summary_alone_answers_from_its_own_rows(definition: str, name: str):
    query = ANSWERED[name]
    assert _run(definition, query, ["raw"]) == _run(definition, query, [])


@pytest.mark.parametrize("definition", sorted(DEFINITIONS))
def test_count_of_column_is_refused_without_rows(definition: str):
    assert _run(definition, "select count(cls) as v;", []) == [(3,)]
    with pytest.raises(NoDatasourceException):
        _run(definition, "select count(cls) as v;", ["raw"])


def _asset(asserts: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.execute_raw_sql(
        "CREATE TABLE raw AS SELECT i AS id, CASE WHEN i % 3 = 0 THEN 'a'"
        " WHEN i % 3 = 1 THEN 'b' ELSE 'c' END AS species FROM range(30) t(i)"
    )
    executor.parse_text(f"""
key id int;
property id.species string;
auto cls <- case when species = 'a' then 'x' else 'y' end;
auto n_by_cls <- count(id) by cls;
root datasource raw (id: id, species: species) grain (id) address raw;
datasource fits (fit_class: cls, n: n_by_cls) grain (cls) address fits
{asserts};
""")
    return executor


def test_assertion_reads_classes_kept_from_the_asset():
    executor = _asset(
        "assert count_distinct(cls) >= prior(count_distinct(cls))\n"
        "assert sum(n_by_cls) >= 0.75 * prior(sum(n_by_cls))"
    )
    fits = executor.environment.datasources["fits"]
    executor.update_datasource(fits)
    executor.execute_raw_sql("DELETE FROM raw WHERE species = 'a'")
    with pytest.raises(DatasourceAssertionError) as info:
        executor.update_datasource(fits)
    assert "= 1, prior(" in str(info.value)
    assert executor.execute_raw_sql(
        "SELECT fit_class, n FROM fits ORDER BY 1"
    ).fetchall() == [("x", 10), ("y", 20)]
