"""Inline `(select ...)` subqueries: desugared to an anonymous single-output
rowset, referenced as a scalar (grain-less -> cross-join) or as a membership
set (`in (select ...)` -> existence semijoin). Covers execute correctness,
single-output validation, and render round-trip on both grammar backends."""

from __future__ import annotations

import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG, ParserBackend
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.model import HydrationError

_MODEL = """
key id int;
property id.val int;
property id.cat int;
datasource t (id: id, val: val, cat: cat) grain (id)
  query '''
    select 1 as id, 10 as val, 1 as cat
    union all select 2, 20, 1
    union all select 3, 30, 2
  ''';
"""


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def backend(request):
    prior = CONFIG.parser_backend
    CONFIG.parser_backend = request.param
    yield request.param
    CONFIG.parser_backend = prior


def _engine():
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_MODEL)
    return engine


@pytest.mark.parametrize(
    "query,expected",
    [
        # scalar in HAVING: sum(60) > max(30) -> row kept
        ("select sum(val) -> total having total > (select max(val) -> mx);", [(60,)]),
        ("select sum(val) -> total having total < (select max(val) -> mx);", []),
        # scalar in WHERE: val > max(30)/2=15 -> ids 2,3
        (
            "select id where val > (select max(val)/2 -> half) order by id asc;",
            [(2,), (3,)],
        ),
        # membership: cats where val>=25 -> {2}; id with cat in {2} -> id 3
        ("select id where cat in (select cat -> c where val >= 25);", [(3,)]),
    ],
)
def test_subquery_execution(backend, query, expected):
    assert _engine().execute_text(query)[-1].fetchall() == expected


def test_multi_output_subquery_rejected(backend):
    query = (
        "select sum(val) -> total having total > (select max(val) -> a, min(val) -> b);"
    )
    with pytest.raises(HydrationError, match="exactly one column"):
        _engine().generate_sql(query)


def test_scalar_subquery_round_trips(backend):
    query = "select sum(val) -> total having total > (select max(val) -> mx);"
    engine = _engine()
    _, parsed = engine.environment.parse(query)
    rendered = Renderer(environment=engine.environment).to_string(parsed[-1])
    # the synthetic `_subquery_*` rowset never leaks as a projected column
    assert "_subquery_" not in rendered
    assert "(select" in rendered.replace(" ", "").replace("\n", "").lower() or (
        "(where" in rendered.replace(" ", "").replace("\n", "").lower()
    )
    # reparse + re-execute yields the same result
    engine2 = _engine()
    reparsed = engine2.execute_text(rendered)[-1].fetchall()
    assert reparsed == [(60,)]
