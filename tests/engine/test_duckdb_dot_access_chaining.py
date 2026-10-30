"""`<concept>.<fn>` dot access to single-arg functions chains, and a concept
declared under such a spelling is rejected (#75)."""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.executor import Executor

_MODEL = """
key id int;
property id.name string;

datasource t (id: id, name: name)
grain (id)
query '''
select * from (values (1, 'ab'), (-2, 'cde'), (-3, 'cde')) t(id, name)
''';
"""


def _executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_MODEL)
    return executor


@pytest.mark.parametrize(
    "query, expected",
    [
        ("select id.abs order by id.abs asc;", [(1,), (2,), (3,)]),
        (
            "select id.abs.sqrt order by id.abs.sqrt asc;",
            [(1.0,), (2**0.5,), (3**0.5,)],
        ),
        ("select name.upper.len order by name.upper.len asc;", [(2,), (3,)]),
        ("select id.count.abs;", [(3,)]),
        ("select id.sum.abs.sqrt;", [(2.0,)]),
        ("select name, id.count.abs order by name asc;", [("ab", 1), ("cde", 2)]),
    ],
)
def test_chained_dot_access(query, expected):
    assert _executor().execute_text(query)[-1].fetchall() == expected


@pytest.mark.parametrize(
    "declaration",
    [
        "auto id.count <- 1;",
        "auto id.abs <- id + 1;",
        "metric id.sum <- 1;",
        "key id.abs int;",
    ],
)
def test_declaration_shadowing_function_access_rejected(declaration):
    with pytest.raises(InvalidSyntaxException, match="already resolves"):
        _executor().parse_text(declaration)


@pytest.mark.parametrize(
    "declaration",
    [
        "property id.label <- 1;",
        "property name.year int;",
        "auto name.year <- 1;",
        "auto id.abs <- abs(id);",
        "auto id.count <- count(id);",
    ],
)
def test_declaration_not_shadowing_function_access_allowed(declaration):
    _executor().parse_text(declaration)


def test_property_form_keeps_function_access():
    executor = _executor()
    executor.parse_text("property id.count <- 7;")
    assert executor.execute_text("select id.count;")[-1].fetchall() == [(3,)]
    assert executor.execute_text("select count order by count asc limit 1;")[
        -1
    ].fetchall() == [(7,)]
