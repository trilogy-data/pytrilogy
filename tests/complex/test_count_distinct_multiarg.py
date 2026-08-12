"""Multi-arg `count_distinct(a, b)` / `count(distinct a, b)` is legal syntax
counting distinct combinations: hydration desugars the argument list through
grain(), so it builds identically to `count_distinct(grain(a, b))`. The SQL
habit `COUNT(DISTINCT a, b)` (TPC-DS q06 eval) previously died in a parse-error
loop; the source text is accepted as authored so it round-trips.

Deliberate Trilogy semantic: grain() is total over NULLs, so a combination
with a missing member still counts — unlike SQL's COUNT(DISTINCT a, b), which
drops rows where any member is NULL.
"""

from __future__ import annotations

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

MODEL = """
key sid int;
property sid.a string;
property sid.b string;

datasource s (sid: sid, a: a, b: b)
grain (sid)
query '''
select 1 as sid, 'x' as a, 'y' as b
union all select 2, 'x', 'y'
union all select 3, 'x', 'z'
union all select 4, null, 'y'
union all select 5, null, 'y'
''';
"""

_FORMS = [
    "select count_distinct(a, b) as n;",
    "select count_distinct(a, b, sid) as n;",
    "select count(distinct a, b) as n;",
    "select COUNT_DISTINCT( a , b ) as n;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _FORMS)
def test_multiarg_count_distinct_parses(backend, body):
    backend(MODEL + body)


def _sql(query: str) -> str:
    exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
    return exec_.generate_sql(MODEL + query)[-1]


def test_multiarg_desugars_to_explicit_grain():
    explicit = _sql("select count_distinct(grain(a, b)) as n;")
    assert _sql("select count_distinct(a, b) as n;") == explicit
    assert _sql("select count(distinct a, b) as n;") == explicit


def test_single_arg_does_not_pick_up_grain_wrapper():
    assert "md5" not in _sql("select count_distinct(a) as n;").lower()


def test_multiarg_counts_combinations_including_null_members():
    exec_ = Dialects.DUCK_DB.default_executor(environment=Environment())
    rows = exec_.execute_text(MODEL + "select count_distinct(a, b) as n;")[
        -1
    ].fetchall()
    # distinct pairs: ('x','y'), ('x','z'), (NULL,'y') — the NULL pair counts
    assert rows[0][0] == 3
