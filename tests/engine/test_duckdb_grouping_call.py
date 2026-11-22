"""`rollup(...)`, `cube(...)` and `grouping_sets(...)` are table-valued calls:
one parenthesized arm select, then its grouping keys."""

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.model import HydrationError

MODEL = """
key eid int;
property eid.department string;
property eid.course string;
property eid.credits int;
datasource enroll (eid: eid, department: department, course: course, credits: credits)
grain (eid)
query '''
select 1 eid, 'math' department, 'calc' course, 4 credits union all
select 2, 'math', 'algebra', 3 union all
select 3, 'art', 'paint', 2
''';
"""

ARM = "(select department, course, sum(credits) as t)"
ORDER = "order by dept asc nulls first, crs asc nulls first"
ROLLUP_ROWS = [
    (None, None, 9),
    ("art", None, 2),
    ("art", "paint", 2),
    ("math", None, 7),
    ("math", "algebra", 3),
    ("math", "calc", 4),
]
CUBE_ROWS = sorted(
    ROLLUP_ROWS + [(None, "algebra", 3), (None, "calc", 4), (None, "paint", 2)],
    key=lambda r: (r[0] is not None, r[0] or "", r[1] is not None, r[1] or ""),
)


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def executor(request, monkeypatch):
    monkeypatch.setattr(CONFIG, "parser_backend", request.param)
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(MODEL)
    return exec


def results(executor, query: str) -> list[list[tuple]]:
    return [
        [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
        for sql in executor.generate_sql(query)
    ]


def rows(executor, query: str) -> list[tuple]:
    return results(executor, query)[-1]


@pytest.mark.parametrize(
    "call,expected",
    [
        (f"rollup({ARM}, department, course)", ROLLUP_ROWS),
        (f"cube({ARM}, department, course)", CUBE_ROWS),
        (
            f"grouping_sets({ARM}, (department, course), (department), ())",
            ROLLUP_ROWS,
        ),
    ],
)
def test_inline_and_named_forms_agree(executor, call, expected):
    inline = f"from {call} -> (dept, crs, t) select dept, crs, t {ORDER};"
    signed = f"with r as {call} -> (dept, crs, t);\nselect r.dept, r.crs, r.t order by r.dept asc nulls first, r.crs asc nulls first;"
    named = f"with r as {call};\nselect r.department, r.course, r.t order by r.department asc nulls first, r.course asc nulls first;"
    assert rows(executor, inline) == expected
    assert rows(executor, signed) == expected
    assert rows(executor, named) == expected


def test_inline_outputs_show_their_signature_names(executor):
    result = executor.execute_text(
        "from rollup((select department, sum(credits) as t), department)"
        " -> (dept, total) select dept, total;"
    )[-1]
    assert list(result.keys()) == ["dept", "total"]


def test_signature_name_may_repeat_a_model_concept(executor):
    query = (
        "from rollup((select department, sum(credits) as t), department)"
        " -> (department, t)\n"
        "select department, t order by department asc nulls first;\n"
        "select department, count(eid) as n order by department asc;"
    )
    assert results(executor, query) == [
        [(None, 9), ("art", 2), ("math", 7)],
        [("art", 1), ("math", 2)],
    ]


def test_two_statements_may_share_signature_names(executor):
    query = (
        "from rollup((select department, sum(credits) as t), department) -> (d, t)\n"
        "select d, t order by d asc nulls first;\n"
        "from rollup((select course, sum(credits) as t), course) -> (d, t)\n"
        "select d, t order by d asc nulls first;"
    )
    assert results(executor, query) == [
        [(None, 9), ("art", 2), ("math", 7)],
        [(None, 9), ("algebra", 3), ("calc", 4), ("paint", 2)],
    ]


def test_arm_filters_before_and_trailing_select_after(executor):
    query = (
        "from rollup((select department, course, sum(credits) as t where credits > 2),"
        " department, course) -> (dept, crs, t)\n"
        f"select dept, crs, t where t > 3 {ORDER};"
    )
    assert rows(executor, query) == [
        (None, None, 7),
        ("math", None, 7),
        ("math", "calc", 4),
    ]


def test_flags_and_keys_by_alias(executor):
    query = (
        "from rollup((select department as d, sum(credits) as t, grouping(d) as g), d)"
        " -> (dept, t, g)\n"
        "select coalesce(dept, 'ALL') as label, t, g order by g asc, label asc;"
    )
    assert rows(executor, query) == [("art", 2, 0), ("math", 7, 0), ("ALL", 9, 1)]


def test_key_need_not_be_projected(executor):
    query = (
        "with r as rollup((select department, sum(credits) as t), department, course);\n"
        "select r.department, r.t order by r.department asc nulls first, r.t asc;"
    )
    assert rows(executor, query) == [
        (None, 9),
        ("art", 2),
        ("math", 3),
        ("math", 4),
        ("math", 7),
    ]


@pytest.mark.parametrize(
    "query,message",
    [
        (
            "from rollup((select department, sum(credits) as t), department)"
            + " select department, t;",
            "names its outputs",
        ),
        (
            "with r as rollup((select department, sum(credits) as t), department)"
            + " -> (a);",
            "declares 1",
        ),
        (
            "with r as rollup((select department, sum(credits) as t), department)"
            + " -> (a string, b);",
            "give a name only",
        ),
    ],
)
def test_signature_errors(executor, query, message):
    with pytest.raises((InvalidSyntaxException, HydrationError), match=message):
        executor.parse_text(query)


@pytest.mark.parametrize(
    "query",
    [
        "with r as rollup((select department, course as crs, sum(credits) as t,"
        + " grouping(department) as g where credits > 2), department, crs);",
        f"with r as cube({ARM}, department, course) -> (dept, crs, total);",
        f"from grouping_sets({ARM}, (department, course), (department), ())"
        + " -> (dept, crs, total)\n"
        + "select dept, crs, total where total > 3 order by dept asc limit 3;",
    ],
)
def test_render_round_trips(query):
    _, statements = Environment().parse(MODEL + query)
    first = Renderer().to_string(statements[-1])
    assert first.split("(")[0].split()[-1] in ("rollup", "cube", "grouping_sets")
    assert "_virt" not in first and "_rollup_" not in first
    _, reparsed = Environment().parse(MODEL + first)
    assert Renderer().to_string(reparsed[-1]) == first


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select *rollup((select department, sum(credits) as t), department)"
            + " having t > 2 order by t desc limit 2;",
            [(None, 9), ("math", 7)],
        ),
        (
            "select *cube((select department, sum(credits) as t,"
            + " grouping(department) as g), department) -> (d, total, --g)"
            + " where total > 2 order by g asc, total desc;",
            [("math", 7), (None, 9)],
        ),
        (
            "select *union((select department as x), (select course as x)) -> (name)"
            + " where name != 'art' order by name asc limit 2;",
            [("algebra",), ("calc",)],
        ),
        (
            "select *except((select department as x), (select 'math' as x)) -> (name);",
            [("art",)],
        ),
    ],
)
def test_spread_runs_a_call_as_the_query(executor, query, expected):
    assert rows(executor, query) == expected
    _, statements = Environment().parse(MODEL + query)
    first = Renderer().to_string(statements[-1])
    assert first.startswith("select *")
    _, reparsed = Environment().parse(MODEL + first)
    assert Renderer().to_string(reparsed[-1]) == first


def test_spread_shows_signature_names_and_hides_dashed_ones(executor):
    result = executor.execute_text(
        "select *rollup((select department, sum(credits) as t,"
        " grouping(department) as g), department) -> (dept, total, --g);"
    )[-1]
    assert list(result.keys()) == ["dept", "total"]


def test_scalar_union_is_still_an_expression(executor):
    query = "select union((select max(credits)), (select min(credits))) as v;"
    assert sorted(rows(executor, query)) == [(2,), (4,)]
