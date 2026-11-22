"""A misshapen grouping call, or the removed `by rollup` clause, is answered
with the call form to write instead."""

import pytest

from trilogy import Environment
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_grouping_call_shape
from trilogy.parsing.v2.model import HydrationError

MODEL = "key eid int;\nproperty eid.department string;\nproperty eid.credits int;\n"
ARM = "select department, sum(credits) as t"


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK], autouse=True)
def backend(request, monkeypatch):
    monkeypatch.setattr(CONFIG, "parser_backend", request.param)


@pytest.mark.parametrize(
    "query",
    [
        f"select *rollup({ARM}, department);",
        f"rollup(({ARM}), department);",
        f"select rollup(({ARM}), department) order by t desc;",
        f"with r as cube(({ARM}), department) having t > 1;",
    ],
)
def test_misshapen_call_names_the_shape(query):
    with pytest.raises(InvalidSyntaxException, match=r"Syntax \[235\]"):
        Environment().parse(MODEL + query)


@pytest.mark.parametrize(
    "clause,call",
    [
        ("by rollup (department)", "rollup"),
        ("by cube (department)", "cube"),
        ("by grouping sets ((department), ())", "grouping_sets"),
    ],
)
def test_removed_clause_names_its_call(clause, call):
    with pytest.raises(HydrationError, match=rf"`select \*{call}\(\(select"):
        Environment().parse(MODEL + f"{ARM} {clause};")


@pytest.mark.parametrize(
    "query,message",
    [
        (f"select *rollup(({ARM}), department) where credits > 1;", "with `having`"),
        (
            f"select *rollup(({ARM} order by t asc), department) order by t desc;",
            "written once",
        ),
        (
            f"select *rollup(({ARM}), department), *cube(({ARM}), department);",
            "one `\\*<call>` per select",
        ),
    ],
)
def test_spread_limits(query, message):
    with pytest.raises(HydrationError, match=message):
        Environment().parse(MODEL + query)


@pytest.mark.parametrize(
    "text",
    [
        f"select *rollup(({ARM}), department);",
        f"from rollup(({ARM}), department) -> (d, t) select d, t frm;",
        f"with r as rollup(({ARM}), department);\nselect r.department frm;",
        "datasource d (x: x) query '''select x from t group by rollup(x)''' oops;",
    ],
)
def test_detector_ignores_well_formed_calls(text):
    assert detect_grouping_call_shape(text, len(text) - 2) is None


def test_grouping_outside_a_call_points_inside_it():
    query = (
        f"with r as rollup(({ARM}), department);\n"
        "select r.department, grouping(r.department) as g;"
    )
    with pytest.raises(InvalidSyntaxException, match="inside the select"):
        Environment().parse(MODEL + query)
