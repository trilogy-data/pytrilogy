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
        # rowset-qualified concepts resolve inside the subquery body
        (
            "with rs as select id, val where val >= 20;select id where id in (select rs.id) order by id asc;",
            [(2,), (3,)],
        ),
    ],
)
def test_subquery_execution(backend, query, expected):
    assert _engine().execute_text(query)[-1].fetchall() == expected


def test_multi_output_subquery_rejected(backend):
    query = (
        "select sum(val) -> total having total > (select max(val) -> a, min(val) -> b);"
    )
    with pytest.raises(HydrationError, match="exactly one column") as excinfo:
        _engine().generate_sql(query)
    meta = excinfo.value.diagnostic.meta
    assert meta is not None and meta.line is not None


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


# A rowset of (val, cat) pairs for rows val>=20 -> {(20, 1), (30, 2)}.
_TUPLE_SETUP = "with rs as select id, val, cat where val >= 20;"


@pytest.mark.parametrize(
    "predicate,expected",
    [
        # 2-col tuple membership: rows whose (val, cat) is in {(20,1),(30,2)}
        ("(val, cat) in (select rs.val, rs.cat)", [(2,), (3,)]),
        # NOT IN is the exact complement
        ("(val, cat) not in (select rs.val, rs.cat)", [(1,)]),
        # 3-col tuple membership (the q14 shape)
        ("(id, val, cat) in (select rs.id, rs.val, rs.cat)", [(2,), (3,)]),
        # correlation: matching against swapped columns (cat, val) matches nothing
        ("(val, cat) in (select rs.cat, rs.val)", []),
        # aliased/computed outputs bind positionally
        ("(val + 1, cat) in (select rs.val + 1, rs.cat)", [(2,), (3,)]),
    ],
)
def test_tuple_subquery_membership(backend, predicate, expected):
    q = _TUPLE_SETUP + f"select id where {predicate} order by id asc;"
    assert _engine().execute_text(q)[-1].fetchall() == expected


@pytest.mark.parametrize("operator", ["in", "not in"])
def test_tuple_membership_inline_matches_named(backend, operator):
    # inline `(select ...)` lowers to the same shape as the named-rowset form
    inline = _TUPLE_SETUP + (
        f"select id where (val, cat) {operator} (select rs.val, rs.cat) order by id asc;"
    )
    named = _TUPLE_SETUP + (
        f"select id where (val, cat) {operator} (rs.val, rs.cat) order by id asc;"
    )
    assert (
        _engine().execute_text(inline)[-1].fetchall()
        == _engine().execute_text(named)[-1].fetchall()
    )


def test_tuple_subquery_null_identity(backend):
    # a null component matches a null component (membership is identity, total),
    # identical to the equivalent named-rowset tuple membership
    model = """
key nid int;
property nid.x int;
property nid.y int;
datasource nt (nid: nid, x: x, y: y) grain (nid)
  query '''
    select 1 as nid, 1 as x, 2 as y
    union all select 2, 3, cast(null as int)
  ''';
with nrs as select nid, x, y;
"""

    def run(pred):
        eng = Dialects.DUCK_DB.default_executor()
        q = model + f"select nid where (x, y) in ({pred}) order by nid asc;"
        return eng.execute_text(q)[-1].fetchall()

    assert run("select nrs.x, nrs.y") == run("nrs.x, nrs.y") == [(1,), (2,)]


@pytest.mark.parametrize(
    "predicate,message",
    [
        # scalar left vs multi-column subquery
        ("val in (select rs.val, rs.cat)", "projects 2 columns"),
        # tuple arity too large / too small for the subquery projection
        (
            "(val, cat) in (select rs.val)",
            "left side has 2 fields but the subquery selects 1",
        ),
        (
            "(val, cat) in (select rs.id, rs.val, rs.cat)",
            "left side has 2 fields but the subquery selects 3",
        ),
    ],
)
def test_tuple_membership_arity_rejected(backend, predicate, message):
    q = _TUPLE_SETUP + f"select id where {predicate};"
    with pytest.raises(HydrationError, match=message) as excinfo:
        _engine().generate_sql(q)
    meta = excinfo.value.diagnostic.meta
    assert meta is not None and meta.line is not None


@pytest.mark.parametrize("kind", ["inline", "named"])
def test_tuple_membership_grainless_output(backend, kind):
    # membership as a bare (grain-less) SELECT-output scalar: the constant-LHS
    # probe has no row source but the existence set must still be wired
    # (the handoff's minimal fixture shape). True for (1,2), False for (1,4).
    rhs = (
        "(select pairs.val, pairs.cat)"
        if kind == "inline"
        else "(pairs.val, pairs.cat)"
    )
    q = (
        "with pairs as select val, cat where val = 20;"
        f"select (20, 1) in {rhs} as present, (20, 2) in {rhs} as cross_pair_absent;"
    )
    assert _engine().execute_text(q)[-1].fetchall() == [(True, False)]


def test_tuple_subquery_round_trips(backend):
    q = _TUPLE_SETUP + (
        "select id where (val, cat) in (select rs.val, rs.cat) order by id asc;"
    )
    engine = _engine()
    _, parsed = engine.environment.parse(q)
    rendered = Renderer(environment=engine.environment).to_string(parsed[-1])
    assert "_subquery_" not in rendered
    # the tuple LHS renders as `(val, cat)`, not the internal `row_tuple(...)`,
    # and the RHS reproduces the inline `(select ...)` form
    assert "row_tuple" not in rendered
    flat = rendered.replace(" ", "").replace("\n", "").lower()
    assert "(val,cat)in(select" in flat
    engine2 = _engine()
    assert engine2.execute_text(_TUPLE_SETUP + rendered)[-1].fetchall() == [(2,), (3,)]
