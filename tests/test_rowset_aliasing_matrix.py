"""Matrix coverage for rowset-output aliasing across consumption shapes.

The q75 bug (`UndefinedConceptException: <outer>.<inner>.<col>`) fired only when a
2-level rowset's *aliased* output (`deduped.yr as yr` -> `yearly.yr`) was
simultaneously filtered, windowed, and selected: the leaf-shorthand resolver
excluded the exact alias match and bound to the deeper source ref
(`yearly.deduped.yr`), an address that later fails lookup.

These tests sweep the aliasing/consumption space so regressions in any single
combination surface here. Contract: a rowset alias output, consumed in any mix
of select / where / window-partition / window-order / order-by, builds valid SQL
(or raises a CLEAN trilogy error) — never an internal `<outer>.<inner>` undefined
concept. Pass-through (unaliased) outputs must still expand to their deep path.
"""

import itertools

import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    UndefinedConceptException,
)

WS = (
    "key id int;\n"
    "property id.ext_sales_price float;\n"
    "property id.channel string;\n"
    "property id.week_seq int;\n"
    "property id.day_name string;\n"
    "datasource sales (id:id, ext:ext_sales_price, channel:channel, "
    "week_seq:week_seq, day_name:day_name) grain (id) address sales;\n"
)

ROWS = (
    "CREATE TABLE sales AS SELECT * FROM (VALUES "
    "(1, 10.0, 'WEB', 5, 'Mon'),(2, 20.0, 'WEB', 5, 'Mon'),"
    "(3, 5.0, 'WEB', 6, 'Tue'),(4, 7.0, 'CATALOG', 6, 'Tue'),"
    "(5, 3.0, 'WEB', 7, 'Wed')) t(id, ext, channel, week_seq, day_name);"
)


def _env(tmp_path):
    (tmp_path / "raw.preql").write_text(WS)
    env = Environment(working_path=str(tmp_path))
    env.parse("import raw as s;")
    return env


def _exe(tmp_path):
    (tmp_path / "raw.preql").write_text(WS)
    env = Environment(working_path=str(tmp_path))
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    exe.execute_raw_sql(ROWS)
    return exe


# A 2-level rowset chain whose second-level output `yearly.wk` is an explicit
# alias of the first level's aliased output `deduped.wk`. This is the q75 shape.
TWO_LEVEL = (
    "import raw as s;\n"
    "with deduped as where s.channel = 'WEB'\n"
    "select s.week_seq as wk, sum(s.ext_sales_price) as net;\n"
    "with yearly as select deduped.wk as wk, sum(deduped.net) as total;\n"
    "auto prev_total <- lag(yearly.total, 1) over "
    "(partition by yearly.wk order by yearly.total asc);\n"
    "auto prev_wk <- lag(yearly.wk, 1) over "
    "(order by yearly.wk asc);\n"
)


def _two_level_query(select_wk, where_wk, window_wk, order_wk):
    """Assemble a final select over the TWO_LEVEL chain toggling how the aliased
    output `yearly.wk` is consumed."""
    outputs = ["yearly.total", "prev_total"]
    if select_wk:
        outputs.insert(0, "yearly.wk")
    if window_wk:
        outputs.append("prev_wk")
    parts = TWO_LEVEL
    if where_wk:
        parts += "where yearly.wk >= 5\n"
    parts += "select " + ", ".join(outputs)
    if order_wk:
        parts += " order by yearly.wk asc"
    parts += ";"
    return parts


@pytest.mark.parametrize(
    "select_wk,where_wk,window_wk,order_wk",
    [
        c
        for c in itertools.product([True, False], repeat=4)
        # at least one consumption of the chain beyond the always-present total
        if any(c)
    ],
)
def test_two_level_alias_consumption_matrix_builds(
    tmp_path, select_wk, where_wk, window_wk, order_wk
):
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        sql = exe.generate_sql(
            _two_level_query(select_wk, where_wk, window_wk, order_wk)
        )
    except InvalidSyntaxException:
        # ORDER BY a non-selected column is a clean author error; the contract
        # only forbids the internal `<outer>.<inner>` undefined / sentinel.
        assert order_wk and not select_wk
        return
    assert sql
    joined = "\n".join(sql)
    assert "yearly.deduped" not in joined
    assert "INVALID_REFERENCE_BUG" not in joined


def test_two_level_alias_q75_exact_shape_builds(tmp_path):
    # The exact failing combination: filter AND window-order AND select the same
    # aliased 2-level output.
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exe.generate_sql(_two_level_query(True, True, True, False))[-1]
    assert "yearly.deduped" not in sql


def test_two_level_alias_consumption_executes(tmp_path):
    exe = _exe(tmp_path)
    rows = exe.execute_query(
        _two_level_query(select_wk=True, where_wk=True, window_wk=True, order_wk=True)
    ).fetchall()
    # WEB weeks: 5 -> 30.0, 6 -> 5.0, 7 -> 3.0; filtered wk>=5 keeps all three.
    # ordered by wk asc; prev_wk is lag of wk.
    wks = [r[0] for r in rows]
    assert wks == sorted(wks)
    assert len(rows) == 3


# --- alias-name shapes -------------------------------------------------------


def test_alias_same_name_as_source_leaf(tmp_path):
    # `deduped.yr as yr` — alias name equals the source leaf. The alias output
    # `yearly.yr` must win over the deeper source ref `yearly.deduped.yr`.
    env = _env(tmp_path)
    env.parse(
        "with deduped as select s.week_seq as yr, sum(s.ext_sales_price) as net;\n"
        "with yearly as select deduped.yr as yr, sum(deduped.net) as total;"
    )
    assert env.concepts["yearly.yr"].address == "yearly.yr"


def test_alias_renamed_each_level(tmp_path):
    # Renaming across both levels still resolves the final alias to itself.
    env = _env(tmp_path)
    env.parse(
        "with d as select s.week_seq as a, sum(s.ext_sales_price) as net;\n"
        "with y as select d.a as b, sum(d.net) as total;"
    )
    assert env.concepts["y.b"].address == "y.b"


def test_passthrough_still_expands_to_deep_path(tmp_path):
    # The other side of the contract: an UNALIASED pass-through keeps the deep
    # canonical address and the leaf-shorthand expands to it.
    env = _env(tmp_path)
    env.parse(
        "with d as select s.week_seq, sum(s.ext_sales_price) as net;\n"
        "with y as select d.week_seq, sum(d.net) as total;"
    )
    assert env.concepts["y.week_seq"].address == "y.d.s.week_seq"


def test_alias_then_passthrough_distinct_resolution(tmp_path):
    # One level aliases, the next passes through: the alias is direct, the
    # pass-through expands. Both in one chain, resolved distinctly.
    env = _env(tmp_path)
    env.parse(
        "with d as select s.week_seq as wk, sum(s.ext_sales_price) as net;\n"
        "with y as select d.wk, sum(d.net) as total;"
    )
    # d.wk is an alias output of d; y passes it through unaliased -> deep path
    assert env.concepts["y.wk"].address == "y.d.wk"


def test_alias_window_partition_and_order_on_alias(tmp_path):
    # Window whose BOTH partition and order key are the aliased 2-level output.
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exe.generate_sql(
        TWO_LEVEL + "auto ranked <- rank() over "
        "(partition by yearly.wk order by yearly.wk desc);\n"
        "where yearly.wk = 5 select yearly.wk, ranked;"
    )[-1]
    assert "yearly.deduped" not in sql


def test_three_level_alias_chain_filter_window_select(tmp_path):
    # Three stacked rowsets, each aliasing the prior's aliased output, then the
    # q75 consumption (filter + window + select) at the top.
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exe.generate_sql(
        "import raw as s;\n"
        "with l0 as select s.week_seq as wk, sum(s.ext_sales_price) as m;\n"
        "with l1 as select l0.wk as wk, sum(l0.m) as m;\n"
        "with l2 as select l1.wk as wk, sum(l1.m) as m;\n"
        "auto prev <- lag(l2.m, 1) over (order by l2.wk asc);\n"
        "where l2.wk = 5 select l2.wk, prev;"
    )[-1]
    assert "l2.l1" not in sql and "l1.l0" not in sql


def test_alias_name_collides_with_top_level_auto(tmp_path):
    # The agent hit this: a top-level `auto prev_qty` and a rowset output alias
    # `as prev_qty` share a name. They live in different namespaces (`local.` vs
    # the rowset's) and must stay distinct, not collapse.
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exe.generate_sql(
        "import raw as s;\n"
        "with d as select s.week_seq as wk, sum(s.ext_sales_price) as net;\n"
        "with y as select d.wk as wk, sum(d.net) as prev_qty;\n"
        "auto prev_qty <- lag(y.prev_qty, 1) over (order by y.wk asc);\n"
        "select y.wk, y.prev_qty, prev_qty order by y.wk;"
    )[-1]
    assert "y.d" not in sql


def test_genuine_ambiguity_preserved_under_alias_fix(tmp_path):
    # The alias short-circuit must not mask genuine two-output ambiguity in a
    # nested pass-through.
    env = _env(tmp_path)
    env.parse(
        "with base as select s.week_seq -> sold.week_seq, "
        "s.week_seq + 1 -> returned.week_seq;"
    )
    env.parse("with nested as select base.sold.week_seq, base.returned.week_seq;")
    with pytest.raises(UndefinedConceptException):
        env.concepts["nested.week_seq"]
