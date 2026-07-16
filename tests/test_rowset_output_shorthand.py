import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import UndefinedConceptException

WS = (
    "key id int;\n"
    "property id.ext_sales_price float;\n"
    "property id.channel string;\n"
    "property id.week_seq int;\n"
    "property id.day_name string;\n"
    "datasource sales (id:id, ext:ext_sales_price, channel:channel, "
    "week_seq:week_seq, day_name:day_name) grain (id) address sales;\n"
)


def _env(tmp_path):
    (tmp_path / "raw.preql").write_text(WS)
    env = Environment(working_path=str(tmp_path))
    env.parse("import raw as s;")
    return env


def test_unaliased_output_resolves_via_leaf_shorthand(tmp_path):
    env = _env(tmp_path)
    env.parse("with base as select s.week_seq, sum(s.ext_sales_price) as m;")
    assert env.concepts["base.week_seq"].address == "base.s.week_seq"
    # aliased output is already short and resolves directly
    assert env.concepts["base.m"].address == "base.m"


def test_partial_path_shorthand_resolves(tmp_path):
    env = _env(tmp_path)
    env.parse("with base as select s.week_seq, sum(s.ext_sales_price) as m;")
    assert env.concepts["base.s.week_seq"].address == "base.s.week_seq"


def test_cross_rowset_isolation(tmp_path):
    env = _env(tmp_path)
    env.parse("with r1 as select s.week_seq, sum(s.ext_sales_price) as m1;")
    env.parse("with r2 as select s.day_name, sum(s.ext_sales_price) as m2;")
    assert env.concepts["r1.week_seq"].address == "r1.s.week_seq"
    with pytest.raises(UndefinedConceptException):
        env.concepts["r2.week_seq"]


def test_import_namespace_stays_strict(tmp_path):
    env = _env(tmp_path)
    # shorthand only collapses within rowset namespaces, never import paths
    with pytest.raises(UndefinedConceptException):
        env.concepts["s.channel.week_seq"]


def test_ambiguous_shorthand_raises(tmp_path):
    env = _env(tmp_path)
    env.parse(
        "with amb as select s.week_seq -> sold.week_seq, "
        "s.week_seq + 1 -> returned.week_seq;"
    )
    with pytest.raises(UndefinedConceptException) as exc:
        env.concepts["amb.week_seq"]
    assert "amb.sold.week_seq" in exc.value.suggestions
    assert "amb.returned.week_seq" in exc.value.suggestions


DAILY = (
    "with daily as select s.week_seq, s.day_name, "
    "sum(s.ext_sales_price) as day_total;\n"
)


@pytest.mark.parametrize(
    "stmt",
    [
        "auto x <- daily.week_seq + 1;",
        "auto x <- sum(daily.day_total) by daily.week_seq;",
        "auto x <- lag(daily.day_total) over (order by daily.week_seq asc);",
        "def f(d) -> sum(daily.day_total ? daily.day_name = d);\n"
        "auto x <- @f('Mon');",
    ],
)
def test_shorthand_resolves_in_concept_phase_single_parse(tmp_path, stmt):
    # auto/def bodies hydrate in the concept phase, before the rowset (in the
    # SAME parse) stages any concept — resolution must consult the rowset's
    # symbol-table forward refs registered at COLLECT_SYMBOLS.
    (tmp_path / "raw.preql").write_text(WS)
    env = Environment(working_path=str(tmp_path))
    env.parse("import raw as s;\n" + DAILY + stmt)
    assert env.concepts["local.x"] is not None


def test_shorthand_window_single_parse_executes(tmp_path):
    (tmp_path / "raw.preql").write_text(WS)
    env = Environment(working_path=str(tmp_path))
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    exe.execute_raw_sql(
        "CREATE TABLE sales AS SELECT * FROM (VALUES "
        "(1, 10.0, 'WEB', 5, 'Mon'),(2, 20.0, 'WEB', 6, 'Tue'),"
        "(3, 5.0, 'WEB', 7, 'Wed')) t(id, ext, channel, week_seq, day_name);"
    )
    rows = exe.execute_query(
        "import raw as s;\n" + DAILY + "auto prev <- lag(daily.day_total) over "
        "(order by daily.week_seq asc);\n"
        "select daily.week_seq, prev order by daily.week_seq asc;"
    ).fetchall()
    assert rows == [(5, None), (6, 10.0), (7, 20.0)]


def test_shorthand_query_builds_and_executes(tmp_path):
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    exe.execute_raw_sql(
        "CREATE TABLE sales AS SELECT * FROM (VALUES "
        "(1, 10.0, 'WEB', 5, 'Mon'),(2, 20.0, 'CATALOG', 5, 'Mon'),"
        "(3, 5.0, 'STORE', 6, 'Tue')) t(id, ext, channel, week_seq, day_name);"
    )
    rows = exe.execute_query(
        "with base as where s.channel in ('WEB','CATALOG') "
        "select s.week_seq, sum(s.ext_sales_price) as day_sales;\n"
        "select base.week_seq, base.day_sales order by base.week_seq;"
    ).fetchall()
    assert rows == [(5, 30.0)]


def test_nested_rowset_shorthand_not_falsely_ambiguous(tmp_path):
    # A nested rowset passing through an unaliased deep-path output keeps BOTH a
    # symbol-table forward-ref (the written shorthand `y1999.base.week_seq`) AND
    # the canonical concept (`y1999.base.s.week_seq`). A downstream leaf-shorthand
    # `y1999.week_seq` subsequence-matches both but they are one output: the
    # resolver must collapse the alias instead of reporting false ambiguity (q64).
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    q = exe.generate_sql(
        "with base as select s.week_seq, sum(s.ext_sales_price) as m;\n"
        "with y1999 as select base.week_seq, base.m;\n"
        "select y1999.week_seq, y1999.m;"
    )
    assert q


def test_nested_rowset_shorthand_executes_consistently(tmp_path):
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    exe.execute_raw_sql(
        "CREATE TABLE sales AS SELECT * FROM (VALUES "
        "(1, 10.0, 'WEB', 5, 'Mon'),(2, 20.0, 'CATALOG', 5, 'Mon'),"
        "(3, 5.0, 'STORE', 6, 'Tue')) t(id, ext, channel, week_seq, day_name);"
    )
    # both the leaf-shorthand and the full canonical path must give the same rows
    short = exe.execute_query(
        "with base as select s.week_seq, sum(s.ext_sales_price) as m;\n"
        "with y1999 as select base.week_seq, base.m;\n"
        "select y1999.week_seq, y1999.m order by y1999.week_seq;"
    ).fetchall()
    full = exe.execute_query(
        "with base as select s.week_seq, sum(s.ext_sales_price) as m;\n"
        "with y1999 as select base.week_seq, base.m;\n"
        "select y1999.base.s.week_seq, y1999.m order by y1999.base.s.week_seq;"
    ).fetchall()
    assert short == full == [(5, 30.0), (6, 5.0)]


@pytest.mark.parametrize("depth", [2, 3, 4])
def test_arbitrary_rowset_nesting_resolves_via_leaf_shorthand(tmp_path, depth):
    # Stack `depth` rowsets each re-selecting the prior layer's passed-through
    # week_seq via leaf-shorthand; the deepest leaf-shorthand must still resolve
    # to one canonical concept regardless of how many alias layers accumulate.
    env = _env(tmp_path)
    lines = ["with r0 as select s.week_seq, sum(s.ext_sales_price) as m;"]
    for i in range(1, depth):
        lines.append(f"with r{i} as select r{i - 1}.week_seq, r{i - 1}.m;")
    last = depth - 1
    lines.append(f"select r{last}.week_seq, r{last}.m;")
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    assert exe.generate_sql("\n".join(lines))


def test_nested_rowset_self_join_shorthand(tmp_path):
    # q64's shape: two child rowsets off one base, self-joined on the passed-
    # through key, each side projected via leaf-shorthand.
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    assert exe.generate_sql(
        "with base as select s.week_seq, sum(s.ext_sales_price) as m;\n"
        "with y1999 as where base.week_seq = 5 select base.week_seq, base.m;\n"
        "with y2000 as where base.week_seq = 6 select base.week_seq, base.m;\n"
        "subset join y2000.week_seq = y1999.week_seq "
        "SELECT y1999.week_seq, y1999.m, y2000.m;"
    )


def test_self_join_rowset_phantom_join_key_not_ambiguous(tmp_path):
    # q02: a third rowset self-joins two sibling rowsets on a key; the OTHER
    # side's same-named join-key column (`nxt.wk`) is referenced ONLY in the
    # join condition, never projected. It leaks as a symbol-table forward-ref
    # `joined.nxt.wk`. A downstream leaf-shorthand `joined.wk` subsequence-matches
    # both it and the genuinely projected `joined.cur.wk`; the phantom join key
    # must not count toward ambiguity (it is never a materializable output).
    env = _env(tmp_path)
    exe = Dialects.DUCK_DB.default_executor(environment=env)
    assert exe.generate_sql(
        "with cur as select s.week_seq as wk, s.day_name as dw, "
        "sum(s.ext_sales_price) as amt;\n"
        "with nxt as select s.week_seq as wk, s.day_name as dw, "
        "sum(s.ext_sales_price) as amt;\n"
        "with joined as "
        "select cur.wk, cur.dw, cur.amt as cur_amt, nxt.amt as nxt_amt "
        "subset join nxt.wk = cur.wk "
        "subset join nxt.dw = cur.dw;\n"
        "select joined.wk;"
    )


@pytest.mark.parametrize(
    "tail",
    [
        "def pv(d) -> sum(joined.amt ? joined.dw = d) by joined.wk;\n"
        "select joined.wk, @pv(0) as v;",
        "def pE() -> sum(joined.amt ? joined.dw = 0) by joined.wk;\n"
        "select joined.wk, @pE() as v;",
        "def pF(d) -> joined.dw + d;\nselect joined.wk, @pF(0) as v;",
        "def pG(d) -> sum(joined.amt) by joined.dw;\nselect joined.dw, @pG(0) as v;",
    ],
)
def test_self_join_phantom_join_key_not_ambiguous_inside_def(tmp_path, tail):
    # q02 recurrence: the phantom join-key leak (`joined.nxt.dw`) only resolves
    # cleanly for direct refs because by then the genuine `joined.cur.dw` is a
    # real/pending concept. A `def` body hydrates in the concept phase, BEFORE
    # the rowset stages any output, so BOTH candidates are bare forward refs and
    # the prior real/pending-based collapse gave up -> false ambiguity. The
    # join-key-leak registry drops the phantom even when nothing has committed.
    env = _env(tmp_path)
    src = (
        "with cur as select s.week_seq as wk, s.day_name as dw, "
        "sum(s.ext_sales_price) as amt;\n"
        "with nxt as select s.week_seq as wk, s.day_name as dw, "
        "sum(s.ext_sales_price) as amt;\n"
        "with joined as "
        "select cur.wk, cur.dw, round(cur.amt / nxt.amt, 2) as amt "
        "subset join nxt.wk = cur.wk + 53 "
        "subset join nxt.dw = cur.dw;\n"
    )
    env.parse(src + tail)
    assert "joined" in env.concepts.rowset_namespaces


def test_self_join_join_key_leak_registry_isolates_other_side(tmp_path):
    # Only the join-condition-exclusive columns are leaks; the projected ones
    # (referenced in both the SELECT list and the join) must NOT be flagged.
    env = _env(tmp_path)
    env.parse(
        "with cur as select s.week_seq as wk, s.day_name as dw, "
        "sum(s.ext_sales_price) as amt;\n"
        "with nxt as select s.week_seq as wk, s.day_name as dw, "
        "sum(s.ext_sales_price) as amt;\n"
        "with joined as "
        "select cur.wk, cur.dw, round(cur.amt / nxt.amt, 2) as amt "
        "subset join nxt.wk = cur.wk + 53 "
        "subset join nxt.dw = cur.dw;\n"
    )
    leaks = env.concepts.rowset_join_key_leaks
    assert "joined.nxt.wk" in leaks
    assert "joined.nxt.dw" in leaks
    assert "joined.cur.wk" not in leaks
    assert "joined.cur.dw" not in leaks


def test_nested_rowset_genuine_ambiguity_still_raises(tmp_path):
    # Collapsing alias forms must NOT mask a real two-output ambiguity: a nested
    # rowset passing through two distinct week_seq outputs leaves `y.week_seq`
    # genuinely ambiguous.
    env = _env(tmp_path)
    env.parse(
        "with base as select s.week_seq -> sold.week_seq, "
        "s.week_seq + 1 -> returned.week_seq;"
    )
    env.parse("with nested as select base.sold.week_seq, base.returned.week_seq;")
    with pytest.raises(UndefinedConceptException):
        env.concepts["nested.week_seq"]
