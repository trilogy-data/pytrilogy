import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import networkx as nx
from pytest import mark, raises

from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.enums import Derivation, FunctionType, Granularity, JoinType, Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import Concept, FunctionCallWrapper, Grain
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import ShowStatement
from trilogy.dialect.mock import DEFAULT_SCALE_FACTOR
from trilogy.executor import Executor
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse_text


def test_basic_query(duckdb_engine: Executor, expected_results):
    graph = generate_graph(duckdb_engine.environment.materialize_for_select())

    list(nx.neighbors(graph, "c~local.count@Grain<local.item,local.store_id>"))
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


@mark.parametrize(
    "agg",
    ["count(1)", "sum(1)", "count(1) by *"],
)
def test_global_literal_aggregate(agg):
    """A global aggregate over a pure literal with no grain key (`count(1)`,
    `sum(1)`, `count(1) by *` -- the documented global-scalar idiom) must build
    and execute to a single constant row, not crash with an unguarded IndexError
    (sourceless QueryDatasource has an empty `datasources` list) nor render an
    empty-SELECT CTE (`by *` groups over a sourceless constant node)."""
    engine = Dialects.DUCK_DB.default_executor()
    text = f"""
key id int;
datasource t (id) grain (id)
  query '''select 1 as id union all select 2''';

select {agg} as c;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert results == [(1,)]


@mark.parametrize(
    "src,expected",
    [
        ("const c <- 10; select c as x where c > 50;", []),
        ("const c <- 10; select c as x where c < 50;", [(10,)]),
        ("parameter p int default 10; select p as x where p > 50;", []),
        ("parameter p int default 10; select p as x where p < 50;", [(10,)]),
    ],
)
def test_where_on_constant_concept_filters(src, expected):
    """A WHERE that references only constant/param concepts folds to a pure
    literal predicate (`c > 50` -> `1 = 0`). That predicate has no row
    arguments, so the scalar-only / independent-scope exemptions used to treat
    it as belonging to another scope and drop it -- the filter silently did
    nothing. It must be applied (empty result when the constant fails it)."""
    engine = Dialects.DUCK_DB.default_executor()
    results = engine.execute_text(src)[-1].fetchall()
    assert [tuple(r) for r in results] == expected


@mark.parametrize("decl", ["numeric", "numeric(15,2)"])
def test_numeric_parameter(decl):
    """`parameter p numeric default 4305.50` must build and execute to a Decimal
    (NUMERIC is first-class elsewhere); previously raised HydrationError with no
    NUMERIC branch in hydrate_parameter."""
    engine = Dialects.DUCK_DB.default_executor()
    results = engine.execute_text(
        f"parameter p {decl} default 4305.50; select p as x;"
    )[-1].fetchall()
    assert results == [(Decimal("4305.50"),)]


def test_where_on_aggregate_with_ratio_of_aggregates():
    """Filtering on a derived aggregate AND selecting a ratio of aggregates at
    the same grain must not leak the ratio expression into GROUP BY (DuckDB:
    'GROUP BY clause cannot contain aggregates')."""
    engine = Dialects.DUCK_DB.default_executor()
    text = """
key id int;
property id.g string;
property id.v int;
datasource t (id, g, v)
  grain (id)
  query '''select 1 as id, 'a' as g, 10 as v
           union all select 2, 'a', 20
           union all select 3, 'b', 5''';

auto a <- sum(v ? v > 5) by g;
auto b <- sum(v ? v > 0) by g;
auto r <- a / b;

where a > 0
select g, r;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert results == [("a", 1.0)]


_ROLLUP_GROUPING_MODEL = """
key sale_id int;
property sale_id.brand string;
property sale_id.class string;
property sale_id.amount float;
datasource sales (sale_id, brand, class, amount)
  grain (sale_id)
  query '''select 1 as sale_id, 'A' as brand, 'X' as class, 10.0 as amount
           union all select 2, 'A', 'Y', 20.0
           union all select 3, 'B', 'X', 30.0
           union all select 4, 'B', 'Y', 100.0''';
"""


def test_inline_aggregate_in_order_by_raises():
    """An inline aggregate in ORDER BY that isn't a SELECT output is rejected —
    aggregates can't be computed in the ordering scope. `grouping()` is the
    motivating case (Bug B3): it must be projected and ordered by alias."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
rowset overall <- select avg(amount) as overall_avg;
select brand, class, sum(amount) as total, --overall.overall_avg
by rollup (brand, class)
having total > overall.overall_avg
order by grouping(brand) desc, grouping(class) desc, brand nulls first;
"""
    with raises(Exception, match="ORDER BY contains aggregate"):
        engine.generate_sql(text)


def test_projected_grouping_in_rollup_orders_by_alias():
    """A `grouping()` projected (here hidden) alongside a `by rollup` aggregate
    and ordered by its alias must compute in the ROLLUP CTE and be referenced
    downstream by alias — not re-emitted as `grouping(col)` in the groupless
    join/filter wrapper (DuckDB: 'GROUPING statement cannot be used without
    groups'). Bug B3."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
rowset overall <- select avg(amount) as overall_avg;
select
    brand,
    class,
    sum(amount) as total,
    --overall.overall_avg,
    --grouping(brand) as gb,
    --grouping(class) as gc
by rollup (brand, class)
having total > overall.overall_avg
order by gb desc, gc desc, brand nulls first, class nulls first;
"""
    sql = engine.generate_sql(text)[-1]
    # grouping() must be computed in the rollup CTE, not re-emitted in the
    # groupless ORDER BY of the final wrapper SELECT.
    assert "grouping(" not in sql.split("ORDER BY")[-1], sql
    results = engine.execute_text(text)[-1].fetchall()
    # overall avg = 40; surviving rollup buckets: grand total (160) and B (130).
    assert [tuple(r) for r in results] == [
        (None, None, 160.0),
        ("B", None, 130.0),
        ("B", "Y", 100.0),
    ]


def test_inline_grouping_in_order_by_resolves_to_projected_alias():
    """The canonical rollup-subtotal ordering idiom: `grouping()` is projected
    (hidden) and ORDER BY references the inline `grouping(dim)` expression — not
    the alias. The dimension is projected under an alias (`brand as brand_type`)
    to exercise the pure-rename normalization. The inline ORDER BY grouping is
    semantically identical to its projected twin and must resolve to that alias,
    not raise `ORDER BY contains aggregate`. q05."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
rowset overall <- select avg(amount) as overall_avg;
select
    brand as brand_type,
    class,
    sum(amount) as total,
    --overall.overall_avg,
    --grouping(brand) as gb,
    --grouping(class) as gc
by rollup (brand, class)
having total > overall.overall_avg
order by
    grouping(brand) asc, brand asc nulls last,
    grouping(class) asc, class asc nulls last;
"""
    sql = engine.generate_sql(text)[-1]
    # The HAVING forces a groupless filter CTE; the inline ORDER BY grouping must
    # resolve to the projected alias and compute in the rollup CTE, not re-emit
    # `grouping(col)` in the groupless wrapper's ORDER BY.
    assert "grouping(" not in sql.split("ORDER BY")[-1], sql
    results = engine.execute_text(text)[-1].fetchall()
    # overall avg = 40; surviving rollup buckets: grand total (160) and B (130).
    assert [(r[0], r[1]) for r in results] == [
        ("B", "Y"),
        ("B", None),
        (None, None),
    ]


def test_inline_aggregate_in_order_by_resolves_to_projected_alias():
    """An inline non-grouping aggregate in ORDER BY that is byte-identical to a
    projected output resolves to that output's alias instead of raising — the
    ordering scope references the materialized column, it does not recompute."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
select brand, sum(amount) as total
order by sum(amount) desc;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [("B", 130.0), ("A", 30.0)]


def test_grouping_without_rollup_raises_clean_error():
    """`grouping()` is meaningless without an enclosing grouping set. Reached from
    the SELECT with no `by rollup`/cube/grouping-sets aggregate to anchor it — as
    a named `auto` concept (V1) or inline in a case (V3) — the grouping wrapper's
    concept gets an unanchorable abstract grain that recurses in build. Reject it
    with a clean author-time error instead of stack-overflowing. q05."""
    for select in (
        # V1: grouping() captured in a named derived concept, no rollup.
        """
auto ct <- case when grouping(brand) = 1 then 'tot' else brand end;
select ct, sum(amount) as g;
""",
        # V3: grouping() inline in a select case, no rollup.
        """
select case when grouping(brand) = 1 then 'tot' else brand end as ct,
    sum(amount) as g;
""",
    ):
        engine = Dialects.DUCK_DB.default_executor()
        engine.execute_text(_ROLLUP_GROUPING_MODEL)
        with raises(InvalidSyntaxException, match="grouping"):
            engine.generate_sql(select)


def test_grouping_over_rowset_output_no_rollup_raises_clean_error():
    """q67 Bug #2: `grouping()` applied to a rowset output in a DOWNSTREAM select
    with no rollup renders a groupless `SELECT grouping(...)` (all outputs are
    materialized rowset columns, so no GROUP BY) -> raw DuckDB "GROUPING statement
    cannot be used without groups". The stray-grouping guard missed it because the
    grouping is reached through a named `auto` concept behind a rename, invisible to
    the shallow scan. Must be a clean author-time error, whether named or inline."""
    for select in (
        # named `auto` concept wrapping grouping() over a rowset output, renamed.
        """
rowset rd <- select brand, class, sum(amount) as summed by rollup (brand, class);
auto g_cat <- grouping(rd.brand);
select rd.brand, rd.class, rd.summed, g_cat as gc;
""",
        # inline grouping() over a rowset output.
        """
rowset rd <- select brand, class, sum(amount) as summed by rollup (brand, class);
select rd.brand, rd.class, rd.summed, grouping(rd.brand) as gc;
""",
    ):
        engine = Dialects.DUCK_DB.default_executor()
        engine.execute_text(_ROLLUP_GROUPING_MODEL)
        with raises(InvalidSyntaxException, match="grouping"):
            engine.generate_sql(select)


def test_grouping_computed_inside_rowset_rollup_downstream_select_executes():
    """The valid sibling of the above: a rowset computes `grouping()` inside its
    OWN `by rollup` pass and materializes it as a column; a downstream select just
    projects that column (a flat read, no new groupless GROUPING()). Must NOT be
    rejected — the stray-grouping guard stops at the rowset boundary."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
rowset rd <- select brand, class, sum(amount) as summed, grouping(brand) as gc
    by rollup (brand, class);
select rd.brand, rd.class, rd.summed, rd.gc
order by rd.gc asc, rd.brand asc nulls last, rd.class asc nulls last;
"""
    results = engine.execute_text(text)[-1].fetchall()
    # gc=0 for every real/subtotal brand row; gc=1 only on the grand-total row.
    assert [r[3] for r in results] == [0, 0, 0, 0, 0, 0, 1]


def test_grouping_in_where_raises_clean_error():
    """`grouping()` in a WHERE clause is semantically invalid: WHERE runs before
    grouping, so there is no grouping set to anchor to. The planner used to
    materialize it as a standalone groupless `GROUPING()` CTE -> generate_sql
    succeeded but DuckDB rejected it ("GROUPING statement cannot be used without
    groups"). Reject it at author time instead. q70."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
where grouping(brand) = 1 or brand = 'B'
select brand, class, sum(amount) as total,
by rollup (brand, class);
"""
    with raises(InvalidSyntaxException, match="WHERE"):
        engine.generate_sql(text)


def test_grouping_in_having_no_rollup_executes():
    """The valid sibling of the WHERE case: `grouping(<group key>)` in HAVING runs
    post-aggregation against the implicit group key, so it is a valid (no-op)
    filter even without an explicit rollup spec — must NOT be rejected. q70."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
select brand, sum(amount) as total,
having grouping(brand) = 0
order by brand asc;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [("A", 30.0), ("B", 130.0)]


def test_grouping_in_named_concept_with_rollup_builds():
    """The valid form of the above: a named `auto` concept wrapping `grouping()`
    builds and runs as long as a `by rollup` aggregate in the select anchors it."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
auto ct <- case when grouping(brand) = 1 then 'tot' else brand end;
select ct, sum(amount) as g
by rollup (brand)
order by ct desc;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [
        ("tot", 160.0),
        ("B", 130.0),
        ("A", 30.0),
    ]


def test_grouping_over_rollup_wrapped_in_expression_colocates():
    """A `by rollup` aggregate wrapped in an expression (`coalesce(... , 0)`, the
    canonical zero-filled-rollup idiom) still anchors `grouping()`. The rollup
    spec must be found by walking into the item lineage, not only when the item
    is itself the bare AggregateWrapper — otherwise grouping() is falsely
    rejected as 'requires a by rollup'. q05."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
select
    brand,
    coalesce(sum(amount), 0) as m,
    grouping(brand) as g
by rollup (brand)
having m > 0
order by g, brand;
"""
    sql = engine.generate_sql(text)[-1]
    # grouping() colocates with the rollup CTE, never re-emitted groupless.
    assert "grouping(" not in sql.split("ORDER BY")[-1], sql
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [
        ("A", 30.0, 0),
        ("B", 130.0, 0),
        (None, 160.0, 1),
    ]


def test_grouping_over_rollup_macro_wrapped_in_expression_colocates():
    """The rollup may reach the projection through BOTH a `def` macro expansion
    and an expression wrapper — `coalesce(@rollup_agg(x), 0)`. The rollup
    AggregateWrapper then sits inside a FunctionCallWrapper (the macro call)
    inside the coalesce Function, so the spec walk must descend through the macro
    wrapper too; otherwise grouping() is falsely rejected as 'requires a by
    rollup'. q05 (def rollup_agg + coalesce-wrapped call)."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
def rollup_agg(m) -> sum(m);
select
    brand,
    coalesce(@rollup_agg(amount), 0) as m,
    grouping(brand) as g
by rollup (brand)
having m > 0
order by g, brand;
"""
    sql = engine.generate_sql(text)[-1]
    assert "grouping(" not in sql.split("ORDER BY")[-1], sql
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [
        ("A", 30.0, 0),
        ("B", 130.0, 0),
        (None, 160.0, 1),
    ]


def test_grouping_over_inline_expression_key_raises_clean_error():
    """`grouping(<inline expression>)` over a `by rollup (<same expression>)` key
    produced invalid SQL: the rollup key is materialized as a column and grouped
    by position, but `grouping(coalesce(...))` re-inlines the expression and never
    matches that column, so DuckDB rejected it with a raw BinderException
    ("GROUPING child must be a grouping column"). Reject it at author time with
    guidance to name the expression instead. The valid named form is covered by
    `test_grouping_in_named_concept_with_rollup_builds`. q05."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
select
    case when grouping(coalesce(brand, class)) = 1 then 'tot'
         else coalesce(brand, class) end as ch,
    sum(amount) as total
by rollup (coalesce(brand, class));
"""
    with raises(InvalidSyntaxException, match="concept .column. reference"):
        engine.generate_sql(text)


def test_grouping_over_aliased_rollup_key_builds():
    """`grouping(<source>)` over a `by rollup (<alias>)` whose key is that source
    under an alias (`brand as b` + `by rollup (b)`) produced invalid SQL: the
    rollup groups by the materialized alias column `b`, but `grouping(brand)`
    rendered against the source column `brand`, which is not a GROUP BY key →
    BinderException ("GROUPING child must be a grouping column"). Normalize the
    grouping() argument to the rollup-key alias so it lines up with the GROUP BY.
    q14 (channel/brand/class/category aliased rollup keys)."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
select
    brand as b,
    sum(amount) as total,
    grouping(brand) as g
by rollup (b)
order by g, b;
"""
    sql = engine.generate_sql(text)[-1]
    # grouping() must reference the aliased rollup-key column, not the source.
    assert 'grouping("brand")' not in sql.replace('"quizzical".', "")
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [
        ("A", 30.0, 0),
        ("B", 130.0, 0),
        (None, 160.0, 1),
    ]


def test_grouping_over_named_expression_key_builds():
    """The fix for the above: name the expression as a concept and use it in both
    the rollup key and `grouping()`. The same intent now compiles and runs."""
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(_ROLLUP_GROUPING_MODEL)
    text = """
auto ch <- coalesce(brand, class);
select
    case when grouping(ch) = 1 then 'tot' else ch end as label,
    sum(amount) as total
by rollup (ch)
order by total asc;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in results] == [
        ("A", 30.0),
        ("B", 130.0),
        ("tot", 160.0),
    ]


def test_null_constant_renders_inline_not_bound():
    """A `null` literal in a projection must render inline as SQL `null`, not as a
    bind parameter — the constant's value is `MagicConstants.NULL`, which the DB
    driver can't transform into a bound value. q05 subtotal/grand-total rows."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    sql = engine.generate_sql("select 1 as x, null as y;")[-1]
    assert ":y" not in sql
    results = engine.execute_text("select 1 as x, null as y;")[-1].fetchall()
    assert [tuple(r) for r in results] == [(1, None)]


_COMPOSITE_ROLLUP_MODEL = """
key chan int;
key oid int;
property <oid, chan>.txt string;
property <oid, chan>.amt float;
property <oid, chan>.prof float;
property <oid, chan>.loss float;
datasource sales (chan: chan, oid: oid, txt: txt, amt: amt, prof: prof, loss: loss)
grain (oid, chan)
query '''select 1 as chan, 1 as oid, 'p' as txt, 10.0 as amt, 5.0 as prof, 1.0 as loss
         union all select 1, 2, 'q', 20.0, 8.0, 2.0
         union all select 2, 1, 'p', 30.0, 9.0, 3.0''';
"""


def test_composite_rollup_aggregate_keeps_group_by():
    """A composite rollup aggregate (`sum(a) - sum(b)`) beside a second rollup
    aggregate and CASE/concat-derived projections of the rollup keys used to
    emit invalid SQL — a group node with no GROUP BY (DuckDB: 'column ... must
    appear in the GROUP BY').

    The fix has three parts: a ROLLUP marks its grouping-key dims (and dims
    derived from them, e.g. `concat('x', txt)`) nullable so the assembly join is
    null-safe/OUTER and the rollup rows survive; the null-safe join keys aren't
    downgraded to `=` past the rollup; and the rollup group node renders its
    GROUP BY even when a sibling rollup is a passthrough.

    Under the SELECT-level `by rollup (...)` clause BOTH operands of the
    composite measure share the one grouping pass, so `profit` now rolls up
    correctly at the subtotal/grand-total rows (this used to be NULL there when
    rollup was attached per-aggregate and `sum(prof)` stayed at leaf grain). q80."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_COMPOSITE_ROLLUP_MODEL)
    text = """
select
  case when chan = 1 then 'aa' else 'bb' end as channel,
  concat('x', txt) as outlet,
  sum(amt) as sales,
  sum(prof) - sum(coalesce(loss, 0)) as profit
by rollup (chan, txt)
order by channel asc, sales asc, profit asc, outlet asc nulls first;
"""
    results = engine.execute_text(text)[-1].fetchall()
    # ROLLUP(chan, txt) preserves leaves + per-channel subtotals + grand total.
    # sales rolls up (10/20 leaves -> 30/30 subtotals -> 60 grand). profit now
    # rolls up too: leaves 4/6/6, subtotals 10/6, grand 16.
    assert [tuple(r) for r in results] == [
        ("aa", "xp", 10.0, 4.0),
        ("aa", "xq", 20.0, 6.0),
        ("aa", None, 30.0, 10.0),
        ("bb", None, 30.0, 6.0),
        ("bb", "xp", 30.0, 6.0),
        ("bb", None, 60.0, 16.0),
    ]


_ROLLUP_SIBLING_MODEL = """
key oid int;
property oid.chan int;
property oid.ent int;
property oid.etxt string;
property oid.amt float;
datasource sales (oid: oid, chan: chan, ent: ent, etxt: etxt, amt: amt)
grain (oid)
query '''select 1 as oid, 1 as chan, 10 as ent, 'AAA' as etxt, 10.0 as amt union all
         select 2 as oid, 1 as chan, 11 as ent, 'BBB' as etxt, 30.0 as amt union all
         select 3 as oid, 2 as chan, 20 as ent, 'CCC' as etxt, 5.0 as amt''';
"""


def test_rollup_sibling_column_join_preserves_subtotals():
    """A projection over a column that is NOT a rollup key but is fetched at the
    rollup grain via a join-back (`etxt`, a sibling of the rollup key `ent`) must
    not lose the rollup rows. The rollup key `ent` is NULL at subtotal/grand-total
    rows; the join carrying `etxt` is plain `=`, so an INNER upgrade would drop
    every NULL-keyed rollup row. `UpgradeOuterFromKeySetEquivalence` wrongly read
    the rollup side's GROUP BY grain as a complete value set and upgraded
    LEFT_OUTER → INNER. q05 (store/catalog/web channel rollup report)."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_ROLLUP_SIBLING_MODEL)
    text = """
select
    case when chan = 1 then 'store' when chan = 2 then 'web' end as label,
    case when chan = 1 then concat('store', etxt)
         when chan = 2 then concat('web', etxt) end as entity_fmt,
    sum(amt) as total
by rollup (chan, ent)
order by label asc nulls last, entity_fmt asc nulls last;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [(r[0], r[1], float(r[2])) for r in results] == [
        ("store", "storeAAA", 10.0),
        ("store", "storeBBB", 30.0),
        ("store", None, 40.0),
        ("web", "webCCC", 5.0),
        ("web", None, 5.0),
        (None, None, 45.0),
    ]


_ROLLUP_HAVING_CROSSROWSET_MODEL = """
key sid int;
property sid.sbrand int;
property sid.samt float;
datasource store_sales (sid: sid, sbrand: sbrand, samt: samt)
grain (sid)
query '''select 1 as sid, 10 as sbrand, 100.0 as samt union all
         select 2, 11, 30.0''';

key wid int;
property wid.wbrand int;
property wid.wamt float;
datasource web_sales (wid: wid, wbrand: wbrand, wamt: wamt)
grain (wid)
query '''select 1 as wid, 10 as wbrand, 5.0 as wamt union all
         select 2, 11, 200.0''';
"""


def test_rollup_having_crossrowset_preserves_subtotals():
    """`by rollup` + `having <agg> > <cross-rowset scalar>` where the rollup keys
    are union/rowset outputs must keep subtotal/grand-total rows and apply the
    HAVING to every group (leaf and super-aggregate). The predicate is routed
    through the finer-dim → grain-key CASE-nulling semijoin
    (`_rewrite_having_finer_dims_to_membership`), whose `is not null` guard on the
    CASE-nulled rollup keys collides with rollup's legitimate NULL subtotal keys —
    silently dropping every subtotal row. The driver of the q14 token sink."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_ROLLUP_HAVING_CROSSROWSET_MODEL)
    text = """
auto overall_total <- sum(samt) + sum(wamt);
auto overall_count <- count(sid) + count(wid);
rowset overall_avg <- select overall_total / overall_count as avg_val;
with by_channel as union(
  (select 'STORE' as channel, sbrand as brand, sum(samt) as total_sales),
  (select 'WEB' as channel, wbrand as brand, sum(wamt) as total_sales)
) -> (channel, brand, total_sales);
select
    by_channel.channel,
    by_channel.brand,
    sum(by_channel.total_sales) as total_sales
by rollup (by_channel.channel, by_channel.brand)
having sum(by_channel.total_sales) > overall_avg.avg_val
order by by_channel.channel asc nulls first, by_channel.brand asc nulls first;
"""
    results = engine.execute_text(text)[-1].fetchall()
    # avg_val = 335/4 = 83.75; HAVING sum(total_sales) > 83.75 applied to every
    # group, subtotals included.
    assert [(r[0], r[1], float(r[2])) for r in results] == [
        (None, None, 335.0),
        ("STORE", None, 130.0),
        ("STORE", 10, 100.0),
        ("WEB", None, 205.0),
        ("WEB", 11, 200.0),
    ]


def test_rollup_having_auto_wrapped_crossrowset_scalar_preserves_subtotals():
    """Same as above, but the cross-rowset scalar is referenced through an `auto`
    indirection (`auto avg_val <- ...; having sum(x) > avg_val`) — the natural
    HAVING form, and exactly what TPC-DS q14 writes. The single-row-scalar detector
    only matched a direct `RowsetItem` ref, so an `auto`/BASIC wrapper (incl. a
    ratio of two rowset outputs) fell through to the finer-dim grain-key semijoin
    and silently dropped every rollup subtotal. Must recurse through BASIC args."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_ROLLUP_HAVING_CROSSROWSET_MODEL)
    text = """
rowset overall <- select sum(samt) + sum(wamt) as total_value, count(sid) + count(wid) as total_count;
auto avg_val <- overall.total_value / overall.total_count;
with by_channel as union(
  (select 'STORE' as channel, sbrand as brand, sum(samt) as total_sales),
  (select 'WEB' as channel, wbrand as brand, sum(wamt) as total_sales)
) -> (channel, brand, total_sales);
select
    by_channel.channel,
    by_channel.brand,
    sum(by_channel.total_sales) as total_sales
by rollup (by_channel.channel, by_channel.brand)
having sum(by_channel.total_sales) > avg_val
order by by_channel.channel asc nulls first, by_channel.brand asc nulls first;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [(r[0], r[1], float(r[2])) for r in results] == [
        (None, None, 335.0),
        ("STORE", None, 130.0),
        ("STORE", 10, 100.0),
        ("WEB", None, 205.0),
        ("WEB", 11, 200.0),
    ]


def test_union_subset_aggregate_preserves_row_multiplicity():
    """A `union(...) -> (...)` that stacks per-row values feeds a grouped rowset
    consumer. Two distinct rows can share the same (non-selected-key) column
    values; summing a subset of the union outputs must sum every stacked row, not
    a de-duplicated set. Regression: the union outputs were all bare KEY grain
    components, so sourcing a strict subset (gid, value) inserted a spurious
    GROUP BY (gid, value) that collapsed the duplicate rows before the SUM."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text("""
key event_id int;
property event_id.event_amount int;
property event_id.gid int;
property event_id.active bool;
datasource events (eid: event_id, gid: gid, amount: event_amount, active: active)
grain (event_id)
query '''select 1 as eid, 1 as gid, 0 as amount, false as active
union all select 2 as eid, 1 as gid, -2 as amount, true as active
union all select 3 as eid, 2 as gid, 6 as amount, true as active
union all select 4 as eid, 2 as gid, 6 as amount, true as active
union all select 5 as eid, 3 as gid, 9 as amount, false as active
union all select 6 as eid, 3 as gid, 1 as amount, true as active
union all select 7 as eid, 4 as gid, 12 as amount, false as active''';
""")
    text = """
with combined as union(
    (where active select event_id as eid, gid as gid, event_amount as value),
    (where not active select event_id as eid, gid as gid, event_amount as value)
) -> (eid, gid, value);
rowset rolled <- select combined.gid as gid, sum(combined.value) as total;
select rolled.gid, rolled.total order by rolled.gid asc;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [(r[0], r[1]) for r in results] == [(1, -2), (2, 12), (3, 10), (4, 12)]


_ROLLUP_WHERE_CROSSROWSET_MODEL = """
key sid int;
property sid.schannel string;
property sid.samt float;
property sid.scnt int;
datasource all_sales (sid: sid, schannel: schannel, samt: samt, scnt: scnt)
grain (sid)
query '''select 1 as sid, 'STORE' as schannel, 100.0 as samt, 1 as scnt union all
         select 2, 'STORE', 30.0, 1 union all
         select 3, 'WEB', 5.0, 1 union all
         select 4, 'WEB', 200.0, 1 union all
         select 5, 'CATALOG', 7.0, 1''';
"""


def test_rollup_where_crossrowset_preserves_grand_total():
    """`by rollup` + a WHERE filtering a rowset field against a cross-rowset scalar
    must keep the subtotal/grand-total rows. The scalar-vs-rowset filter was
    force-evaluated at the rollup output level (a filter-only ROWSET row-arg forces
    condition eval), dragging the finer input into the rollup output → an
    enrichment join back over the NULL-keyed subtotal grain + a re-applied filter
    that dropped every subtotal. The filter must instead push *below* the rollup.
    Covers both the aggregated field (`sum(ch.total_sales)` filtered on
    `ch.total_sales`) and a sibling field (filter on `ch.total_sales`, project only
    `sum(ch.sale_count)`). The form the q14 agent actually used (HAVING after `by
    rollup` won't parse) — the highest-impact q14 blocker."""
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_ROLLUP_WHERE_CROSSROWSET_MODEL)
    prefix = """
auto overall_avg_sale <- sum(samt) by * / count(scnt) by *;
with ch as
select schannel as channel, sum(samt) as total_sales, count(scnt) as sale_count;
"""
    # avg = 342/5 = 68.4; channels kept: STORE(130), WEB(205); CATALOG(7) dropped.
    # Rollup over the kept channels keeps the grand-total row.
    aggregated = engine.execute_text(prefix + """
select ch.channel, sum(ch.total_sales) as total_sales, sum(ch.sale_count) as total_count
where ch.total_sales > overall_avg_sale
by rollup (ch.channel)
order by ch.channel asc nulls first;
""")[-1].fetchall()
    assert [(r[0], float(r[1]), r[2]) for r in aggregated] == [
        (None, 335.0, 4),
        ("STORE", 130.0, 2),
        ("WEB", 205.0, 2),
    ]
    # Sibling field: filtered field is not the aggregated one; still must push below.
    sibling = engine.execute_text(prefix + """
select ch.channel, sum(ch.sale_count) as total_count
where ch.total_sales > overall_avg_sale
by rollup (ch.channel)
order by ch.channel asc nulls first;
""")[-1].fetchall()
    assert [(r[0], r[1]) for r in sibling] == [(None, 4), ("STORE", 2), ("WEB", 2)]


def test_predicate_not_pushed_past_window_order_key():
    """A filter on a window's ORDER BY key (an aggregate also materialized in
    the upstream group) must not be pushed below/into the window — SQL WHERE
    precedes window evaluation, so dropping those rows changes the lead/lag.
    The q59 family: a cross-year `lead` needs both years' rows present; filtering
    `flag = 1` early would strip the year-2 rows and yield 0 results."""
    engine = Dialects.DUCK_DB.default_executor()
    text = """
key rid int;
property rid.store int;
property rid.wk int;
property rid.yr int;
property rid.sales int;
datasource t (rid, store, wk, yr, sales)
  grain (rid)
  query '''select 1 as rid, 1 as store, 10 as wk, 1 as yr, 100 as sales
           union all select 2, 1, 62, 2, 50''';

auto flag <- max(yr) by store, wk;
auto sales_sum <- sum(sales) by store, wk;
auto nwk <- wk - (case when flag = 2 then 52 else 0 end);
auto ratio <- sales_sum / lead(sales_sum, 1) over (partition by store, nwk order by flag asc);

select store as s, wk as w, ratio as r, --flag,
having flag = 1 and ratio is not null
order by w asc;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [(r.s, r.w, r.r) for r in results] == [(1, 10, 2.0)]


_NESTED_FILTER_AGG_MEMBERSHIP_MODEL = """
key order_number int;
key item_id int;
property <order_number, item_id>.warehouse int;
property <order_number, item_id>.profit float;
property <order_number, item_id>.state string;
property <order_number, item_id>._ret int?;
auto is_returned <- _ret is not null;

datasource sales (
    order_number: order_number,
    item_id: item_id,
    warehouse: warehouse,
    profit: profit,
    state: state
)
grain (order_number, item_id)
query '''
    select 100 as order_number, 1 as item_id, 1 as warehouse, 10.0 as profit, 'IL' as state
    union all select 100, 2, 2, 20.0, 'IL'
    union all select 200, 1, 1, 5.0, 'IL'
    union all select 300, 1, 1, 7.0, 'CA'
    union all select 300, 2, 2, 8.0, 'CA'
''';

datasource returns (
    item_id: ~item_id,
    order_number: ~order_number,
    _ret: _ret
)
grain (order_number, item_id)
query '''select 200 as order_number, 1 as item_id, 200 as _ret''';
"""

# A filtered-membership key whose condition references another filtered concept
# (`applicable_orders is not null`) plus per-key aggregate(s). The q94 shape.
_NESTED_FILTER_AGG_MEMBERSHIP_QUERY = """
auto applicable_orders <- order_number ? state = 'IL';
auto qualifying_orders <- order_number
  ? applicable_orders is not null
    and count_distinct(warehouse) by order_number > 1
    and bool_or(is_returned) by order_number is not true;
where order_number in qualifying_orders
select order_number as o, sum(profit) as total_profit
order by o;
"""


def test_nested_filter_per_key_aggregate_membership_not_in_group_by():
    """Bug: a filtered-membership key (`order ? applicable_orders is not null AND
    count_distinct(...) by order > 1 AND ...`) whose condition mixes a nested
    filter concept with per-key aggregates was lowered as a grouped filter that
    rendered the membership CASE — aggregates and all — into the GROUP BY
    (DuckDB: 'GROUP BY clause cannot contain aggregates'). The per-key aggregates
    must be materialized in their own grouped CTE and the membership CASE
    evaluated at row grain — never inside a GROUP BY."""
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_NESTED_FILTER_AGG_MEMBERSHIP_MODEL)
    sql = executor.generate_sql(_NESTED_FILTER_AGG_MEMBERSHIP_QUERY)[-1]
    for block in re.findall(
        r"GROUP BY(.*?)(?:HAVING|ORDER BY|\)|$)", sql, re.DOTALL | re.IGNORECASE
    ):
        assert "count(" not in block.lower(), f"aggregate in GROUP BY:\n{sql}"
        assert "bool_or(" not in block.lower(), f"aggregate in GROUP BY:\n{sql}"


def test_nested_filter_per_key_aggregate_membership_executes():
    """End-to-end for the q94 shape: only order 100 qualifies — state IL
    (applicable), two distinct warehouses (count_distinct > 1), and no return
    (bool_or(is_returned) is not true). Orders 200 (one warehouse, returned) and
    300 (state CA) are excluded."""
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_NESTED_FILTER_AGG_MEMBERSHIP_MODEL)
    results = executor.execute_text(_NESTED_FILTER_AGG_MEMBERSHIP_QUERY)[-1].fetchall()
    assert [(r.o, float(r.total_profit)) for r in results] == [(100, 30.0)]


_DERIVED_MEMBERSHIP_MODEL = """
key sale_item int;
property sale_item.amt float;
datasource sales (id: sale_item, amt: amt) grain (sale_item)
query '''select 1 id, 10.0 amt union all select 2 id, 20.0 amt''';

key ret_item int;
property ret_item.refund float;
datasource returns (id: ret_item, refund: refund) grain (ret_item)
query '''select 1 id, 5.0 refund''';
"""


@mark.parametrize(
    "query,expected",
    [
        ("select amt where sale_item in ret_item;", [(10.0,)]),
        ("select amt where sale_item not in ret_item;", [(20.0,)]),
        (
            "auto has_return <- sale_item in ret_item; select amt where has_return;",
            [(10.0,)],
        ),
        (
            "auto no_return <- sale_item not in ret_item; select amt where no_return;",
            [(20.0,)],
        ),
        (
            "select sale_item as s, sale_item in ret_item as has_return order by s asc;",
            [(1, True), (2, False)],
        ),
    ],
)
def test_derived_membership_existence(query, expected):
    """A cross-model membership (`a in b`) carries existence semantics whether it
    is an inline WHERE atom OR a derived/projected boolean (`auto flag <- a in b`,
    `select ... as flag`). The derived form must route the RHS through an
    existence subquery, not demand a join between the unconnected models."""
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_DERIVED_MEMBERSHIP_MODEL)
    sql = executor.generate_sql(query)[-1]
    assert "in (select" in sql.lower(), f"membership not rendered as subquery:\n{sql}"
    results = executor.execute_text(_DERIVED_MEMBERSHIP_MODEL + query)[-1].fetchall()
    assert [
        tuple(float(v) if isinstance(v, Decimal) else v for v in r) for r in results
    ] == expected


def test_window_over_rollup_preserves_grouping_rows():
    """A window with `partition by` over a `sum() by rollup` must run on the
    materialised rollup output, not be re-planned via a join-back on
    (partition_key, aggregate_value). That join is non-unique and NULL-bearing,
    so ROLLUP subtotal/grand-total rows get dropped or duplicated."""
    engine = Dialects.DUCK_DB.default_executor()
    text = """
key id int;
property id.g1 string;
property id.g2 string;
property id.v int;
datasource t (id, g1, g2, v)
  grain (id)
  query '''select 1 as id,'a' as g1,'x' as g2,10 as v
           union all select 2,'a','y',20
           union all select 3,'b','x',5''';

auto total <- sum(v);
auto rnk   <- rank(g1) over (partition by g1 order by total desc);

select g1 as r_g1, g2 as r_g2, total, rnk
by rollup (g1, g2)
order by total desc nulls first;
"""
    results = engine.execute_text(text)[-1].fetchall()

    def key(r):
        return (r[0] or "", r[1] or "", r[2], r[3])

    actual = sorted(((r.r_g1, r.r_g2, r.total, r.rnk) for r in results), key=key)
    assert actual == sorted(
        [
            (None, None, 35, 1),
            ("a", None, 30, 1),
            ("a", "y", 20, 2),
            ("a", "x", 10, 3),
            ("b", "x", 5, 1),
            ("b", None, 5, 1),
        ],
        key=key,
    )


def test_window_partition_by_grouping_level_over_rollup():
    """The q70 family: `grouping()` columns and the `sum()` rollup must share a
    single ROLLUP CTE so the window can partition by a derived hierarchy level
    without cross-joining two misaligned rollup CTEs on the NULL-keyed rows."""
    engine = Dialects.DUCK_DB.default_executor()
    text = """
key id int;
property id.g1 string;
property id.g2 string;
property id.v int;
datasource t (id, g1, g2, v)
  grain (id)
  query '''select 1 as id,'a' as g1,'x' as g2,10 as v
           union all select 2,'a','y',20
           union all select 3,'b','x',5''';

auto total <- sum(v);
auto gg1 <- grouping(g1);
auto gg2 <- grouping(g2);
auto level <- gg1 + gg2;
auto rnk <- rank(g1) over (partition by level order by total desc);

select g1 as r_g1, g2 as r_g2, total, level, rnk
by rollup (g1, g2)
order by level desc nulls first, total desc nulls first;
"""
    results = engine.execute_text(text)[-1].fetchall()
    assert [(r.r_g1, r.r_g2, r.total, r.level, r.rnk) for r in results] == [
        (None, None, 35, 2, 1),
        ("a", None, 30, 1, 1),
        ("b", None, 5, 1, 2),
        ("a", "y", 20, 0, 1),
        ("a", "x", 10, 0, 2),
        ("b", "x", 5, 0, 3),
    ]


def test_concept_derivation():
    duckdb_engine = Dialects.DUCK_DB.default_executor()
    test_datetime = datetime(hour=12, day=1, month=2, year=2022, second=34)

    duckdb_engine.execute_text(
        f"""const test <- cast('{test_datetime.isoformat()}' as datetime);
    """
    )
    for property, check in [
        ["date", test_datetime.date()],
        ["hour", test_datetime.hour],
        ["second", test_datetime.second],
        ["minute", test_datetime.minute],
        ["year", test_datetime.year],
        ["month", test_datetime.month],
    ]:
        # {test_datetime.isoformat()}
        test_query = f""" 


        select local.test.{property};
        
        """
        query = duckdb_engine.parse_text(test_query)
        name = f"local.test.{property}"
        assert duckdb_engine.environment.concepts[name].address == name
        assert query[-1].output_columns[0].address == f"local.test.{property}"
        results = duckdb_engine.execute_text(test_query)[0].fetchall()
        assert results[0][0] == check
    for truncation in [
        "month",
        "year",
    ]:
        test_query = f"""
        select date_trunc(local.test, {truncation}) -> test_{truncation}_start;
        """
        results = duckdb_engine.execute_text(test_query)[0].fetchall()
        assert results[0][0] == test_datetime.replace(
            day=1,
            month=1 if truncation == "year" else test_datetime.month,
            hour=0,
            minute=0,
            second=0,
        )


def test_boolean_derivation():
    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()

    results = executor.execute_text("""const test <- 1 is not null;
    const nulls <- null is null;
    const gt <- 5 > 3;
    const lt <- 2 < 3;
    const gte <- 3 >= 3;
    const lte <- 2 <= 3;
    const eq <- 4 = 4;


        select test,
    nulls,
    gt,
    lt,
    gte,
    lte,
    eq
     ;
    """)

    assert results[0].fetchall()[0][0] is True

    results = executor.execute_text(""" const rows <- unnest([1,2,3,4,5]);

    auto big <- rows >3;

    select rows, big
    order by rows asc;
    """)
    assert results[0].fetchall() == [
        (1, False),
        (2, False),
        (3, False),
        (4, True),
        (5, True),
    ]


def test_render_query(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.generate_sql("""select total_count;""")[0]

    assert "total" in results


def test_aggregate_at_grain(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select avg_count_per_product;""")[
        0
    ].fetchall()
    assert results[0].avg_count_per_product == expected_results["avg_count_per_product"]


def test_empty_string(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select '' as empty_string;""")[0].fetchall()
    assert results[0].empty_string == ""


def test_order_of_operations(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_query("""
    const x <- 7;
    const y <- 8;

    auto z <- x + y;
    auto a <- z/2;
                                          
    select a;
""").fetchall()
    assert results[0].a == 7.5, results[0].a


def test_constant_derivation(
    duckdb_engine: Executor,
):
    results = duckdb_engine.execute_text(
        """select 1 as test; key x int; datasource funky_monkey (x) query '''select 1 as x'''; select x+1 as test2;"""
    )
    assert results[0].fetchall()[0].test == 2


def test_constants(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""const usd_conversion <- 2;

    auto converted_total_count <-  total_count * usd_conversion;
    
    select converted_total_count ;
    """)[0].fetchall()
    # expected_results["converted_total_count"]

    scaled_metric = duckdb_engine.environment.concepts["converted_total_count"]

    assert (
        duckdb_engine.environment.concepts["usd_conversion"].granularity
        == Granularity.SINGLE_ROW
    )
    parent_arg: Concept = duckdb_engine.environment.concepts[
        [
            x.address
            for x in scaled_metric.lineage.concept_arguments
            if x.address == "local.total_count"
        ][0]
    ]
    assert (
        len(
            duckdb_engine.environment.concepts[
                parent_arg.lineage.concept_arguments[0].address
            ].grain.components
        )
        == 2
    )
    # assert Grain(components = [duckdb_engine.environment.concepts['usd_conversion']]) == Grain()
    assert results[0].converted_total_count == expected_results["converted_total_count"]


def test_constant_typing(duckdb_engine: Executor, expected_results):
    duckdb_engine.execute_text("""import std.net;

const image_url <- 'www.example.com'::string::url_image;

select
image_url, 
'www.example.com'::string::url_image as image_url2;

    """)
    for concept_name in ["image_url", "image_url2"]:
        concept = duckdb_engine.environment.concepts[concept_name]
        assert "url_image" in concept.datatype.traits, concept.lineage
        assert concept.purpose == Purpose.CONSTANT, concept.lineage


def test_unnest(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""const array <- [1,2,3];
    """)
    array = duckdb_engine.environment.concepts["array"]
    assert array.lineage
    assert array.lineage.arguments[0] == [1, 2, 3]

    results = duckdb_engine.execute_text("""const array <- [1,2,3];
const unnest_array <- unnest(array);
    
    select unnest_array
    order by unnest_array asc;
    """)[0].fetchall()
    assert [x.unnest_array for x in results] == [1, 2, 3]


def test_partial(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """select item, sum(discount_value)-> total_discount
        order by item desc;
    """
    )[0].fetchall()
    assert len(results) == 2


def test_show(duckdb_engine: Executor, expected_results):
    test = """show 
select
    item, 
    sum(discount_value)-> total_discount
order by item desc;
    """

    _, parsed_0 = parse_text(test, duckdb_engine.environment)

    assert len(parsed_0) == 1
    assert isinstance(parsed_0[0], ShowStatement)

    parsed = duckdb_engine.parse_text(test)

    assert len(parsed) == 1
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert '"fact_items"."item" as "item"' in results[0]["__preql_internal_query_text"]


def test_show_persist(duckdb_engine: Executor):
    test = """
    
    auto random_data <- unnest([1,2,3,4, 88, 99]);

    datasource fact_random(
      random_data)
    grain (random_data)
    address fct_random
    state unpublished;
    
    show overwrite fact_random;"""

    duckdb_engine.parse_text(test)

    results = duckdb_engine.execute_text(test)[-1].fetchall()
    assert len(results) == 1


def test_show_concepts(duckdb_engine: Executor):
    test = """show concepts;"""
    parsed = duckdb_engine.parse_text(test)

    assert len(parsed) == 1
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == len(
        [k for k, v in duckdb_engine.environment.concepts.items() if not v.is_internal]
    )


def test_show_datasources(duckdb_engine: Executor):
    test = """show datasources;"""

    with raises(NotImplementedError):
        duckdb_engine.parse_text(test)
        duckdb_engine.execute_text(test)


def test_rollback(duckdb_engine: Executor, expected_results):
    try:
        _ = duckdb_engine.execute_raw_sql("select abc")
    except Exception:
        pass

    results = duckdb_engine.execute_raw_sql("select 1")
    assert results.fetchall()[0] == (1,)


def test_basic_dates(duckdb_engine: Executor):
    test = """
  auto today <- current_datetime();
  auto tomorrow <- date_add(today, day, 1);
  auto yesterday <- date_sub(today, day, 1);
  select 
    tomorrow,
    yesterday,
    date_diff(today, today, day)->zero,
    date_trunc(today, year) -> current_year,
    day_of_week(today) -> today_dow
  ;
    """
    duckdb_engine.parse_text(test)
    assert (
        duckdb_engine.environment.concepts["tomorrow"].granularity
        == Granularity.SINGLE_ROW
    )
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results[0]) == 5


def test_rowset(duckdb_engine: Executor):
    test = """const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

rowset my_rowset <- select x, z where z = 1;

select my_rowset.x, my_rowset.z;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain.components == {
        x.address,
    }
    assert str(z) == "local.z@Grain<local.x>"
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1


def test_rowset_agg(duckdb_engine: Executor):
    test = """const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

rowset my_rowset <- select x, max(z)->max_rank;

select my_rowset.x, my_rowset.max_rank;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain == Grain(components=[x])
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 4


def test_rowset_join(duckdb_engine: Executor):
    test = """
key x int;

datasource x_data (
x:x)
query '''
select unnest([1,2,2,3]) as x
'''
;
    
const y <- 5;
auto z <- rank x order by x desc;
auto w <- rank x order by x asc;

rowset my_rowset <- select x, max(z)->max_rank;

select x, w, my_rowset.max_rank;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain == Grain(components=[x])
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3
    for row in results:
        if row.x == 2:
            assert row.my_rowset_max_rank == 2


def test_default_engine(default_duckdb_engine: Executor):
    test = """
  auto today <- current_datetime();
  
  select 
    date_add(today, day, 1)->tomorrow,
    date_diff(today, today, day)->zero,
    date_trunc(today, year) -> current_year 
  ;
    """
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results[0]) == 3


def test_complex(default_duckdb_engine: Executor):
    test = """
const list <- [1,2,2,3];
const orid <- unnest(list);

auto half_orid <- ((orid+17)/2);

select 
    orid,
    half_orid,
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    listc = default_duckdb_engine.environment.concepts["list"]
    assert listc.purpose == Purpose.CONSTANT
    orid = default_duckdb_engine.environment.concepts["orid"]
    half = default_duckdb_engine.environment.concepts["half_orid"]
    assert orid.address in [x.address for x in half.concept_arguments]
    assert set([x for x in half.keys]) == {
        "local.orid",
    }
    assert half.lineage.operator == FunctionType.DIVIDE
    assert half.derivation == Derivation.BASIC
    assert half.granularity == Granularity.MULTI_ROW
    assert len(results) == 4


def test_agg_demo(default_duckdb_engine: Executor):
    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource agg_example(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
''';

auto avg_customer_orders <- avg(customer_orders);

select 
    avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    build_env = default_duckdb_engine.environment.materialize_for_select()
    customer_orders = build_env.concepts["customer_orders"]
    assert set([x for x in customer_orders.keys]) == {"local.customer"}
    assert set([x for x in customer_orders.grain.components]) == {"local.customer"}

    customer_orders_2 = customer_orders
    assert set([x for x in customer_orders_2.keys]) == {"local.customer"}
    assert set([x for x in customer_orders_2.grain.components]) == {"local.customer"}

    count_by_customer = build_env.concepts[
        "avg_customer_orders"
    ].lineage.concept_arguments[0]
    # assert isinstance(count_by_customer, AggregateWrapper)
    assert set([x for x in count_by_customer.keys]) == {"local.customer"}
    assert set([x for x in count_by_customer.grain.components]) == {"local.customer"}
    assert len(results) == 1
    assert results[0].avg_customer_orders == 2
    assert round(results[0].avg_store_orders, 2) == 1.33


def test_constant_group(default_duckdb_engine: Executor):
    test = """
const x <- 1;
const x2 <- x+1;

auto constant_group_orid <- unnest([1,2,3]);
property constant_group_orid.mod_two <- constant_group_orid % 2;

select 
    mod_two,
    x2
order by
    mod_two asc
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (0, 2)


def test_mod_parse_order():
    test = """
const x <- unnest([1,2,3,4]);

with even_squares as select 
    x, 
    x*x as x_squared
having x_squared %2  = 0;

select 
    even_squares.x_squared
order by
    even_squares.x_squared asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)
    assert len(results) == 2


def test_raw_sql():
    test = """
raw_sql('''
select unnest([1,2,3,4]) as x
order by x asc
''')
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (1,)
    assert len(results) == 4


def test_numeric():
    test = """
const number <- 1.456789;
const reduced <- cast(number as numeric(3,2));

select reduced;
"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (Decimal("1.46"),)
    assert len(results) == 1


def test_cast_timestamptz_to_date():
    test = """
key id int;
property id.data_updated_through timestamp;
property id.data_updated_through_no_tz datetime;

datasource test_data (
    id: id,
    data_updated_through: data_updated_through,
    data_updated_through_no_tz: data_updated_through_no_tz
)
grain (id)
query '''
select 1 as id, 
'2024-01-15 10:30:00-05:00'::timestamptz as data_updated_through,
'2024-01-15 10:30:00'::timestamp as data_updated_through_no_tz
union all
select 2 as id, 
'2024-01-15 22:30:00-05:00'::timestamptz as data_updated_through,
'2024-01-15 22:30:00'::timestamp as data_updated_through_no_tz
''';

auto update_date <- cast(data_updated_through as date);
auto update_datetime <- cast(data_updated_through as datetime);
auto update_date_no_tz <- cast(data_updated_through_no_tz as date);
select id, update_date, update_date_no_tz, update_datetime order by id asc;
"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 2
    # Row 1: 10:30:00-05:00 = 15:30:00 UTC, same day (2024-01-15)
    assert results[0].update_date == date(2024, 1, 15)
    assert results[0].update_date_no_tz == date(2024, 1, 15)
    assert results[0].update_datetime == datetime(2024, 1, 15, 15, 30)

    # Row 2: 22:30:00-05:00 = 03:30:00+1 UTC, next day (2024-01-16)
    # This tests that we properly convert to UTC before extracting date
    assert results[1].update_date == date(2024, 1, 16)
    assert results[1].update_date_no_tz == date(2024, 1, 15)


def test_duckdb_string_quotes():
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(r"""
        const csv <- '''this string has quotes ' like this''';

    select csv;
        """)

    results = results.fetchall()

    assert results[0].csv == """this string has quotes ' like this"""


def test_demo_recursive_error():
    query = """key idx int;
property idx.idx_val int;
datasource numbers(
    idx: idx,
    x: idx_val
)
grain (idx)
query '''
select 1 idx, 1 x
union all
select 2, 2
union all
select 3, 2
union all
select 4, 3
''';

SELECT
  idx_val,
  count(idx) as number_count
order by
    idx_val asc;"""

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(query)

    results = results.fetchall()


def test_union():
    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(r"""
key space_one int;
key space_two int;


property space_one.one_name string;
property space_two.two_name string;

auto space_all <- union(space_one, space_two);
auto name <- union(one_name, two_name);

datasource sone (
x: space_one,
y: one_name )
grain (space_one)
query '''
select 2 as x , 'test' as y
union all
select 1, 'fun'
''';


datasource stwo (
x: space_two,
y: two_name )
grain (space_two)
query '''
select 4 as x, 'bravo' as y
union all
select 5, 'alpha'
''';



select
    space_all,
    name,
order by
    space_all asc
limit 100;
        """)

    results = list(results.fetchall())

    assert results[0].space_all == 1
    assert results[0].name == "fun"
    assert results[-1].space_all == 5


def test_multi_select_mutation():
    exec = Dialects.DUCK_DB.default_executor()

    queries = exec.parse_text("""

auto x <- 1;
                    
select
    x + 1 -> x_next;
                    
select
    x + 2 -> x_next;
                    
""")

    for idx, x in enumerate(queries):
        results = exec.execute_query(x).fetchall()
        assert results[0].x_next == 2 + idx


def test_commit():
    exec = Dialects.DUCK_DB.default_executor()
    exec.connection.begin()
    exec.execute_raw_sql("create table test (test int);")
    exec.connection.commit()
    exec.connection.begin()
    exec.execute_raw_sql("insert into test values (1);")
    exec.execute_raw_sql("insert into test values (2);")
    results = exec.execute_raw_sql("select * from test;").fetchall()
    assert len(results) == 2
    exec.connection.rollback()
    results = exec.execute_raw_sql("select * from test;").fetchall()
    assert len(results) == 0


def test_duckdb_date_add():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    r = executor.execute_query(r"""
auto today <- date_add(current_datetime(), day, -3);
select today;
""")

    results = r.fetchall()

    assert results[0].today.date() == datetime.now().date() - timedelta(days=3)


def test_duckdb_alias():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    r = executor.execute_query(r"""
auto today <- date_add(current_datetime(), day, -3);
select today as tomorrow, today;
""")

    results = r.fetchall()

    assert results[0].today.date() == datetime.now().date() - timedelta(days=3)
    assert results[0].tomorrow.date() == datetime.now().date() - timedelta(days=3)


def test_function_parsing():
    query = """
    import function_test as game;

where @game.is_team_game('pelican') = true
select game.home_team.name, count(game.id)->game_count;"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.parse_text(query)


def test_global_aggregate_inclusion():
    """check that including a global aggregate constant in output select doesn't force changed evaluation order"""
    DebuggingHook()
    query = """
    key id int;
key date date;
property <id, date>.score int;

datasource raw (
    id: id,
    date: date,
    score: score
)
grain (id,date)
query '''
select 1 as id, '2023-01-01' as date, 10 as score
union all
select 2, '2023-01-02', 20
union all
select 3, '2023-01-03', 30
union all
select 4, '2023-01-03', 40
''';

auto max_date <- max(date) by *;

"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text("""where date = max_date and id >2
select date, avg(score) as avg_id;""")[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_id == 35.0

    results = executor.execute_text("""where date = max_date and id >2
select max_date, date, avg(score) as avg_id;""")[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_id == 35.0


def test_recursive():
    DebuggingHook()

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.environment.parse("""import recursive;
# traverse parent-> id until you hit a null
auto first_parent <- recurse_edge(id, parent);""")

    assert (
        executor.environment.concepts["first_parent"].derivation == Derivation.RECURSIVE
    )
    executor.generate_sql("""where
first_parent = 1    
select id, label
order by label asc;
""")[-1]
    results = executor.execute_text("""where
first_parent = 1
select id, label;
""")[0].fetchall()
    assert len(results) == 4
    assert results[0].label == "A"


def test_recursive_enrichment():
    DebuggingHook()

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.environment.parse("""
import recursive;
import recursive as parent;
# traverse parent-> id until you hit a null
auto first_parent <- recurse_edge(id, parent);  

merge first_parent into parent.id;                 
                               
                               """)

    assert (
        "local.first_parent",
        "parent.id",
        JoinType.FULL,
    ) in executor.environment.merges
    build_env = executor.environment.materialize_for_select()
    recursive = build_env.alias_origin_lookup["local.first_parent"]
    assert recursive.derivation == Derivation.RECURSIVE, "recursive should be recursive"

    results = executor.execute_text("""where
first_parent = 1
select id, parent.label;
""")[0].fetchall()
    assert len(results) == 4
    assert results[-1].parent_label == "A"

    results = executor.execute_text("""where
parent.label = 'A'
select count(id) as a_children;
""")[0].fetchall()
    assert len(results) == 1
    assert results[0].a_children == 4


def test_tuple_constant(default_duckdb_engine: Executor):
    DebuggingHook()
    test = """
const list <- (1,2,3,4);

select list;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].list == [1, 2, 3, 4]


def test_in_with_array(default_duckdb_engine: Executor):

    DebuggingHook()
    test = """

const list <- [1,2,3,4];

const two <- 2;

where two in list
select 
    two;
    
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].two == 2


def test_map(default_duckdb_engine: Executor):

    DebuggingHook()
    test = """

const map <- {1:2, 3:4};

select 
    map;
    
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].map == {1: 2, 3: 4}


def test_regexp(default_duckdb_engine: Executor):

    DebuggingHook()
    test = """

const values <- unnest(['apple', 'banana', 'cherry', 'date']);

select 

    regexp_contains(values, '^a.*') as starts_with_a,
    regexp_extract(values, '^a(.*)') as after_a,
    regexp_extract(values, '^a(.*)', 1) as after_a_explicit,
    regexp_extract(values, '^a.*') as no_capture
where starts_with_a = true
order by 
    starts_with_a asc;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].starts_with_a is True
    assert results[0].after_a == "pple"
    assert results[0].after_a_explicit == "pple"
    assert results[0].no_capture == "apple"


def test_window_calc():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const list <- [1,2,3,4,5];
const orid <- unnest(list);

select 
     orid,
     case when rank sum(orid) order by orid asc  = rank sum(orid) order by orid asc then rank sum(orid) order by orid asc else rank sum(orid) order by orid asc end as window_rank
having
    4 < sum(orid) 
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1


def test_replace():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest(['apple', 'banana', 'cherry', 'date']);
select
    replace(values, 'a', 'o') as replaced_values
order by    
    replaced_values asc;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 4
    assert results[0].replaced_values == "bonono"
    assert results[1].replaced_values == "cherry"
    assert results[2].replaced_values == "dote"
    assert results[3].replaced_values == "opple"


def test_sum_bool():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest([true, false, true, false]);

select sum(values) as true_count
where values = true;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].true_count == 2


def test_log():
    DebuggingHook()
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest([1, 10, 100, 1000]);

select 
    log(values) as log_values,
    log(values,2) as log_base_2,
    values
order by values asc;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 4
    assert results[0].log_values == 0
    assert results[1].log_values == 1
    assert results[2].log_values == 2
    assert results[3].log_values == 3


def test_trim():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest([ ' abc ', ' def', 'jkl ', 'mon']);

select trim(values) as trimmed_values order by trimmed_values asc;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 4
    assert results[0].trimmed_values == "abc"
    assert results[1].trimmed_values == "def"
    assert results[2].trimmed_values == "jkl"
    assert results[3].trimmed_values == "mon"


def test_array_to_string():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- [ ' abc ', ' def', 'jkl ', 'mon'];

select array_to_string(values, ', ') as value_string;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].value_string == " abc ,  def, jkl , mon"


def test_not_value():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const value <- unnest([ true, null, false]);

select value where not value;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert default_duckdb_engine.environment.concepts["value"].datatype == DataType.BOOL
    assert len(results) == 2, str(
        default_duckdb_engine.environment.concepts["value"].lineage
    )


def test_mock_statement():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    test = """
import std.metric;
import std.color;
import std.net;

key x int;
key y int;

property x.name string;
property x.email string::email_address;
property x.favorite_color string::hex;
property x.value float::kn;
property x.numeric numeric::kn;
property x.created_at timestamp;
property x.created_at_date date;
property x.labels array<string>;

key year date;
key string_key string;
key bool_key bool;
key date_key date;
key datetime_key datetime;


datasource example (
x, 
y,
name,

value,
numeric,
created_at,
created_at_date,

)
grain (x,y)
address `my-gbq-table.my-project.tbl_example`;


datasource enrichment (
x,
labels,
email,
favorite_color,
)
grain (x)
address `my-gbq-table.my-project.tbl_enrichment`;

datasource years (
    year
)
grain (year)
address `my-gbq-table.my-project.tbl_years`;


datasource keys (
    datetime_key,
    date_key,
    string_key,
    bool_key,
    date_key)
grain (datetime_key, string_key, bool_key, date_key)
address `my-gbq-table.my-project.tbl_keys`;
    

mock datasource example, enrichment, years, keys;

select x, labels, email, favorite_color;
"""

    results = default_duckdb_engine.execute_text(test)[-1].fetchall()
    assert len(results) == DEFAULT_SCALE_FACTOR
    assert isinstance(results[0].x, int)
    assert isinstance(results[0].labels, list)
    assert "@" in results[0].email


def test_group_syntax():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    test = """
key x int;
key y int;

datasource example (
x,
y
)
grain (x)
query '''
select 1 as x, 1 as y union all select 2 as x, 2 as y union all select 3 as x, 3 as y''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """select
    round(avg(x) by y/avg(x) by y, 2) as rounded;
    """

    results = default_duckdb_engine.parse_text(test)

    default_duckdb_engine.generate_sql(results[0])


def test_connection_management():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""select 1 as test;""")
    executor.close()
    executor.execute_text("""select 1 as test;""")


def test_proper_basic_unnest_handling():
    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    test = """const prime <- unnest([2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);

def cube_plus_one(x) -> (x * x * x + 1);

WHERE 
    prime_cubed_plus_one % 7 = 0
SELECT
    prime,
    @cube_plus_one(prime) as prime_cubed_plus_one
ORDER BY
    prime asc
LIMIT 10;"""
    executor.parse_text(test)
    c = executor.environment.concepts["prime_cubed_plus_one"]
    lineage = (
        c.lineage.content if isinstance(c.lineage, FunctionCallWrapper) else c.lineage
    )
    if lineage.operator == FunctionType.CONSTANT:  # type: ignore
        raise ValueError(
            "prime_cubed_plus_one should not be constant {}".format(c.lineage)
        )
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 5


@mark.skip("Date spine not yet supported")
def test_date_spine():
    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    with executor.generator.rendering.temporary(parameters=False):
        test = """key prime date;
        property prime.val int;
        datasource primes(  
            prime,
            val
        )
        grain(prime)
        query '''
    SELECT current_date AS prime, 2 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 1 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 2 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 3 DAY AS prime, 4 as  val
    UNION ALL
    SELECT current_date - INTERVAL 10 DAY AS prime, 5 as  val
        '''
        ;


        auto first_date <- min(prime) by *;
        auto last_date <- max(prime) by *;

        SELECT
            date_spine(prime, current_date()) as filled_in_dates,
            sum val order by filled_in_dates asc -> day_count
        ;
        """
        sql = executor.generate_sql(executor.parse_text(test)[-1])
        results = executor.execute_text(test)[-1].fetchall()

        assert len(results) == 8, sql


def test_date_spine_merge():
    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    with executor.generator.rendering.temporary(parameters=False):
        test = """key prime date;
        property prime.val int;
        datasource primes(  
            prime,
            val
        )
        query '''
    SELECT current_date AS prime, 2 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 1 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 2 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 3 DAY AS prime, 4 as  val
    UNION ALL
    SELECT current_date - INTERVAL 10 DAY AS prime, 5 as  val
        '''
        ;


        auto base_spine <- date_spine(date_add(current_date(), day, -10), current_date());

        merge prime into ~base_spine;

        SELECT
            base_spine,
            sum sum(val) by base_spine order by base_spine asc -> day_count
        ;
        """
        sql = executor.generate_sql(executor.parse_text(test)[-1])
        results = executor.execute_text(test)[-1].fetchall()

        assert results[-1].day_count == 18, sql


def test_const_equivalence_merge():
    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    with executor.generator.rendering.temporary(parameters=False):
        test = """key orid int;
        auto orid_2 <- unnest([1,2,3,4,5]);

        property orid.val int;

        datasource orders (
            ~orid,
            val
        )
        grain (orid)
        query '''
        select 1 as orid, 10 as val
        union all
        select 2, 20
        ''';

        merge orid into ~orid_2;

        select orid_2, val;
        """

        sql = executor.generate_sql(executor.parse_text(test)[-1])
        results = executor.execute_text(test)[-1].fetchall()
        assert len(results) == 5, sql


def test_multi_select_align_aggregate():
    # Aligning a per-arm aggregate must not push it into each arm's GROUP BY
    # (DuckDB rejects aggregates in GROUP BY).
    exec = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    exec.execute_raw_sql("CREATE TABLE s(sid int, cat varchar)")
    exec.execute_raw_sql("CREATE TABLE w(wid int, cat2 varchar)")
    exec.execute_raw_sql(
        "INSERT INTO s VALUES (1,'a'),(2,'a'),(3,'b'); INSERT INTO w VALUES (4,'a'),(5,'b'),(6,'b')"
    )
    queries = exec.parse_text("""
key sid int; property sid.cat string;
datasource s (sid:sid, cat:cat) grain (sid) address s;
key wid int; property wid.cat2 string;
datasource w (wid:wid, cat2:cat2) grain (wid) address w;

SELECT cat as g1, count(sid) as cnt1,
MERGE
SELECT cat2 as g2, count(wid) as cnt2,
ALIGN grp: g1, g2 and lc: cnt1, cnt2
ORDER BY grp;
""")
    sql = exec.generate_sql(queries[-1])[-1]
    assert "count" in sql.lower()
    results = exec.execute_query(queries[-1]).fetchall()
    assert results


def test_multi_select_having_hidden_derive_arg_no_outer_group():
    # A multiselect outer is a pure FULL JOIN of pre-aggregated arms; it must
    # never re-group. When each arm hides a derive-arg column whose source key
    # is absent from the align keys (here `--dt.yr as yr_a`, keyed by `date_id`,
    # consumed only by `coalesce(yr_a, 0)`), that column inflates the joined
    # pregrain past the align-key grain. The outer used to emit a spurious
    # GROUP BY that omitted the raw aggregate projections (`coalesce(cnt_a, 0)`),
    # which DuckDB rejects: column "cnt_a" must appear in the GROUP BY clause.
    exec = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    exec.execute_raw_sql("CREATE TABLE item(item_id int, name varchar)")
    exec.execute_raw_sql("CREATE TABLE dt(date_id int, yr int)")
    exec.execute_raw_sql(
        "CREATE TABLE sale(sid int, item_id int, date_id int, chan varchar)"
    )
    exec.execute_raw_sql(
        "INSERT INTO item VALUES (1,'x'),(2,'y');"
        "INSERT INTO dt VALUES (101,1999),(102,2000);"
        "INSERT INTO sale VALUES "
        "(1,1,101,'a'),(2,1,102,'a'),(3,2,101,'a'),"
        "(4,1,101,'b'),(5,2,102,'b'),(6,2,101,'b')"
    )
    queries = exec.parse_text("""
key item_id int; property item_id.name string;
datasource item (item_id:item_id, name:name) grain (item_id) address item;
key date_id int; property date_id.yr int;
datasource dt (date_id:date_id, yr:yr) grain (date_id) address dt;
key sid int; property sid.item_id int; property sid.date_id int; property sid.chan string;
datasource sale (sid:sid, item_id:item_id, date_id:date_id, chan:chan)
    grain (sid) address sale;

WHERE chan = 'a'
SELECT --item_id.name as g1, --date_id.yr as yr_a, --count(sid) as cnt_a,
MERGE
WHERE chan = 'b'
SELECT --item_id.name as g2, --date_id.yr as yr_b, --count(sid) as cnt_b,
ALIGN grp: g1, g2
DERIVE coalesce(cnt_a,0) as c1, coalesce(cnt_b,0) as c2,
    coalesce(yr_a,0) as ya, coalesce(yr_b,0) as yb
HAVING c2 <= c1
ORDER BY grp asc;
""")
    sql = exec.generate_sql(queries[-1])[-1]
    # the outer SELECT (over the FULL JOIN of the arm CTEs) must not GROUP BY
    outer = sql[sql.rindex("FULL JOIN") :]
    assert "GROUP BY" not in outer
    results = exec.execute_query(queries[-1]).fetchall()
    assert results


def test_multi_select_derive():
    exec = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    queries = exec.parse_text("""

auto x <- 1;
                    
select
    1-> x_val,
    x + 1 -> x_next
merge               
select
    2-> y_val,
    x + 2 -> y_next
align val:x_val, y_val
derive x_next + y_next -> total
;
                    
""")

    for idx, x in enumerate(queries):
        print(x.output_columns)
        results = exec.execute_query(x).fetchall()
        assert results[0].x_next == 2 + idx
        assert results[0].total == 5


def test_multi_select_derive_import():
    exec = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    queries = exec.parse_text("""

auto x <- 1;

with rows as             
select
    1-> x_val,
    x + 1 -> x_next
merge               
select
    2-> y_val,
    x + 2 -> y_next
align val:x_val, y_val
derive x_next + y_next -> total
;

""")
    exec2 = Dialects.DUCK_DB.default_executor()
    exec2.environment.add_import("dependent", exec.environment, None)

    assert exec2.environment.concepts["dependent.rows.x_next"]
    queries = exec2.parse_text("""
        select
        dependent.rows.x_next, dependent.rows.total
        ;
        """)

    for idx, x in enumerate(queries):
        print(x.output_columns)
        results = exec2.execute_query(x).fetchall()
        assert results[0].dependent_rows_x_next == 2 + idx
        assert results[0].dependent_rows_total == 5


def test_order_by_count():
    exec = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    exec.parse_text("""
key state string;
property state.count int;
datasource origin (
state: state,
count: count
)
grain (state)
query '''
select 'CA' as state, 10 as count
union all
select 'NY', 20
union all
select 'TX', 30 
''';

select
   state,
   count, 
   order by count desc;

""")


def test_existence():
    exec = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    results = exec.execute_text("""
key state string;
property state.count int;
datasource origin (
state: state,
count: count
)
grain (state)
query '''
select 'CA' as state, 10 as count
union all
select 'NY', 20
union all
select 'TX', 30 
''';

where state in (state? count>20)
select
   state,
;

where state in state? count>20
select
   state,
;
""")
    assert results[-1].fetchall()[0].state == "TX"


def test_string_functions():
    environment = Environment()
    _, queries = environment.parse("""
    const greeting <- '  Hello, World!  ';
    select
        greeting,
        lower(greeting) -> greeting_lower,
        upper(greeting) -> greeting_upper,
            len(greeting) -> greeting_length,
            trim(greeting) -> greeting_trimmed,
            ltrim(greeting) -> greeting_ltrimmed,
            rtrim(greeting) -> greeting_rtrimmed,
            substring(greeting, 3, 5) -> greeting_substring,
            replace(greeting, 'World', 'Trilogy') -> greeting_replaced,
            concat(greeting, ' Welcome to Trilogy.') -> greeting_concatenated,
            greeting like '%world%' -> contains_world,
            greeting ilike '%WORLD%' -> contains_world_case_insensitive
    ;


        """)

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    results = executor.execute_query(queries[-1]).fetchall()
    row = results[0]

    assert row.greeting == "  Hello, World!  "
    assert row.greeting_lower == "  hello, world!  "
    assert row.greeting_upper == "  HELLO, WORLD!  "
    assert row.greeting_length == 17
    assert row.greeting_trimmed == "Hello, World!"
    assert row.greeting_ltrimmed == "Hello, World!  "
    assert row.greeting_rtrimmed == "  Hello, World!"
    assert row.greeting_substring == "Hello"
    assert row.greeting_replaced == "  Hello, Trilogy!  "
    assert row.greeting_concatenated == "  Hello, World!   Welcome to Trilogy."
    assert row.contains_world is False
    assert row.contains_world_case_insensitive is True


def test_datetime_functions():
    environment = Environment()
    _, queries = environment.parse("""
    const order_id <- 1;
    const order_timestamp <- current_datetime();
    select
        order_id,
        order_timestamp,
        date(order_timestamp) -> order_date,
        datetime(order_timestamp) -> order_timestamp_datetime,
        timestamp(order_timestamp) -> order_timestamp_dos,
        second(order_timestamp) -> order_second,
        minute(order_timestamp) -> order_minute,
        hour(order_timestamp) -> order_hour,
        day(order_timestamp) -> order_day,
        week(order_timestamp) -> order_week,
        month(order_timestamp) -> order_month,
        quarter(order_timestamp) -> order_quarter,
        year(order_timestamp) -> order_year,
        date_trunc(order_timestamp, month) -> order_month_trunc,
        date_add(order_timestamp, month, 1) -> one_month_post_order,
        date_sub(order_timestamp, month, 1) -> one_month_pre_order,
        date_trunc(order_timestamp, day) -> order_day_trunc,
        date_trunc(order_timestamp, year) -> order_year_trunc,
        date_trunc(order_timestamp, hour) -> order_hour_trunc,
        date_trunc(order_timestamp, minute) -> order_minute_trunc,
        date_trunc(order_timestamp, second) -> order_second_trunc,
        date_trunc(order_timestamp, quarter) -> order_quarter_trunc,
        date_trunc(order_timestamp, week) -> order_week_trunc,
        date_part(order_timestamp, month) -> order_month_part,
        date_part(order_timestamp, day) -> order_day_part,
        date_part(order_timestamp, year) -> order_year_part,
        date_part(order_timestamp, hour) -> order_hour_part,
        date_part(order_timestamp, minute) -> order_minute_part,
        date_part(order_timestamp, second) -> order_second_part,
        date_part(order_timestamp, quarter) -> order_quarter_part,
        date_part(order_timestamp, week) -> order_week_part,
        date_part(order_timestamp, day_of_week) -> order_day_of_week_part,
        month_name(order_timestamp) -> order_month_name,
        day_name(order_timestamp) -> order_day_name,
        format_time(order_timestamp, '%Y-%m-%d %H:%M:%S') -> order_timestamp_strftime,
        parse_time(format_time(order_timestamp, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S') -> order_timestamp_parse,
        date_sub(order_timestamp, day, 30) -> thirty_days_ago,
        date_diff(thirty_days_ago, order_timestamp, day) -> date_diff_days
    ;


        """)

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    results = executor.execute_query(queries[-1]).fetchall()
    row = results[0]

    # Basic identity checks
    assert row.order_id == 1

    # date_part extractions should match direct extractions
    assert row.order_second == row.order_second_part
    assert row.order_minute == row.order_minute_part
    assert row.order_hour == row.order_hour_part
    assert row.order_day == row.order_day_part
    assert row.order_week == row.order_week_part
    assert row.order_month == row.order_month_part
    assert row.order_quarter == row.order_quarter_part
    assert row.order_year == row.order_year_part

    # date_trunc in DuckDB returns date type, so check date components only
    assert row.order_second_trunc.microsecond == 0
    assert (
        row.order_minute_trunc.second == 0 and row.order_minute_trunc.microsecond == 0
    )
    assert row.order_hour_trunc.minute == 0 and row.order_hour_trunc.second == 0
    assert row.order_day_trunc.hour == 0 and row.order_day_trunc.minute == 0
    assert row.order_month_trunc.day == 1 and row.order_month_trunc.hour == 0
    assert row.order_year_trunc.month == 1 and row.order_year_trunc.day == 1
    assert (
        row.order_quarter_trunc.month in (1, 4, 7, 10)
        and row.order_quarter_trunc.day == 1
    )

    # date_add/date_sub relationships
    assert row.one_month_post_order > row.order_timestamp
    assert row.one_month_pre_order < row.order_timestamp
    assert row.thirty_days_ago < row.order_timestamp

    # date_diff should be 30 days
    assert row.date_diff_days == 30

    # time component ranges
    assert 0 <= row.order_second <= 59
    assert 0 <= row.order_minute <= 59
    assert 0 <= row.order_hour <= 23
    assert 1 <= row.order_day_of_week_part <= 7

    # month_name and day_name should be strings
    assert isinstance(row.order_month_name, str) and len(row.order_month_name) > 0
    assert isinstance(row.order_day_name, str) and len(row.order_day_name) > 0

    # format_time and parse_time round-trip
    assert row.order_timestamp_parse is not None


def test_hex_function():
    environment = Environment()
    _, queries = environment.parse("""
    const word <- 'abc';
    select
        hex(word) -> word_hex
    ;
        """)

    executor = Dialects.DUCK_DB.default_executor(environment=environment)
    results = executor.execute_query(queries[-1]).fetchall()
    assert results[0].word_hex == "616263"


def test_dense_rank_window():
    environment = Environment()
    _, queries = environment.parse("""
key item_id int;
property item_id.score int;
property item_id.item_dense_rank <- dense_rank item_id by score desc;

datasource items (
    item_id: item_id,
    score: score
)
grain (item_id)
query '''
    select 1 as item_id, 10 as score
    union all select 2, 10
    union all select 3, 5
''';

select item_id, score, item_dense_rank
order by item_id asc;
        """)

    executor = Dialects.DUCK_DB.default_executor(environment=environment)
    results = executor.execute_query(queries[-1]).fetchall()
    # items 1 and 2 tie at score 10 -> dense_rank 1; item 3 at score 5 -> dense_rank 2
    rows = {r.item_id: r.item_dense_rank for r in results}
    assert rows[1] == 1
    assert rows[2] == 1
    assert rows[3] == 2


def test_anon_function_canonical_address_matches_named():
    """`year(x_date)` (named call) and `x_date.year` (anonymous attr access)
    should resolve to the same canonical_address since they share lineage."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text("""
key id int;
property id.x_date date;

auto year_via_func <- year(x_date);

datasource events (
    id: id,
    x_date: x_date
)
grain (id)
query '''select 1 as id, '2024-01-01'::date as x_date''';

select x_date.year, year_via_func;
""")
    build_env = executor.environment.materialize_for_select()
    via_func = build_env.concepts["year_via_func"]
    via_attr = build_env.concepts["x_date.year"]
    assert (
        via_func.canonical_address == via_attr.canonical_address
    ), f"{via_func.canonical_address} != {via_attr.canonical_address}"


def test_anon_function_resolves_from_precomputed_source():
    """If a precomputed datasource binds `year_via_func`, querying via the
    anonymous `x_date.year` form should resolve from that source rather than
    re-deriving from the base `x_date`, since they share canonical address."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text("""
key id int;
property id.x_date date;

auto year_via_func <- year(x_date);
auto x_count <- count(id);

datasource events (
    id: id,
    x_date: x_date
)
grain (id)
query '''select 1 as id, '2024-01-01'::date as x_date''';

datasource events_by_year (
    year_via_func: year_via_func,
    x_count: x_count
)
grain (year_via_func)
query '''select 2024 as year_via_func, 2 as x_count union all select 2025, 1''';
""")

    sql_named = executor.generate_sql("select year_via_func, x_count;")[-1]
    assert "events_by_year" in sql_named, sql_named

    sql_attr = executor.generate_sql("select x_date.year, x_count;")[-1]
    assert "events_by_year" in sql_attr, sql_attr


def test_duckdb_aggregate_grouping_modes_render():
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text("""
key a int;
key b int;
property <a, b>.x int;
datasource test_data (
    a: a,
    b: b,
    x: x
)
grain (a, b)
query '''
    select 1 as a, 1 as b, 10 as x
    union all
    select 1 as a, 2 as b, 20 as x
''';
""")

    rollup_sql = executor.generate_sql("select a, b, sum(x) as sx by rollup (a, b);")[
        -1
    ]
    cube_sql = executor.generate_sql("select a, b, sum(x) as sx by cube (a, b);")[-1]
    grouping_sets_sql = executor.generate_sql(
        "select a, b, sum(x) as sx by grouping sets ((a, b), (a), ());"
    )[-1]

    assert "GROUP BY" in rollup_sql
    assert "ROLLUP (1, 2)" in rollup_sql
    assert "CUBE (1, 2)" in cube_sql
    assert "GROUPING SETS ((1, 2), (1), ())" in grouping_sets_sql


def test_duckdb_grouping_functions_with_rollup():
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text("""
key a int;
key b int?;
property <a, b>.x int;
datasource test_data (
    a: a,
    b: b,
    x: x
)
grain (a, b)
query '''
    select 1 as a, 1 as b, 10 as x
    union all
    select 1 as a, 2 as b, 20 as x
''';
""")

    sql = executor.generate_sql("""
SELECT
    a,
    b,
    sum(x) as sx,
    grouping(a) as ga,
    grouping_id(a, b) as gid
by rollup (a, b)
ORDER BY
    gid asc,
    a asc nulls first,
    b asc nulls first;
""")[-1]
    results = list(executor.execute_raw_sql(sql).fetchall())

    assert "ROLLUP" in sql
    assert "grouping(" in sql
    assert "grouping_id(" in sql
    assert results == [
        (1, 1, 10, 0, 0),
        (1, 2, 20, 0, 0),
        (1, None, 30, 0, 1),
        (None, None, 30, 1, 3),
    ]


_GROUPING_ID_CASE_MODEL = """
key a int;
key b int?;
property <a, b>.x int;
datasource test_data (
    a: a,
    b: b,
    x: x
)
grain (a, b)
query '''
    select 1 as a, 1 as b, 10 as x
    union all
    select 1 as a, 2 as b, 20 as x
''';
"""

# grouping_id() nested in a `case` deriving the rollup level, over a `by rollup`
# aggregate referenced by name (the TPC-DS-native idiom). No explicit `by rollup`
# on the grouping_id itself.
_GROUPING_ID_CASE_QUERY = """
auto total <- sum(x);
auto lvl <- case
    when grouping_id(a, b) = 3 then 2
    when grouping_id(a, b) = 1 then 1
    else 0 end;
select a, b, total, lvl
by rollup (a, b)
order by lvl asc, a asc nulls first, b asc nulls first;
"""


def test_grouping_id_in_case_over_named_rollup_colocates():
    """Bug: a `grouping_id(...)` nested in a `case` deriving the rollup level,
    over a `by rollup` aggregate referenced by name, parsed STANDARD-mode and
    stranded in a separate groupless CTE (`GROUP BY 1, 2`) — DuckDB rejects
    `grouping_id` there ('GROUPING statement cannot be used without groups').
    It must co-locate in the single ROLLUP CTE that gives it context."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text(_GROUPING_ID_CASE_MODEL)
    sql = executor.generate_sql(_GROUPING_ID_CASE_QUERY)[-1]
    assert "ROLLUP" in sql
    assert "grouping_id(" in sql
    # The only GROUP BY is the ROLLUP; grouping_id is not stranded in its own
    # plain-GROUP-BY CTE.
    assert sql.count("GROUP BY") == 1, sql


def test_grouping_id_in_case_over_named_rollup_executes():
    """End-to-end: the same query runs and the derived level is correct — 0 for
    leaf rows, 1 for the `b` subtotal, 2 for the grand total."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text(_GROUPING_ID_CASE_MODEL)
    results = list(executor.execute_text(_GROUPING_ID_CASE_QUERY)[-1].fetchall())
    assert [tuple(r) for r in results] == [
        (1, 1, 10, 0),
        (1, 2, 20, 0),
        (1, None, 30, 1),
        (None, None, 30, 2),
    ]


_GROUPING_HAVING_MODEL = """
key a int;
key b int?;
key c int?;
property <a, b, c>.x int;
datasource test_data (
    a: a,
    b: b,
    c: c,
    x: x
)
grain (a, b, c)
query '''
    select 1 as a, 1 as b, 1 as c, 10 as x
    union all
    select 1 as a, 2 as b, 1 as c, 20 as x
    union all
    select 2 as a, 1 as b, 2 as c, 30 as x
''';
"""

# `grouping()` summed in HAVING (to keep only certain rollup levels) together with
# a rowset-membership filter (`a in qualifying.a`) that forces an extra filter/join
# CTE. The membership filter strands the rollup behind a downstream CTE and the
# grouping() in HAVING renders against that groupless CTE.
_GROUPING_HAVING_QUERY = """
with qualifying as
select a where x > 5;
auto total <- sum(x);
auto overall_avg <- avg(x) by *;
select
    a, b, c,
    total,
    --overall_avg
where a in qualifying.a
by rollup (a, b, c)
having total > overall_avg
  and (grouping(a) + grouping(b) + grouping(c)) in (0, 1, 4)
order by a asc, b asc nulls last, c asc nulls last;
"""


def test_grouping_sum_in_having_with_membership_filter_colocates():
    """Bug: a `grouping()` sum in HAVING that restricts rollup levels, when a
    rowset-membership filter pushes the aggregate behind an extra CTE, stranded
    the `grouping(col)` in that groupless join/filter CTE — DuckDB rejects it
    ('GROUPING function is not supported here'). The grouping wrappers must align
    with the rollup spec so they co-locate in the ROLLUP CTE."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text(_GROUPING_HAVING_MODEL)
    sql = executor.generate_sql(_GROUPING_HAVING_QUERY)[-1]
    assert "ROLLUP" in sql
    # grouping() must not render against the outer groupless filter/join CTE.
    results = list(executor.execute_raw_sql(sql).fetchall())
    assert [tuple(r) for r in results] == [
        (2, 1, 2, 30),
        (2, 1, None, 30),
    ]


def test_grouping_membership_in_having_routes_to_having_not_where():
    """Bug: a `grouping()` membership (`grouping(a) in (0,1)`) in HAVING was
    classified scalar and rendered in the pre-aggregation WHERE (DuckDB rejects
    `grouping()` there), while `grouping(a) = 0` correctly went to HAVING. A
    membership must be placed by its left operand, so an aggregate/grouping left
    reaches HAVING."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text(_GROUPING_ID_CASE_MODEL)
    query = """
auto total <- sum(x);
select a, b, total
by rollup (a, b)
having (grouping(a) + grouping(b)) in (0, 1)
order by a asc, b asc nulls last;
"""
    sql = executor.generate_sql(query)[-1]
    assert "ROLLUP" in sql
    assert "HAVING" in sql
    assert "WHERE" not in sql
    results = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    assert results == [(1, 1, 10), (1, 2, 20), (1, None, 30)]


_NAMED_AGG_WHERE_MODEL = """
key rid int;
property rid.item int;
property rid.ch string;
datasource rows (rid: rid, item: item, ch: ch)
grain (rid)
query '''select 1 rid,100 item,'S' ch union all select 2,100,'W'
         union all select 3,200,'W' union all select 4,300,'S' ''';
auto f_byitem <- count(rid ? ch = 'S') by item > 0;
"""


def test_named_aggregate_concept_in_where_routes_to_having():
    """Bug: a named/derived concept whose lineage is a Comparison wrapping an
    aggregate (`count(...) by item > 0`), referenced in a top-level WHERE, was
    classified scalar and rendered as an inline aggregate inside SQL `WHERE`
    (DuckDB rejects aggregates there). The concept-reference indirection must
    route to HAVING just like the equivalent inline form does."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text(_NAMED_AGG_WHERE_MODEL)
    query = "where f_byitem select item order by item;"
    sql = executor.generate_sql(query)[-1]
    assert "HAVING" in sql
    assert "WHERE" not in sql.split("HAVING")[1]
    results = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    assert results == [(100,), (300,)]


def test_rollup_macro_in_having_vs_scalar_colocates():
    """A rollup aggregate filtered in HAVING against a `by *` scalar
    (`having total > overall_avg`). Under the SELECT-level `by rollup (...)`
    clause there is no per-aggregate rollup, so HAVING references the projected
    rollup alias `total` (which carries the rollup grain); that comparison must
    colocate in the ROLLUP CTE and not emit the `INVALID_REFERENCE_BUG` sentinel.
    NOTE: the old q14 form filtered on a SEPARATE `def` macro carrying its own
    `by rollup` — that per-aggregate rollup no longer exists; a fresh HAVING
    aggregate can't independently inherit the clause, so we filter on the alias."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text(_GROUPING_ID_CASE_MODEL)
    query = """
auto overall_avg <- avg(x) by *;
select a, b,
    sum(x) as total,
    --overall_avg
by rollup (a, b)
having total > overall_avg
order by a asc nulls first, b asc nulls first;
"""
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    assert "ROLLUP" in sql
    results = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    assert results == [(None, None, 30), (1, None, 30), (1, 2, 20)]


def test_duckdb_rollup_passthrough_outer_no_regroup():
    # Regression: when an outer CTE projects rollup-aggregate outputs that are
    # forwarded passthroughs from a parent CTE which already applied the
    # rollup, the outer CTE must NOT re-emit GROUP BY ROLLUP. Re-emitting it
    # produces a DuckDB BinderException because passthrough columns aren't
    # inside an aggregate, and conceptually it would double-aggregate the
    # parent's rolled-up rows.
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(), rendering=Rendering(parameters=False)
    )
    executor.parse_text("""
key store_id int;
property store_id.state int;
property store_id.county int;
key date_id int;
property date_id.month_seq int;
key sale_id int;
property sale_id.profit int;
property sale_id.sale_store_id int?;
property sale_id.sale_date_id int?;

datasource stores ( store_id: store_id, state: state, county: county )
grain (store_id)
query '''
    select 1 as store_id, 10 as state, 100 as county
    union all select 2 as store_id, 10 as state, 101 as county
    union all select 3 as store_id, 20 as state, 200 as county
''';
datasource dates ( date_id: date_id, month_seq: month_seq )
grain (date_id)
query '''
    select 1 as date_id, 1200 as month_seq
    union all select 2 as date_id, 1205 as month_seq
    union all select 3 as date_id, 1300 as month_seq
''';
datasource sales (
    sale_id: sale_id,
    profit: profit,
    sale_store_id: sale_store_id,
    sale_date_id: sale_date_id
)
grain (sale_id)
query '''
    select 1 as sale_id, 5 as profit, 1 as sale_store_id, 1 as sale_date_id
    union all select 2 as sale_id, 7 as profit, 1 as sale_store_id, 1 as sale_date_id
    union all select 3 as sale_id, 3 as profit, 2 as sale_store_id, 2 as sale_date_id
    union all select 4 as sale_id, 8 as profit, 3 as sale_store_id, 1 as sale_date_id
''';
""")

    sql = executor.generate_sql("""
merge sale_store_id into ~store_id;
merge sale_date_id into ~date_id;

auto state_total <- sum(profit ? month_seq between 1200 and 1300) by state;
auto state_rnk <- rank(state) over (order by state_total desc);
rowset top_states <- where month_seq between 1200 and 1300
    select state as ts_state, --state_rnk, having state_rnk <= 2;

where month_seq between 1200 and 1300 and state in top_states.ts_state
SELECT
    sum(profit) as total_sum,
    state as s_state,
    county as s_county,
    grouping(state) + grouping(county) as lochierarchy,
    rank(state, county) over (partition by lochierarchy,
        case when grouping(county) = 0 then state else null end
        order by total_sum desc) as rank_within_parent,
by rollup (state, county)
ORDER BY lochierarchy desc nulls first;
""")[-1]

    # Exactly one ROLLUP — the inner aggregating CTE — not re-emitted at the
    # outer projection layer.
    assert sql.count("ROLLUP") == 1, sql
    # The outer SELECT must execute without binder error.
    list(executor.execute_raw_sql(sql).fetchall())


_HAVING_SUBSTITUTION_SCHEMA = """
key sale_id int;
property sale_id.store_id int;
property sale_id.customer_id int;
property sale_id.amount int;

datasource sales (
    sale_id: sale_id,
    store_id: store_id,
    customer_id: customer_id,
    amount: amount
)
grain (sale_id)
query '''
    select 1 as sale_id, 1 as store_id, 1 as customer_id, 100 as amount
    union all select 2 as sale_id, 1 as store_id, 2 as customer_id, 800 as amount
    union all select 3 as sale_id, 2 as store_id, 1 as customer_id, 50 as amount
    union all select 4 as sale_id, 2 as store_id, 2 as customer_id, 100 as amount
''';
"""


def test_having_substitutes_off_grain_aggregate_e2e():
    # Data:
    #   (store=1, cust=1, 100), (store=1, cust=2, 800),
    #   (store=2, cust=1, 50),  (store=2, cust=2, 100)
    # Per-(store, customer) totals: 100, 800, 50, 100.
    # Per-store totals: store 1 = 900, store 2 = 150.
    # HAVING per_customer_store_total < 0.6 * store_total:
    #   (1,1) 100 < 540 ✓; (1,2) 800 < 540 ✗;
    #   (2,1) 50 < 90 ✓;   (2,2) 100 < 90 ✗  → 2 rows.
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_HAVING_SUBSTITUTION_SCHEMA)
    text = """
select
    store_id,
    customer_id,
    sum(amount) as total,
    sum(amount) by store_id as store_total
having sum(amount) < 0.6 * sum(amount) by store_id
order by store_id asc, customer_id asc;
"""
    sql = executor.generate_sql(text)[-1]
    assert "INVALID_REFERENCE" not in sql, sql
    rows = executor.execute_text(text)[0].fetchall()
    assert [(r.store_id, r.customer_id, r.total, r.store_total) for r in rows] == [
        (1, 1, 100, 900),
        (2, 1, 50, 150),
    ]


def test_having_substitutes_nested_aggregate_e2e():
    # Same data — per-(customer, store) totals: 100, 800, 50, 100.
    # avg(sum by customer_id, store_id) by store_id:
    #   store 1 → avg(100, 800) = 450; store 2 → avg(50, 100) = 75.
    # HAVING total > per-store avg-of-customer-totals:
    #   (1,1) 100 > 450 ✗; (1,2) 800 > 450 ✓;
    #   (2,1) 50 > 75 ✗;   (2,2) 100 > 75 ✓  → 2 rows.
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_HAVING_SUBSTITUTION_SCHEMA)
    text = """
select
    store_id,
    customer_id,
    sum(amount) as total,
    avg(sum(amount) by customer_id, store_id) by store_id as avg_per_store
having sum(amount) > avg(sum(amount) by customer_id, store_id) by store_id
order by store_id asc, customer_id asc;
"""
    sql = executor.generate_sql(text)[-1]
    assert "INVALID_REFERENCE" not in sql, sql
    rows = executor.execute_text(text)[0].fetchall()
    assert [(r.store_id, r.customer_id, r.total, r.avg_per_store) for r in rows] == [
        (1, 2, 800, 450),
        (2, 2, 100, 75),
    ]


def test_window_function_in_having_lowers_to_qualify_e2e():
    # Per-store totals: store 1 = 900, store 2 = 150. A window predicate in
    # HAVING can't live in WHERE/HAVING; it must lower to QUALIFY so the rank is
    # evaluated after grouping. rank desc <= 1 keeps only store 1.
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_HAVING_SUBSTITUTION_SCHEMA)
    text = """
select
    store_id,
    sum(amount) as total
having rank() over (order by sum(amount) desc) <= 1
order by store_id asc;
"""
    sql = executor.generate_sql(text)[-1]
    assert "QUALIFY" in sql, sql
    assert "HAVING" not in sql, sql
    rows = executor.execute_text(text)[0].fetchall()
    assert [(r.store_id, r.total) for r in rows] == [(1, 900)]


def test_window_function_in_having_rejected_without_qualify():
    # Dialects without QUALIFY (e.g. SQLite) can't lower a window in HAVING and
    # must raise a clear, actionable error rather than emit invalid SQL.
    executor = Dialects.SQLITE.default_executor(environment=Environment())
    executor.parse_text(_HAVING_SUBSTITUTION_SCHEMA)
    text = """
select
    store_id,
    sum(amount) as total
having rank() over (order by sum(amount) desc) <= 1;
"""
    with raises(InvalidSyntaxException, match="Window functions are not"):
        executor.generate_sql(text)


_ORDER_BY_CONSTANT_SCHEMA = """key id int;
property id.sid string;
property id.iid string;
property id.v int;
datasource t (id, sid, iid, v)
  grain (id)
  query '''select 1 as id, 's1' as sid, 'i1' as iid, 10 as v
           union all select 2, 's1', 'i2', 20
           union all select 3, 's2', 'i1', 5''';

auto by_store <- sum(v) by sid;
auto by_item  <- sum(v) by iid;
"""


def test_order_by_constant_across_aggregate_ctes():
    # A constant select column ordered by, in a multi-aggregate-CTE plan, must
    # reference the materialized CTE column in ORDER BY rather than re-emitting
    # the bind parameter (which DuckDB rejects: "Parameter not supported in
    # ORDER BY clause"). The literal is still parameterized where it's defined.
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(_ORDER_BY_CONSTANT_SCHEMA)
    text = """select 'store' as channel, sid, by_store, by_item
order by channel asc, sid asc nulls first;"""
    sql = executor.generate_sql(text)[-1]
    order_by = sql.split("ORDER BY")[-1]
    assert "$1" not in order_by and ":channel" not in order_by, sql
    assert '"channel"' in order_by, sql
    rows = executor.execute_text(text)[0].fetchall()
    assert all(r.channel == "store" for r in rows)
    assert [r.sid for r in rows] == sorted(r.sid for r in rows)
