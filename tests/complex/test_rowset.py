from pathlib import Path

from trilogy.core.models.environment import Environment
from trilogy.parser import parse


def test_rowset_imported_nested_namespace(tmp_path: Path) -> None:
    inner = tmp_path / "inner.preql"
    inner.write_text("""
key id int;
property id.zip string;
property id.state string;

datasource stores (
    id: id,
    zip: zip,
    state: state,
)
grain (id)
address stores;
""")
    env = Environment(working_path=tmp_path)
    env, _ = parse(
        """import inner as store;

key sale_id int;
property sale_id.amount float;

datasource sales (
    id: sale_id,
    store_id: store.id,
    amount: amount,
)
grain (sale_id)
address sales;

rowset by_state <- select
    store.id,
    store.state,
    sum(amount) -> total,
;

auto state_avg <- avg(by_state.total) by by_state.store.id;

select
    by_state.store.id,
    by_state.total,
    state_avg,
;
""",
        env,
    )
    assert "by_state.store.id" in env.concepts
    assert "by_state.store.state" in env.concepts
    assert "by_state.total" in env.concepts


def test_rowset() -> None:
    declarations = """
key user_id int metadata(description="the description");
key post_id int;
metric total_posts <- count(post_id) by *;
auto total_posts_auto <- count(post_id) by *;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;

auto user_post_count <- count(post_id) by user_id;

rowset top_users <- select user_id, user_post_count,  user_post_count / total_posts as post_ratio
having post_ratio > .05;

select
    top_users.user_id,
    top_users.user_post_count
;
    """
    _env, _parsed = parse(declarations)


def test_rowset_grain() -> None:
    declarations = """
key user_id int metadata(description="the description");
key post_id int;
metric total_posts <- count(post_id) by *;
auto total_posts_auto <- count(post_id) by *;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;

auto user_post_count <- count(post_id) by user_id;

rowset top_users <- select user_id, user_post_count,  user_post_count / total_posts as post_ratio
having post_ratio > .05;

select
    top_users.user_id,
    top_users.user_post_count
;
    """
    _env, _parsed = parse(declarations)


def test_rowset_alias_name_collision() -> None:
    # Two rowsets aliasing different source concepts to the SAME output name
    # ("cust_id") must produce independent results: buyers_a.cust_id should
    # project bill, buyers_b.cust_id should project ship. The shared `id`
    # key in both rowsets gives discovery a join target so the outer SELECT
    # is resolvable; the alias collision is the part this test exercises.
    declarations = """
key id int;
key bill_id int;
key ship_id int;

datasource orders (
    id: id,
    bill: bill_id,
    ship: ship_id,
)
grain (id)
address orders;

with buyers_a as
SELECT
    id,
    bill_id as cust_id
;

with buyers_b as
SELECT
    id,
    ship_id as cust_id
;

SELECT
    id,
    buyers_a.cust_id as a_cust,
    buyers_b.cust_id as b_cust,
;
"""
    from trilogy import Dialects
    from trilogy.dialect.config import DuckDBConfig

    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
    sql = engine.generate_sql(declarations)[-1]
    # Each rowset body projects the source column under a rowset-scoped name —
    # the mangled body-local (`_buyers_a_cust_id`), the handle name
    # (`buyers_a_cust_id`), or straight to the outer alias (`a_cust`) when
    # CollapseSingleParent folds the rename chain into the scan. Any depth
    # keeps the invariant this test pins: the chains never SWAP — bill only
    # ever renders under an a-side name, ship only under a b-side name. The
    # executing row check lives in test_rowset_alias_collision_rows.
    a_names = ["_buyers_a_cust_id", "buyers_a_cust_id", "a_cust"]
    b_names = ["_buyers_b_cust_id", "buyers_b_cust_id", "b_cust"]
    assert any(
        f'"orders"."bill" as "{name}"' in sql for name in a_names
    ), f"buyers_a.cust_id should project bill, sql was:\n{sql}"
    assert any(
        f'"orders"."ship" as "{name}"' in sql for name in b_names
    ), f"buyers_b.cust_id should project ship, sql was:\n{sql}"
    assert not any(
        f'"orders"."ship" as "{name}"' in sql for name in a_names
    ), f"ship leaked into the a-side chain:\n{sql}"
    assert not any(
        f'"orders"."bill" as "{name}"' in sql for name in b_names
    ), f"bill leaked into the b-side chain:\n{sql}"


def test_rowset_alias_name_collision_lineage() -> None:
    declarations = """
key id int;
key bill_id int;
key ship_id int;

datasource orders (
    id: id,
    bill: bill_id,
    ship: ship_id,
)
grain (id)
address orders;

with buyers_a as
SELECT
    bill_id as cust_id
;

with buyers_b as
SELECT
    ship_id as cust_id
;
"""
    env = Environment()
    env.parse(declarations)

    def trace_to_root(c):
        seen = []
        while c is not None and getattr(c, "lineage", None) is not None:
            seen.append(c.address)
            lineage = c.lineage
            inner = None
            if hasattr(lineage, "content"):
                inner = env.concepts.get(lineage.content.address)
            elif hasattr(lineage, "arguments") and lineage.arguments:
                arg = lineage.arguments[0]
                if hasattr(arg, "address"):
                    inner = env.concepts.get(arg.address)
            c = inner
        if c is not None:
            seen.append(c.address)
        return seen

    a_chain = trace_to_root(env.concepts["buyers_a.cust_id"])
    b_chain = trace_to_root(env.concepts["buyers_b.cust_id"])
    assert (
        a_chain[-1] == "local.bill_id"
    ), f"buyers_a.cust_id should resolve to local.bill_id but chain is {a_chain}"
    assert (
        b_chain[-1] == "local.ship_id"
    ), f"buyers_b.cust_id should resolve to local.ship_id but chain is {b_chain}"


def test_rowset_alias_collision_distinct_aggregates() -> None:
    # Two rowsets aliasing *different aggregates* to the same name `total`.
    # Each alias is private to its rowset, so rs_a.total must aggregate
    # count(x) and rs_b.total must aggregate sum(y) with no cross-talk and
    # no INVALID_REFERENCE_BUG. Invariant guard for the per-rowset alias
    # namespacing (complements the column-alias collision tests above).
    declarations = """
key id int;
property id.x int;
property id.y int;
property id.grp_key int;

datasource facts (
    id: id,
    x: x,
    y: y,
    grp: grp_key,
)
grain (id)
address facts;

with rs_a as
SELECT
    grp_key,
    count(x) -> total
;

with rs_b as
SELECT
    grp_key,
    sum(y) -> total
;

SELECT
    grp_key,
    rs_a.total as a,
    rs_b.total as b,
;
"""
    from trilogy import Dialects
    from trilogy.dialect.config import DuckDBConfig

    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
    sql = engine.generate_sql(declarations)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    assert 'count("facts"."x")' in sql, f"rs_a.total should be count(x):\n{sql}"
    assert 'sum("facts"."y")' in sql, f"rs_b.total should be sum(y):\n{sql}"


def test_rowset_alias_collision_rows() -> None:
    """Executing check for the alias-collision shape: two rowsets renaming
    DIFFERENT source columns to the same name, joined back through their
    shared `id` key. Regression: the renamed handles carried no key
    association, the FINAL merge grain collapsed to empty, and the merge
    cross-joined `FULL JOIN ... on 1=1` (3 rows -> 9-row cartesian). The
    planning-status parity case cannot catch this — the cartesian still
    executes — so the rows are pinned here."""
    declarations = """
key id int;
key bill_id int;
key ship_id int;

datasource orders (
    id: id,
    bill: bill_id,
    ship: ship_id,
)
grain (id)
query '''
select 1 id, 100 bill, 200 ship
union all select 2, 101, 201
union all select 3, 102, 202
''';

with buyers_a as
SELECT
    id,
    bill_id as cust_id
;

with buyers_b as
SELECT
    id,
    ship_id as cust_id
;

SELECT
    id,
    buyers_a.cust_id as a_cust,
    buyers_b.cust_id as b_cust,
order by
    id asc
;
"""
    from trilogy import Dialects

    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    sql = engine.generate_sql(declarations)[-1]
    assert " 1=1" not in sql, sql
    rows = engine.execute_text(declarations)[-1].fetchall()
    assert [tuple(r) for r in rows] == [(1, 100, 200), (2, 101, 201), (3, 102, 202)]


def test_rowset_alias_collision_distinct_aggregate_rows() -> None:
    """Executing check for the distinct-aggregates collision shape: two
    rowsets aliasing different aggregates over the same grouping key.
    Regression: the aggregate-rowset boundary refused to expose its grain
    key, neither BASIC rename advertised a projection grain, and the FINAL
    merge cross-joined all three contributors ON 1=1 (wrong totals paired
    with wrong keys)."""
    declarations = """
key id int;
property id.x int;
property id.y int;
property id.grp_key int;

datasource facts (
    id: id,
    x: x,
    y: y,
    grp: grp_key,
)
grain (id)
query '''
select 1 id, 10 x, 100 y, 7 grp union all
select 2 id, 20 x, 200 y, 7 grp union all
select 3 id, 30 x, 300 y, 8 grp
''';

with rs_a as
SELECT
    grp_key,
    count(x) -> total
;

with rs_b as
SELECT
    grp_key,
    sum(y) -> total
;

SELECT
    grp_key,
    rs_a.total as a,
    rs_b.total as b,
order by grp_key asc
;
"""
    from trilogy import Dialects

    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    sql = engine.generate_sql(declarations)[-1]
    assert " 1=1" not in sql, sql
    rows = engine.execute_text(declarations)[-1].fetchall()
    assert [tuple(r) for r in rows] == [(7, 2, 300), (8, 1, 300)]


def test_basic_expression_over_rowset_output_keeps_scoped_join_keys() -> None:
    declarations = """
key sale_id int;
property sale_id.w_sqft float;
property sale_id.channel string;

key date_id int;
property date_id.month int;

datasource sales (
    id: sale_id,
    sqft: w_sqft,
    channel: channel,
)
grain (sale_id)
address sales;

datasource dates (
    id: date_id,
    month: month,
)
grain (date_id)
address dates;

rowset all_months <- select
    month,
    1 as join_key,
;

rowset wh_groups <- where channel = 'WEB'
select
    w_sqft,
    1 as join_key,
;

select
    wh_groups.w_sqft * 2 as r,
    all_months.month,
subset join all_months.join_key = wh_groups.join_key
where all_months.month is not null
order by
    all_months.month asc nulls first,
    r asc nulls first
;
"""
    from trilogy import Dialects
    from trilogy.dialect.config import DuckDBConfig

    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
    sql = engine.generate_sql(declarations)[-1]
    assert "JOIN" in sql
