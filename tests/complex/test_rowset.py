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
    env, parsed = parse(declarations)


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
    env, parsed = parse(declarations)


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
    assert (
        '"orders"."bill" as "buyers_a_cust_id"' in sql
    ), f"buyers_a.cust_id should project bill, sql was:\n{sql}"
    assert (
        '"orders"."ship" as "buyers_b_cust_id"' in sql
    ), f"buyers_b.cust_id should project ship, sql was:\n{sql}"


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
