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
