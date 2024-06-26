from trilogy.parser import parse


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
    address bigquery-public-data.stackoverflow.post_history
;

auto user_post_count <- count(post_id) by user_id;

rowset top_users <- select user_id, user_post_count,  user_post_count / total_posts as post_ratio
where post_ratio > .05;

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
    address bigquery-public-data.stackoverflow.post_history
;

auto user_post_count <- count(post_id) by user_id;

rowset top_users <- select user_id, user_post_count,  user_post_count / total_posts as post_ratio
where post_ratio > .05;

select
    top_users.user_id,
    top_users.user_post_count
;
    """
    env, parsed = parse(declarations)
