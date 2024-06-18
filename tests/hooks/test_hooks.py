from preql.hooks.base_hook import BaseHook, Select
from preql.core.models import RowsetDerivation
from preql import parse


def test_base():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address bigquery-public-data.stackoverflow.post_history
;

select
    user_id,
    count(post_id) -> user_post_count
;

metric avg_user_post_count <- avg(user_post_count);


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address bigquery-public-data.stackoverflow.users
;


select
    avg_user_post_count
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]
    BaseHook().process_rowset_info(
        RowsetDerivation(name="test", select=select, namespace="test")
    )