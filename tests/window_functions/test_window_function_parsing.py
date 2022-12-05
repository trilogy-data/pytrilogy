# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.parser import parse


def test_select():
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


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address bigquery-public-data.stackoverflow.users
;

select
    top 10 user_id,
    count(post_id)->post_count_2
;

select
    top 10 user_id by post_count
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["user_id"]])
