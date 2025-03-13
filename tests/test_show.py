from trilogy import Dialects
from trilogy.core.statements.author import ShowStatement
from trilogy.parser import parse


def test_show_bigquery():
    declarations = """
    key user_id int metadata(description="the description");
    property user_id.display_name string metadata(description="The display name ");
    property user_id.about_me string metadata(description="User provided description");
    key post_id int;


    datasource posts (
        user_id: user_id,
        id: post_id
        )
        grain (post_id)
        address `bigquery-public-data.stackoverflow.post_history`
    ;


    datasource users (
        id: user_id,
        display_name: display_name,
        about_me: about_me,
        )
        grain (user_id)
        address `bigquery-public-data.stackoverflow.users`
    ;


        """
    env, parsed = parse(declarations)

    q1 = """
    metric post_count<- count(post_id);
    metric distinct_post_count <- count_distinct(post_id);
    
    metric user_count <- count(user_id);
    
    show select
        post_count,
        distinct_post_count,
        user_count
    ;"""
    env, parsed = parse(q1, environment=env)
    select: ShowStatement = parsed[-1]

    query = (
        Dialects.DUCK_DB.default_executor(environment=env)
        .execute_query(select)
        .fetchall()
    )
    assert (
        "FULL JOIN cheerful on 1=1" in query[0]["__preql_internal_query_text"]
    ), query[0]["__preql_internal_query_text"]
