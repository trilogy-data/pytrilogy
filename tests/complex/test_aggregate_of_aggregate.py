import re

from trilogy.core.models import SelectStatement
from trilogy.core.query_processor import process_query
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse


def test_select():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);
metric user_post_count <- count(post_id) by user_id;
metric avg_user_post_count <-avg(user_post_count) by user_id;


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


select
    avg_user_post_count
;


    """
    env, parsed = parse(declarations)
    select: SelectStatement = parsed[-1]

    query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

    generator = BigqueryDialect()
    sql = generator.compile_statement(query)

    assert re.search(r"(count\([A-z0-9\_]+\.`id`\) as `user_post_count`)", sql)
    assert re.search(
        r"avg\([A-z0-9\_]+\.`user_post_count`\) as `avg_user_post_count`", sql
    )
