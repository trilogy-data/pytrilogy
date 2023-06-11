from preql.core.models import Select
from preql.core.query_processor import process_query
from preql.dialect.bigquery import BigqueryDialect
from preql.hooks.query_debugger import DebuggingHook
from preql.parser import parse
import re


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

    query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

    generator = BigqueryDialect()
    sql = generator.compile_statement(query)
    print(sql)

    assert re.search(r'(count\([A-z0-9\_]+\.`post_id`\) as `user_post_count`)', sql)
    assert re.search(r"avg\([A-z0-9\_]+\.`user_post_count`\) as `avg_user_post_count`",sql)
