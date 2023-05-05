from preql.core.models import Select
from preql.core.query_processor import process_query
from preql.dialect.bigquery import BigqueryDialect
from preql.hooks.query_debugger import DebuggingHook
from preql.parser import parse


def test_select():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric user_post_count <-count(post_id*2) by user_id;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address bigquery-public-data.stackoverflow.post_history
;


metric avg_user_post_count_double <- avg(user_post_count)*2;

select
    avg_user_post_count_double
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

    generator = BigqueryDialect()
    sql = generator.compile_statement(query)
