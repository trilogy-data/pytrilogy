from trilogy.core.enums import Granularity
from trilogy.core.models.author import Grain
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse


def test_select() -> None:
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
    address `bigquery-public-data.stackoverflow.post_history`
;


auto avg_user_post_count_double <- avg(user_post_count)*2;

select
    avg_user_post_count_double
;


    """
    env, parsed = parse(declarations)
    select: SelectStatement = parsed[-1]

    query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

    generator = BigqueryDialect()
    generator.compile_statement(query)


def test_constant_aggregate() -> None:
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


    """
    env, parsed = parse(declarations)

    assert env.concepts["total_posts"].granularity == Granularity.SINGLE_ROW
    assert env.concepts["total_posts_auto"].granularity == Granularity.SINGLE_ROW

    env, parsed = env.parse(
        """select
    user_id,
    total_posts,
    total_posts_auto
;"""
    )
    assert env.concepts["total_posts"].granularity == Granularity.SINGLE_ROW
    assert env.concepts["total_posts_auto"].granularity == Granularity.SINGLE_ROW
    select: SelectStatement = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["user_id"]])

    query = process_query(statement=select, environment=env, hooks=[DebuggingHook()])

    generator = BigqueryDialect()
    generator.compile_statement(query)
