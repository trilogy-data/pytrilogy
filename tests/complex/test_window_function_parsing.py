from preql.core.models import Select
from preql.core.models import WindowItem
from preql.core.processing.concept_strategies_v2 import source_concepts
from preql.core.query_processor import process_query, get_query_datasources
from preql.dialect.bigquery import BigqueryDialect
from preql.parser import parse


def test_select():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

property user_rank <- rank user_id by post_count desc;


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
    user_id,
    user_rank,
    post_count
where 
    user_rank<10
limit 100
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    assert isinstance(env.concepts["user_rank"].lineage, WindowItem)

    ds = source_concepts(
        [env.concepts["post_count"]], [env.concepts["user_id"]], environment=env
    ).resolve()
    # ds.validate()
    ds.get_alias(env.concepts["post_count"].with_grain(ds.grain))

    get_query_datasources(environment=env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=env)
    query.ctes[0]

    generator = BigqueryDialect()
    generator.compile_statement(query)
