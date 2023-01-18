# from preql.compiler import compile
from preql.core.models import Select, Grain, Window, WindowOrder, GrainWindow, WindowItem
from preql.parser import parse
from preql.core.hooks import GraphHook

# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.core.query_processor import process_query, get_datasource_by_concept_and_grain, get_query_datasources
from preql.parser import parse
from preql.dialect.sql_server import SqlServerDialect
from preql.dialect.bigquery import BigqueryDialect
from logging import DEBUG
from preql.constants import logger

logger.setLevel(DEBUG)

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
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    assert isinstance(env.concepts['user_rank'].lineage, WindowItem)

    ds = get_datasource_by_concept_and_grain(env.concepts["user_rank"], Grain(components=[env.concepts['user_id']]),
                                             environment=env)


    ds = get_datasource_by_concept_and_grain(env.concepts["post_count"], Grain(components=[env.concepts['user_id']]),
                                             environment=env)
    # ds.validate()
    ds.get_alias(env.concepts["post_count"].with_grain(ds.grain))


    concepts, datasources = get_query_datasources(
        environment=env,statement=select
    )
    # raise ValueError

    query = process_query(statement=select, environment=env, hooks=[GraphHook()])
    expected_base = query.ctes[0]

    for cte in query.ctes:
       if cte.name.startswith('cte_posts_at_user_id'):
           cte.output_columns = cte.output_columns + [env.concepts['user_id'].with_grain(Grain(components=[env.concepts['user_id']]))]
           for x in cte.output_columns:
               print(x)


    generator = BigqueryDialect()
    sql = generator.compile_statement(query)
    print(sql)
