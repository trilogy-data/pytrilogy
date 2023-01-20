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
where user_rank<10
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

        if cte.name.startswith('cte_users_at_user_id_posts_at_user'):
            print(cte.base_name)
            print(cte.base_alias)
            print(len(cte.parent_ctes))
            print(len(cte.joins))
            print('datasources')
            print(len(cte.source.datasources))
            for ds in cte.source.datasources:
                print(ds.name)
            print('source info')
            for key, item in cte.source.source_map.items():
                print(key)
                print(item)
            print(cte.source.joins)
            for x in cte.parent_ctes:
                print(x)
            for key, item in cte.source_map.items():
                print(key)
                print(item)


    generator = BigqueryDialect()
    sql = generator.compile_statement(query)

    for join in query.joins:
        print(join)
    print(sql)
