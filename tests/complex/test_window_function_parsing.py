# from preql.compiler import compile
from preql.core.models import Select, Grain, Window, WindowOrder, GrainWindow, WindowItem
from preql.parser import parse
from preql.core.hooks import GraphHook

# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.core.query_processor import process_query, get_datasource_by_concept_and_grain
from preql.parser import parse
from preql.dialect.sql_server import SqlServerDialect
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

key top_users <- top 10 user_id by post_count desc;


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
    top_users,
    post_count
;


    """
    env, parsed = parse(declarations)
    select: Select = parsed[-1]

    # first, check to make sure post_count is gotten properly
    # base_check = env.concepts["post_count"].with_grain()
    # ds = get_datasource_by_concept_and_grain(base_check, Grain(components=[]),
    #                                          environment=env)

    target_grain = Grain(components=[env.concepts["top_users"]])
    assert isinstance(env.concepts['top_users'].lineage, WindowItem)

    ds = get_datasource_by_concept_and_grain(env.concepts["post_count"], Grain(components=[env.concepts['top_users']]),
                                             environment=env)

    assert env.concepts["post_count"].with_grain(env.concepts['top_users']) in (ds.output_concepts)
    ds.validate()
    print('######')
    ds = get_datasource_by_concept_and_grain(env.concepts["post_count"], Grain(components=[env.concepts['top_users']]),
                                             environment=env)
    ds.validate()
    print('##########')
    print(ds)
    print(len(ds.datasources))
    print(ds.datasources[0])
    print([c.name for c in ds.output_concepts])
    for key, value in ds.source_map.items():
        print(key)
        print(value)
    print(ds.grain)
    ds.get_alias(env.concepts["post_count"].with_grain(ds.grain))

    query = process_query(statement=select, environment=env, hooks=[GraphHook()])
    for cte in query.ctes:
        print(cte.name)
        print(cte.grain)
        for x in cte.output_columns:
            print(x)
        print('-----')
    expected_base = query.ctes[0]

    ds  = expected_base.source
    print(expected_base.source_map)
    print('---ds debug-')
    print(len(ds.datasources))
    print(ds.datasources)

    for concept in ds.output_concepts:
        print(concept)

    ds.get_alias(env.concepts["post_count"].with_grain(env.concepts['top_users']))

    generator = SqlServerDialect()
    sql = generator.compile_statement(query)
    print(sql)
