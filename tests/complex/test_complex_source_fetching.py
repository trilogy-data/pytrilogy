# from preql.compiler import compile
from preql.core.models import Select, Grain, Window, WindowOrder, GrainWindow
from preql.parser import parse
from preql.core.hooks import GraphHook

# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.core.query_processor import process_query, get_query_datasources, get_datasource_by_concept_and_grain, QueryDatasource, Datasource, datasource_to_ctes
from preql.parser import parse
from preql.dialect.sql_server import SqlServerDialect

def test_complex_source():
    from preql.constants import logger
    from logging import StreamHandler, DEBUG
    logger.setLevel(DEBUG)
    logger.addHandler(StreamHandler())
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
    post_id = env.concepts['post_id']
    avg_user_post_count = env.concepts['avg_user_post_count']
    user_post_count = env.concepts['user_post_count']

    expected_parent = get_datasource_by_concept_and_grain(user_post_count,
                                                              Grain(components=[env.concepts['user_id']]), env, None)
    print(expected_parent.identifier)
    print(expected_parent.output_concepts[0])

    expected_cte = datasource_to_ctes(expected_parent)

    assert set(expected_parent.source_map.keys()) == set(['post_id', 'user_id'])

    assert user_post_count in expected_parent.output_concepts

    datasource = get_datasource_by_concept_and_grain(
        avg_user_post_count, Grain(), env, None
    )

    print(datasource.identifier)
    assert isinstance(datasource, QueryDatasource)
    assert datasource.output_concepts[0] == avg_user_post_count
    assert len(datasource.datasources) == 1
    parent = datasource.datasources[0]
    assert parent == expected_parent
    print(parent.identifier)
    assert isinstance(parent, QueryDatasource)
    assert user_post_count in parent.output_concepts

    assert set(parent.source_map.keys()) == set(['post_id', 'user_id'])

    root = parent.datasources[0]
    assert isinstance(root, Datasource)

    assert post_id in root.concepts


    ctes = datasource_to_ctes(datasource)
    assert len(ctes ) == 2

    # now validate
    select: Select = Select(selection = [avg_user_post_count])

    query = process_query(statement=select, environment=env, hooks=[GraphHook()])
    cte = query.ctes[1]
    assert cte.output_columns[0] == avg_user_post_count
    assert len(query.ctes)== 2

    generator = SqlServerDialect()
    sql = generator.compile_statement(query)
    print(sql)