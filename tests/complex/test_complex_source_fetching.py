# from preql.compiler import compile


# from preql.compiler import compile
from preql.core.models import Select, Grain, Datasource, QueryDatasource
from preql.core.query_processor import (
    process_query,
    get_datasource_by_concept_and_grain,
    datasource_to_ctes,
)
from preql.dialect.sql_server import SqlServerDialect


def test_aggregate_of_property_function(stackoverflow_environment):
    env = stackoverflow_environment
    avg_user_post_count = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: Select = Select(selection=[avg_user_post_count, user_id])

    query = process_query(statement=select, environment=env, )
    generator = SqlServerDialect()
    for cte in query.ctes:
        if avg_user_post_count in cte.output_columns:
            # checks on join
            # alias = cte.get_alias(avg_user_post_count)
            rendered = generator.render_concept_sql(avg_user_post_count, cte)
            assert rendered == 'avg(length(posts."text")) as "user_avg_post_length"'

    sql = generator.compile_statement(query)


def test_aggregate_of_aggregate(stackoverflow_environment):

    env = stackoverflow_environment
    post_id = env.concepts["post_id"]
    avg_user_post_count = env.concepts["avg_user_post_count"]
    user_post_count = env.concepts["user_post_count"]

    posts = env.datasources["posts"]
    post_grain = Grain(components=[env.concepts["post_id"]])

    assert posts.grain == post_grain

    expected_parent = get_datasource_by_concept_and_grain(
        user_post_count, Grain(components=[env.concepts["user_id"]]), env, None
    )

    assert posts.grain == post_grain

    assert set(expected_parent.source_map.keys()) == set(
        ["local.post_id", "local.user_id"]
    )

    assert user_post_count in expected_parent.output_concepts

    datasource = get_datasource_by_concept_and_grain(
        avg_user_post_count, Grain(), env, None
    )

    assert isinstance(datasource, QueryDatasource)
    assert datasource.grain == Grain()
    # ensure we identify aggregates of aggregates properly
    assert datasource.identifier == "posts_at_local_user_id_at_abstract"
    assert datasource.output_concepts[0] == avg_user_post_count
    assert len(datasource.datasources) == 1
    parent = datasource.datasources[0]
    assert parent == expected_parent

    assert isinstance(parent, QueryDatasource)
    assert user_post_count in parent.output_concepts

    assert set(parent.source_map.keys()) == set(["local.post_id", "local.user_id"])

    root = parent.datasources[0]
    assert isinstance(root, Datasource)
    assert posts == root
    assert post_id in root.concepts

    ctes = datasource_to_ctes(datasource)

    final_cte = ctes[1]
    assert len(final_cte.parent_ctes) > 0
    assert len(ctes) == 2

    # now validate
    select: Select = Select(selection=[avg_user_post_count])

    query = process_query(statement=select, environment=env, hooks=[GraphHook()])
    parent_cte = query.ctes[0]
    cte = query.ctes[1]
    assert cte.output_columns[0] == avg_user_post_count
    assert parent_cte.output_columns[0] == user_post_count
    assert len(query.ctes) == 2
    assert len(cte.parent_ctes) > 0
    assert cte.parent_ctes[0] == query.ctes[0]

    generator = SqlServerDialect()
    sql = generator.compile_statement(query)
