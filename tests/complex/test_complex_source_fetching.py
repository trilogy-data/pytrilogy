# from preql.compiler import compile


# from preql.compiler import compile
from preql.core.models import Select, Grain, Datasource, QueryDatasource
from preql.core.processing.concept_strategies_v2 import source_concepts
from preql.core.query_processor import process_query, datasource_to_ctes
from preql.dialect.sql_server import SqlServerDialect
import re

def test_aggregate_of_property_function(stackoverflow_environment):
    env = stackoverflow_environment
    avg_user_post_count = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: Select = Select(selection=[avg_user_post_count, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    for cte in query.ctes:
        found = False
        if avg_user_post_count in cte.output_columns:
            rendered = generator.render_concept_sql(avg_user_post_count, cte)
            assert (
                rendered
                == 'avg(cte_posts_at_local_post_id_at_local_post_id_3009661045417896."post_length") as "user_avg_post_length"'
            )
            found = True
        if found:
            break
    assert found
    generator.compile_statement(query)


def test_aggregate_to_grain(stackoverflow_environment):
    env = stackoverflow_environment
    avg_post_length = env.concepts["avg_post_length_by_post_id"]
    user_id = env.concepts["user_id"]
    select: Select = Select(selection=[avg_post_length, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    for cte in query.ctes:
        found = False
        if avg_post_length in cte.output_columns:
            rendered = generator.render_concept_sql(avg_post_length, cte)

            assert (
                re.search(r'avg\([0-9A-z\_]+\."post_length"\) as "avg_post_length_by_post_id"', rendered)
                )
            found = True
        if found:
            break
    assert found
    generator.compile_statement(query)


def test_aggregate_of_aggregate(stackoverflow_environment):
    env = stackoverflow_environment
    post_id = env.concepts["post_id"]
    avg_user_post_count = env.concepts["avg_user_post_count"]
    user_post_count = env.concepts["user_post_count"]

    posts = env.datasources["posts"]
    post_grain = Grain(components=[env.concepts["post_id"]])

    assert posts.grain == post_grain

    expected_parent = source_concepts(
        [user_post_count], [env.concepts["user_id"]], env, None
    ).resolve()

    assert posts.grain == post_grain

    assert set(expected_parent.source_map.keys()) == set(
        ["local.post_id", "local.user_id"]
    )

    assert user_post_count in expected_parent.output_concepts

    datasource = source_concepts([avg_user_post_count], [], env, None).resolve()

    assert isinstance(datasource, QueryDatasource)
    assert datasource.grain == Grain()
    # ensure we identify aggregates of aggregates properly
    assert (
        datasource.identifier == "posts_at_local_post_id_at_local_user_id_at_abstract"
    )
    assert datasource.output_concepts[0] == avg_user_post_count
    assert len(datasource.datasources) == 1
    parent = datasource.datasources[0]
    # assert parent == expected_parent

    assert isinstance(parent, QueryDatasource)
    assert user_post_count in parent.output_concepts

    assert set(parent.source_map.keys()) == set(["local.post_id", "local.user_id"])

    root = parent.datasources[0].datasources[0]
    assert isinstance(root, Datasource)
    assert posts == root
    assert post_id in root.concepts

    ctes = datasource_to_ctes(datasource)

    final_cte = ctes[0]
    assert len(final_cte.parent_ctes) > 0

    # now validate
    select: Select = Select(selection=[avg_user_post_count])

    query = process_query(statement=select, environment=env, hooks=[])
    cte = query.base
    parent_cte = cte.parent_ctes[0]

    assert avg_user_post_count in cte.output_columns
    assert user_post_count in parent_cte.output_columns
    assert len(query.ctes) == 3
    assert len(cte.parent_ctes) > 0

    generator = SqlServerDialect()
    generator.compile_statement(query)
