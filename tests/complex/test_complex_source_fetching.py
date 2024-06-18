# from preql.compiler import compile


# from preql.compiler import compile
from preql.core.models import (
    SelectStatement,
    Grain,
    Datasource,
    QueryDatasource,
    Environment,
)
from preql.core.processing.concept_strategies_v3 import search_concepts, generate_graph
from preql.core.query_processor import process_query, datasource_to_ctes
from preql.dialect.sql_server import SqlServerDialect
from preql.core.enums import Purpose
import re


def test_aggregate_of_property_function(stackoverflow_environment: Environment) -> None:
    env: Environment = stackoverflow_environment
    avg_user_post_count = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: SelectStatement = SelectStatement(selection=[avg_user_post_count, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    # raise SyntaxError(generator.compile_statement(query))
    for cte in query.ctes:
        found = False
        if avg_user_post_count.address in [z.address for z in cte.output_columns]:
            rendered = generator.render_concept_sql(avg_user_post_count, cte)
            '"post_length") as "user_avg_post_length"' in rendered
            found = True
        if found:
            break
    assert found
    generator.compile_statement(query)


def test_aggregate_to_grain(stackoverflow_environment: Environment):
    env = stackoverflow_environment
    avg_post_length = env.concepts["avg_post_length_by_post_id"]
    user_id = env.concepts["user_id"]
    select: SelectStatement = SelectStatement(selection=[avg_post_length, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    for cte in query.ctes:
        found = False
        if avg_post_length in cte.output_columns:
            rendered = generator.render_concept_sql(avg_post_length, cte)

            assert re.search(
                r'avg\([0-9A-z\_]+\."post_length"\) as "avg_post_length_by_post_id"',
                rendered,
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

    assert user_post_count.purpose == Purpose.METRIC

    posts = env.datasources["posts"]
    post_grain = Grain(components=[env.concepts["post_id"]])

    assert posts.grain == post_grain

    expected_parent = search_concepts(
        [user_post_count, env.concepts["user_id"]],
        environment=env,
        depth=0,
        g=generate_graph(env),
    ).resolve()

    assert posts.grain == post_grain

    assert set(expected_parent.source_map.keys()) == set(
        ["local.user_post_count", "local.user_id", "local.post_id"]
    )

    assert user_post_count in expected_parent.output_concepts

    datasource = search_concepts(
        [avg_user_post_count], environment=env, depth=0, g=generate_graph(env)
    ).resolve()

    assert isinstance(datasource, QueryDatasource)
    assert datasource.grain == Grain()
    # ensure we identify aggregates of aggregates properly
    assert datasource.output_concepts[0] == avg_user_post_count
    assert len(datasource.datasources) == 1
    parent = datasource.datasources[0]
    # assert parent == expected_parent

    assert isinstance(parent, QueryDatasource)
    assert user_post_count in parent.output_concepts

    assert set([x.address for x in parent.output_concepts]) == set(
        ["local.user_post_count", "local.user_id"]
    )

    root = parent.datasources[0].datasources[0]
    assert isinstance(root, Datasource)
    assert posts == root
    assert post_id in root.concepts

    ctes = datasource_to_ctes(datasource, {})

    final_cte = ctes[0]
    assert len(final_cte.parent_ctes) > 0

    # now validate
    select: SelectStatement = SelectStatement(selection=[avg_user_post_count])

    query = process_query(statement=select, environment=env, hooks=[])
    cte = query.base
    parent_cte = cte.parent_ctes[0]

    assert avg_user_post_count in cte.output_columns
    assert user_post_count in parent_cte.output_columns
    assert len(query.ctes) == 3
    assert len(cte.parent_ctes) > 0

    generator = SqlServerDialect()
    generator.compile_statement(query)
