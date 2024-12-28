# from trilogy.compiler import compile


# from trilogy.compiler import compile
import re

from trilogy.core.enums import Purpose
from trilogy.core.execute_models import (
    Datasource,
    BoundEnvironment,
    Grain,
    QueryDatasource,
)
from trilogy.core.author_models import Environment
from trilogy.core.author_models import SelectStatement
from trilogy.core.processing.concept_strategies_v3 import (
    generate_graph,
    search_concepts,
)
from trilogy.core.query_processor import datasource_to_cte, process_query
from trilogy.dialect.sql_server import SqlServerDialect


def test_aggregate_of_property_function(stackoverflow_environment: Environment) -> None:
    env: Environment = stackoverflow_environment
    avg_user_post_count = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: SelectStatement = SelectStatement(selection=[avg_user_post_count, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    for cte in query.ctes:
        found = False
        if avg_user_post_count.address in [z.address for z in cte.output_columns]:
            rendered = generator.render_concept_sql(avg_user_post_count.instantiate(env), cte)
            '"post_length") as "user_avg_post_length"' in rendered
            found = True
        if found:
            break
    assert found
    generator.compile_statement(query)


def test_aggregate_to_grain(stackoverflow_environment: Environment):
    env = stackoverflow_environment
    avg_post_length = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: SelectStatement = SelectStatement(selection=[avg_post_length, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    for cte in query.ctes:
        found = False
        if avg_post_length.address in cte.output_columns:
            rendered = generator.render_concept_sql(avg_post_length.instantiate(env), cte)

            assert re.search(
                r'avg\([0-9A-z\_]+\."post_length"\) as "user_avg_post_length"',
                rendered,
            ), generator.compile_statement(query)
            found = True
        if found:
            break
    assert found


def test_aggregate_of_aggregate(stackoverflow_environment:Environment):
    env = stackoverflow_environment.instantiate()
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

    ctes = datasource_to_cte(datasource, {})

    final_cte = ctes
    assert len(final_cte.parent_ctes) > 0

    # now validate
    select: SelectStatement = SelectStatement(selection=[avg_user_post_count])

    query = process_query(statement=select, environment=env, hooks=[])
    cte = query.base
    parent_cte = cte.parent_ctes[0]

    assert avg_user_post_count in cte.output_columns
    assert user_post_count in parent_cte.output_columns
    assert len(query.ctes) == 2
    assert len(cte.parent_ctes) > 0

    generator = SqlServerDialect()
    generator.compile_statement(query)
