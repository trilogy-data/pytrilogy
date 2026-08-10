# from trilogy.compiler import compile


# from trilogy.compiler import compile
import re

from trilogy.core.models.build import Factory
from trilogy.core.models.environment import (
    Environment,
)
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.sql_server import SqlServerDialect


def test_aggregate_of_property_function(stackoverflow_environment: Environment) -> None:
    env: Environment = stackoverflow_environment

    avg_user_post_count = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: SelectStatement = SelectStatement(selection=[avg_user_post_count, user_id])

    factory = Factory(environment=env)

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    # raise SyntaxError(generator.compile_statement(query))
    for cte in query.ctes:
        found = False
        if avg_user_post_count.address in [z.address for z in cte.output_columns]:
            rendered = generator.render_concept_sql(
                factory.build(avg_user_post_count), cte
            )
            assert 'avg(length("posts"."text")) as "user_avg_post_length"' in rendered
            found = True
        if found:
            break
    assert found
    generator.compile_statement(query)


def test_aggregate_to_grain(stackoverflow_environment: Environment):
    env = stackoverflow_environment
    build_env = env.materialize_for_select()
    avg_post_length = env.concepts["user_avg_post_length"]
    user_id = env.concepts["user_id"]
    select: SelectStatement = SelectStatement(selection=[avg_post_length, user_id])

    query = process_query(statement=select, environment=env)
    generator = SqlServerDialect()
    build_avg_post_length = build_env.concepts["user_avg_post_length"]
    for cte in query.ctes:
        found = False
        if build_avg_post_length in cte.output_columns:
            rendered = generator.render_concept_sql(build_avg_post_length, cte)

            assert re.search(
                r'avg\(length\("posts"."text"\)\) as "user_avg_post_length"',
                rendered,
            ), generator.compile_statement(query)
            found = True
        if found:
            break
    assert found
