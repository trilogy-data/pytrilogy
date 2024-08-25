
from trilogy.core.models import Environment
from trilogy import parse
from trilogy.dialect.base import BaseDialect


def test_merge_concepts():
    env1 = Environment()
    env2 = Environment()
    for env in [env1, env2]:
        parse(
            """
    key one int;
        
    datasource num1 (
        one:one
    ) 
    grain (one)
    address num1;
            """,
            env,
        )
    env1.add_import("env2", env2)
    env1.parse("""merge one into env2.one;""")
    # c = test_environment.concepts['one']

    bd = BaseDialect()
    _, queries = env1.parse(
        """
               select
               one,
               env2.one;"""
    )
    queries = bd.generate_queries(environment=env1, statements=queries)
    for query in queries:
        compiled = bd.compile_statement(query)
        assert " on " not in compiled
