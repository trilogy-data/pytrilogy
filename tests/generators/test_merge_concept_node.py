from trilogy.core.processing.node_generators import gen_concept_merge_node
from trilogy.core.processing.concept_strategies_v3 import search_concepts

from trilogy.core.models import Environment
from trilogy import parse
from trilogy.core.env_processor import generate_graph
from trilogy.core.models import MergeStatement
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
    env1.parse("""merge one, env2.one;""")
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

        assert "on num1.`one` =" in compiled
