from preql.core.processing.node_generators import gen_concept_merge_node
from preql.core.processing.concept_strategies_v3 import search_concepts

from preql.core.models import Environment
from preql import parse
from preql.core.env_processor import generate_graph
from preql.core.models import MergeStatement
from preql.dialect.base import BaseDialect


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
    # assert c.with_default_grain().grain.components == [c,]
    merge_concept = env1.concepts["__merge_one_env2_one"]
    assert isinstance(merge_concept.lineage, MergeStatement)
    gnode = gen_concept_merge_node(
        concept=merge_concept,
        local_optional=[env1.concepts["one"], env1.concepts["env2.one"]],
        environment=env1,
        g=generate_graph(env1),
        depth=0,
        source_concepts=search_concepts,
    )
    assert len(gnode.parents) == 2
    assert len(gnode.node_joins) == 1
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
        for cte in query.ctes:
            if (
                len(cte.output_columns) == 2
                and "local.__merge_one_env2_one" in cte.source_map
            ):
                assert cte.source_map["local.__merge_one_env2_one"] == ""

        assert "env2_num1.`one` as `__merge_one_env2_one`" in compiled
