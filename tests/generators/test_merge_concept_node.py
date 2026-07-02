from trilogy import parse
from trilogy.core.enums import JoinType
from trilogy.core.models.environment import Environment
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
    _, queries = env1.parse("""
               select
               one+1 as one_inc,
               env2.one;""")
    queries = bd.generate_queries(environment=env1, statements=queries)
    for query in queries:
        compiled = bd.compile_statement(query)
        assert " on " not in compiled


def query_to_lines(query):
    return [x.strip() for x in query.strip().split("\n") if x.strip()]


def test_merge_concept_remapping():
    env1 = Environment()
    env2 = Environment()
    parse(
        """
key one int;
property one.name string;
    
datasource num1 (
    one:one,
    name:name
) 
grain (one)
address num1;
        """,
        env1,
    )
    parse(
        """
key one int;
property one.name string;
             
datasource num1 (
    one:one,
    name:name
) 
grain (one)
address num1;
""",
        env2,
    )
    env1.add_import("env2", env2)
    env1.parse("""merge one into env2.one;""")
    # merge no longer rewrites the author env in place; it records a join pair
    # (a non-partial merge is a FULL identity join)
    assert ("local.one", "env2.one", JoinType.FULL) in env1.merges
    assert env1.concepts["name"].keys == {"local.one"}
    assert env1.datasources["num1"].grain.components == {"local.one"}
    assert env1.alias_origin_lookup == {}

    bd = BaseDialect()
    _, queries = env1.parse("""
               select
               one,
               one as two;""")
    queries = bd.generate_queries(environment=env1, statements=queries)
    for query in queries:
        compiled = bd.compile_statement(query)
        assert query_to_lines(compiled) == query_to_lines("""SELECT
             	`env2_num1`.`one` as `one`,
             	`env2_num1`.`one` as `two`
             FROM
             	`num1` as `env2_num1`"""), compiled
