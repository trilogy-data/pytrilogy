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
    # assert key_candidate.model_dump() == env1.concepts["env2.one"].model_dump()
    assert env1.concepts["name"].keys == (env1.concepts["env2.one"],), [
        x.address for x in env1.concepts["name"].keys
    ]

    assert env1.datasources["num1"].grain.components == [
        env1.concepts["env2.one"],
    ], [x.address for x in env1.datasources["num1"].grain.components]

    assert env1.concepts["env2.one"].address in [
        y.address for y in env1.datasources["num1"].output_concepts
    ]

    assert (
        len({x.alias: x.concept.address for x in env1.datasources["num1"].columns}) == 2
    )

    assert str(env1.concepts["name"].with_grain(env1.concepts["env2.one"])) in [
        str(x) for x in env1.datasources["num1"].concepts
    ]
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
        assert query_to_lines(compiled) == query_to_lines(
            """SELECT
             	env2_num1.`one` as `env2_one`,
             	env2_num1.`one` as `env2_one`
             FROM
             	num1 as env2_num1"""
        )
