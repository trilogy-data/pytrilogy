from trilogy import Dialects
from trilogy.core.env_processor import concept_to_node, generate_graph
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators.node_merge_node import (
    determine_induced_minimal_nodes,
    gen_merge_node,
    reinject_common_join_keys_v2,
)


def test_demo_merge(normalized_engine, test_env: Environment):
    assert "passenger.last_name" in test_env.concepts
    normalized_engine.environment = test_env
    concepts = set(list(normalized_engine.environment.concepts.keys()))
    assert "passenger.last_name" in concepts
    assert "rich_info.last_name" in set([x for x in concepts if x.startswith("r")])

    test = """SELECT
passenger.last_name,
count(passenger.id) -> family_count
    MERGE
    SELECT
rich_info.last_name,
rich_info.net_worth_1918_dollars
    ALIGN join_last_name:passenger.last_name, rich_info.last_name
    WHERE 
rich_info.net_worth_1918_dollars is not null
and passenger.last_name is not null;

    """
    # raw = executor.generate_sql(test)
    results = normalized_engine.execute_text(test)[-1].fetchall()

    assert len(results) == 8


def test_demo_merge_rowset(normalized_engine, test_env: Environment):
    assert "passenger.last_name" in test_env.concepts
    normalized_engine.environment = test_env
    concepts = set(list(normalized_engine.environment.concepts.keys()))
    assert "passenger.last_name" in concepts
    assert "rich_info.last_name" in set([x for x in concepts if x.startswith("r")])

    test = """rowset test <-SELECT
passenger.last_name,
count(passenger.id) -> family_count
    MERGE
    SELECT
rich_info.last_name,
rich_info.net_worth_1918_dollars
    ALIGN join_last_name:passenger.last_name, rich_info.last_name
    WHERE 
rich_info.net_worth_1918_dollars is not null
and passenger.last_name is not null;

SELECT
    test.join_last_name,
    test.family_count
;

    """
    # raw = executor.generate_sql(test)
    results = normalized_engine.execute_text(test)[-1].fetchall()
    assert len(results) == 8


def test_merged_env_behavior(normalized_engine, test_env: Environment):
    assert "passenger.last_name" in test_env.concepts
    normalized_engine.environment = test_env
    history = History(base_environment=test_env)
    test_pre = """
merge rich_info.last_name into ~passenger.last_name;
    """
    normalized_engine.parse_text(test_pre)
    test_env = test_env.materialize_for_select()
    g = generate_graph(test_env)
    found = search_concepts(
        [
            test_env.concepts[c]
            for c in [
                "passenger.last_name",
                "rich_info.net_worth_1918_dollars",
                "rich_info.last_name",
            ]
        ],
        history=history,
        g=g,
        environment=test_env,
        depth=0,
    )

    assert found


def test_merge_concept_readdition():
    env = Environment()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    engine = Dialects.DUCK_DB.default_executor(environment=env)

    engine.parse_text(""" 
key x int;
key x2 int;

key y int;

datasource x (
    x)
grain (x)
query '''
select 1 as x union all select 2 as x union all select 3 as x;
''';


datasource x2 (
    x2,
    y
    )
grain (x2)
query '''
select * from (values (1, 10), (2, 20), (3, 30)) as t(x2, y);
''';

merge x2 into x;

""")
    test_env = env.materialize_for_select()
    g = generate_graph(test_env)
    target_concepts = [test_env.concepts[c] for c in ["x", "y"]]
    nodelist = [concept_to_node(c) for c in target_concepts]
    final = g.subgraph(nodelist).copy()
    reinjection = reinject_common_join_keys_v2(g, final, {})
    assert not reinjection, "We should not inject synonyms"


def test_demo_merge_rowset_with_condition(normalized_engine, test_env: Environment):
    assert "passenger.last_name" in test_env.concepts
    normalized_engine.environment = test_env
    concepts = set(list(normalized_engine.environment.concepts.keys()))
    assert normalized_engine.environment.namespace == "local"
    assert "passenger.last_name" in concepts

    assert "rich_info.last_name" in set([x for x in concepts if x.startswith("r")])
    assert "rich_info.net_worth_1918_dollars" in set(
        [x for x in concepts if x.startswith("r")]
    )

    test_pre = """merge rich_info.last_name into ~passenger.last_name;"""
    normalized_engine.parse_text(test_pre)
    # raw = executor.generate_sql(test)
    history = History(base_environment=test_env)
    test_env = test_env.materialize_for_select()
    g = generate_graph(test_env)
    # from trilogy.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    target_select_concepts = [
        test_env.concepts[c]
        for c in ["passenger.last_name", "rich_info.net_worth_1918_dollars"]
    ]
    build_env = normalized_engine.environment.materialize_for_select()
    path = determine_induced_minimal_nodes(
        g,
        nodelist=[concept_to_node(x) for x in target_select_concepts],
        accept_partial=False,
        filter_downstream=False,
        environment=build_env,
    )

    assert path

    found = search_concepts(
        [
            test_env.concepts[c]
            for c in [
                "passenger.last_name",
                "rich_info.net_worth_1918_dollars",
                "rich_info.full_name",
            ]
        ],
        history=history,
        g=g,
        environment=test_env,
        depth=0,
    )

    assert found

    mn = gen_merge_node(
        target_select_concepts,
        history=history,
        environment=test_env,
        g=g,
        depth=0,
        source_concepts=search_concepts,
        accept_partial=False,
        search_conditions=None,
    )
    assert mn


def test_demo_merge_rowset_e2e(normalized_engine, test_env: Environment):
    # assert test_env.concept_links[test_env.concepts["passenger.last_name"]][0] == test_env.concepts["rich_info.last_name"]
    from logging import DEBUG

    from trilogy.constants import logger

    logger.setLevel(DEBUG)
    normalized_engine.environment = test_env
    test = """    
merge rich_info.last_name into ~passenger.last_name;
SELECT
    passenger.last_name,
    rich_info.net_worth_1918_dollars,
WHERE
    rich_info.net_worth_1918_dollars is not null
    and passenger.last_name is not null
ORDER BY 
    passenger.last_name desc ;"""
    results = normalized_engine.execute_text(test)[-1].fetchall()

    assert len(results) == 8


def test_cast_merge(normalized_engine, test_env: Environment):

    normalized_engine.environment = test_env
    # avg(rich_info.net_worth_1918_dollars_float) as average_cabin_net_worth;
    test = """

merge rich_info.last_name into ~passenger.last_name;

select count(passenger.id ? rich_info.net_worth_1918_dollars_float is not null) as rich_people;
"""
    normalized_engine.execute_query(test)
