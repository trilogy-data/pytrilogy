from preql.core.models import Environment
from preql.core.processing.node_generators.merge_node import (
    gen_merge_node,
    identify_ds_join_paths,
)
from preql.core.processing.concept_strategies_v3 import search_concepts
from preql.core.env_processor import generate_graph


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

select 
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
    test_pre = """
merge passenger.last_name, rich_info.last_name;
    """
    normalized_engine.parse_text(test_pre)
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
        g=g,
        environment=test_env,
        depth=0,
    )

    assert found


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

    test_pre = """merge passenger.last_name, rich_info.last_name;"""
    normalized_engine.parse_text(test_pre)
    # raw = executor.generate_sql(test)
    g = generate_graph(test_env)
    # from preql.hooks.graph_hook import GraphHook
    # GraphHook().query_graph_built(g)
    target_select_concepts = [
        test_env.concepts[c]
        for c in ["passenger.last_name", "rich_info.net_worth_1918_dollars"]
    ]

    path = identify_ds_join_paths(
        target_select_concepts,
        g,
        test_env.datasources["rich_info.rich_info"],
        accept_partial=False,
        fail=True,
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
        g=g,
        environment=test_env,
        depth=0,
    )

    assert found

    mn = gen_merge_node(
        target_select_concepts,
        environment=test_env,
        g=g,
        depth=0,
        source_concepts=search_concepts,
        accept_partial=False,
    )
    assert mn


def test_demo_merge_rowset_e2e(normalized_engine, test_env: Environment):
    # assert test_env.concept_links[test_env.concepts["passenger.last_name"]][0] == test_env.concepts["rich_info.last_name"]
    normalized_engine.environment = test_env
    test = """    
merge passenger.last_name, rich_info.last_name;
SELECT
    passenger.last_name,
    rich_info.net_worth_1918_dollars,
WHERE
    rich_info.net_worth_1918_dollars is not null
    and passenger.last_name is not null
ORDER BY 
    passenger.last_name desc ;"""
    parsed = normalized_engine.parse_text(test)[-1]
    assert parsed.output_columns[0].address == "passenger.last_name"
    assert parsed.output_columns[1].address == "rich_info.net_worth_1918_dollars"

    results = normalized_engine.execute_text(test)[-1].fetchall()

    assert len(results) == 8
