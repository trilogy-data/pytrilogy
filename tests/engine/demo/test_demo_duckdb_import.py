from preql.core.models import Environment


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
    test = """

merge passenger.last_name, rich_info.last_name;
    
SELECT
    passenger.last_name,
    rich_info.net_worth_1918_dollars,
WHERE
    rich_info.net_worth_1918_dollars is not null
    and passenger.last_name is not null;

    """
    # raw = executor.generate_sql(test)
    parsed = normalized_engine.parse_text(test)[-1]
    assert parsed.output_concepts[0].address == "passenger.last_name"
    assert parsed.output_concepts[1].address == "rich_info.net_worth_1918_dollars"
    results = normalized_engine.execute_text(test)[-1].fetchall()

    assert len(results) == 8
