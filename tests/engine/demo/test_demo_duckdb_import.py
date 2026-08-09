from trilogy.core.models.environment import Environment


def test_demo_merge(normalized_engine, test_env: Environment):
    assert "passenger.last_name" in test_env.concepts
    normalized_engine.environment = test_env
    concepts = set(normalized_engine.environment.concepts.keys())
    assert "passenger.last_name" in concepts
    assert "rich_info.last_name" in {x for x in concepts if x.startswith("r")}

    test = """SELECT
passenger.last_name,
count(passenger.id) -> family_count
    MERGE
    SELECT
rich_info.last_name,
rich_info.net_worth_1918_dollars
    ALIGN join_last_name:passenger.last_name, rich_info.last_name
    HAVING
rich_info.net_worth_1918_dollars is not null
and passenger.last_name is not null;

    """
    # raw = executor.generate_sql(test)
    results = normalized_engine.execute_text(test)[-1].fetchall()

    assert len(results) == 8


def test_demo_merge_rowset(normalized_engine, test_env: Environment):
    assert "passenger.last_name" in test_env.concepts
    normalized_engine.environment = test_env
    concepts = set(normalized_engine.environment.concepts.keys())
    assert "passenger.last_name" in concepts
    assert "rich_info.last_name" in {x for x in concepts if x.startswith("r")}

    test = """rowset test <-SELECT
passenger.last_name,
count(passenger.id) -> family_count
    MERGE
    SELECT
rich_info.last_name,
rich_info.net_worth_1918_dollars
    ALIGN join_last_name:passenger.last_name, rich_info.last_name
    HAVING
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
