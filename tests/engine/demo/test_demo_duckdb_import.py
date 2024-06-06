from preql.core.models import Environment



def test_demo_merge(normalized_engine, test_env:Environment):


    assert 'passenger.last_name' in test_env.concepts
    normalized_engine.environment=test_env
    concepts = set(list(normalized_engine.environment.concepts.keys()))
    assert 'passenger.last_name' in concepts
    assert 'rich_info.last_name' in set([x for x in concepts if x.startswith('r')])

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
