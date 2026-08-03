from trilogy import Dialects

MODEL = """
key id int;
property id.g string;
property id.v int;
datasource values (id, g, v)
grain (id)
query '''
select 1 id, 'a' g, 10 v
union all select 2, 'a', 1
union all select 3, 'b', 1
''';

auto filtered <- sum(v ? v > 5) by g;
"""


def test_filtered_aggregate_moves_to_input_when_consumer_rejects_null() -> None:
    engine = Dialects.DUCK_DB.default_executor()
    query = engine.generate_sql(MODEL + "where filtered > 0 select g, filtered;")[-1]

    assert "sum(CASE" not in query
    assert engine.execute_text(MODEL + "where filtered > 0 select g, filtered;")[
        -1
    ].fetchall() == [("a", 10)]


def test_filtered_aggregate_preserves_empty_groups_without_rejection() -> None:
    engine = Dialects.DUCK_DB.default_executor()
    text = MODEL + "select g, filtered order by g asc;"
    query = engine.generate_sql(text)[-1]

    assert "sum(CASE" in query
    assert engine.execute_text(text)[-1].fetchall() == [("a", 10), ("b", None)]


def test_filtered_aggregate_does_not_narrow_unfiltered_aggregate() -> None:
    engine = Dialects.DUCK_DB.default_executor()
    text = MODEL + "auto total <- sum(v) by g; where filtered > 0 select g, total;"

    assert engine.execute_text(text)[-1].fetchall() == [("a", 11)]


def test_filtered_aggregate_moves_when_global_rollup_ignores_nulls() -> None:
    engine = Dialects.DUCK_DB.default_executor()
    text = MODEL + "auto highest <- max(filtered) by *; select highest;"
    query = engine.generate_sql(text)[-1]

    assert "sum(CASE" not in query
    assert engine.execute_text(text)[-1].fetchall() == [(10,)]
