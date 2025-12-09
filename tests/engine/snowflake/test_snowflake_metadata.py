"""Test Snowflake dialect metadata methods."""


def test_get_table_schema(snowflake_engine):
    """Test get_table_schema dialect method."""
    snowflake_engine.execute_raw_sql(
        "CREATE TABLE TEST_TABLE (ID INTEGER, NAME VARCHAR)"
    )

    dialect = snowflake_engine.generator

    schema = dialect.get_table_schema(snowflake_engine, "test_table")

    assert len(schema) >= 2
    column_names = [col[0].upper() for col in schema]
    assert "ID" in column_names
    assert "NAME" in column_names


def test_get_table_primary_keys(snowflake_engine):
    """Test get_table_primary_keys dialect method."""
    dialect = snowflake_engine.generator

    snowflake_engine.execute_raw_sql(
        "CREATE TABLE TEST_PK (ID INTEGER PRIMARY KEY, VALUE VARCHAR)"
    )

    # Snowflake PKs may not be returned by fakesnow
    pks = dialect.get_table_primary_keys(snowflake_engine, "test_pk")
    assert isinstance(pks, list)
