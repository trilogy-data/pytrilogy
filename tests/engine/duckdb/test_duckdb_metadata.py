def test_get_table_schema(duckdb_engine):
    """Test get_table_schema dialect method."""
    dialect = duckdb_engine.generator

    schema = dialect.get_table_schema(duckdb_engine, "items")

    assert len(schema) == 4
    column_names = [col[0] for col in schema]
    assert "item" in column_names
    assert "value" in column_names
    assert "count" in column_names
    assert "store_id" in column_names


def test_get_table_primary_keys(duckdb_engine):
    """Test get_table_primary_keys dialect method."""
    dialect = duckdb_engine.generator

    # items table has no PK defined
    pks = dialect.get_table_primary_keys(duckdb_engine, "items")

    assert pks == []
