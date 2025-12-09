"""Test BigQuery dialect metadata methods."""


def test_get_table_schema(bigquery_executor):
    """Test get_table_schema dialect method using public dataset."""
    dialect = bigquery_executor.generator

    # Use a public dataset table
    schema = dialect.get_table_schema(
        bigquery_executor, "usa_1910_current", schema="bigquery-public-data.usa_names"
    )

    assert len(schema) > 0
    column_names = [col[0] for col in schema]
    assert "state" in column_names or "name" in column_names


def test_get_table_primary_keys(bigquery_executor):
    """Test get_table_primary_keys dialect method (should return empty)."""
    dialect = bigquery_executor.generator

    # BigQuery doesn't enforce PKs
    pks = dialect.get_table_primary_keys(
        bigquery_executor, "usa_1910_current", schema="bigquery-public-data.usa_names"
    )

    assert pks == []
