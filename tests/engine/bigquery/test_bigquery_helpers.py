"""Test BigQuery dialect helper functions."""

from trilogy.dialect.bigquery import parse_bigquery_table_name


def test_parse_bigquery_table_name_simple():
    """Test parsing simple table name without dots."""
    table_name, schema = parse_bigquery_table_name("mytable")
    assert table_name == "mytable"
    assert schema is None


def test_parse_bigquery_table_name_with_schema():
    """Test parsing table name with explicit schema provided."""
    table_name, schema = parse_bigquery_table_name("mytable", schema="myschema")
    assert table_name == "mytable"
    assert schema == "myschema"


def test_parse_bigquery_table_name_dataset_table():
    """Test parsing dataset.table format."""
    table_name, schema = parse_bigquery_table_name("mydataset.mytable")
    assert table_name == "mytable"
    assert schema == "mydataset"


def test_parse_bigquery_table_name_project_dataset_table():
    """Test parsing project.dataset.table format."""
    table_name, schema = parse_bigquery_table_name("myproject.mydataset.mytable")
    assert table_name == "mytable"
    assert schema == "myproject.mydataset"


def test_parse_bigquery_table_name_with_explicit_schema_overrides():
    """Test that explicit schema parameter takes precedence."""
    table_name, schema = parse_bigquery_table_name(
        "mydataset.mytable", schema="explicit_schema"
    )
    assert table_name == "mydataset.mytable"
    assert schema == "explicit_schema"


def test_parse_bigquery_table_name_public_dataset():
    """Test parsing real BigQuery public dataset format."""
    table_name, schema = parse_bigquery_table_name(
        "bigquery-public-data.usa_names.usa_1910_current"
    )
    assert table_name == "usa_1910_current"
    assert schema == "bigquery-public-data.usa_names"
