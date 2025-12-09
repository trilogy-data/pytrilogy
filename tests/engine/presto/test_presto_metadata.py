"""Test Presto/Trino dialect metadata methods."""

import pytest


@pytest.mark.skip(reason="Presto tests require live database")
def test_get_table_schema():
    """Placeholder for Presto get_table_schema test."""
    pass


@pytest.mark.skip(reason="Presto tests require live database")
def test_get_table_primary_keys():
    """Test that Presto always returns empty list for PKs."""
    pass
