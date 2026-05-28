from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy import Dialects
from trilogy.authoring import DataType
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.models.core import EnumType, TraitDataType
from trilogy.dialect.base import BaseDialect
from trilogy.scripts.ingest import (
    _check_column_combination_uniqueness,
    _process_column,
    canonicalize_names,
    detect_nullability_from_sample,
    detect_rich_type,
    detect_unique_key_combinations,
)
from trilogy.scripts.ingest_helpers.fk_inference import FKBinding
from trilogy.scripts.ingest_helpers.foreign_keys import parse_foreign_keys
from trilogy.scripts.ingest_helpers.formatting import (
    canonicolize_name,
    find_common_prefix,
)
from trilogy.scripts.ingest_helpers.typing import (
    MAX_ENUM_VALUE_LENGTH,
    _enum_from_values,
    detect_enum_types,
)
from trilogy.scripts.trilogy import cli

_DIALECT = BaseDialect()


def test_ingest():
    path = Path(__file__).parent
    runner = CliRunner()
    config_dir = path / "config_directory"
    args = [
        "ingest",
        "world_capitals",
        "--config",
        str(config_dir / "trilogy.toml"),
        "--env",
        "test=fun",
    ]
    results = runner.invoke(
        cli,
        args,
    )
    if results.exception:
        raise results.exception
    assert results.exit_code == 0

    # Check that the file was created in the raw directory
    raw_dir = config_dir / "raw"
    assert raw_dir.exists()

    output_file = raw_dir / "world_capitals.preql"
    assert output_file.exists()

    # Read and verify the content has the expected structure
    content = output_file.read_text()
    assert "country" in content
    assert "capital" in content
    # The grain detection should identify country as a key
    assert "key country" in content.lower() or "country: country" in content.lower()


def test_ingest_with_db_primary_key():
    """Test that ingesting a table with a database-defined primary key uses it as the grain.

    This test verifies that the get_table_primary_keys method correctly detects primary keys
    from the database schema and uses them as the grain for the ingested table.
    """
    path = Path(__file__).parent
    runner = CliRunner()
    config_dir = path / "config_directory"
    args = [
        "ingest",
        "users_with_pk",
        "--config",
        str(config_dir / "trilogy.toml"),
    ]
    results = runner.invoke(
        cli,
        args,
    )
    if results.exception:
        raise results.exception
    assert results.exit_code == 0

    # Check that the file was created in the raw directory
    raw_dir = config_dir / "raw"
    assert raw_dir.exists()

    output_file = raw_dir / "users_with_pk.preql"
    assert output_file.exists()

    # Read and verify the content has the expected structure
    content = output_file.read_text()
    assert "user_id" in content
    assert "username" in content
    assert "email" in content

    # The grain detection should identify user_id as the key based on the primary key
    assert "key user_id" in content.lower()

    # MUST verify that database primary key detection worked
    # This is the critical assertion - we should not fall back to sample data detection
    assert "Using primary key from database as grain" in results.output, (
        "Primary key detection failed! Expected 'Using database primary keys as grain' "
        f"in output, but got: {results.output}"
    )


def test_ingest_heap_table_no_primary_keys():
    """Test ingesting a heap table with no primary keys or unique columns.

    This test verifies that tables with duplicates (heap tables) are handled correctly
    by creating a datasource with no grain.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text("""CREATE TABLE heap_table (
                event_type VARCHAR,
                event_value INTEGER,
                timestamp TIMESTAMP
            );
            INSERT INTO heap_table VALUES ('click', 1, '2024-01-01 10:00:00');
            INSERT INTO heap_table VALUES ('click', 1, '2024-01-01 10:00:00');
            INSERT INTO heap_table VALUES ('view', 2, '2024-01-01 10:00:00');
            INSERT INTO heap_table VALUES ('view', 2, '2024-01-01 10:00:00');""")

        config_content = f"""[engine]
dialect = "duckdb"

[setup]
sql = ["{setup_sql_file.as_posix()}"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "heap_table",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
            ],
        )

        print(result.output)
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        output_file = tmppath / "raw" / "heap_table.preql"
        assert output_file.exists()

        content = output_file.read_text()
        assert "event_type" in content
        assert "event_value" in content
        assert "timestamp" in content

        # Verify no grain was detected
        assert "No primary key or unique grain" in result.output

        # The datasource should have no grain components
        # All columns should be marked as keys (since there's no grain)
        assert "key event_type" in content.lower()
        assert "key event_value" in content.lower()
        assert "key timestamp" in content.lower()


def test_ingest_with_cli_dialect_override():
    """Test ingest with dialect specified in CLI, overriding config."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text("""CREATE TABLE test_table (id INTEGER, name VARCHAR);
INSERT INTO test_table VALUES (1, 'test');""")

        config_content = f"""[engine]
dialect = "bigquery"

[setup]
sql = ["{setup_sql_file.as_posix()}"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "test_table",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
            ],
        )

        print(result.output)
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        output_file = tmppath / "raw" / "test_table.preql"
        assert output_file.exists()


def test_ingest_with_config_dialect_only():
    """Test ingest with dialect from config file only (no CLI dialect)."""
    path = Path(__file__).parent
    runner = CliRunner()
    config_dir = path / "config_directory"

    result = runner.invoke(
        cli,
        [
            "ingest",
            "world_capitals",
            "--config",
            str(config_dir / "trilogy.toml"),
        ],
    )

    print(result.output)
    if result.exception:
        raise result.exception
    assert result.exit_code == 0

    output_file = config_dir / "raw" / "world_capitals.preql"
    assert output_file.exists()


def test_ingest_no_dialect_specified():
    """Test that ingest fails when no dialect is specified."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        config_content = """[engine]

[setup]
sql = []
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "test_table",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
            ],
        )

        print(result.output)
        assert result.exit_code == 1
        assert "No dialect specified" in result.output


def test_ingest_with_schema():
    """Test ingest with schema parameter."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text("""CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_table (id INTEGER, name VARCHAR);
INSERT INTO test_schema.test_table VALUES (1, 'test');""")

        config_content = f"""[engine]
dialect = "duckdb"

[setup]
sql = ["{setup_sql_file.as_posix()}"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "test_table",
                "duckdb",
                "--schema",
                "test_schema",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
            ],
        )

        print(result.output)
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        output_file = tmppath / "raw" / "test_table.preql"
        assert output_file.exists()

        content = output_file.read_text()
        assert "test_schema.test_table" in content


def test_ingest_no_tables_specified():
    """Test that ingest fails when no tables are specified."""
    path = Path(__file__).parent
    runner = CliRunner()
    config_dir = path / "config_directory"

    result = runner.invoke(
        cli,
        [
            "ingest",
            "",
            "--config",
            str(config_dir / "trilogy.toml"),
        ],
    )

    print(result.output)
    assert result.exit_code == 1
    assert "No sources specified" in result.output


def test_ingest_with_debug_flag_success():
    """Test ingest with --debug flag on successful ingestion."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text("""CREATE TABLE debug_test (id INTEGER, name VARCHAR);
INSERT INTO debug_test VALUES (1, 'test');""")

        config_content = f"""[engine]
dialect = "duckdb"

[setup]
sql = ["{setup_sql_file.as_posix()}"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "debug_test",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
                "--debug",
            ],
        )

        print(result.output)
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        output_file = tmppath / "raw" / "debug_test.preql"
        assert output_file.exists()


def test_ingest_with_debug_flag_on_error():
    """Test that --debug flag causes traceback to be printed on error.

    Note: This test tries to ingest two tables where the first succeeds
    but the second has an issue that triggers an exception in the rendering phase.
    This ensures the traceback printing code path (ingest.py:590-593) is exercised.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text("""CREATE TABLE good_table (id INTEGER);
INSERT INTO good_table VALUES (1);""")

        config_content = f"""[engine]
dialect = "duckdb"

[setup]
sql = ["{setup_sql_file.as_posix()}"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "good_table,nonexistent_table",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
                "--debug",
            ],
        )

        print(result.output)
        # Should successfully ingest first table but fail on second
        assert "good_table" in result.output
        assert (
            "Failed to ingest nonexistent_table" in result.output
            or "No columns found" in result.output
        )


def test_ingest_without_debug_flag_on_error():
    """Test that without --debug flag, no traceback is printed on error."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text("""CREATE TABLE existing_table (id INTEGER);
INSERT INTO existing_table VALUES (1);""")

        config_content = f"""[engine]
dialect = "duckdb"

[setup]
sql = ["{setup_sql_file.as_posix()}"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "nonexistent_table",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
            ],
        )

        print(result.output)
        assert result.exit_code == 1
        assert "Failed to ingest nonexistent_table" in result.output
        assert "Traceback" not in result.output


class TestSnakeCaseNormalization:
    """Test snake_case conversion for concept names."""

    def test_already_snake_case(self):
        assert canonicolize_name("user_id") == "user_id"
        assert canonicolize_name("first_name") == "first_name"

    def test_camel_case(self):
        assert canonicolize_name("userId") == "user_id"
        assert canonicolize_name("firstName") == "first_name"
        assert canonicolize_name("customerFirstName") == "customer_first_name"

    def test_pascal_case(self):
        assert canonicolize_name("UserId") == "user_id"
        assert canonicolize_name("FirstName") == "first_name"
        assert canonicolize_name("CustomerFirstName") == "customer_first_name"

    def test_with_spaces(self):
        assert canonicolize_name("User ID") == "user_id"
        assert canonicolize_name("First Name") == "first_name"
        assert canonicolize_name("Customer First Name") == "customer_first_name"

    def test_with_special_chars(self):
        assert canonicolize_name("user-id") == "user_id"
        assert canonicolize_name("first.name") == "first_name"
        assert canonicolize_name("user@email") == "user_email"

    def test_mixed_formats(self):
        assert canonicolize_name("UserID-2023") == "user_id_2023"
        assert canonicolize_name("First Name (Primary)") == "first_name_primary"

    def test_all_caps(self):
        assert canonicolize_name("ID") == "id"
        assert canonicolize_name("URL") == "url"

    def test_numbers(self):
        assert canonicolize_name("address1") == "address1"
        assert canonicolize_name("line2") == "line2"


class TestRichTypeDetection:
    """Test rich type detection for specialized data types."""

    def test_latitude_detection(self):
        assert detect_rich_type("lat", DataType.FLOAT) == ("std.geography", "latitude")
        assert detect_rich_type("latitude", DataType.FLOAT) == (
            "std.geography",
            "latitude",
        )
        assert detect_rich_type("user_lat", DataType.FLOAT) == (
            "std.geography",
            "latitude",
        )
        assert detect_rich_type("lat_degrees", DataType.FLOAT) == (
            "std.geography",
            "latitude",
        )

    def test_longitude_detection(self):
        assert detect_rich_type("lon", DataType.FLOAT) == (
            "std.geography",
            "longitude",
        )
        assert detect_rich_type("lng", DataType.FLOAT) == (
            "std.geography",
            "longitude",
        )
        assert detect_rich_type("longitude", DataType.FLOAT) == (
            "std.geography",
            "longitude",
        )
        assert detect_rich_type("user_lng", DataType.FLOAT) == (
            "std.geography",
            "longitude",
        )

    def test_city_detection(self):
        assert detect_rich_type("city", DataType.STRING) == ("std.geography", "city")
        assert detect_rich_type("user_city", DataType.STRING) == (
            "std.geography",
            "city",
        )

    def test_country_detection(self):
        assert detect_rich_type("country", DataType.STRING) == (
            "std.geography",
            "country",
        )
        assert detect_rich_type("user_country", DataType.STRING) == (
            "std.geography",
            "country",
        )

    def test_country_code_detection(self):
        assert detect_rich_type("country_code", DataType.STRING) == (
            "std.geography",
            "country_code",
        )
        assert detect_rich_type("countrycode", DataType.STRING) == (
            "std.geography",
            "country_code",
        )

    def test_zip_code_detection(self):
        assert detect_rich_type("zip", DataType.STRING) == (
            "std.geography",
            "us_zip_code",
        )
        assert detect_rich_type("zipcode", DataType.STRING) == (
            "std.geography",
            "us_zip_code",
        )
        assert detect_rich_type("zip_code", DataType.STRING) == (
            "std.geography",
            "us_zip_code",
        )
        assert detect_rich_type("postal_code", DataType.STRING) == (
            "std.geography",
            "us_zip_code",
        )

    def test_email_detection(self):
        emails = ["a@x.com", "b@y.org"]
        assert detect_rich_type("email", DataType.STRING, emails) == (
            "std.net",
            "email_address",
        )
        assert detect_rich_type("email_address", DataType.STRING, emails) == (
            "std.net",
            "email_address",
        )
        assert detect_rich_type("user_email", DataType.STRING, emails) == (
            "std.net",
            "email_address",
        )

    def test_url_detection(self):
        urls = ["https://example.com", "http://x.org/p"]
        assert detect_rich_type("url", DataType.STRING, urls) == ("std.net", "url")
        assert detect_rich_type("website", DataType.STRING, urls) == ("std.net", "url")

    def test_ip_detection(self):
        ips = ["10.0.0.1", "192.168.1.255"]
        assert detect_rich_type("ip", DataType.STRING, ips) == (
            "std.net",
            "ipv4_address",
        )
        assert detect_rich_type("ipv4", DataType.STRING, ips) == (
            "std.net",
            "ipv4_address",
        )
        assert detect_rich_type("ip_address", DataType.STRING, ips) == (
            "std.net",
            "ipv4_address",
        )

    def test_value_gate_rejects_name_only_match(self):
        # Named like a rich type, but the values aren't — the gate rejects it.
        assert detect_rich_type("channel_email", DataType.STRING, ["N", "Y"]) == (
            None,
            None,
        )
        assert detect_rich_type("ip_address", DataType.STRING, ["unknown"]) == (
            None,
            None,
        )
        # A value-gated type also needs values to confirm against.
        assert detect_rich_type("email", DataType.STRING) == (None, None)
        assert detect_rich_type("email", DataType.STRING, []) == (None, None)

    def test_value_gate_does_not_affect_ungated_types(self):
        # Geography types have no value gate — name-based detection still works
        # with no sample values supplied.
        assert detect_rich_type("latitude", DataType.FLOAT) == (
            "std.geography",
            "latitude",
        )
        assert detect_rich_type("city", DataType.STRING) == ("std.geography", "city")

    def test_wrong_datatype(self):
        # Latitude should be FLOAT, not STRING
        assert detect_rich_type("latitude", DataType.STRING) == (None, None)
        # Email should be STRING, not INT
        assert detect_rich_type("email", DataType.INTEGER) == (None, None)

    def test_no_match(self):
        assert detect_rich_type("user_id", DataType.INTEGER) == (None, None)
        assert detect_rich_type("amount", DataType.FLOAT) == (None, None)
        assert detect_rich_type("description", DataType.STRING) == (None, None)


class TestCheckColumnCombinationUniqueness:
    """Test the helper function for checking column combination uniqueness."""

    def test_empty_sample_rows(self):
        """Empty sample should return False."""
        assert _check_column_combination_uniqueness([0], []) is False
        assert _check_column_combination_uniqueness([0, 1], []) is False

    def test_single_column_unique(self):
        """Single column with unique values should return True."""
        sample_rows = [(1, "a"), (2, "b"), (3, "c")]
        assert _check_column_combination_uniqueness([0], sample_rows) is True

    def test_single_column_not_unique(self):
        """Single column with duplicate values should return False."""
        sample_rows = [(1, "a"), (1, "b"), (3, "c")]
        assert _check_column_combination_uniqueness([0], sample_rows) is False

    def test_two_column_combination_unique(self):
        """Two columns that are unique together should return True."""
        sample_rows = [(1, "a", "x"), (1, "b", "y"), (2, "a", "z")]
        assert _check_column_combination_uniqueness([0, 1], sample_rows) is True

    def test_two_column_combination_not_unique(self):
        """Two columns that are not unique together should return False."""
        sample_rows = [(1, "a", "x"), (1, "a", "y"), (2, "b", "z")]
        assert _check_column_combination_uniqueness([0, 1], sample_rows) is False

    def test_three_column_combination_unique(self):
        """Three columns that are unique together should return True."""
        sample_rows = [
            (1, "a", "x", "extra"),
            (1, "a", "y", "extra"),
            (1, "b", "x", "extra"),
        ]
        assert _check_column_combination_uniqueness([0, 1, 2], sample_rows) is True

    def test_three_column_combination_not_unique(self):
        """Three columns that are not unique together should return False."""
        sample_rows = [
            (1, "a", "x", "extra"),
            (1, "a", "x", "different"),
            (1, "b", "x", "extra"),
        ]
        assert _check_column_combination_uniqueness([0, 1, 2], sample_rows) is False

    def test_non_contiguous_indices(self):
        """Non-contiguous column indices should work correctly."""
        sample_rows = [
            (1, "ignore", "a", "x"),
            (2, "ignore", "b", "x"),
            (3, "ignore", "c", "x"),
        ]
        # Columns 0 and 2 together are unique
        assert _check_column_combination_uniqueness([0, 2], sample_rows) is True
        # Column 3 alone is not unique
        assert _check_column_combination_uniqueness([3], sample_rows) is False

    def test_with_null_values(self):
        """Should handle None/null values correctly."""
        sample_rows = [(1, None), (2, None), (3, "a")]
        # First column is unique
        assert _check_column_combination_uniqueness([0], sample_rows) is True
        # Second column has duplicates (two None values)
        assert _check_column_combination_uniqueness([1], sample_rows) is False


class TestDetectUniqueKeyCombinations:
    """Test detection of unique key combinations from sample data."""

    def test_empty_inputs(self):
        """Empty inputs should return empty list."""
        assert detect_unique_key_combinations([], []) == []
        assert detect_unique_key_combinations(["a", "b"], []) == []
        assert detect_unique_key_combinations([], [(1, 2)]) == []

    def test_single_column_key(self):
        """Should detect single column unique key."""
        column_names = ["id", "name", "email"]
        sample_rows = [(1, "Alice", "a@test.com"), (2, "Bob", "b@test.com")]
        result = detect_unique_key_combinations(column_names, sample_rows)
        # ID is unique
        assert ["id"] in result

    def test_prefer_single_column_keys(self):
        """Should prefer single column keys over multi-column keys."""
        column_names = ["id", "name", "email"]
        sample_rows = [(1, "Alice", "a@test.com"), (2, "Bob", "b@test.com")]
        result = detect_unique_key_combinations(column_names, sample_rows)
        # Should find single column keys and stop there
        assert ["id"] in result
        # Should not return multi-column combinations if single column works
        assert all(len(combo) == 1 for combo in result)

    def test_two_column_composite_key(self):
        """Should detect two-column composite key when no single column is unique."""
        column_names = ["user_id", "order_id", "item"]
        sample_rows = [
            (1, 100, "A"),
            (1, 101, "A"),  # item "A" repeated, so not unique
            (2, 100, "B"),
        ]
        result = detect_unique_key_combinations(column_names, sample_rows)
        # user_id and order_id together are unique
        assert ["user_id", "order_id"] in result

    def test_three_column_composite_key(self):
        """Should detect three-column composite key."""
        column_names = ["a", "b", "c", "d"]
        sample_rows = [
            (1, 1, 1, "x"),
            (1, 1, 1, "y"),  # First three columns repeat
            (1, 1, 2, "x"),
            (1, 2, 1, "x"),
        ]
        result = detect_unique_key_combinations(
            column_names, sample_rows, max_key_size=3
        )
        # a, b, c together are NOT unique, but a, b, c with one more may be
        # Actually with this data, b, c, d should be unique
        assert len(result) > 0
        # Check that result contains a 3-column combination
        assert any(len(combo) == 3 for combo in result)

    def test_max_key_size_limit(self):
        """Should respect max_key_size parameter."""
        column_names = ["a", "b", "c", "d"]
        sample_rows = [
            (1, 1, 1, 1),
            (1, 1, 1, 2),
            (1, 1, 2, 1),
            (1, 2, 1, 1),
        ]
        # With max_key_size=2, should not find any unique key
        result = detect_unique_key_combinations(
            column_names, sample_rows, max_key_size=2
        )
        assert result == []

        # With max_key_size=3, should find a 3-column key
        result = detect_unique_key_combinations(
            column_names, sample_rows, max_key_size=3
        )
        assert len(result) > 0
        assert all(len(combo) == 3 for combo in result)

    def test_multiple_single_column_keys(self):
        """Should find all single column unique keys."""
        column_names = ["id", "email", "username"]
        sample_rows = [
            (1, "a@test.com", "alice"),
            (2, "b@test.com", "bob"),
        ]
        result = detect_unique_key_combinations(column_names, sample_rows)
        # All three columns are unique
        assert len(result) == 3
        assert ["id"] in result
        assert ["email"] in result
        assert ["username"] in result

    def test_no_unique_keys(self):
        """Should return empty list when no unique keys exist within max_key_size."""
        column_names = ["a", "b"]
        sample_rows = [
            (1, 1),
            (1, 1),
        ]
        result = detect_unique_key_combinations(column_names, sample_rows)
        assert result == []


class TestEnumFromValues:
    """Test the enum decision logic over a column's distinct values."""

    def test_constrained_string_values(self):
        result = _enum_from_values(DataType.STRING, ["open", "closed"], 10)
        assert isinstance(result, EnumType)
        assert result.type == DataType.STRING
        assert result.values == ["closed", "open"]

    def test_integer_values(self):
        result = _enum_from_values(DataType.INTEGER, [2, 0, 1], 10)
        assert isinstance(result, EnumType)
        assert result.values == [0, 1, 2]

    def test_too_many_values(self):
        vals = [f"v{i}" for i in range(11)]
        assert _enum_from_values(DataType.STRING, vals, 10) is None

    def test_empty_values(self):
        assert _enum_from_values(DataType.STRING, [], 10) is None

    def test_ineligible_base_type(self):
        assert _enum_from_values(DataType.FLOAT, [1.0, 2.0], 10) is None

    def test_long_text_value_rejected(self):
        # A single long free-text value disqualifies the whole column.
        vals = ["short", "x" * (MAX_ENUM_VALUE_LENGTH + 1)]
        assert _enum_from_values(DataType.STRING, vals, 10) is None

    def test_single_value_allowed(self):
        result = _enum_from_values(DataType.STRING, ["only"], 10)
        assert isinstance(result, EnumType)
        assert result.values == ["only"]

    def test_values_sorted_consistently(self):
        result = _enum_from_values(DataType.STRING, ["c", "a", "b"], 10)
        assert result is not None
        assert result.values == ["a", "b", "c"]

    def test_custom_cutoff(self):
        vals = [str(i) for i in range(5)]
        assert _enum_from_values(DataType.STRING, vals, 3) is None
        assert isinstance(_enum_from_values(DataType.STRING, vals, 5), EnumType)


class TestDetectEnumTypes:
    """Test SQL-based enum detection over a live source."""

    def test_detects_over_full_table(self):
        exc = Dialects.DUCK_DB.default_executor()
        exc.execute_raw_sql(
            "CREATE TABLE t AS SELECT i AS id, "
            "['open', 'closed'][i % 2 + 1] AS status FROM range(200) tbl(i)"
        )
        result = detect_enum_types(
            exc, "t", [("id", DataType.INTEGER), ("status", DataType.STRING)]
        )
        assert set(result) == {"status"}
        assert result["status"].values == ["closed", "open"]

    def test_full_table_distinct_not_head_sample(self):
        # Leading rows are uniform but the column is high-cardinality overall;
        # a head-sample heuristic would wrongly promote it to an enum.
        exc = Dialects.DUCK_DB.default_executor()
        exc.execute_raw_sql(
            "CREATE TABLE t AS SELECT "
            "CASE WHEN i < 150 THEN 0 ELSE i END AS v FROM range(300) tbl(i)"
        )
        assert detect_enum_types(exc, "t", [("v", DataType.INTEGER)]) == {}


class TestDetectNullabilityFromSample:
    """Test nullability detection from sample data."""

    def test_column_with_nulls(self):
        """Column with NULL values should be detected as nullable."""
        sample_rows = [(1, "a"), (2, None), (3, "c")]
        assert detect_nullability_from_sample(1, sample_rows) is True

    def test_column_without_nulls(self):
        """Column without NULL values should not be detected as nullable."""
        sample_rows = [(1, "a"), (2, "b"), (3, "c")]
        assert detect_nullability_from_sample(1, sample_rows) is False

    def test_first_column_null(self):
        """Should work for first column."""
        sample_rows = [(None, "a"), (2, "b"), (3, "c")]
        assert detect_nullability_from_sample(0, sample_rows) is True

    def test_last_column_null(self):
        """Should work for last column."""
        sample_rows = [("a", "b", None), ("x", "y", "z")]
        assert detect_nullability_from_sample(2, sample_rows) is True

    def test_all_nulls(self):
        """Column with all NULL values should be nullable."""
        sample_rows = [(1, None), (2, None), (3, None)]
        assert detect_nullability_from_sample(1, sample_rows) is True

    def test_empty_sample(self):
        """Empty sample should return False (not nullable)."""
        sample_rows = []
        assert detect_nullability_from_sample(0, sample_rows) is False

    def test_single_row_with_null(self):
        """Single row with NULL should be detected."""
        sample_rows = [(1, None)]
        assert detect_nullability_from_sample(1, sample_rows) is True

    def test_single_row_without_null(self):
        """Single row without NULL should not be nullable."""
        sample_rows = [(1, "a")]
        assert detect_nullability_from_sample(1, sample_rows) is False


def _make_concept_mapping(col_names: list[str]) -> dict[str, str]:
    """Helper to create concept mapping for tests."""
    return canonicalize_names(col_names)


class TestProcessColumn:
    """Test the _process_column helper function."""

    def test_basic_column_processing(self):
        """Test basic column processing with simple types."""
        col = ("user_id", "INTEGER", "NO", None)
        grain_components = ["user_id"]
        sample_rows = []
        concept_mapping = _make_concept_mapping(["user_id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Check concept
        assert concept.name == "user_id"
        assert concept.datatype == DataType.INTEGER
        assert concept.purpose == Purpose.KEY
        assert concept.modifiers == []
        # Metadata is auto-created but description should be None
        assert concept.metadata.description is None

        # Check column assignment
        assert column_assignment.alias == "user_id"
        assert column_assignment.modifiers == []

        # No rich type
        assert rich_import is None

    def test_property_column(self):
        """Test column that is not in grain (property)."""
        col = ("first_name", "VARCHAR(100)", "YES", None)
        grain_components = ["user_id"]
        sample_rows = []
        concept_mapping = _make_concept_mapping(["first_name"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should be a property, not a key
        assert concept.purpose == Purpose.PROPERTY
        assert concept.name == "first_name"

    def test_nullable_column_from_schema(self):
        """Test nullable column detection from schema."""
        col = ("email", "VARCHAR(255)", "YES", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["email"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should be nullable based on schema
        assert Modifier.NULLABLE in concept.modifiers
        assert Modifier.NULLABLE in column_assignment.modifiers

    def test_non_nullable_column_from_schema(self):
        """Test non-nullable column detection from schema."""
        col = ("id", "INTEGER", "NO", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should not be nullable
        assert Modifier.NULLABLE not in concept.modifiers
        assert Modifier.NULLABLE not in column_assignment.modifiers

    def test_nullable_from_sample_data(self):
        """Test nullable detection from sample data."""
        col = ("name", "VARCHAR(100)", "NO", None)
        grain_components = []
        sample_rows = [("Alice",), (None,), ("Bob",)]
        concept_mapping = _make_concept_mapping(["name"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should be nullable based on sample data, overriding schema
        assert Modifier.NULLABLE in concept.modifiers

    def test_non_nullable_from_sample_data(self):
        """Test non-nullable detection from sample data."""
        col = ("name", "VARCHAR(100)", "YES", None)
        grain_components = []
        sample_rows = [("Alice",), ("Bob",), ("Charlie",)]
        concept_mapping = _make_concept_mapping(["name"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should not be nullable based on sample data, overriding schema
        assert Modifier.NULLABLE not in concept.modifiers

    def test_column_with_comment(self):
        """Test column with a comment creates metadata."""
        col = ("user_id", "INTEGER", "NO", "Unique identifier for the user")
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["user_id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should have metadata with description
        assert concept.metadata is not None
        assert concept.metadata.description == "Unique identifier for the user"

    def test_column_with_empty_comment(self):
        """Test column with empty/whitespace comment doesn't create metadata description."""
        col = ("user_id", "INTEGER", "NO", "   ")
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["user_id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should not have description for whitespace comment
        assert concept.metadata.description is None

    def test_rich_type_detection_email(self):
        """Test rich type detection for email."""
        col = ("user_email", "VARCHAR(255)", "YES", None)
        grain_components = []
        sample_rows = [("a@x.com",), ("b@y.org",)]
        concept_mapping = _make_concept_mapping(["user_email"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should detect email rich type
        assert rich_import == "std.net"
        assert hasattr(concept.datatype, "traits")
        assert "email_address" in concept.datatype.traits

    def test_rich_type_detection_latitude(self):
        """Test rich type detection for latitude."""
        col = ("location_lat", "FLOAT", "YES", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["location_lat"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should detect latitude rich type
        assert rich_import == "std.geography"
        assert hasattr(concept.datatype, "traits")
        assert "latitude" in concept.datatype.traits

    def test_snake_case_normalization(self):
        """Test that column names are normalized to snake_case."""
        col = ("UserFirstName", "VARCHAR(100)", "YES", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["UserFirstName"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Concept name should be snake_case
        assert concept.name == "user_first_name"
        # Column assignment alias should preserve original
        assert column_assignment.alias == "UserFirstName"

    def test_column_alias_preserves_original_name(self):
        """Test that column assignment alias preserves original column name."""
        col = ("User-ID", "INTEGER", "NO", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["User-ID"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Concept name is normalized
        assert concept.name == "user_id"
        # Alias preserves original
        assert column_assignment.alias == "User-ID"

    def test_minimal_column_tuple(self):
        """Test processing column with minimal information (no nullable/comment)."""
        col = ("id", "INT")
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Should default to nullable when not specified
        assert Modifier.NULLABLE in concept.modifiers
        assert concept.metadata.description is None

    def test_enum_type_applied(self):
        """A precomputed enum is applied as the column datatype."""
        col = ("status", "VARCHAR", "NO", None)
        concept_mapping = _make_concept_mapping(["status"])
        enum = EnumType(type=DataType.STRING, values=["closed", "open"])

        concept, _, rich_import = _process_column(
            0, col, [], [], concept_mapping, _DIALECT, enum
        )

        assert concept.datatype is enum
        assert rich_import is None

    def test_enum_combined_with_rich_type(self):
        """A column that is both enum-constrained and a rich type whose values
        confirm it gets a trait wrapping the enum, plus the trait's import."""
        col = ("user_email", "VARCHAR", "NO", None)
        concept_mapping = _make_concept_mapping(["user_email"])
        enum = EnumType(type=DataType.STRING, values=["a@x.com", "b@y.com"])

        concept, _, rich_import = _process_column(
            0, col, [], [], concept_mapping, _DIALECT, enum
        )

        assert isinstance(concept.datatype, TraitDataType)
        assert isinstance(concept.datatype.type, EnumType)
        assert concept.datatype.type.values == ["a@x.com", "b@y.com"]
        assert "email_address" in concept.datatype.traits
        assert rich_import == "std.net"

    def test_enum_named_like_rich_type_but_values_dont_match(self):
        """A Y/N flag named 'channel_email' is an enum only — the value gate
        keeps it from being misclassified as an email address."""
        col = ("channel_email", "VARCHAR", "NO", None)
        concept_mapping = _make_concept_mapping(["channel_email"])
        enum = EnumType(type=DataType.STRING, values=["N", "Y"])

        concept, _, rich_import = _process_column(
            0, col, [], [], concept_mapping, _DIALECT, enum
        )

        assert concept.datatype is enum
        assert rich_import is None


class TestFindCommonPrefix:
    """Test finding common prefix in column names."""

    def test_no_common_prefix(self):
        """No common prefix should return empty string."""
        assert find_common_prefix(["user_id", "email", "name"]) == ""
        assert find_common_prefix(["foo", "bar", "baz"]) == ""

    def test_all_columns_same_prefix(self):
        """All columns with same prefix should return that prefix."""
        assert (
            find_common_prefix(["ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk"])
            == "ss_"
        )
        assert find_common_prefix(["user_id", "user_name", "user_email"]) == "user_"

    def test_partial_common_prefix(self):
        """Partial common prefix should be detected."""
        assert (
            find_common_prefix(
                ["store_sales_id", "store_sales_date", "store_sales_amount"]
            )
            == "store_sales_"
        )

    def test_empty_list(self):
        """Empty list should return empty string."""
        assert find_common_prefix([]) == ""

    def test_single_item(self):
        """Single item should return empty string."""
        assert find_common_prefix(["user_id"]) == ""

    def test_no_underscore_in_common_part(self):
        """Common part without underscore should not be considered a prefix."""
        assert find_common_prefix(["customer", "custom"]) == ""

    def test_prefix_too_short(self):
        """Prefix that would leave no content should not be used."""
        names = ["s_", "s_"]
        result = find_common_prefix(names)
        # This should return empty because stripping would leave nothing
        assert result == ""

    def test_case_insensitive(self):
        """Prefix detection should be case-insensitive."""
        assert (
            find_common_prefix(["SS_SOLD_DATE_SK", "ss_sold_time_sk", "SS_ITEM_SK"])
            == "ss_"
        )

    def test_multiple_underscores(self):
        """Should find the longest prefix ending with underscore."""
        assert (
            find_common_prefix(["tpc_ds_store_sales_id", "tpc_ds_store_sales_date"])
            == "tpc_ds_store_sales_"
        )


class TestStripCommonPrefix:
    """Test stripping common prefix from column names."""

    def test_strip_simple_prefix(self):
        """Should strip simple common prefix."""
        names = ["ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk"]
        result = canonicalize_names(names)
        assert result == {
            "ss_sold_date_sk": "sold_date_sk",
            "ss_sold_time_sk": "sold_time_sk",
            "ss_item_sk": "item_sk",
        }

    def test_no_prefix_to_strip(self):
        """When no common prefix, names should remain unchanged."""
        names = ["user_id", "email", "name"]
        result = canonicalize_names(names)
        assert result == {"user_id": "user_id", "email": "email", "name": "name"}

    def test_empty_list(self):
        """Empty list should return empty dict."""
        assert canonicalize_names([]) == {}

    def test_single_item(self):
        """Single item should remain unchanged."""
        result = canonicalize_names(["user_id"])
        assert result == {"user_id": "user_id"}

    def test_preserves_case_in_output(self):
        """Should normalize to snake_case in the stripped output."""
        names = ["SS_SOLD_DATE_SK", "SS_SOLD_TIME_SK", "SS_ITEM_SK"]
        result = canonicalize_names(names)
        assert result == {
            "SS_SOLD_DATE_SK": "sold_date_sk",
            "SS_SOLD_TIME_SK": "sold_time_sk",
            "SS_ITEM_SK": "item_sk",
        }

    def test_complex_prefix(self):
        """Should handle complex multi-level prefixes."""
        names = ["store_sales_id", "store_sales_date", "store_sales_amount"]
        result = canonicalize_names(names)
        assert result == {
            "store_sales_id": "id",
            "store_sales_date": "date",
            "store_sales_amount": "amount",
        }


class TestProcessColumnWithPrefixStripping:
    """Test _process_column with prefix stripping."""

    def test_column_with_prefix_mapping(self):
        """Test that prefix mapping is applied to concept names."""
        col = ("ss_sold_date_sk", "INTEGER", "NO", None)
        grain_components = []
        sample_rows = []
        prefix_mapping = {"ss_sold_date_sk": "sold_date_sk"}

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, prefix_mapping, _DIALECT
        )

        # Concept name should have prefix stripped
        assert concept.name == "sold_date_sk"
        # Column assignment alias should preserve original name
        assert column_assignment.alias == "ss_sold_date_sk"

    def test_column_without_prefix_mapping(self):
        """Test that columns work without prefix mapping."""
        col = ("user_id", "INTEGER", "NO", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["user_id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping, _DIALECT
        )

        # Concept name should be normalized without stripping
        assert concept.name == "user_id"
        assert column_assignment.alias == "user_id"

    def test_column_with_empty_prefix_mapping(self):
        """Test column with empty prefix mapping dict."""
        col = ("user_id", "INTEGER", "NO", None)
        grain_components = []
        sample_rows = []
        prefix_mapping = _make_concept_mapping(["user_id"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, prefix_mapping, _DIALECT
        )

        # Should work normally
        assert concept.name == "user_id"
        assert column_assignment.alias == "user_id"


class TestFileSourceDetection:
    """Detection helpers for distinguishing file paths from table names."""

    def test_local_csv_is_file(self):
        from trilogy.scripts.ingest import _looks_like_file_source

        assert _looks_like_file_source("data.csv")
        assert _looks_like_file_source("/abs/path/foo.parquet")
        assert _looks_like_file_source("./relative/x.tsv")

    def test_url_is_file(self):
        from trilogy.scripts.ingest import _looks_like_file_source

        assert _looks_like_file_source("https://example.com/data.csv")
        assert _looks_like_file_source("gs://bucket/x.parquet")
        assert _looks_like_file_source("s3://bucket/x.tsv")

    def test_table_name_is_not_file(self):
        from trilogy.scripts.ingest import _looks_like_file_source

        assert not _looks_like_file_source("users")
        assert not _looks_like_file_source("schema.users")
        assert not _looks_like_file_source("users_with_pk")

    def test_unsupported_extension_is_not_file(self):
        from trilogy.scripts.ingest import _looks_like_file_source

        # JSON isn't supported by the trilogy `file` grammar today
        assert not _looks_like_file_source("data.json")
        assert not _looks_like_file_source("foo.txt")

    def test_datasource_name_from_path(self):
        from trilogy.scripts.ingest import _datasource_name_from_path

        assert _datasource_name_from_path("/a/b/sales.csv") == "sales"
        assert _datasource_name_from_path("UserData.parquet") == "user_data"
        assert _datasource_name_from_path("https://x.com/data.csv?token=abc") == "data"

    def test_datasource_name_falls_back_to_source_when_blank(self):
        from trilogy.scripts.ingest import _datasource_name_from_path

        # When canonicalization strips everything we still emit a usable identifier.
        assert _datasource_name_from_path("___.csv") == "source"
        assert _datasource_name_from_path("@@@@.csv") == "source"


class TestResolveFileSource:
    """`_resolve_file_source` decides URL passthrough vs. local-path resolution."""

    def test_unsupported_extension_raises(self):
        from trilogy.scripts.ingest import _resolve_file_source

        with pytest.raises(ValueError, match="Unsupported file extension"):
            _resolve_file_source("/tmp/data.json")

    def test_url_passes_through_unchanged(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _resolve_file_source

        url = "https://example.com/data.parquet"
        loc, addr_type = _resolve_file_source(url)
        assert loc == url
        assert addr_type == AddressType.PARQUET

    def test_glob_passes_through_unchanged(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _resolve_file_source

        glob = "/tmp/data/*.csv"
        loc, addr_type = _resolve_file_source(glob)
        assert loc == glob
        assert addr_type == AddressType.CSV

    def test_local_path_is_resolved_absolute(self):
        from trilogy.scripts.ingest import _resolve_file_source

        loc, _ = _resolve_file_source("./relative.csv")
        assert Path(loc).is_absolute()


class TestFileIntrospectionSource:
    """`_file_introspection_source` builds the read_* SQL fragment."""

    def test_csv_uses_read_csv_auto(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _file_introspection_source

        assert _file_introspection_source("/a/b.csv", AddressType.CSV) == (
            "read_csv_auto('/a/b.csv')"
        )

    def test_tsv_uses_tab_delimiter(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _file_introspection_source

        sql = _file_introspection_source("/a/b.tsv", AddressType.TSV)
        assert sql.startswith("read_csv_auto(")
        assert "delim='\\t'" in sql

    def test_parquet_uses_read_parquet(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _file_introspection_source

        assert _file_introspection_source("/a/b.parquet", AddressType.PARQUET) == (
            "read_parquet('/a/b.parquet')"
        )

    def test_unsupported_addr_type_raises(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _file_introspection_source

        with pytest.raises(ValueError, match="Unsupported file address type"):
            _file_introspection_source("/a/b", AddressType.QUERY)

    def test_single_quote_in_path_is_escaped(self):
        from trilogy.core.enums import AddressType
        from trilogy.scripts.ingest import _file_introspection_source

        # SQL injection guard: path with a literal apostrophe must be escaped.
        sql = _file_introspection_source("/a/b's.csv", AddressType.CSV)
        assert sql == "read_csv_auto('/a/b''s.csv')"


class TestMaybeLoadHttpfs:
    """`_maybe_load_httpfs` only fires for remote URLs."""

    def test_local_path_skipped(self):
        from trilogy.scripts.ingest import _maybe_load_httpfs

        # No exec calls expected — pass a sentinel that would error if used.
        class Boom:
            def execute_raw_sql(self, *a, **kw):
                raise AssertionError("should not call execute_raw_sql for local path")

        _maybe_load_httpfs(Boom(), "/tmp/data.csv")  # type: ignore[arg-type]

    def test_remote_url_loads_extension(self):
        from trilogy.scripts.ingest import _maybe_load_httpfs

        calls: list[str] = []

        class Recorder:
            def execute_raw_sql(self, sql: str):
                calls.append(sql)
                return None

        _maybe_load_httpfs(Recorder(), "https://example.com/x.csv")  # type: ignore[arg-type]
        assert any("httpfs" in s.lower() for s in calls)

    def test_remote_url_swallows_install_error(self):
        from trilogy.scripts.ingest import _maybe_load_httpfs

        class Failing:
            def execute_raw_sql(self, sql: str):
                raise RuntimeError("network down")

        # Should warn but not raise.
        _maybe_load_httpfs(Failing(), "gs://bucket/x.parquet")  # type: ignore[arg-type]


def test_ingest_heap_csv_no_unique_grain():
    """A CSV with no unique columns falls back to a grainless datasource."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "events.csv"
        # Every row identical → no unique key combination
        csv_path.write_text(
            "event_type,event_value\n" "click,1\n" "click,1\n" "click,1\n" "click,1\n"
        )
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", str(csv_path), "--output", str(out_dir)])
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "No primary key or unique grain" in result.output
        content = (out_dir / "events.preql").read_text()
        # No grain clause emitted
        assert "grain (" not in content


def test_ingest_name_with_multiple_sources_rejected():
    """--name only makes sense when ingesting a single source."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv1 = tmppath / "a.csv"
        csv1.write_text("id\n1\n")
        csv2 = tmppath / "b.csv"
        csv2.write_text("id\n2\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                f"{csv1},{csv2}",
                "--output",
                str(tmppath / "raw"),
                "--name",
                "merged",
            ],
        )
        assert result.exit_code == 1
        assert "--name can only be set" in result.output


def test_ingest_env_parse_error():
    """--env with bad input fails fast with a clear error."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "x.csv"
        csv_path.write_text("a\n1\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                str(csv_path),
                "--output",
                str(tmppath / "raw"),
                # Path that doesn't exist and isn't KEY=VALUE → ValueError
                "--env",
                "/nonexistent/path/that/does/not/exist",
            ],
        )
        assert result.exit_code == 1


def test_ingest_table_name_override():
    """--name applied to a table source renames the generated datasource."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        setup_sql = tmppath / "setup.sql"
        setup_sql.write_text(
            "CREATE TABLE raw_users (id INTEGER PRIMARY KEY, email VARCHAR);\n"
            "INSERT INTO raw_users VALUES (1,'a@x.com');"
        )
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n'
            f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
        )
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "raw_users",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
                "--name",
                "users",
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert (out_dir / "users.preql").exists()
        assert not (out_dir / "raw_users.preql").exists()


def test_ingest_table_with_fk_to_other_table():
    """FK relationships are written into the dependent table's preql."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        setup_sql = tmppath / "setup.sql"
        setup_sql.write_text(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR);\n"
            "CREATE TABLE orders (\n"
            "  order_id INTEGER PRIMARY KEY,\n"
            "  customer_id INTEGER,\n"
            "  total DOUBLE\n"
            ");\n"
            "INSERT INTO customers VALUES (1,'alice'),(2,'bob');\n"
            "INSERT INTO orders VALUES (10,1,99.5),(11,2,42.0);"
        )
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n'
            f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
        )
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "customers,orders",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
                "--fks",
                "orders.customer_id:customers.id",
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "Processing foreign key relationships" in result.output
        assert (out_dir / "orders.preql").exists()
        assert (out_dir / "customers.preql").exists()


def test_ingest_no_dialect_no_files_fails_fast():
    """Without dialect or file sources we need a dialect — error out."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        # trilogy.toml without an engine section
        config_file = tmppath / "trilogy.toml"
        config_file.write_text("[setup]\nsql = []\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "some_table",
                "--config",
                str(config_file),
                "--output",
                str(tmppath / "raw"),
            ],
        )
        assert result.exit_code == 1
        assert "No dialect specified" in result.output


def test_ingest_failure_recovers_for_subsequent_sources():
    """A failed source rolls back the transaction; later sources still succeed."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        good = tmppath / "good.csv"
        good.write_text("id,name\n1,alice\n2,bob\n")
        # Path that does not exist on disk → DuckDB IO Error → rollback
        bad = tmppath / "missing.csv"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                f"{bad},{good}",
                "--output",
                str(tmppath / "raw"),
            ],
        )
        # Exit 0 because at least one source succeeded.
        assert result.exit_code == 0
        assert (tmppath / "raw" / "good.preql").exists()
        assert "Failed to ingest" in result.output


def test_ingest_debug_traceback_for_file_failure():
    """--debug on a file failure prints a traceback."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--debug",
                "ingest",
                str(tmppath / "missing.csv"),
                "--output",
                str(tmppath / "raw"),
            ],
        )
        assert result.exit_code == 1
        assert "Traceback" in result.output


class TestIngestSummaryRow:
    """The typed summary record exposed for display."""

    def test_ok_status(self):
        from trilogy.scripts.ingest import IngestSummaryRow

        row = IngestSummaryRow(
            source="x.csv", output="raw/x.preql", columns="3", grain="id", status="ok"
        )
        assert row.ok

    def test_failure_status(self):
        from trilogy.scripts.ingest import IngestSummaryRow

        row = IngestSummaryRow(
            source="x.csv",
            output="-",
            columns="-",
            grain="-",
            status="failed: OperationalError",
        )
        assert not row.ok


def test_ingest_local_csv_file():
    """End-to-end ingest of a local CSV without a configured trilogy.toml."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "orders.csv"
        # Distinct, high-cardinality emails so the column stays a rich type
        # rather than being promoted to an enum.
        rows = "\n".join(f"{i},user{i}@test.com,{i * 10}.50" for i in range(1, 16))
        csv_path.write_text(f"order_id,customer_email,total\n{rows}\n")
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["ingest", str(csv_path), "--output", str(out_dir)],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        out_file = out_dir / "orders.preql"
        assert out_file.exists()
        content = out_file.read_text()
        assert "order_id" in content
        assert "customer_email" in content
        # File address syntax must use backticks (round-trips with the parser)
        assert "file `" in content
        assert content.rstrip().endswith("`;") or "`;" in content
        # Email rich type detected
        assert "email_address" in content


def test_ingest_local_parquet_file():
    """Ingest a local parquet file via DuckDB."""
    import tempfile

    duckdb = pytest.importorskip("duckdb")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        parquet_path = tmppath / "events.parquet"
        conn = duckdb.connect()
        conn.execute(
            "COPY (SELECT 1 AS event_id, 'login' AS event_type, "
            "TIMESTAMP '2024-01-01 00:00:00' AS event_ts UNION ALL "
            "SELECT 2, 'click', TIMESTAMP '2024-01-01 00:01:00') "
            f"TO '{parquet_path.as_posix()}' (FORMAT PARQUET)"
        )
        conn.close()
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["ingest", str(parquet_path), "--output", str(out_dir)],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        out_file = out_dir / "events.preql"
        assert out_file.exists()
        content = out_file.read_text()
        assert "event_id" in content
        assert "event_type" in content
        assert "file `" in content


def test_ingest_file_with_name_override():
    """--name renames the generated datasource."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "data.csv"
        csv_path.write_text("id,name\n1,alice\n2,bob\n")
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                str(csv_path),
                "--output",
                str(out_dir),
                "--name",
                "customers",
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert (out_dir / "customers.preql").exists()
        assert not (out_dir / "data.preql").exists()


def test_ingest_mixed_table_and_file_under_duckdb():
    """DuckDB can introspect both tables and files in one call."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "events.csv"
        csv_path.write_text("event_id,event_type\n1,click\n2,view\n")
        setup_sql = tmppath / "setup.sql"
        setup_sql.write_text(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR);\n"
            "INSERT INTO customers VALUES (1,'alice'),(2,'bob');"
        )
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n'
            f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
        )
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                f"customers,{csv_path}",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert (out_dir / "customers.preql").exists()
        assert (out_dir / "events.preql").exists()
        # Table source uses `address …`; file source uses `file `…``.
        assert "address customers" in (out_dir / "customers.preql").read_text()
        assert "file `" in (out_dir / "events.preql").read_text()


def test_ingest_file_with_non_duckdb_dialect_rejected():
    """File ingest requires duckdb; other dialects fail fast."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "x.csv"
        csv_path.write_text("a,b\n1,2\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                str(csv_path),
                "snowflake",
                "--output",
                str(tmppath / "raw"),
            ],
        )
        assert result.exit_code == 1
        assert "duckdb" in result.output.lower()


def test_ingest_file_round_trips_through_parser():
    """The generated .preql for a file source must reparse cleanly."""
    import tempfile

    from trilogy.parsing.parse_engine_v2 import parse_text

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "round.csv"
        csv_path.write_text("id,name\n1,alice\n2,bob\n")
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["ingest", str(csv_path), "--output", str(out_dir)],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        content = (out_dir / "round.preql").read_text()
        # Must reparse without error - guards the renderer fix.
        env, stmts = parse_text(content)
        from trilogy.core.models.datasource import Address as ParsedAddress
        from trilogy.core.models.datasource import Datasource as ParsedDatasource

        ds = next(s for s in stmts if isinstance(s, ParsedDatasource))
        assert isinstance(ds.address, ParsedAddress)
        assert ds.address.location.endswith("round.csv")


def test_ingest_csv_detects_enum_and_round_trips():
    """A low-cardinality column is ingested as an enum and the .preql reparses."""
    import tempfile

    from trilogy.parsing.parse_engine_v2 import parse_text

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "tickets.csv"
        statuses = ["open", "closed", "pending"]
        lines = ["ticket_id,status"] + [f"{i},{statuses[i % 3]}" for i in range(30)]
        csv_path.write_text("\n".join(lines) + "\n")
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["ingest", str(csv_path), "--output", str(out_dir)],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        content = (out_dir / "tickets.preql").read_text()
        assert "enum<string>[" in content

        env, _ = parse_text(content)
        concept = env.concepts["local.status"]
        assert isinstance(concept.datatype, EnumType)
        assert set(concept.datatype.values) == {"open", "closed", "pending"}


def test_ingest_csv_enum_with_rich_trait_round_trips():
    """A column that is both enum-constrained and a rich type renders as
    enum<...>[...]::trait and reparses into a TraitDataType wrapping the enum."""
    import tempfile

    from trilogy.parsing.parse_engine_v2 import parse_text

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_path = tmppath / "contacts.csv"
        # `email` matches the email rich type; only 2 distinct values -> also enum.
        lines = ["id,email"] + [
            f"{i},{'a@x.com' if i % 2 else 'b@x.com'}" for i in range(30)
        ]
        csv_path.write_text("\n".join(lines) + "\n")
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(cli, ["ingest", str(csv_path), "--output", str(out_dir)])
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        content = (out_dir / "contacts.preql").read_text()
        assert "enum<string>[" in content
        assert "::email_address" in content

        env, _ = parse_text(content)
        datatype = env.concepts["local.email"].datatype
        assert isinstance(datatype, TraitDataType)
        assert isinstance(datatype.type, EnumType)
        assert datatype.type.values == ["a@x.com", "b@x.com"]
        assert "email_address" in datatype.traits


def test_ingest_groups_properties_declaration():
    """Properties sharing a grain key render as one grouped declaration."""
    import tempfile

    from trilogy.parsing.parse_engine_v2 import parse_text

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        setup_sql = tmppath / "setup.sql"
        setup_sql.write_text(
            "CREATE TABLE customer_demographics (\n"
            "  cd_demo_sk INTEGER PRIMARY KEY,\n"
            "  cd_gender VARCHAR,\n"
            "  cd_marital_status VARCHAR,\n"
            "  cd_purchase_estimate INTEGER\n"
            ");\n"
            "INSERT INTO customer_demographics VALUES "
            "(1,'M','S',5000),(2,'F','M',3000);"
        )
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n'
            f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
        )
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "customer_demographics",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        content = (out_dir / "customer_demographics.preql").read_text()
        assert "properties demo_sk (" in content
        assert content.count("property ") == 0

        env, _ = parse_text(content)
        for name in ("gender", "marital_status", "purchase_estimate"):
            assert env.concepts[f"local.{name}"].purpose == Purpose.PROPERTY


def test_ingest_single_property_not_grouped():
    """A lone property stays an individual declaration, not a grouped block."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        setup_sql = tmppath / "setup.sql"
        setup_sql.write_text(
            "CREATE TABLE solo (id INTEGER PRIMARY KEY, name VARCHAR);\n"
            "INSERT INTO solo VALUES (1,'a'),(2,'b');"
        )
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n'
            f'[setup]\nsql = ["{setup_sql.as_posix()}"]\n'
        )
        out_dir = tmppath / "raw"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "ingest",
                "solo",
                "duckdb",
                "--config",
                str(config_file),
                "--output",
                str(out_dir),
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        content = (out_dir / "solo.preql").read_text()
        assert "properties " not in content
        assert "property id.name" in content


def test_parse_foreign_keys():
    """Test parsing of foreign key specifications."""
    # Single FK
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {
        "store_sales": {
            "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=True)
        }
    }

    # Multiple FKs
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk,store_sales.ss_item_sk:item.i_item_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {
        "store_sales": {
            "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=True),
            "ss_item_sk": FKBinding("item.i_item_sk", partial=True),
        }
    }

    # Empty string
    result = parse_foreign_keys("")
    assert result == {}

    # None
    result = parse_foreign_keys(None)
    assert result == {}
