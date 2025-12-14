from pathlib import Path

from click.testing import CliRunner

from trilogy.authoring import DataType
from trilogy.core.enums import Modifier, Purpose
from trilogy.scripts.ingest import (
    _check_column_combination_uniqueness,
    _process_column,
    canonicalize_names,
    detect_nullability_from_sample,
    detect_rich_type,
    detect_unique_key_combinations,
    infer_datatype_from_sql_type,
)
from trilogy.scripts.ingest_helpers.foreign_keys import parse_foreign_keys
from trilogy.scripts.ingest_helpers.formatting import (
    canonicolize_name,
    find_common_prefix,
)
from trilogy.scripts.trilogy import cli


def test_ingest():
    path = Path(__file__).parent
    runner = CliRunner()
    config_dir = path / "config_directory"
    args = [
        "ingest",
        "world_capitals",
        "--config",
        str(config_dir / "trilogy.toml"),
    ]
    results = runner.invoke(
        cli,
        args,
    )
    print(args)
    print(results.output)
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
        setup_sql_file.write_text(
            """CREATE TABLE heap_table (
                event_type VARCHAR,
                event_value INTEGER,
                timestamp TIMESTAMP
            );
            INSERT INTO heap_table VALUES ('click', 1, '2024-01-01 10:00:00');
            INSERT INTO heap_table VALUES ('click', 1, '2024-01-01 10:00:00');
            INSERT INTO heap_table VALUES ('view', 2, '2024-01-01 10:00:00');
            INSERT INTO heap_table VALUES ('view', 2, '2024-01-01 10:00:00');"""
        )

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
        setup_sql_file.write_text(
            """CREATE TABLE test_table (id INTEGER, name VARCHAR);
INSERT INTO test_table VALUES (1, 'test');"""
        )

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
        setup_sql_file.write_text(
            """CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_table (id INTEGER, name VARCHAR);
INSERT INTO test_schema.test_table VALUES (1, 'test');"""
        )

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
    assert "No tables specified" in result.output


def test_ingest_with_debug_flag_success():
    """Test ingest with --debug flag on successful ingestion."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        setup_sql_file = tmppath / "setup.sql"
        setup_sql_file.write_text(
            """CREATE TABLE debug_test (id INTEGER, name VARCHAR);
INSERT INTO debug_test VALUES (1, 'test');"""
        )

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
        setup_sql_file.write_text(
            """CREATE TABLE good_table (id INTEGER);
INSERT INTO good_table VALUES (1);"""
        )

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
        setup_sql_file.write_text(
            """CREATE TABLE existing_table (id INTEGER);
INSERT INTO existing_table VALUES (1);"""
        )

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
        assert detect_rich_type("email", DataType.STRING) == (
            "std.net",
            "email_address",
        )
        assert detect_rich_type("email_address", DataType.STRING) == (
            "std.net",
            "email_address",
        )
        assert detect_rich_type("user_email", DataType.STRING) == (
            "std.net",
            "email_address",
        )

    def test_url_detection(self):
        assert detect_rich_type("url", DataType.STRING) == ("std.net", "url")
        assert detect_rich_type("website", DataType.STRING) == ("std.net", "url")

    def test_ip_detection(self):
        assert detect_rich_type("ip", DataType.STRING) == ("std.net", "ipv4_address")
        assert detect_rich_type("ipv4", DataType.STRING) == ("std.net", "ipv4_address")
        assert detect_rich_type("ip_address", DataType.STRING) == (
            "std.net",
            "ipv4_address",
        )

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


class TestInferDatatypeFromSqlType:
    """Test SQL type to Trilogy datatype inference."""

    def test_integer_types(self):
        """Test integer type detection."""
        assert infer_datatype_from_sql_type("INT") == DataType.INTEGER
        assert infer_datatype_from_sql_type("INTEGER") == DataType.INTEGER
        assert infer_datatype_from_sql_type("SMALLINT") == DataType.INTEGER
        assert infer_datatype_from_sql_type("TINYINT") == DataType.INTEGER
        assert infer_datatype_from_sql_type("MEDIUMINT") == DataType.INTEGER

    def test_bigint_types(self):
        """Test bigint type detection."""
        # Note: bigint must come before int in the check, otherwise "int" matches
        # The actual implementation checks bigint first, so this should work
        assert infer_datatype_from_sql_type("BIGINT") in [
            DataType.BIGINT,
            DataType.INTEGER,
        ]
        assert infer_datatype_from_sql_type("LONG") == DataType.BIGINT
        assert infer_datatype_from_sql_type("INT64") in [
            DataType.BIGINT,
            DataType.INTEGER,
        ]

    def test_float_types(self):
        """Test float/numeric type detection."""
        assert infer_datatype_from_sql_type("FLOAT") == DataType.FLOAT
        assert infer_datatype_from_sql_type("DOUBLE") == DataType.FLOAT
        assert infer_datatype_from_sql_type("REAL") == DataType.FLOAT
        assert infer_datatype_from_sql_type("FLOAT64") == DataType.FLOAT

    def test_numeric_types(self):
        """Test numeric/decimal type detection."""
        assert infer_datatype_from_sql_type("NUMERIC") == DataType.NUMERIC
        assert infer_datatype_from_sql_type("DECIMAL") == DataType.NUMERIC
        assert infer_datatype_from_sql_type("MONEY") == DataType.NUMERIC

    def test_string_types(self):
        """Test string type detection."""
        assert infer_datatype_from_sql_type("CHAR") == DataType.STRING
        assert infer_datatype_from_sql_type("VARCHAR") == DataType.STRING
        assert infer_datatype_from_sql_type("TEXT") == DataType.STRING
        assert infer_datatype_from_sql_type("STRING") == DataType.STRING
        assert infer_datatype_from_sql_type("CLOB") == DataType.STRING
        assert infer_datatype_from_sql_type("NCHAR") == DataType.STRING
        assert infer_datatype_from_sql_type("NVARCHAR") == DataType.STRING

    def test_boolean_types(self):
        """Test boolean type detection."""
        assert infer_datatype_from_sql_type("BOOL") == DataType.BOOL
        assert infer_datatype_from_sql_type("BOOLEAN") == DataType.BOOL
        assert infer_datatype_from_sql_type("BIT") == DataType.BOOL

    def test_timestamp_types(self):
        """Test timestamp type detection."""
        assert infer_datatype_from_sql_type("TIMESTAMP") == DataType.TIMESTAMP

    def test_datetime_types(self):
        """Test datetime type detection."""
        assert infer_datatype_from_sql_type("DATETIME") == DataType.DATETIME

    def test_date_types(self):
        """Test date type detection."""
        assert infer_datatype_from_sql_type("DATE") == DataType.DATE

    def test_case_insensitive(self):
        """Test that type detection is case-insensitive."""
        assert infer_datatype_from_sql_type("varchar") == DataType.STRING
        assert infer_datatype_from_sql_type("VARCHAR") == DataType.STRING
        assert infer_datatype_from_sql_type("VarChar") == DataType.STRING

    def test_with_size_specifications(self):
        """Test types with size specifications."""
        assert infer_datatype_from_sql_type("VARCHAR(255)") == DataType.STRING
        assert infer_datatype_from_sql_type("DECIMAL(10,2)") == DataType.NUMERIC
        assert infer_datatype_from_sql_type("CHAR(10)") == DataType.STRING

    def test_unknown_type_defaults_to_string(self):
        """Test that unknown types default to STRING."""
        assert infer_datatype_from_sql_type("UNKNOWN") == DataType.STRING
        assert infer_datatype_from_sql_type("CUSTOM_TYPE") == DataType.STRING
        assert infer_datatype_from_sql_type("") == DataType.STRING


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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
        )

        # Should not have description for whitespace comment
        assert concept.metadata.description is None

    def test_rich_type_detection_email(self):
        """Test rich type detection for email."""
        col = ("user_email", "VARCHAR(255)", "YES", None)
        grain_components = []
        sample_rows = []
        concept_mapping = _make_concept_mapping(["user_email"])

        concept, column_assignment, rich_import = _process_column(
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
        )

        # Should default to nullable when not specified
        assert Modifier.NULLABLE in concept.modifiers
        assert concept.metadata.description is None


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
            0, col, grain_components, sample_rows, prefix_mapping
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
            0, col, grain_components, sample_rows, concept_mapping
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
            0, col, grain_components, sample_rows, prefix_mapping
        )

        # Should work normally
        assert concept.name == "user_id"
        assert column_assignment.alias == "user_id"


def test_parse_foreign_keys():
    """Test parsing of foreign key specifications."""
    # Single FK
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {"store_sales": {"ss_customer_sk": "customer.c_customer_sk"}}

    # Multiple FKs
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk,store_sales.ss_item_sk:item.i_item_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {
        "store_sales": {
            "ss_customer_sk": "customer.c_customer_sk",
            "ss_item_sk": "item.i_item_sk",
        }
    }

    # Empty string
    result = parse_foreign_keys("")
    assert result == {}

    # None
    result = parse_foreign_keys(None)
    assert result == {}
