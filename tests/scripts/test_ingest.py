from pathlib import Path

import pytest
from click.testing import CliRunner

from trilogy.authoring import DataType
from trilogy.scripts.ingest import detect_rich_type, to_snake_case
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


class TestSnakeCaseNormalization:
    """Test snake_case conversion for concept names."""

    def test_already_snake_case(self):
        assert to_snake_case("user_id") == "user_id"
        assert to_snake_case("first_name") == "first_name"

    def test_camel_case(self):
        assert to_snake_case("userId") == "user_id"
        assert to_snake_case("firstName") == "first_name"
        assert to_snake_case("customerFirstName") == "customer_first_name"

    def test_pascal_case(self):
        assert to_snake_case("UserId") == "user_id"
        assert to_snake_case("FirstName") == "first_name"
        assert to_snake_case("CustomerFirstName") == "customer_first_name"

    def test_with_spaces(self):
        assert to_snake_case("User ID") == "user_id"
        assert to_snake_case("First Name") == "first_name"
        assert to_snake_case("Customer First Name") == "customer_first_name"

    def test_with_special_chars(self):
        assert to_snake_case("user-id") == "user_id"
        assert to_snake_case("first.name") == "first_name"
        assert to_snake_case("user@email") == "user_email"

    def test_mixed_formats(self):
        assert to_snake_case("UserID-2023") == "user_id_2023"
        assert to_snake_case("First Name (Primary)") == "first_name_primary"

    def test_all_caps(self):
        assert to_snake_case("ID") == "id"
        assert to_snake_case("URL") == "url"

    def test_numbers(self):
        assert to_snake_case("address1") == "address1"
        assert to_snake_case("line2") == "line2"


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
        assert detect_rich_type("email", DataType.STRING) == ("std.net", "email_address")
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
