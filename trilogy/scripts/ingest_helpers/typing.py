import re
from typing import Any

from trilogy.authoring import (
    DataType,
)

# Rich type detection mappings
RICH_TYPE_PATTERNS: dict[str, dict[str, Any]] = {
    "geography": {
        "latitude": {
            "patterns": [r"(?:^|_)lat(?:$|_)", r"(?:^|_)latitude(?:$|_)"],
            "import": "std.geography",
            "type_name": "latitude",
            "base_type": DataType.FLOAT,
        },
        "longitude": {
            "patterns": [
                r"(?:^|_)lon(?:$|_)",
                r"(?:^|_)lng(?:$|_)",
                r"(?:^|_)long(?:$|_)",
                r"(?:^|_)longitude(?:$|_)",
            ],
            "import": "std.geography",
            "type_name": "longitude",
            "base_type": DataType.FLOAT,
        },
        "city": {
            "patterns": [r"(?:^|_)city(?:$|_)"],
            "import": "std.geography",
            "type_name": "city",
            "base_type": DataType.STRING,
        },
        "country": {
            "patterns": [r"(?:^|_)country(?:$|_)"],
            "import": "std.geography",
            "type_name": "country",
            "base_type": DataType.STRING,
        },
        "country_code": {
            "patterns": [r"country_code", r"countrycode"],
            "import": "std.geography",
            "type_name": "country_code",
            "base_type": DataType.STRING,
        },
        "us_state": {
            "patterns": [r"(?:^|_)state(?:$|_)", r"us_state"],
            "import": "std.geography",
            "type_name": "us_state",
            "base_type": DataType.STRING,
        },
        "us_zip_code": {
            "patterns": [r"(?:^|_)zip(?:$|_)", r"zipcode", r"zip_code", r"postal_code"],
            "import": "std.geography",
            "type_name": "us_zip_code",
            "base_type": DataType.STRING,
        },
    },
    "net": {
        "email_address": {
            "patterns": [r"(?:^|_)email(?:$|_)", r"email_address"],
            "import": "std.net",
            "type_name": "email_address",
            "base_type": DataType.STRING,
        },
        "url": {
            "patterns": [r"(?:^|_)url(?:$|_)", r"(?:^|_)website(?:$|_)"],
            "import": "std.net",
            "type_name": "url",
            "base_type": DataType.STRING,
        },
        "ipv4_address": {
            "patterns": [r"(?:^|_)ip(?:$|_)", r"(?:^|_)ipv4(?:$|_)", r"ip_address"],
            "import": "std.net",
            "type_name": "ipv4_address",
            "base_type": DataType.STRING,
        },
    },
}


def detect_rich_type(
    column_name: str, base_datatype: DataType
) -> tuple[str, str] | tuple[None, None]:
    """Detect if a column name matches a rich type pattern.

    Returns: (import_path, type_name) or (None, None) if no match

    Note: When multiple patterns match, the one with the longest matched
    string is preferred to ensure more specific matches win.
    """
    column_lower = column_name.lower()

    # Collect all matches and sort by matched string length (longest first) to prefer more specific matches
    matches = []

    for _, types in RICH_TYPE_PATTERNS.items():
        for _, config in types.items():
            # Only consider if base types match
            if config["base_type"] != base_datatype:
                continue

            # Check if any pattern matches
            for pattern in config["patterns"]:
                match = re.search(pattern, column_lower)
                if match:
                    # Store match with the length of the matched string for sorting
                    matched_length = len(match.group())
                    matches.append(
                        (matched_length, config["import"], config["type_name"])
                    )
                    break  # Only need one match per type

    # Return the most specific match (longest matched string)
    if matches:
        matches.sort(reverse=True)  # Sort by matched string length descending
        return str(matches[0][1]), str(matches[0][2])

    return None, None


def infer_datatype_from_sql_type(sql_type: str) -> DataType:
    """Infer Trilogy datatype from SQL type string."""
    sql_type_lower = sql_type.lower()

    # Integer types
    if any(
        t in sql_type_lower
        for t in ["int", "integer", "smallint", "tinyint", "mediumint"]
    ):
        return DataType.INTEGER
    if any(t in sql_type_lower for t in ["bigint", "long", "int64"]):
        return DataType.BIGINT

    # Numeric/decimal types
    if any(t in sql_type_lower for t in ["numeric", "decimal", "money"]):
        return DataType.NUMERIC
    if any(t in sql_type_lower for t in ["float", "double", "real", "float64"]):
        return DataType.FLOAT

    # String types
    if any(
        t in sql_type_lower
        for t in ["char", "varchar", "text", "string", "clob", "nchar", "nvarchar"]
    ):
        return DataType.STRING

    # Boolean
    if any(t in sql_type_lower for t in ["bool", "boolean", "bit"]):
        return DataType.BOOL

    # Date/Time types
    if "timestamp" in sql_type_lower:
        return DataType.TIMESTAMP
    if "datetime" in sql_type_lower:
        return DataType.DATETIME
    if "date" in sql_type_lower:
        return DataType.DATE

    # Default to string for unknown types
    return DataType.STRING
