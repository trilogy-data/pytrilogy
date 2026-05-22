import re
from typing import Any

from trilogy.core.models.core import DataType, EnumType

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


# Columns whose sampled values form a fixed set no larger than this are
# promoted to an enum datatype during ingest.
DEFAULT_ENUM_MAX_DISTINCT = 10

# Base types eligible for enum promotion — the grammar's enum_type rule only
# accepts string or integer literal members.
_ENUM_ELIGIBLE_TYPES = frozenset({DataType.STRING, DataType.INTEGER, DataType.BIGINT})


def detect_enum_type(
    column_index: int,
    base_datatype: DataType,
    sample_rows: list[tuple],
    max_distinct: int = DEFAULT_ENUM_MAX_DISTINCT,
) -> EnumType | None:
    """Promote a column to an enum when its sampled values form a small fixed set.

    Requires a sample larger than the cutoff so a low distinct count reflects a
    real constraint rather than a tiny table (this also excludes unique keys).
    """
    if base_datatype not in _ENUM_ELIGIBLE_TYPES or len(sample_rows) <= max_distinct:
        return None
    distinct: set[Any] = set()
    for row in sample_rows:
        value = row[column_index]
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            return None
        distinct.add(value)
        if len(distinct) > max_distinct:
            return None
    if not distinct:
        return None
    return EnumType(type=base_datatype, values=sorted(distinct))
