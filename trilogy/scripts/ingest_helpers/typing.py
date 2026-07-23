import re
from typing import TYPE_CHECKING, Any

from trilogy.core.models.core import DataType, EnumType, ValidatedType, ValueRange

if TYPE_CHECKING:
    from trilogy.executor import Executor

# Integer-family base types a numeric date-part trait may wrap (the ingester
# resolves whole-number columns to either, depending on the source).
_INT_TYPES = frozenset({DataType.INTEGER, DataType.BIGINT})

# Rich type detection mappings
RICH_TYPE_PATTERNS: dict[str, dict[str, Any]] = {
    # Date-part columns (std.date traits). Gated by an inclusive value RANGE as
    # well as the name so a same-named-but-unrelated column (e.g. d_month_seq,
    # values ~1200) is not misclassified.
    "date": {
        "year": {
            "patterns": [r"(?:^|_)year(?:$|_)", r"(?:^|_)yr(?:$|_)"],
            "import": "std.date",
            "type_name": "year",
            "base_type": _INT_TYPES,
            "value_range": (1000, 9999),
        },
        "month": {
            "patterns": [
                r"(?:^|_)month(?:$|_)",
                r"(?:^|_)moy(?:$|_)",
                r"month_of_year",
            ],
            "import": "std.date",
            "type_name": "month",
            "base_type": _INT_TYPES,
            "value_range": (1, 12),
        },
        "day": {
            "patterns": [r"(?:^|_)dom(?:$|_)", r"day_of_month"],
            "import": "std.date",
            "type_name": "day",
            "base_type": _INT_TYPES,
            "value_range": (1, 31),
        },
        "day_of_week": {
            "patterns": [
                r"(?:^|_)dow(?:$|_)",
                r"day_of_week",
                r"(?:^|_)weekday(?:$|_)",
            ],
            "import": "std.date",
            "type_name": "day_of_week",
            "base_type": _INT_TYPES,
            "value_range": (0, 7),
        },
        "quarter": {
            "patterns": [
                r"(?:^|_)quarter(?:$|_)",
                r"(?:^|_)qoy(?:$|_)",
                r"(?:^|_)qtr(?:$|_)",
            ],
            "import": "std.date",
            "type_name": "quarter",
            "base_type": _INT_TYPES,
            "value_range": (1, 4),
        },
        "week": {
            "patterns": [r"(?:^|_)week(?:$|_)", r"(?:^|_)woy(?:$|_)", r"week_of_year"],
            "import": "std.date",
            "type_name": "week",
            "base_type": _INT_TYPES,
            "value_range": (1, 53),
        },
    },
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
            "value_pattern": r"^\S+@\S+$",
        },
        "url": {
            "patterns": [r"(?:^|_)url(?:$|_)", r"(?:^|_)website(?:$|_)"],
            "import": "std.net",
            "type_name": "url",
            "base_type": DataType.STRING,
            "value_pattern": r"://",
        },
        "ipv4_address": {
            "patterns": [r"(?:^|_)ip(?:$|_)", r"(?:^|_)ipv4(?:$|_)", r"ip_address"],
            "import": "std.net",
            "type_name": "ipv4_address",
            "base_type": DataType.STRING,
            "value_pattern": r"^\d{1,3}(\.\d{1,3}){3}$",
        },
    },
}


def _values_match_gate(config: dict[str, Any], sample_values: list[Any] | None) -> bool:
    """Whether a value-gated rich type may apply.

    An ungated config always passes. A `value_pattern` (regex) or `value_range`
    ((lo, hi) inclusive, for integer date parts) requires non-empty sample
    values that all satisfy the gate, so a column merely *named* like a rich
    type is not misclassified by name alone.
    """
    value_pattern = config.get("value_pattern")
    value_range = config.get("value_range")
    if value_pattern is None and value_range is None:
        return True
    if not sample_values:
        return False
    if value_pattern is not None:
        regex = re.compile(value_pattern)
        if not all(regex.search(str(v)) is not None for v in sample_values):
            return False
    if value_range is not None:
        lo, hi = value_range
        for v in sample_values:
            if isinstance(v, bool) or not isinstance(v, int):
                return False
            if not lo <= v <= hi:
                return False
    return True


def detect_rich_type(
    column_name: str,
    base_datatype: DataType,
    sample_values: list[Any] | None = None,
) -> tuple[str, str] | tuple[None, None]:
    """Detect if a column matches a rich type by name (and, for value-gated
    types, by its actual values).

    Returns: (import_path, type_name) or (None, None) if no match.

    When multiple patterns match, the longest matched string wins (more
    specific matches are preferred). A config carrying a `value_pattern` only
    matches when every sampled value satisfies it — e.g. a Y/N "channel_email"
    flag is not an email address.
    """
    column_lower = column_name.lower()

    matches = []
    for _, types in RICH_TYPE_PATTERNS.items():
        for _, config in types.items():
            allowed = config["base_type"]
            allowed_set = allowed if isinstance(allowed, frozenset) else {allowed}
            if base_datatype not in allowed_set:
                continue
            if not _values_match_gate(config, sample_values):
                continue
            for pattern in config["patterns"]:
                match = re.search(pattern, column_lower)
                if match:
                    matches.append(
                        (len(match.group()), config["import"], config["type_name"])
                    )
                    break  # Only need one match per type

    # Return the most specific match (longest matched string)
    if matches:
        matches.sort(reverse=True)
        return str(matches[0][1]), str(matches[0][2])

    return None, None


# Columns whose distinct values form a fixed set no larger than this are
# promoted to an enum datatype during ingest.
DEFAULT_ENUM_MAX_DISTINCT = 10

# Values longer than this are treated as free text rather than enum members —
# a handful of long strings is almost always descriptive text, not a domain.
MAX_ENUM_VALUE_LENGTH = 50

# Base types eligible for enum promotion — the grammar's enum_type rule only
# accepts string or integer literal members.
_ENUM_ELIGIBLE_TYPES = frozenset({DataType.STRING, DataType.INTEGER, DataType.BIGINT})


def _enum_from_values(
    base_datatype: DataType, values: list[Any], max_distinct: int
) -> EnumType | None:
    """Build an EnumType from a column's distinct non-null values, or None when
    the values don't look like a fixed domain (too many, or long free text)."""
    if base_datatype not in _ENUM_ELIGIBLE_TYPES:
        return None
    if not values or len(values) > max_distinct:
        return None
    if any(isinstance(v, bool) or not isinstance(v, (str, int)) for v in values):
        return None
    if any(isinstance(v, str) and len(v) > MAX_ENUM_VALUE_LENGTH for v in values):
        return None
    return EnumType(type=base_datatype, values=sorted(values))


_BOUNDS_INT_TYPES = frozenset({DataType.INTEGER, DataType.BIGINT})
_BOUNDS_FLOAT_TYPES = frozenset(
    {DataType.FLOAT, DataType.DOUBLE, DataType.NUMERIC, DataType.NUMBER}
)


def detect_numeric_bounds(
    executor: "Executor",
    from_clause: str,
    columns: list[tuple[str, DataType]],
    skip: set[str],
) -> dict[str, ValidatedType]:
    """Infer declared-range validators from a full-table MIN/MAX scan.

    Integer columns get tight inclusive bounds ``[min..max]`` — exact for the
    ingested source, refreshed on re-ingest. Continuous (float/numeric)
    measures only get a structural sign bound ``[0..]`` when all values are
    non-negative: tight float bounds are brittle against data growth.
    ``skip`` excludes key/enum columns (keys grow; an enum is already the
    domain)."""
    eligible = [
        (n, b)
        for n, b in columns
        if n not in skip and (b in _BOUNDS_INT_TYPES or b in _BOUNDS_FLOAT_TYPES)
    ]
    if not eligible:
        return {}
    quote = executor.generator.safe_quote
    exprs = ", ".join(f"min({quote(n)}), max({quote(n)})" for n, _ in eligible)
    row = executor.execute_raw_sql(f"SELECT {exprs} FROM {from_clause}").fetchall()[0]
    result: dict[str, ValidatedType] = {}
    for i, (name, base) in enumerate(eligible):
        lo, hi = row[2 * i], row[2 * i + 1]
        if lo is None or hi is None:
            continue
        if base in _BOUNDS_INT_TYPES:
            if isinstance(lo, bool) or not isinstance(lo, int):
                continue
            result[name] = ValidatedType(
                type=base, ranges=(ValueRange(min=int(lo), max=int(hi)),)
            )
        elif lo >= 0:
            result[name] = ValidatedType(type=base, ranges=(ValueRange(min=0),))
    return result


def detect_enum_types(
    executor: "Executor",
    from_clause: str,
    columns: list[tuple[str, DataType]],
    max_distinct: int = DEFAULT_ENUM_MAX_DISTINCT,
) -> dict[str, EnumType]:
    """Detect enum columns by querying the source for true distinct values.

    Querying the full source (not a head sample) avoids both missing enums in
    small tables and falsely promoting a column whose leading rows happen to be
    low-cardinality. `columns` is (column name, resolved base datatype); the
    return maps column name -> EnumType for the columns that qualify.
    """
    eligible = [(n, b) for n, b in columns if b in _ENUM_ELIGIBLE_TYPES]
    if not eligible:
        return {}
    quote = executor.generator.safe_quote
    count_exprs = ", ".join(f"count(DISTINCT {quote(n)})" for n, _ in eligible)
    # fetchall (not fetchone) so the cursor is exhausted — a dangling cursor
    # keeps the underlying file handle open on Windows.
    counts = executor.execute_raw_sql(
        f"SELECT {count_exprs} FROM {from_clause}"
    ).fetchall()[0]

    result: dict[str, EnumType] = {}
    for (name, base), ndistinct in zip(eligible, counts):
        if ndistinct is None or not 1 <= ndistinct <= max_distinct:
            continue
        qname = quote(name)
        rows = executor.execute_raw_sql(
            f"SELECT DISTINCT {qname} FROM {from_clause} WHERE {qname} IS NOT NULL"
        ).fetchall()
        enum = _enum_from_values(base, [r[0] for r in rows], max_distinct)
        if enum is not None:
            result[name] = enum
    return result
