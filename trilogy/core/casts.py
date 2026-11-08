"""What a cast means, whatever the engine (formal/Trilogy/Casts.lean).

A string reads as a bool as DuckDB reads it: one of these spellings, in any
case. A parse-time fold of a literal and every dialect's rendering share it.
"""

import re
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from trilogy.core.models.core import DataType, arg_to_datatype

TRUE_TEXT = ("true", "t", "1", "yes", "y")
FALSE_TEXT = ("false", "f", "0", "no", "n")


def text_to_bool(value: str) -> bool:
    folded = value.lower()
    if folded in TRUE_TEXT:
        return True
    if folded in FALSE_TEXT:
        return False
    raise ValueError(f"Cannot cast {value!r} to bool")


# A string reads as a number when, between whitespace, it is a signed decimal
# with an optional exponent; as a date when it is `YYYY-MM-DD` naming a real
# day, optionally followed by a time of day. Any other string has no reading
# and casts to NULL (formal/Trilogy/Casts.lean, `read_number_exact`). The
# patterns are POSIX/RE2 and need no backslash or brace, so every dialect's
# string literal and parameter syntax holds them as written.
SPACE = " \t\n\r"
NUMBER_PATTERN = (
    "^[[:space:]]*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?[[:space:]]*$"
)
DATE_PATTERN = (
    "^[[:space:]]*[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
    "([ T]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]+)?)?[[:space:]]*$"
)
_NUMBER = re.compile(NUMBER_PATTERN.replace("[[:space:]]", "[ \t\n\r]"))
_DATE = re.compile(DATE_PATTERN.replace("[[:space:]]", "[ \t\n\r]"))


def text_to_number(value: str) -> Decimal | None:
    """The exact value a string names, or None when it has no reading."""
    if not _NUMBER.fullmatch(value):
        return None
    return Decimal(value.strip(SPACE))


def text_to_int(value: str) -> int | None:
    number = text_to_number(value)
    if number is None:
        return None
    return int(number.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def text_to_decimal(value: str, scale: int) -> Decimal | None:
    number = text_to_number(value)
    if number is None:
        return None
    return number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


def text_to_float(value: str) -> float | None:
    number = text_to_number(value)
    return None if number is None else float(number)


def text_to_date(value: str) -> date | None:
    if not _DATE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value.strip(SPACE)[:10])
    except ValueError:
        return None


# A string cast to one of these is read as above, so a non-null string can
# cast to NULL: the cast is NULL-introducing from a string
# (formal/Trilogy/Readings.lean, `read_null_introducing`).
NUMBER_READ_TARGETS = frozenset(
    {
        DataType.INTEGER,
        DataType.BIGINT,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.NUMBER,
        DataType.NUMERIC,
    }
)
TEXT_READ_TARGETS = NUMBER_READ_TARGETS | {DataType.DATE}


def base_type(datatype: Any) -> Any:
    return getattr(datatype, "data_type", datatype)


def cast_reads_text(source: Any, target: Any) -> bool:
    """A cast from a string that can have no reading (a bool included)."""
    return base_type(source) == DataType.STRING and base_type(target) in (
        TEXT_READ_TARGETS | {DataType.BOOL}
    )


def cast_introduces_null(arguments: list[Any]) -> bool:
    """A CAST(value, type) whose value is a string read as another type."""
    return len(arguments) == 2 and cast_reads_text(
        arg_to_datatype(arguments[0]), arguments[1]
    )
