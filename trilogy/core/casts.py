"""What a cast means, whatever the engine (formal/Trilogy/Casts.lean).

A string reads as a bool as DuckDB reads it: one of these spellings, in any
case. A parse-time fold of a literal and every dialect's rendering share it.
"""

import re
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, localcontext
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
# with an optional exponent (a float also reads `inf`, `infinity` and `nan`);
# as a date when it is a year, then a month and a day of one or two digits
# split by `-` or `/`, naming a real day, optionally followed by a time of day;
# as a datetime when it is such a date, a time of day and an optional zone
# (`Z`, `+HH`, `+HHMM`, `+HH:MM`), naming the UTC instant. A number past the
# target's range has no reading. Any other string has no reading and casts to
# NULL (formal/Trilogy/Readings.lean, `read_number_exact`;
# formal/Trilogy/Instants.lean, `read_instant_exact`). The patterns are
# POSIX/RE2 and need no backslash or brace, so every dialect's string literal
# and parameter syntax holds them as written.
SPACE = " \t\n\r"
_S = "[[:space:]]*"
NUMERAL = "[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?"
FLOAT_SPECIAL = "[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?|[nN][aA][nN]"
DAY = "[0-9][0-9][0-9][0-9](-[0-9][0-9]?-[0-9][0-9]?|/[0-9][0-9]?/[0-9][0-9]?)"
CLOCK = "[ T]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]+)?"
ZONE = "Z|[+-]([01][0-9]|2[0-3])(:?[0-5][0-9])?"
NUMBER_PATTERN = f"^{_S}{NUMERAL}{_S}$"
FLOAT_PATTERN = f"^{_S}({NUMERAL}|{FLOAT_SPECIAL}){_S}$"
DATE_PATTERN = f"^{_S}{DAY}({CLOCK})?{_S}$"
DATETIME_PATTERN = f"^{_S}{DAY}({CLOCK}({ZONE})?)?{_S}$"
# a read string names a zone iff a `Z` or a sign follows the seconds
ZONED = ":[0-9.]*[Z+-]"
INT64 = (-(2**63), 2**63 - 1)
POSITIVE_INFINITY = ("inf", "+inf", "infinity", "+infinity")
NEGATIVE_INFINITY = ("-inf", "-infinity")


def _python(pattern: str) -> re.Pattern:
    return re.compile(pattern.replace("[[:space:]]", "[ \t\n\r]"))


_NUMBER, _FLOAT, _DATE, _DATETIME = map(
    _python, (NUMBER_PATTERN, FLOAT_PATTERN, DATE_PATTERN, DATETIME_PATTERN)
)
_INSTANT = re.compile(
    r"(\d{4})[-/](\d\d?)[-/](\d\d?)"
    r"(?:[ T](\d\d):(\d\d):(\d\d)(?:\.(\d+))?(?:Z|([+-])(\d\d):?(\d\d)?)?)?"
)


def text_to_number(value: str) -> Decimal | None:
    """The exact value a string names, or None when it has no reading."""
    if not _NUMBER.fullmatch(value):
        return None
    return Decimal(value.strip(SPACE))


def _rounded(number: Decimal, scale: int) -> Decimal | None:
    """`number` rounded half away to `scale` places, or None when it is past
    any type's range (a 400-digit exponent)."""
    if number.adjusted() > 76:
        return None
    with localcontext(prec=160):
        return number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


def text_to_int(value: str, bounds: tuple[int, int] = INT64) -> int | None:
    """The integer read, or None past `bounds` (the target's range)."""
    number = text_to_number(value)
    read = None if number is None else _rounded(number, 0)
    if read is None or not bounds[0] <= read <= bounds[1]:
        return None
    return int(read)


def text_to_decimal(
    value: str, scale: int, precision: int | None = None
) -> Decimal | None:
    """The decimal read, or None with more than `precision - scale` whole
    digits."""
    number = text_to_number(value)
    read = None if number is None else _rounded(number, scale)
    if read is None:
        return None
    if precision is not None and abs(read) >= Decimal(10) ** (precision - scale):
        return None
    return read


def text_to_float(value: str) -> float | None:
    if not _FLOAT.fullmatch(value):
        return None
    return float(value.strip(SPACE))


def text_to_date(value: str) -> date | None:
    if not _DATE.fullmatch(value):
        return None
    year, month, day = re.split("[-/ T]", value.strip(SPACE))[:3]
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def text_to_datetime(value: str) -> datetime | None:
    """The UTC instant a string names, at microseconds (a longer fraction is
    truncated), naive; a string with no zone is read as UTC."""
    if not _DATETIME.fullmatch(value):
        return None
    year, month, day, hour, minute, second, fraction, sign, zh, zm = _INSTANT.fullmatch(
        value.strip(SPACE)
    ).groups()  # type: ignore[union-attr]
    try:
        local = datetime(
            int(year),
            int(month),
            int(day),
            int(hour or 0),
            int(minute or 0),
            int(second or 0),
            int((fraction or "").ljust(6, "0")[:6]),
        )
        offset = timedelta(hours=int(zh or 0), minutes=int(zm or 0))
        return local + offset if sign == "-" else local - offset
    except (ValueError, OverflowError):
        return None


def text_to_instant(value: str, target: Any) -> date | datetime | None:
    """A string read as a date, a naive UTC datetime or an aware timestamp."""
    if target == DataType.DATE:
        return text_to_date(value)
    read = text_to_datetime(value)
    if read is None or target == DataType.DATETIME:
        return read
    return read.replace(tzinfo=UTC)


# `date(s)`, `datetime(s)` and `timestamp(s)` of a string are its cast
READ_FUNCTION_TARGETS = {
    "date": DataType.DATE,
    "datetime": DataType.DATETIME,
    "timestamp": DataType.TIMESTAMP,
}


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
INSTANT_READ_TARGETS = frozenset({DataType.DATETIME, DataType.TIMESTAMP})
TEXT_READ_TARGETS = NUMBER_READ_TARGETS | {DataType.DATE} | INSTANT_READ_TARGETS
FLOAT_READ_TARGETS = frozenset({DataType.FLOAT, DataType.DOUBLE, DataType.NUMBER})


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
