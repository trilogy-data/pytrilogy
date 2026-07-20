"""Display formatting driven by rich types (traits such as currency).

Tables and charts share these helpers so a `float::usd` concept renders as
money with the right symbol, numeric columns right-align, and so on.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from trilogy.core.models.core import DataType, TraitDataType

# Currency trait -> display symbol/prefix. Unlisted currencies render plain.
CURRENCY_SYMBOLS: Dict[str, str] = {
    "usd": "$",
    "cad": "$",
    "aud": "$",
    "nzd": "$",
    "mxn": "$",
    "ars": "$",
    "clp": "$",
    "cop": "$",
    "eur": "€",
    "gbp": "£",
    "jpy": "¥",
    "cny": "¥",
    "chf": "CHF ",
    "hkd": "HK$",
    "sgd": "S$",
    "inr": "₹",
    "krw": "₩",
    "rub": "₽",
    "brl": "R$",
    "zar": "R ",
    "try": "₺",
    "thb": "฿",
    "ils": "₪",
    "sek": "kr ",
    "nok": "kr ",
    "dkk": "kr ",
    "pln": "zł ",
}

_NUMERIC_BASE_TYPES = {
    DataType.INTEGER,
    DataType.BIGINT,
    DataType.FLOAT,
    DataType.NUMERIC,
    DataType.NUMBER,
}


def _traits(datatype: Any) -> List[str]:
    return list(datatype.traits) if isinstance(datatype, TraitDataType) else []


def _base_type(datatype: Any) -> Any:
    return datatype.type if isinstance(datatype, TraitDataType) else datatype


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def currency_symbol(datatype: Any) -> Optional[str]:
    """Symbol for a datatype carrying a known currency trait, else None."""
    for trait in _traits(datatype):
        symbol = CURRENCY_SYMBOLS.get(trait)
        if symbol is not None:
            return symbol
    return None


def is_numeric(datatype: Any) -> bool:
    """Whether a datatype's base type is numeric."""
    return _base_type(datatype) in _NUMERIC_BASE_TYPES


def is_hex_color(datatype: Any) -> bool:
    """Whether a datatype carries the `hex` trait (a hex color string)."""
    return "hex" in _traits(datatype)


# std.date integer part types: label as plain integers (2020, not 2,020).
_INTEGER_DATE_PART_TRAITS = {
    "year",
    "decade",
    "century",
    "month",
    "quarter",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "day_of_week",
}


def is_integer_date_part(datatype: Any) -> bool:
    """Whether a datatype carries a std.date integer part trait (year, ...)."""
    return not _INTEGER_DATE_PART_TRAITS.isdisjoint(_traits(datatype))


def _format_magnitude(value: Any) -> str:
    """Group digits; show up to two decimals, dropping a trailing `.00`."""
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.2f}"


def format_currency(value: Any, symbol: str) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}{symbol}{_format_magnitude(abs(value))}"


def format_value(value: Any, datatype: Any) -> str:
    """Render a single value for display, applying currency formatting."""
    if value is None:
        return ""
    symbol = currency_symbol(datatype)
    if symbol is not None and _is_number(value):
        return format_currency(value, symbol)
    return str(value)


def field_datatype(layer: Any, field_name: str) -> Any:
    """Datatype of a chart layer's field, matched by safe address."""
    query = layer.query
    if query is None:
        return None
    for column in query.output_columns:
        if column.safe_address == field_name:
            return column.datatype
    return None


def axis_label_expr(datatype: Any) -> Optional[str]:
    """A Vega-Lite `labelExpr` that formats a currency axis, else None."""
    symbol = currency_symbol(datatype)
    if symbol is None:
        return None
    return f"'{symbol}' + format(datum.value, ',.0f')"
