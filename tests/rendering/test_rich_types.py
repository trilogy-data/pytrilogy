from decimal import Decimal

from trilogy.core.models.core import DataType, TraitDataType
from trilogy.rendering.rich_types import (
    axis_label_expr,
    currency_symbol,
    format_currency,
    format_value,
    is_numeric,
)

_USD = TraitDataType(type=DataType.FLOAT, traits=["usd"])
_EUR = TraitDataType(type=DataType.NUMERIC, traits=["eur"])
_PERCENT = TraitDataType(type=DataType.FLOAT, traits=["percent"])


def test_currency_symbol():
    assert currency_symbol(_USD) == "$"
    assert currency_symbol(_EUR) == "€"
    assert currency_symbol(DataType.FLOAT) is None
    assert currency_symbol(_PERCENT) is None


def test_format_currency_groups_and_trims():
    assert format_currency(Decimal("143000.0"), "$") == "$143,000"
    assert format_currency(350.5, "$") == "$350.50"
    assert format_currency(-5000, "$") == "-$5,000"


def test_format_value():
    assert format_value(Decimal("120000"), _USD) == "$120,000"
    assert format_value(340, DataType.INTEGER) == "340"
    assert format_value(None, _USD) == ""
    assert format_value("North", DataType.STRING) == "North"


def test_is_numeric():
    assert is_numeric(_USD) is True
    assert is_numeric(DataType.INTEGER) is True
    assert is_numeric(DataType.STRING) is False


def test_axis_label_expr():
    assert axis_label_expr(_USD) == "'$' + format(datum.value, ',.0f')"
    assert axis_label_expr(DataType.FLOAT) is None
