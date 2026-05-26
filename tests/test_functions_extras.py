"""Coverage for trilogy.core.functions helpers and FunctionFactory edge cases."""

from __future__ import annotations

import pytest

from trilogy.core.enums import DatePart, FunctionType, Purpose
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.functions import (
    FunctionFactory,
    get_date_part_output,
    get_date_trunc_output,
)


def test_get_date_part_output_year_month_day():
    # Smoke-test the common branches return TraitDataType-wrapped INTEGER.
    out_year = get_date_part_output([None, DatePart.YEAR])
    out_month = get_date_part_output([None, DatePart.MONTH])
    out_day = get_date_part_output([None, DatePart.DAY])
    assert "year" in out_year.traits
    assert "month" in out_month.traits
    assert "day" in out_day.traits


def test_get_date_part_output_rejects_unknown():
    class _Bogus:
        pass

    with pytest.raises(InvalidSyntaxException, match="Date part not supported"):
        get_date_part_output([None, _Bogus()])


def test_get_date_trunc_output_returns_date_or_datetime():
    from trilogy.core.models.core import DataType

    assert get_date_trunc_output([None, DatePart.YEAR]) == DataType.DATE
    assert get_date_trunc_output([None, DatePart.HOUR]) == DataType.DATETIME


def test_get_date_trunc_output_rejects_unknown():
    class _Bogus:
        pass

    with pytest.raises(InvalidSyntaxException, match="Date truncation not supported"):
        get_date_trunc_output([None, _Bogus()])


def test_function_factory_unknown_operator_raises():
    """An operator that isn't a real FunctionType member raises 'not in registry'."""

    class _Fake:
        pass

    factory = FunctionFactory()
    with pytest.raises(ValueError, match="not in registry"):
        factory.create_function([], _Fake())  # type: ignore[arg-type]


def test_function_factory_requires_environment_for_args():
    factory = FunctionFactory(environment=None)
    # FunctionType.COUNT is registered and accepts args.
    with pytest.raises(ValueError, match="Environment required"):
        factory.create_function([1], FunctionType.COUNT)


def test_purpose_metric_for_aggregate_when_output_purpose_unset(test_environment):
    """The fallback "AGGREGATE → METRIC" branch fires when the function's
    config has no `output_purpose` set — the default for aggregate ops like
    sum/avg/count when invoked from FunctionFactory."""
    factory = FunctionFactory(environment=test_environment)
    func = factory.create_function(
        [test_environment.concepts["order_id"]], FunctionType.COUNT
    )
    assert func.output_purpose == Purpose.METRIC
