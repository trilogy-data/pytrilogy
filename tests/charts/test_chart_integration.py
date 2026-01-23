"""Integration tests for chart statement execution."""

import pytest

from trilogy import Executor
from trilogy.core.enums import ChartType
from trilogy.dialect.enums import Dialects
from trilogy.dialect.results import ChartResult


def test_execute_chart_statement():
    """Test full chart statement execution pipeline."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    results = list(
        exec.execute_text(
            "chart bar set x_axis: category set y_axis: value "
            'from select "A" as category, 10 as value;'
        )
    )

    assert len(results) == 1
    result = results[0]
    assert isinstance(result, ChartResult)
    assert result.data == [{"category": "A", "value": 10}]
    assert result.config.chart_type == ChartType.BAR
    assert result.config.x_fields == ["category"]
    assert result.config.y_fields == ["value"]
    # Altair chart should be generated
    assert result.chart is not None


def test_execute_chart_with_multiple_rows():
    """Test chart with multiple data rows using datasource."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    results = list(exec.execute_text("""
            key category string;
            property category.value int;

            datasource chart_data (
                cat: category,
                val: value
            )
            grain (category)
            query '''
            select 'A' as cat, 10 as val
            union all select 'B', 20
            union all select 'C', 30
            ''';

            chart bar set x_axis: category set y_axis: value
            from select category, value;
            """))

    # Last result should be the chart
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    assert len(chart_results) == 1
    result = chart_results[0]
    assert len(result.data) == 3
    assert result.config.chart_type == ChartType.BAR


def test_execute_line_chart():
    """Test line chart execution."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    results = list(
        exec.execute_text(
            "chart line set x_axis: x set y_axis: y " "from select 1 as x, 10 as y;"
        )
    )

    assert len(results) == 1
    result = results[0]
    assert result.config.chart_type == ChartType.LINE


def test_execute_chart_with_color():
    """Test chart with color field."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    results = list(
        exec.execute_text(
            "chart bar set x_axis: category set y_axis: value set color: region "
            'from select "A" as category, 10 as value, "north" as region;'
        )
    )

    assert len(results) == 1
    result = results[0]
    assert result.config.color_field == "region"


def test_chart_field_validation_x_field():
    """Test that chart validates x_field exists in select output."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    with pytest.raises(ValueError, match="not in select output"):
        list(
            exec.execute_text(
                "chart bar set x_axis: missing_field set y_axis: value "
                'from select "A" as category, 10 as value;'
            )
        )


def test_chart_field_validation_y_field():
    """Test that chart validates y_field exists in select output."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    with pytest.raises(ValueError, match="not in select output"):
        list(
            exec.execute_text(
                "chart bar set x_axis: category set y_axis: missing_field "
                'from select "A" as category, 10 as value;'
            )
        )


def test_chart_field_validation_color_field():
    """Test that chart validates optional color_field exists in select output."""
    exec = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())

    with pytest.raises(ValueError, match="not in select output"):
        list(
            exec.execute_text(
                "chart bar set x_axis: category set y_axis: value set color: missing "
                'from select "A" as category, 10 as value;'
            )
        )


def test_chart_result_protocol():
    """Test ChartResult implements ResultProtocol correctly."""
    from trilogy.core.statements.author import ChartConfig

    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["x"],
        y_fields=["y"],
    )
    result = ChartResult(chart="mock_chart", data=[{"x": 1, "y": 2}], config=config)

    # Test all ResultProtocol methods
    assert result.keys() == ["chart"]

    fetched = result.fetchone()
    assert fetched["chart"] == "mock_chart"

    fetched_all = result.fetchall()
    assert len(fetched_all) == 1
    assert fetched_all[0]["chart"] == "mock_chart"

    fetched_many = result.fetchmany(10)
    assert len(fetched_many) == 1

    # Test iteration
    for row in result:
        assert row["chart"] == "mock_chart"
