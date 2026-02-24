from pathlib import Path

from pytest import raises

from trilogy import Environment
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.execute import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    DataType,
    Purpose,
    raise_helpful_join_validation_error,
)


def test_raise_helpful_join_validation_error():

    with raises(InvalidSyntaxException):
        raise_helpful_join_validation_error(
            concepts=[
                BuildConcept(
                    name="test_concept",
                    canonical_name="test_concept",
                    datatype=DataType.INTEGER,
                    purpose=Purpose.KEY,
                    build_is_aggregate=False,
                    grain=BuildGrain(),
                )
            ],
            left_datasource=BuildDatasource(name="left_ds", columns=[], address="agsg"),
            right_datasource=BuildDatasource(
                name="right_ds", columns=[], address="agsg"
            ),
        )
    with raises(InvalidSyntaxException):
        raise_helpful_join_validation_error(
            concepts=[
                BuildConcept(
                    name="test_concept",
                    canonical_name="test_concept",
                    datatype=DataType.INTEGER,
                    purpose=Purpose.KEY,
                    build_is_aggregate=False,
                    grain=BuildGrain(),
                )
            ],
            left_datasource=None,
            right_datasource=BuildDatasource(
                name="right_ds", columns=[], address="agsg"
            ),
        )
    with raises(InvalidSyntaxException):
        raise_helpful_join_validation_error(
            concepts=[
                BuildConcept(
                    name="test_concept",
                    canonical_name="test_concept",
                    datatype=DataType.INTEGER,
                    purpose=Purpose.KEY,
                    build_is_aggregate=False,
                    grain=BuildGrain(),
                )
            ],
            left_datasource=BuildDatasource(name="left_ds", columns=[], address="agsg"),
            right_datasource=None,
        )


def test_build_datasource_source_resolution():
    env = Environment(working_path=Path(__file__).parent)
    env.parse(
        """
key x int;
property x.val float;
auto _total_val <- sum(val) by *;

datasource totals(
_total_val
    )
address abc_123l;

select
    sum(val) as total_val;
   """
    )
    build_env = env.materialize_for_select()
    built_ds = build_env.datasources["totals"]
    total_val = build_env.concepts["local.total_val"]
    built_ds.get_alias(total_val)
