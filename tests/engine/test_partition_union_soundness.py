"""A `complete where` family answers an unfiltered query only when its arms
partition the discriminator's domain (`formal/Trilogy/Sourcing.lean`): every
value, NULL included, in exactly one arm."""

from datetime import date, datetime

import pytest

from trilogy import Dialects
from trilogy.core.enums import ComparisonOperator
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.core import DataType
from trilogy.core.processing.condition_utility import (
    partitions_domain,
    reduce_expression,
)

LT, LTE, GT, GTE, EQ, IS = (
    ComparisonOperator.LT,
    ComparisonOperator.LTE,
    ComparisonOperator.GT,
    ComparisonOperator.GTE,
    ComparisonOperator.EQ,
    ComparisonOperator.IS,
)


def _model(datatype: str, claims: list[str], rows: list[tuple[int, str]]) -> str:
    data = " union all ".join(f"select {i} as id, {v} as x" for i, v in rows)
    out = f"key id int;\nproperty id.x {datatype};\n"
    for n, claim in enumerate(claims):
        out += f"""partial datasource arm{n} (id: id, x: x)
grain (id)
complete where {claim}
query '''select * from ({data}) where {claim} ''';
"""
    return out


def _count(model: str, gate: str = "") -> int:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    return executor.execute_text(f"{gate}select count(id) as c;")[-1].fetchall()[0][0]


ENUM = "enum<string>['a', 'b']"
T = "'2024-01-01 00:00:00'::datetime"
T20 = "'2024-01-01 20:00:00'::datetime"


@pytest.mark.parametrize(
    "datatype, claims, rows",
    [
        (
            "float",
            ["x < 100.0", "x > 100.0"],
            [(1, "50.0"), (2, "100.0"), (3, "150.0")],
        ),
        (
            "datetime",
            [f"x <= {T}", f"x >= {T20}"],
            [(1, "'2024-01-01 10:00:00'::timestamp")],
        ),
        ("int", ["x <= 10", "x >= 5"], [(1, "1"), (2, "7"), (3, "20")]),
        ("int?", ["x <= 10", "x > 10"], [(1, "1"), (2, "NULL::int"), (3, "20")]),
        (f"{ENUM}?", ["x = 'a'", "x = 'b'"], [(1, "'a'"), (2, "NULL")]),
    ],
    ids=["float_cut", "datetime_subday_gap", "overlap", "nullable", "nullable_enum"],
)
def test_non_partition_is_not_a_source(datatype, claims, rows):
    with pytest.raises(UnresolvableQueryException):
        _count(_model(datatype, claims, rows))


@pytest.mark.parametrize(
    "datatype, claims, rows, gate, expected",
    [
        ("float", ["x < 100.0", "x >= 100.0"], [(1, "100.0"), (2, "150.0")], "", 2),
        (
            "datetime",
            [f"x < {T}", f"x >= {T}"],
            [(1, "'2024-01-01'::timestamp")],
            "",
            1,
        ),
        ("int", ["x <= 10", "x >= 11"], [(1, "1"), (2, "11")], "", 2),
        ("int", ["x <= 10", "10 < x"], [(1, "1"), (2, "11")], "", 2),
        (
            "int?",
            ["x <= 10", "x > 10", "x is null"],
            [(1, "1"), (2, "NULL::int"), (3, "20")],
            "",
            3,
        ),
        (
            "int?",
            ["x <= 10", "x > 10"],
            [(1, "1"), (2, "NULL::int"), (3, "20")],
            "where x is not null ",
            2,
        ),
        (
            f"{ENUM}?",
            ["x = 'a'", "x = 'b'", "x is null"],
            [(1, "'a'"), (2, "NULL")],
            "",
            2,
        ),
        (
            f"{ENUM}?",
            ["x = 'a'", "x = 'b'"],
            [(1, "'a'"), (2, "NULL")],
            "where x = 'a' ",
            1,
        ),
    ],
    ids=[
        "float",
        "datetime",
        "int_adjacent",
        "reversed",
        "null_arm",
        "gated",
        "enum_null_arm",
        "enum_gated",
    ],
)
def test_partition_unions(datatype, claims, rows, gate, expected):
    assert _count(_model(datatype, claims, rows), gate) == expected


def test_cells_decide_dense_and_discrete_domains():
    assert not reduce_expression(DataType.FLOAT, [(LT, 100.0), (GT, 100.0)])
    assert reduce_expression(DataType.FLOAT, [(LT, 100.0), (GTE, 100.0)])
    assert not reduce_expression(DataType.FLOAT, [(LTE, 10), (GTE, 11)])
    assert reduce_expression(DataType.INTEGER, [(LTE, 10), (GTE, 11)])
    assert not reduce_expression(DataType.INTEGER, [(LTE, 9.5), (GTE, 10.4)])
    assert reduce_expression(
        DataType.DATE, [(LTE, date(2024, 1, 1)), (GT, date(2024, 1, 1))]
    )
    t = datetime(2024, 1, 1)
    assert not reduce_expression(
        DataType.DATETIME, [(LTE, t), (GTE, datetime(2024, 1, 1, 20))]
    )
    assert not reduce_expression(DataType.INTEGER, [(LTE, 10), (GT, 10)], nullable=True)
    assert reduce_expression(
        DataType.INTEGER, [(LTE, 10), (GT, 10), (IS, None)], nullable=True
    )


def test_partition_requires_disjoint_arms():
    assert reduce_expression(DataType.INTEGER, [(LTE, 10), (GTE, 5)])
    assert not partitions_domain(DataType.INTEGER, [(LTE, 10), (GTE, 5)])
    assert partitions_domain(DataType.INTEGER, [(LT, 5), (EQ, 5), (GT, 5)])
    assert partitions_domain(DataType.BOOL, [(EQ, True), (EQ, False)])
    assert not partitions_domain(DataType.BOOL, [(EQ, True), (EQ, False)], True)


def test_union_answer_projects_in_select_order():
    model = _model("int", ["x <= 10", "x > 10"], [(1, "1"), (2, "20")])
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    result = executor.execute_text("select x, id as row_id order by row_id asc;")[-1]
    assert list(result.keys()) == ["x", "row_id"]
    assert result.fetchall() == [(1, 1), (20, 2)]
