from decimal import Decimal

from trilogy import Dialects
from trilogy.core.models.core import (
    DataType,
    NumericType,
    TraitDataType,
)
from trilogy.dialect.base import union_arm_cast_target

MODEL = r"""
key k1 int;
key k2 int;

property k1.amt numeric(15,2);
property k2.pad float;

auto all_k <- union(k1, k2);
auto all_amt <- union(amt, pad);

datasource sales (
    x: k1,
    y: amt)
grain (k1)
query '''
select 1 as x, cast(0.1 as numeric(15,2)) as y
union all
select 2, cast(0.2 as numeric(15,2))
''';

datasource returns (
    x: k2,
    y: pad)
grain (k2)
query '''
select 3 as x, cast(0 as float) as y
union all
select 4, cast(0 as float)
''';
"""


def test_union_arm_cast_target():
    numeric = NumericType(precision=15, scale=2)
    assert union_arm_cast_target(DataType.FLOAT, numeric) == numeric
    assert union_arm_cast_target(DataType.INTEGER, DataType.FLOAT) == DataType.FLOAT
    assert union_arm_cast_target(numeric, numeric) is None
    assert union_arm_cast_target(DataType.STRING, DataType.STRING) is None
    assert union_arm_cast_target(DataType.STRING, DataType.INTEGER) is None
    assert (
        union_arm_cast_target(
            TraitDataType(type=DataType.FLOAT, traits=["usd"]), numeric
        )
        == numeric
    )
    assert (
        union_arm_cast_target(numeric, TraitDataType(type=numeric, traits=["usd"]))
        is None
    )


def test_union_float_arm_coerced_to_numeric():
    exec = Dialects.DUCK_DB.default_executor()
    query = MODEL + "\nselect all_k, all_amt order by all_k asc;"
    sql = exec.generate_sql(query)[-1]
    assert 'cast("returns"."y" as numeric(15,2))' in sql, sql
    assert 'cast("sales"."y"' not in sql, sql

    rows = exec.execute_query(query).fetchall()
    assert [r.all_amt for r in rows] == [
        Decimal("0.10"),
        Decimal("0.20"),
        Decimal("0.00"),
        Decimal("0.00"),
    ]


def test_union_sum_does_not_drift():
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(
        MODEL + "\nwith u as select all_k, all_amt;\nselect sum(u.all_amt) -> total;"
    ).fetchall()
    assert rows[0].total == Decimal("0.30")


def test_double_accepted_by_aggregate_functions():
    # double is an 8-byte float; it must be a valid input to sum/avg/etc. so
    # authors can use `cast(0 as double)` placeholders without float32 drift.
    exec = Dialects.DUCK_DB.default_executor()
    q = (
        "const vals <- unnest([112458734.70, 0.01, 0.02]);\n"
        "with u as select cast(vals as double) as d;\n"
        "select sum(u.d) as s, avg(u.d) as a, max(u.d) as mx, min(u.d) as mn;"
    )
    row = exec.execute_query(q).fetchall()[0]
    # float32 would drift to ~112458736.03; double stays precise
    assert abs(row.s - 112458734.73) < 1e-6, row.s
    assert row.mx == 112458734.70
    assert row.mn == 0.01
