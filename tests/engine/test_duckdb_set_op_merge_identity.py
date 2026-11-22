"""Regression for #1078: merging two same-identifier set-op sources (two
aggregates sharing an intersect/except subtree) dropped ``set_operator``,
reverting the merge to UNION_ALL and leaving a join bound to a source no CTE
was built for."""

import pytest

from trilogy import Dialects
from trilogy.core.enums import SetOperator, SourceType
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import QueryDatasource

MODEL = """
key sale_id int;
property sale_id.channel string;
property sale_id.brand int;
property sale_id.amt float;
datasource sales (sale_id: sale_id, channel: channel, brand: brand, amt: amt)
grain (sale_id)
query '''
select 1 sale_id, 'STORE' channel, 10 brand, 1.0 amt union all
select 2, 'WEB', 10, 2.0 union all
select 3, 'STORE', 20, 4.0 union all
select 4, 'WEB', 30, 8.0
''';
"""

# STORE brands {10, 20}; WEB brands {10, 30}
SET_OPS = {
    "intersect": ("intersect", "STORE", "WEB", [10]),
    "except": ("except", "STORE", "WEB", [20]),
    "except_reversed": ("except", "WEB", "STORE", [30]),
    "union": ("union", "STORE", "WEB", [10, 10, 20, 30]),
}

# (final select, expected row given the set-op keys)
SELECTS = {
    "sum_count": (
        "sum(kept.amt) as total, count(kept.brand) as n",
        lambda keys: (15.0, 3),
    ),
    "sum_count_grain": (
        "sum(kept.amt) as total, count(grain(kept.brand)) as n",
        lambda keys: (15.0, 3),
    ),
    "sum_max_min": (
        "sum(kept.amt) as total, max(kept.amt) as mx, min(kept.brand) as mn",
        lambda keys: (15.0, 8.0, 10),
    ),
    "with_set_op_count": (
        "sum(kept.amt) as total, count(kept.brand) as n, count(both.k) as m",
        lambda keys: (15.0, 3, len(set(keys))),
    ),
    "set_op_aggregates": (
        "count(both.k) as a, sum(both.k) as b",
        lambda keys: (len(set(keys)), sum(keys)),
    ),
}


def _query(op: str, first: str, second: str, select: str) -> str:
    return f"""
with both as {op}(
    (where channel = '{first}' select brand as b),
    (where channel = '{second}' select brand as b)
) -> (k);

with kept as
select brand, sum(amt) as amt
subset join both.k = brand;

select {select};
"""


@pytest.mark.parametrize("select_name", SELECTS)
@pytest.mark.parametrize("op_name", SET_OPS)
def test_multiple_aggregates_over_set_op_subset_join(
    op_name: str, select_name: str
) -> None:
    op, first, second, keys = SET_OPS[op_name]
    select, expected = SELECTS[select_name]
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(MODEL)
    sql = ex.generate_sql(_query(op, first, second, select))[-1]
    if op != "union":
        assert op.upper() in sql
    row = ex.execute_raw_sql(sql).fetchall()
    assert [tuple(float(v) for v in r) for r in row] == [
        tuple(float(v) for v in expected(keys))
    ]


def _arm(limit: int) -> QueryDatasource:
    return QueryDatasource(
        input_concepts=[],
        output_concepts=[],
        datasources=[],
        grain=BuildGrain(),
        joins=[],
        source_map={},
        limit=limit,
    )


def _set_op(operator: SetOperator) -> QueryDatasource:
    # arm order deliberately unsorted: a UNION_ALL would re-sort it
    return QueryDatasource(
        input_concepts=[],
        output_concepts=[],
        datasources=[_arm(2), _arm(1)],
        grain=BuildGrain(),
        joins=[],
        source_map={},
        source_type=SourceType.UNION,
        set_operator=operator,
    )


@pytest.mark.parametrize("operator", [SetOperator.EXCEPT, SetOperator.INTERSECT])
def test_merge_preserves_set_operator_and_arm_order(operator: SetOperator) -> None:
    left, right = _set_op(operator), _set_op(operator)
    assert left.identifier == right.identifier
    merged = left + right
    assert merged.set_operator is operator
    assert [d.identifier for d in merged.datasources] == [
        d.identifier for d in left.datasources
    ]
    assert merged.identifier == left.identifier


def test_merge_union_all_identifier_stable() -> None:
    left, right = _set_op(SetOperator.UNION_ALL), _set_op(SetOperator.UNION_ALL)
    merged = left + right
    assert merged.set_operator is SetOperator.UNION_ALL
    assert merged.identifier == left.identifier
