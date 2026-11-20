import pytest

from tests.engine.test_padded_null_pairing import TWO_REGIONS
from trilogy import Dialects, Environment
from trilogy.core.optimizations.value_set_join_upgrade import (
    UpgradeOuterFromKeySetEquivalence,
)

ROWSETS = """
key cust_id int;
key order_id int;
property order_id.yr int;
datasource orders (order_id, cust_id, yr) grain (order_id)
query '''select 10 order_id, 1 cust_id, 2001 yr union all select 11, 2, 2002
union all select 12, 1, 2002''';
rowset early <- where yr = 2001 select cust_id as k;
rowset late <- where yr = 2002 select cust_id as k;
"""


@pytest.fixture(autouse=True)
def planner_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        UpgradeOuterFromKeySetEquivalence,
        "optimize",
        lambda self, cte, inverse_map: (False, None),
    )


def _joins_and_rows(model: str, query: str) -> tuple[list[str], list[tuple]]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(query)[-1]
    joins = [line.strip() for line in sql.splitlines() if " JOIN " in line]
    rows = executor.execute_text(query)[-1].fetchall()
    return joins, sorted(rows, key=str)


def test_subset_join_into_a_rowset_boundary_keeps_only_the_anchor():
    joins, rows = _joins_and_rows(
        ROWSETS, "select late.k, early.k subset join early.k = late.k;"
    )
    assert [j.split(" JOIN ")[0] for j in joins] == ["LEFT OUTER"], joins
    assert rows == [(1, 1), (2, 2)]


def test_partial_fact_beside_its_complete_dimension_is_typed_inner():
    joins, rows = _joins_and_rows(
        TWO_REGIONS, "select bucket, sum(amount) as s where name is null;"
    )
    customer_join = next(j for j in joins if "customer_id" in j)
    assert customer_join.startswith("INNER JOIN"), joins
    assert rows == [("z", None)]
