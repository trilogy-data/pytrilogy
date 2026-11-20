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


UNFILTERED = """
key id int;
key bill_id int;
key ship_id int;
datasource orders (id: id, bill: bill_id, ship: ship_id) grain (id)
query '''select 1 id, 100 bill, 200 ship union all select 2, 101, 201''';
with buyers_a as select id, bill_id as cust_id;
with buyers_b as select id, ship_id as cust_id;
"""


def test_unfiltered_rowsets_beside_their_anchor_are_typed_inner():
    joins, rows = _joins_and_rows(
        UNFILTERED,
        "select id, buyers_a.cust_id as a, buyers_b.cust_id as b "
        "subset join buyers_a.id = id and buyers_b.id = id;",
    )
    assert [j.split(" JOIN ")[0] for j in joins] == ["INNER", "INNER"], joins
    assert rows == [(1, 100, 200), (2, 101, 201)]


TWINS = """
key cust_id int;
key order_id int;
property order_id.amount int;
key visit_id int;
property visit_id.pages int;
datasource orders (order_id, ?cust_id, amount) grain (order_id)
query '''select 1 order_id, 1 cust_id, 10 amount union all select 2, null, 20''';
datasource visits (visit_id, ?cust_id, pages) grain (visit_id)
query '''select 1 visit_id, 1 cust_id, 5 pages union all select 2, null, 7''';
"""


def test_twin_reads_of_one_complete_key_are_typed_inner():
    joins, rows = _joins_and_rows(
        TWINS, "select cust_id, sum(amount) as spend, sum(pages) as seen;"
    )
    assert [j.split(" JOIN ")[0] for j in joins] == ["INNER"], joins
    assert "is not distinct from" in joins[0]
    assert rows == [(1, 10, 5), (None, 20, 7)]
