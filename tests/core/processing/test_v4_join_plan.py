import pytest

from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import strategy_builder
from trilogy.core.processing.v4_helper.join_plan import JoinPlan, PairingKind

MODEL = """
key order_id int;
key customer_id int;
property customer_id.customer_name string;
property customer_id.region string;
property order_id.amount float;

datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    amount: amount,
)
grain (order_id)
query '''
select 1 order_id, 10 customer_id, 5.0 amount
union all select 2, 10, 7.0
union all select 3, 11, 1.0
''';

datasource customers (
    customer_id: customer_id,
    customer_name: customer_name,
    region: region,
)
grain (customer_id)
query '''
select 10 customer_id, 'a' customer_name, 'east' region
union all select 11, 'b', 'west'
''';
"""


@pytest.fixture
def plans(monkeypatch) -> list[JoinPlan]:
    captured: list[JoinPlan] = []
    planned = strategy_builder.plan_final_join

    def capture(*args, **kwargs) -> JoinPlan:
        plan = planned(*args, **kwargs)
        captured.append(plan)
        return plan

    monkeypatch.setattr(strategy_builder, "plan_final_join", capture)
    return captured


def _run(query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL)
    return [tuple(row) for row in executor.execute_text(query)[-1].fetchall()]


def _pairing_keys(plan: JoinPlan) -> set[str]:
    return set(plan.join_keys)


def test_aggregate_beside_its_dimension_is_a_lookup_on_the_key(plans):
    rows = _run(
        "select customer_id, customer_name, sum(amount) -> total order by customer_id;"
    )
    assert rows == [(10, "a", 12.0), (11, "b", 1.0)]
    merge = plans[-1]
    assert _pairing_keys(merge) == {"local.customer_id"}
    assert {p.kind for p in merge.pairings} == {PairingKind.LOOKUP}
    assert len(merge.islands()) == 1


def test_root_carries_only_the_key_its_sibling_pairs_on(plans):
    _run("select region, sum(amount) by customer_id -> total, customer_id;")
    roots = [carry for carry in plans[0].carry_demands.values()]
    assert roots and all(carry <= {"local.customer_id"} for carry in roots)


def test_global_scalar_beside_rows_is_a_scalar_pairing(plans):
    rows = _run(
        "select customer_id, sum(amount) -> total, sum(amount) by * -> grand "
        "order by customer_id;"
    )
    assert rows == [(10, 12.0, 13.0), (11, 1.0, 13.0)]
    merge = plans[-1]
    scalar = [p for p in merge.pairings if p.kind == PairingKind.SCALAR]
    assert scalar
    assert all(not p.keys and not p.implied for p in scalar)
    assert "local.customer_id" not in merge.permitted_keys
    assert len(merge.islands()) == 1
