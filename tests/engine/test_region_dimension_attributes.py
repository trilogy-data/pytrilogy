"""A `~` region with two demanded dimension attributes, one read by a
derivation on the FACT stream and one plain (docs/keyspace_phase_plan.md,
open item 3). The twin is the materialization oracle; hand rows are the
contract where both twins could be wrong together.

Three planner bugs pinned here, each pre-existing:

- the aggregate over `tier_amount` beside `name` INNER-joined the domain: the
  solid stream reads the domain's dimension scan for `tier`, so the guard
  upgrade read the span as rendering exclusively from the solid side and a
  FINAL non-null proof narrowed the domain's LEFT (`join_upgrade._downgrade`);
- `ensure_content_preservation` preserved the users dimension after
  `orders RIGHT JOIN items`, padding the solid stream and evaluating
  `state_qty`'s ELSE on user 3;
- two row streams at FD-related grains (`pair_cost` at (order, product),
  `state_qty` at item) paired on the one requested key and fanned out.
"""

import pytest

from trilogy import Dialects
from trilogy.executor import Executor

_BASE = """
key customer_id int;
property customer_id.name string;
property customer_id.tier string;
key order_id int;
property order_id.amount int;

datasource customers (customer_id: customer_id, name: name, tier: tier)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name, 'gold' as tier
union all select 2, 'bob', 'silver'
union all select 3, 'cat', 'gold' ''';
"""
_ORDERS = """select 100 as order_id, 1 as customer_id, 10 as amount
union all select 101, 1, 20
union all select 102, 2, 30"""

DERIVED = _BASE + f"""
datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount)
grain (order_id)
query '''{_ORDERS}''';

auto tier_amount <- case when tier = 'gold' then amount * 2 else amount end;
auto silver_amount <- case when tier = 'silver' then amount else 0 end;
auto big <- amount > 15;
auto tier_label <- concat(tier, ':', name);
"""

MATERIALIZED = _BASE + f"""
property order_id.tier_amount int;
property order_id.silver_amount int;
property order_id.big bool;
property customer_id.tier_label string;

datasource orders (
    order_id: order_id, customer_id: ~customer_id, amount: amount,
    ta: tier_amount, sa: silver_amount, big: big,
)
grain (order_id)
query '''select o.*,
    case when c.tier = 'gold' then o.amount * 2 else o.amount end as ta,
    case when c.tier = 'silver' then o.amount else 0 end as sa,
    o.amount > 15 as big
from ({_ORDERS}) o
join (select 1 as customer_id, 'gold' as tier union all select 2, 'silver') c
    on o.customer_id = c.customer_id''';

datasource customer_labels (customer_id: customer_id, tl: tier_label)
grain (customer_id)
query '''select 1 as customer_id, 'gold:ann' as tl
union all select 2, 'silver:bob'
union all select 3, 'gold:cat' ''';
"""

# orders: 100 (ann, 10 -> 20), 101 (ann, 20 -> 40), 102 (bob, 30 -> 30); cat none
TWIN_CASES: list[tuple[str, list[tuple]]] = [
    (
        "select customer_id, name, tier_amount",
        [(1, "ann", 20), (1, "ann", 40), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier, tier_amount",
        [
            (1, "ann", "gold", 20),
            (1, "ann", "gold", 40),
            (2, "bob", "silver", 30),
            (3, "cat", "gold", None),
        ],
    ),
    (
        "select name, tier, tier_amount",
        [
            ("ann", "gold", 20),
            ("ann", "gold", 40),
            ("bob", "silver", 30),
            ("cat", "gold", None),
        ],
    ),
    (
        "select customer_id, name, sum(tier_amount) as t",
        [(1, "ann", 60), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier, sum(tier_amount) as t",
        [(1, "ann", "gold", 60), (2, "bob", "silver", 30), (3, "cat", "gold", None)],
    ),
    (
        "select customer_id, name, sum(silver_amount) as s",
        [(1, "ann", 0), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, coalesce(sum(tier_amount), 0) as t",
        [(1, "ann", 60), (2, "bob", 30), (3, "cat", 0)],
    ),
    (
        "select customer_id, name, tier, sum(tier_amount) as t where tier = 'gold'",
        [(1, "ann", "gold", 60), (3, "cat", "gold", None)],
    ),
    (
        "select customer_id, name, tier_amount where tier_amount is null",
        [(3, "cat", None)],
    ),
    (
        "select order_id, name, tier, tier_amount",
        [
            (100, "ann", "gold", 20),
            (101, "ann", "gold", 40),
            (102, "bob", "silver", 30),
            (None, "cat", "gold", None),
        ],
    ),
    (
        "select name, tier, count(order_id) as n",
        [("ann", "gold", 2), ("bob", "silver", 1), ("cat", "gold", 0)],
    ),
    (
        "select tier, count(customer_id) as c, sum(tier_amount) as t",
        [("gold", 2, 60), ("silver", 1, 30)],
    ),
    (
        "select customer_id, name, tier_amount, big",
        [
            (1, "ann", 20, False),
            (1, "ann", 40, True),
            (2, "bob", 30, True),
            (3, "cat", None, None),
        ],
    ),
    (
        "select name, tier_label, sum(tier_amount) as t",
        [("ann", "gold:ann", 60), ("bob", "silver:bob", 30), ("cat", "gold:cat", None)],
    ),
    (
        "select customer_id, name, tier_amount where big or big is null",
        [(1, "ann", 40), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, silver_amount",
        [(1, "ann", 0), (2, "bob", 30), (3, "cat", None)],
    ),
]

# two families: users {1 CA west, 2 NY east, 3 TX south (no order)}, products
# {10 A 2, 20 B 3, 30 C 4 (never sold)}; items bind ~product_id and ~user_id
_FBASE = """
key user_id int;
property user_id.state string;
property user_id.region string;
key product_id int;
property product_id.brand string;
property product_id.cost int;
key order_id int;
property order_id.amount int;
key item_id int;
property item_id.qty int;

datasource users (user_id: user_id, state: state, region: region)
grain (user_id)
query '''select 1 as user_id, 'CA' as state, 'west' as region
union all select 2, 'NY', 'east'
union all select 3, 'TX', 'south' ''';

datasource products (product_id: product_id, brand: brand, cost: cost)
grain (product_id)
query '''select 10 as product_id, 'A' as brand, 2 as cost
union all select 20, 'B', 3
union all select 30, 'C', 4''';

datasource orders (order_id: order_id, user_id: ~user_id, amount: amount)
grain (order_id)
query '''select 100 as order_id, 1 as user_id, 50 as amount
union all select 101, 2, 60
union all select 102, 1, 70''';
"""
_ITEMS = """select 1000 as item_id, 100 as order_id, 10 as product_id, 1 as user_id, 5 as qty
union all select 1001, 100, 20, 1, 7
union all select 1002, 101, 10, 2, 11
union all select 1003, 102, 20, 1, 13"""

FORKED_DERIVED = _FBASE + f"""
datasource items (
    item_id: item_id, order_id: order_id, product_id: ~product_id,
    user_id: ~user_id, qty: qty,
)
grain (item_id)
query '''{_ITEMS}''';

auto pair_cost <- cost * amount;
auto state_qty <- case when state = 'CA' then qty else 0 end;
auto total_pair_cost <- sum(pair_cost);
"""

FORKED_MATERIALIZED = _FBASE + f"""
property item_id.pair_cost int;
property item_id.state_qty int;

datasource items (
    item_id: item_id, order_id: order_id, product_id: ~product_id,
    user_id: ~user_id, qty: qty, pc: pair_cost, sq: state_qty,
)
grain (item_id)
query '''select i.*,
    p.cost * o.amount as pc,
    case when u.state = 'CA' then i.qty else 0 end as sq
from ({_ITEMS}) i
join (select 10 as product_id, 2 as cost union all select 20, 3) p
    on i.product_id = p.product_id
join (select 100 as order_id, 50 as amount union all select 101, 60 union all select 102, 70) o
    on i.order_id = o.order_id
join (select 1 as user_id, 'CA' as state union all select 2, 'NY') u
    on i.user_id = u.user_id''';

auto total_pair_cost <- sum(pair_cost);
"""

# items: 1000 (o100 u1 p10 q5 pc100 sq5) 1001 (o100 u1 p20 q7 pc150 sq7)
#        1002 (o101 u2 p10 q11 pc120 sq0) 1003 (o102 u1 p20 q13 pc210 sq13)
FORKED_CASES: list[tuple[str, list[tuple]]] = [
    (
        "select item_id, product_id, pair_cost, state_qty",
        [
            (1000, 10, 100, 5),
            (1001, 20, 150, 7),
            (1002, 10, 120, 0),
            (1003, 20, 210, 13),
            (None, 30, None, None),
        ],
    ),
    (
        "select item_id, product_id, user_id, brand, region, pair_cost, state_qty",
        [
            (1000, 10, 1, "A", "west", 100, 5),
            (1001, 20, 1, "B", "west", 150, 7),
            (1002, 10, 2, "A", "east", 120, 0),
            (1003, 20, 1, "B", "west", 210, 13),
            (None, 30, None, "C", None, None, None),
            (None, None, 3, None, "south", None, None),
        ],
    ),
    (
        "select item_id, order_id, product_id, user_id, state, brand, pair_cost, state_qty",
        [
            (1000, 100, 10, 1, "CA", "A", 100, 5),
            (1001, 100, 20, 1, "CA", "B", 150, 7),
            (1002, 101, 10, 2, "NY", "A", 120, 0),
            (1003, 102, 20, 1, "CA", "B", 210, 13),
            (None, None, 30, None, None, "C", None, None),
            (None, None, None, 3, "TX", None, None, None),
        ],
    ),
    (
        "select user_id, region, sum(state_qty) as sq",
        [(1, "west", 25), (2, "east", 0), (3, "south", None)],
    ),
    (
        "select user_id, state, region, sum(state_qty) as sq",
        [(1, "CA", "west", 25), (2, "NY", "east", 0), (3, "TX", "south", None)],
    ),
    (
        "select region, brand, sum(pair_cost) as t, sum(state_qty) as sq",
        [
            ("west", "A", 100, 5),
            ("west", "B", 360, 20),
            ("east", "A", 120, 0),
            (None, "C", None, None),
            ("south", None, None, None),
        ],
    ),
    (
        "select product_id, brand, cost, total_pair_cost",
        [(10, "A", 2, 220), (20, "B", 3, 360), (30, "C", 4, None)],
    ),
    (
        "select item_id, brand, region, pair_cost where pair_cost is null",
        [(None, "C", None, None), (None, None, "south", None)],
    ),
]


def _executor(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    return executor


@pytest.fixture(scope="module")
def twins():
    return _executor(DERIVED), _executor(MATERIALIZED)


@pytest.fixture(scope="module")
def forked_twins():
    return _executor(FORKED_DERIVED), _executor(FORKED_MATERIALIZED)


def _key(row: tuple) -> tuple:
    return tuple((v is None, str(v)) for v in row)


def _rows(executor: Executor, query: str) -> list[tuple]:
    return sorted(
        (tuple(r) for r in executor.execute_text(query + ";")[-1].fetchall()),
        key=_key,
    )


@pytest.mark.parametrize("query,expected", TWIN_CASES, ids=[q for q, _ in TWIN_CASES])
def test_dimension_attribute_read_on_the_fact_stream(twins, query, expected):
    derived, materialized = twins
    rows = _rows(derived, query)
    assert rows == _rows(materialized, query)
    assert rows == sorted(expected, key=_key)


@pytest.mark.parametrize(
    "query,expected", FORKED_CASES, ids=[q for q, _ in FORKED_CASES]
)
def test_two_families_with_attributes_on_both_streams(forked_twins, query, expected):
    derived, materialized = forked_twins
    rows = _rows(derived, query)
    assert rows == _rows(materialized, query)
    assert rows == sorted(expected, key=_key)
