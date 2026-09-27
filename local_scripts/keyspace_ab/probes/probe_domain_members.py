"""Rows-first probe for docs/keyspace_phase_plan.md open item 3: a `~` region
with two demanded dimension attributes, one read by a derivation on the FACT
stream (`tier_amount <- case when tier = 'gold' ...`, so `tier` is peeled onto
the solid rows) and one plain (`name`), on the derived/materialized twin; then
the two-family forked model (`cost` under `pair_cost`, `brand` plain; `state`
under `state_qty`, `region` plain) with hand rows.

`--sql <n>` prints query n's SQL on the derived model (`--model F` for the
forked one); `--q <queries...>` runs ad-hoc spellings on both twins."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from trilogy import Dialects

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
CASES: list[tuple[str, list[tuple] | None]] = [
    (
        "select customer_id, name, tier_amount",
        [(1, "ann", 20), (1, "ann", 40), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, tier, tier_amount",
        [(1, "gold", 20), (1, "gold", 40), (2, "silver", 30), (3, "gold", None)],
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
        "select name, tier_amount",
        [("ann", 20), ("ann", 40), ("bob", 30), ("cat", None)],
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
        "select tier, tier_amount",
        [("gold", 20), ("gold", 40), ("silver", 30), ("gold", None)],
    ),
    (
        "select customer_id, name, sum(tier_amount) as t",
        [(1, "ann", 60), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select name, tier, sum(tier_amount) as t",
        [("ann", "gold", 60), ("bob", "silver", 30), ("cat", "gold", None)],
    ),
    (
        "select customer_id, name, tier_amount where tier = 'gold'",
        [(1, "ann", 20), (1, "ann", 40), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier_amount where tier_amount is null",
        [(3, "cat", None)],
    ),
    (
        "select customer_id, name, tier where tier_amount is null",
        [(3, "cat", "gold")],
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
        "select customer_id, name, amount, tier_amount",
        [
            (1, "ann", 10, 20),
            (1, "ann", 20, 40),
            (2, "bob", 30, 30),
            (3, "cat", None, None),
        ],
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
        "select customer_id, name, tier_label, tier_amount",
        [
            (1, "ann", "gold:ann", 20),
            (1, "ann", "gold:ann", 40),
            (2, "bob", "silver:bob", 30),
            (3, "cat", "gold:cat", None),
        ],
    ),
    (
        "select name, tier_label, sum(tier_amount) as t",
        [("ann", "gold:ann", 60), ("bob", "silver:bob", 30), ("cat", "gold:cat", None)],
    ),
    (
        "select customer_id, name, tier_amount where big",
        [(1, "ann", 40), (2, "bob", 30)],
    ),
    (
        "select customer_id, name, tier_amount where big or big is null",
        [(1, "ann", 40), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier_amount where name != 'bob'",
        [(1, "ann", 20), (1, "ann", 40), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier, sum(tier_amount) as t where tier = 'gold'",
        [(1, "ann", "gold", 60), (3, "cat", "gold", None)],
    ),
    (
        "select customer_id, name, coalesce(sum(tier_amount), 0) as t",
        [(1, "ann", 60), (2, "bob", 30), (3, "cat", 0)],
    ),
    (
        "select customer_id, name, tier_amount, order_id",
        [
            (1, "ann", 20, 100),
            (1, "ann", 40, 101),
            (2, "bob", 30, 102),
            (3, "cat", None, None),
        ],
    ),
    (
        "select customer_id, name, silver_amount",
        [(1, "ann", 0), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier, silver_amount",
        [(1, "ann", "gold", 0), (2, "bob", "silver", 30), (3, "cat", "gold", None)],
    ),
    (
        "select customer_id, name, sum(silver_amount) as s",
        [(1, "ann", 0), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, sum(silver_amount) as s",
        [(1, 0), (2, 30), (3, None)],
    ),
    (
        "select customer_id, sum(tier_amount) as t",
        [(1, 60), (2, 30), (3, None)],
    ),
    (
        "select customer_id, name, sum(amount) as t",
        [(1, "ann", 30), (2, "bob", 30), (3, "cat", None)],
    ),
    (
        "select customer_id, name, tier, sum(tier_amount) as t",
        [(1, "ann", "gold", 60), (2, "bob", "silver", 30), (3, "cat", "gold", None)],
    ),
    (
        "select tier, count(customer_id) as c",
        [("gold", 2), ("silver", 1)],
    ),
    (
        "select name, count(customer_id) as c, sum(tier_amount) as t",
        [("ann", 1, 60), ("bob", 1, 30), ("cat", 1, None)],
    ),
    (
        "select tier, sum(tier_amount) as t",
        [("gold", 60), ("silver", 30)],
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
FORKED_CASES: list[tuple[str, list[tuple] | None]] = [
    (
        "select item_id, product_id, user_id, brand, pair_cost",
        [
            (1000, 10, 1, "A", 100),
            (1001, 20, 1, "B", 150),
            (1002, 10, 2, "A", 120),
            (1003, 20, 1, "B", 210),
            (None, 30, None, "C", None),
            (None, None, 3, None, None),
        ],
    ),
    (
        "select item_id, product_id, user_id, brand, cost, pair_cost",
        [
            (1000, 10, 1, "A", 2, 100),
            (1001, 20, 1, "B", 3, 150),
            (1002, 10, 2, "A", 2, 120),
            (1003, 20, 1, "B", 3, 210),
            (None, 30, None, "C", 4, None),
            (None, None, 3, None, None, None),
        ],
    ),
    (
        "select product_id, brand, sum(pair_cost) as t",
        [(10, "A", 220), (20, "B", 360), (30, "C", None)],
    ),
    (
        "select brand, total_pair_cost",
        [("A", 220), ("B", 360), ("C", None)],
    ),
    (
        "select product_id, brand, cost, total_pair_cost",
        [(10, "A", 2, 220), (20, "B", 3, 360), (30, "C", 4, None)],
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
        "select item_id, brand, region, pair_cost where brand != 'B'",
        [(1000, "A", "west", 100), (1002, "A", "east", 120), (None, "C", None, None)],
    ),
    (
        "select item_id, brand, region, pair_cost where pair_cost is null",
        [(None, "C", None, None), (None, None, "south", None)],
    ),
    (
        "select product_id, brand, count(item_id) as n, sum(pair_cost) as t",
        [(10, "A", 2, 220), (20, "B", 2, 360), (30, "C", 0, None)],
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
]


def _ex(model: str):
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(model)
    return ex


def _key(r):
    return tuple((v is None, str(v)) for v in r)


def _rows(ex, q):
    try:
        return sorted(
            (tuple(r) for r in ex.execute_text(q + ";")[-1].fetchall()), key=_key
        )
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:200]}"


def _run(label, derived_model, materialized_model, cases):
    d, m = _ex(derived_model), _ex(materialized_model)
    bad = 0
    for i, (q, want) in enumerate(cases):
        rd, rm = _rows(d, q), _rows(m, q)
        want_sorted = sorted(want, key=_key) if want is not None else None
        oracle = "SAME" if rd == rm else "DIFF"
        hand = (
            "" if want is None else (" hand=OK " if rd == want_sorted else " hand=BAD")
        )
        print(f"[{label}{i}] oracle={oracle}{hand} {q}")
        if rd != rm or (want is not None and rd != want_sorted):
            bad += 1
            print(f"      derived:      {rd}")
            print(f"      materialized: {rm}")
            if want is not None:
                print(f"      want:         {want_sorted}")
    print(f"\n{label}: {bad} of {len(cases)} differ\n")
    return bad


if __name__ == "__main__":
    forked = "--model" in sys.argv and sys.argv[sys.argv.index("--model") + 1] == "F"
    dm, mm, cases = (
        (FORKED_DERIVED, FORKED_MATERIALIZED, FORKED_CASES)
        if forked
        else (DERIVED, MATERIALIZED, CASES)
    )
    if "--sql" in sys.argv:
        n = int(sys.argv[sys.argv.index("--sql") + 1])
        q, _ = cases[n]
        ex = _ex(dm)
        print(q)
        print(_rows(ex, q))
        print(ex.generate_sql(q + ";")[-1])
        sys.exit()
    if "--q" in sys.argv:
        d, m = _ex(dm), _ex(mm)
        for q in sys.argv[sys.argv.index("--q") + 1 :]:
            if q.startswith("--"):
                break
            if "--raise" in sys.argv:
                d.execute_text(q + ";")
            rd, rm = _rows(d, q), _rows(m, q)
            print(
                f"{'SAME' if rd == rm else 'DIFF'} {q}\n      derived:      {rd}\n      materialized: {rm}"
            )
        sys.exit()
    total = _run("T", DERIVED, MATERIALIZED, CASES)
    total += _run("F", FORKED_DERIVED, FORKED_MATERIALIZED, FORKED_CASES)
    print(f"{total} differ in all")
