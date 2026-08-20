"""Row-level contract for the two kinds of NULL over a 3-dimension span.

The model (docs/partial_bridge_pinning.md) separates two orthogonal binding
declarations on a fact column:

- ``~`` (partial): the column covers a SUBSET of the key's members. Licenses
  domain extension — each unmatched dimension member enters the result once,
  padding-NULL everywhere outside its FD closure.
- ``?`` (nullable): the column carries NULL as a VALUE (a guest order's
  customer). Value NULLs group on their own, survive as fact rows at every
  grain, and are removed by ``is not null`` filters.

Fixture: three dims, all `~` on the fact; customer and product also carry
value NULLs (`~?`). customer imports a transitive address dim, exercising the
off-spine licensing rule (an unlicensed transitive dim must not extend).

Data:
  customers: 1 ann (addr 200/CA), 2 bob (201/NY), 3 cat (200/CA, never orders)
  addresses: 200 CA, 201 NY, 300 ORPHAN (nobody's address)
  products : 10 A, 20 B, 30 C (never sold)
  stores   : 5 SF, 6 NYC, 7 LA (no sales)
  orders   : 100(c1,p10,s5,$50) 101(c2,p10,s6,$60) 102(c1,p20,s5,$70)
             103(GUEST,p20,s6,$80) 104(c2,NOPROD,s5,$90)
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment

_ADDR = """key sk int;
property sk.state string?;

datasource customer_address (ca_sk: sk, ca_state: state)
grain (sk)
query '''select 200 as ca_sk, 'CA' as ca_state union all
select 201, 'NY' union all select 300, 'ORPHAN' ''';
"""

_CUST = """import addr as current_address;

key sk int;
property sk.name string;

datasource customers (c_sk: sk, c_name: name, c_addr: {addr_modifier}current_address.sk)
grain (sk)
query '''select 1 as c_sk, 'ann' as c_name, 200 as c_addr union all
select 2, 'bob', 201 union all select 3, 'cat', 200''';
"""

_PROD = """key sk int;
property sk.brand string;

datasource products (p_sk: sk, p_brand: brand)
grain (sk)
query '''select 10 as p_sk, 'A' as p_brand union all
select 20, 'B' union all select 30, 'C' ''';
"""

_STORE = """key sk int;
property sk.city string;

datasource stores (s_sk: sk, s_city: city)
grain (sk)
query '''select 5 as s_sk, 'SF' as s_city union all
select 6, 'NYC' union all select 7, 'LA' ''';
"""

_ENTRY = """import cust as customer;
import prod as product;
import store as store;

key order_id int;
property order_id.amount float;

auto total_amount <- sum(amount);

datasource orders (
    o_id: order_id,
    c_sk: ~?customer.sk,
    p_sk: ~?product.sk,
    s_sk: ~store.sk,
    amount: amount,
)
grain (order_id)
query '''select 100 as o_id, 1 as c_sk, 10 as p_sk, 5 as s_sk, 50.0 as amount union all
select 101, 2, 10, 6, 60.0 union all
select 102, 1, 20, 5, 70.0 union all
select 103, null, 20, 6, 80.0 union all
select 104, 2, null, 5, 90.0''';
"""


def _build(tmp: Path, addr_modifier: str):
    (tmp / "addr.preql").write_text(_ADDR)
    (tmp / "cust.preql").write_text(_CUST.format(addr_modifier=addr_modifier))
    (tmp / "prod.preql").write_text(_PROD)
    (tmp / "store.preql").write_text(_STORE)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=str(tmp))
    )
    executor.execute_text(_ENTRY)
    return executor


@pytest.fixture(scope="module")
def matrix(tmp_path_factory):
    return _build(tmp_path_factory.mktemp("matrix"), addr_modifier="")


@pytest.fixture(scope="module")
def matrix_addr_partial(tmp_path_factory):
    return _build(tmp_path_factory.mktemp("matrix_ap"), addr_modifier="~")


def _rows(executor, query: str):
    return [
        tuple(float(v) if hasattr(v, "as_tuple") else v for v in r)
        for r in executor.execute_text(query)[0].fetchall()
    ]


def test_dim_key_alone_is_dim_domain(matrix):
    assert _rows(matrix, "select customer.sk order by customer.sk asc;") == [
        (1,),
        (2,),
        (3,),
    ]


def test_agg_by_dim_key(matrix):
    """Extension row for the never-ordering customer AND a value-NULL group
    for the guest order, as distinct rows."""
    assert _rows(
        matrix,
        "select customer.sk, total_amount order by customer.sk asc nulls last;",
    ) == [(1, 120.0), (2, 150.0), (3, None), (None, 80.0)]


def test_two_key_span(matrix):
    assert (
        _rows(
            matrix,
            """select customer.sk, product.sk, total_amount
        order by customer.sk asc nulls last, product.sk asc nulls last;""",
        )
        == [
            (1, 10, 50.0),
            (1, 20, 70.0),
            (2, 10, 60.0),
            (2, None, 90.0),
            (3, None, None),
            (None, 20, 80.0),
            (None, 30, None),
        ]
    )


def test_three_key_span_with_attrs(matrix):
    assert (
        _rows(
            matrix,
            """select customer.sk, product.sk, store.sk, customer.name, product.brand, store.city, total_amount
        order by customer.sk asc nulls last, product.sk asc nulls last, store.sk asc nulls last;""",
        )
        == [
            (1, 10, 5, "ann", "A", "SF", 50.0),
            (1, 20, 5, "ann", "B", "SF", 70.0),
            (2, 10, 6, "bob", "A", "NYC", 60.0),
            (2, None, 5, "bob", None, "SF", 90.0),
            (3, None, None, "cat", None, None, None),
            (None, 20, 6, None, "B", "NYC", 80.0),
            (None, 30, None, None, "C", None, None),
            (None, None, 7, None, None, "LA", None),
        ]
    )


def test_anchored_by_fact_key(matrix):
    assert (
        _rows(
            matrix,
            """select order_id, customer.sk, product.sk, store.sk, total_amount
        order by order_id asc nulls last, customer.sk asc nulls last, product.sk asc nulls last, store.sk asc nulls last;""",
        )
        == [
            (100, 1, 10, 5, 50.0),
            (101, 2, 10, 6, 60.0),
            (102, 1, 20, 5, 70.0),
            (103, None, 20, 6, 80.0),
            (104, 2, None, 5, 90.0),
            (None, 3, None, None, None),
            (None, None, 30, None, None),
            (None, None, None, 7, None),
        ]
    )


def test_attr_only_span(matrix):
    assert _rows(
        matrix,
        "select customer.name, product.brand order by customer.name asc nulls last, product.brand asc nulls last;",
    ) == [
        ("ann", "A"),
        ("ann", "B"),
        ("bob", "A"),
        ("bob", None),
        ("cat", None),
        (None, "B"),
        (None, "C"),
    ]


def test_transitive_attr_no_license_no_leak(matrix):
    """current_address is reached only through customers' complete FK binding:
    it contributes attributes but must NOT extend (no ORPHAN row)."""
    assert (
        _rows(
            matrix,
            """select customer.sk, product.sk, customer.current_address.state
        order by customer.sk asc nulls last, product.sk asc nulls last;""",
        )
        == [
            (1, 10, "CA"),
            (1, 20, "CA"),
            (2, 10, "NY"),
            (2, None, "NY"),
            (3, None, "CA"),
            (None, 20, None),
            (None, 30, None),
        ]
    )


def test_agg_by_transitive_attr_no_license(matrix):
    assert _rows(
        matrix,
        "select customer.current_address.state, total_amount order by customer.current_address.state asc nulls last;",
    ) == [("CA", 120.0), ("NY", 150.0), (None, 80.0)]


def test_agg_by_two_attrs(matrix):
    assert _rows(
        matrix,
        "select product.brand, store.city, total_amount order by product.brand asc nulls last, store.city asc nulls last;",
    ) == [
        ("A", "NYC", 60.0),
        ("A", "SF", 50.0),
        ("B", "NYC", 80.0),
        ("B", "SF", 70.0),
        ("C", None, None),
        (None, "LA", None),
        (None, "SF", 90.0),
    ]


def test_pin_filters_value_nulls_and_extensions(matrix):
    """The two-key pin drops BOTH extension rows and value-NULL fact rows."""
    assert (
        _rows(
            matrix,
            """where customer.sk is not null and product.sk is not null
        select customer.sk, product.sk, total_amount
        order by customer.sk asc, product.sk asc;""",
        )
        == [(1, 10, 50.0), (1, 20, 70.0), (2, 10, 60.0)]
    )


def test_single_key_pin(matrix):
    """Pinning one key keeps the other side's value NULLs and the pinned
    side's real members (extension rows for the pinned key survive its own
    pin only through other keys' filters — here customer 3's row remains
    because customer.sk=3 IS not null)."""
    assert (
        _rows(
            matrix,
            """where customer.sk is not null
        select customer.sk, product.sk, total_amount
        order by customer.sk asc nulls last, product.sk asc nulls last;""",
        )
        == [
            (1, 10, 50.0),
            (1, 20, 70.0),
            (2, 10, 60.0),
            (2, None, 90.0),
            (3, None, None),
        ]
    )


def test_licensed_transitive_dim_extends(matrix_addr_partial):
    """With customers binding the address FK `~`, the address dimension IS
    licensed: ORPHAN appears as an extension row."""
    assert _rows(
        matrix_addr_partial,
        "select customer.current_address.sk, customer.current_address.state, total_amount order by customer.current_address.sk asc nulls last;",
    ) == [
        (200, "CA", 120.0),
        (201, "NY", 150.0),
        (300, "ORPHAN", None),
        (None, None, 80.0),
    ]


def test_licensed_transitive_attr_span(matrix_addr_partial):
    assert (
        _rows(
            matrix_addr_partial,
            """select customer.sk, product.sk, customer.current_address.state
        order by customer.sk asc nulls last, product.sk asc nulls last, customer.current_address.state asc nulls last;""",
        )
        == [
            (1, 10, "CA"),
            (1, 20, "CA"),
            (2, 10, "NY"),
            (2, None, "NY"),
            (3, None, "CA"),
            (None, 20, None),
            (None, 30, None),
            (None, None, "ORPHAN"),
        ]
    )
