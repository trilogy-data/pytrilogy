"""An authored scoped-join key outside the right side's grain is a join
condition, not an implied column: two fact tables sharing (item, ticket) can
disagree on the customer, and the declared customer equality must stay in the
ON clause once both grain keys are paired too (TPC-DS q17)."""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment

CUSTOMER = """key customer_sk int;
datasource customer (c_sk: customer_sk) grain (customer_sk) address customer_t;
"""

ITEM = """key item_sk int;
datasource item (i_sk: item_sk) grain (item_sk) address item_t;
"""

FACT = """import customer as customer;
import item as item;
key ticket_number int;
properties <ticket_number, item.item_sk> ({measure} int);
datasource {name} (
    t: ticket_number,
    i: item.item_sk,
    c: {binding}customer.customer_sk,
    q: ?{measure}
)
grain (item.item_sk, ticket_number)
address {name}_t;
"""

SETUP = """
create table customer_t as select * from (values (1), (2)) t(c_sk);
create table item_t as select * from (values (10), (11)) t(i_sk);
create table sales_t as select * from (values
    (100, 10, 1, 5), (100, 11, 1, 6), (101, 10, 2, 7), (102, 10, 2, 8)
) t(t, i, c, q);
create table returns_t as select * from (values
    (100, 10, 1, 1), (100, 11, 2, 2), (101, 10, 2, 3)
) t(t, i, c, q);
"""

EQUAL = """equal join ss.customer.customer_sk = sr.customer.customer_sk
equal join ss.item.item_sk = sr.item.item_sk
equal join ss.ticket_number = sr.ticket_number"""

SUBSET = """subset join sr.customer.customer_sk = ss.customer.customer_sk
subset join sr.item.item_sk = ss.item.item_sk
subset join sr.ticket_number = ss.ticket_number"""


def _executor(tmp_path: Path, binding: str):
    (tmp_path / "customer.preql").write_text(CUSTOMER)
    (tmp_path / "item.preql").write_text(ITEM)
    for name, measure in (("sales", "quantity"), ("returns", "return_quantity")):
        (tmp_path / f"{name}.preql").write_text(
            FACT.format(name=name, measure=measure, binding=binding)
        )
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql(SETUP)
    return executor


def _query(joins: str) -> str:
    return f"""import sales as ss;
import returns as sr;
select count(ss.quantity) as n, count(sr.return_quantity) as m,
    sum(ss.quantity * sr.return_quantity) as paired
{joins};"""


@pytest.mark.parametrize(
    "joins,expected",
    [(EQUAL, (2, 2, 5 + 21)), (SUBSET, (4, 2, 5 + 21))],
    ids=["equal", "subset"],
)
def test_declared_customer_key_stays_in_join(tmp_path, joins, expected):
    executor = _executor(tmp_path, "")
    query = _query(joins)
    sql = executor.generate_sql(query)[-1]
    assert '"c" = ' in sql, sql
    assert tuple(executor.execute_text(query)[-1].fetchone()) == expected


@pytest.mark.parametrize("joins", [EQUAL, SUBSET], ids=["equal", "subset"])
def test_partial_customer_key_never_pairs_mismatched_rows(tmp_path, joins):
    executor = _executor(tmp_path, "~")
    row = executor.execute_text(_query(joins))[-1].fetchone()
    assert row.paired == 5 + 21
