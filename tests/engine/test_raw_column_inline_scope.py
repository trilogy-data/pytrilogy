"""When a `raw()` datasource may be folded into a joining consumer.

The text renders verbatim, so folding it beside another table is sound only on
two counts, and both are checked here against the hazard they exist to stop:

- *Resolution*: every word in the text must be a declared column of this
  datasource that no other source in the consumer's scope also exposes.
  Otherwise the unqualified reference is ambiguous (`_probe_ambiguous`).
- *Value*: the text reads as if the datasource had a row wherever it lands, so
  it stays per-row correct only where every result row carries one. On the
  optional side of an outer join it reads FALSE where the scan's own column
  would read NULL (`_probe_outer_join`).

A raw column over a column the datasource does not declare cannot be proven to
belong to its own table, so it keeps its CTE too.
"""

import duckdb
from pytest import raises

from trilogy import Dialects, Executor

MODEL = """
key order_id int;
key customer_id int;
property order_id.amount float;
property order_id.returned_at datetime;
property order_id.is_returned bool;
property customer_id.customer_name string;

datasource orders (
    o_id: order_id,
    o_cust: customer_id,
    o_amount: amount,
) grain (order_id) address orders;

datasource returns (
    r_order: {key}order_id,
    {returned_at}
    raw('''r_returned_at IS NOT NULL'''): is_returned,
) grain (order_id) address returns;

datasource customers (
    c_id: customer_id,
    c_name: customer_name,
    {customer_extra}
) grain (customer_id) address customers;
"""

DECLARED = "r_returned_at: returned_at,"

TABLES = """
CREATE TABLE orders AS SELECT 1 o_id, 1 o_cust, 10.0 o_amount
    UNION ALL SELECT 2, 1, 20.0 UNION ALL SELECT 3, 2, 30.0;
CREATE TABLE returns AS SELECT 1 r_order, TIMESTAMP '2020-01-01' r_returned_at
    UNION ALL SELECT 3, CAST(NULL AS TIMESTAMP);
CREATE TABLE customers AS SELECT 1 c_id, 'ann' c_name{customer_column}
    UNION ALL SELECT 2, 'bob'{customer_value};
"""

QUERY = "select order_id, is_returned, customer_name order by order_id asc;"


def _executor(
    key: str = "",
    returned_at: str = DECLARED,
    customer_extra: str = "",
    customer_column: str = "",
    customer_value: str = "",
) -> Executor:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        TABLES.format(
            customer_column=customer_column,
            customer_value=customer_value,
        )
    )
    model = MODEL.format(
        key=key, returned_at=returned_at, customer_extra=customer_extra
    )
    if customer_extra:
        model = model.replace(
            "property customer_id.customer_name string;",
            "property customer_id.customer_name string;\n"
            "property customer_id.signup_at datetime;",
        )
    executor.execute_text(model)
    return executor


def _sql_and_rows(executor: Executor) -> tuple[str, list[tuple]]:
    query = executor.generate_sql(QUERY)[-1]
    return query, executor.execute_raw_sql(query).fetchall()


def test_raw_column_reference_folds_into_an_inner_joined_consumer():
    sql, rows = _sql_and_rows(_executor())
    assert "WITH" not in sql, sql
    assert 'FROM\n    "returns"' in sql, sql
    assert "r_returned_at IS NOT NULL as" in sql, sql
    assert rows == [(1, True, "ann"), (3, False, "bob")]


def test_raw_column_reference_keeps_its_cte_when_a_sibling_shares_the_name():
    executor = _executor(
        customer_extra="r_returned_at: signup_at,",
        customer_column=", TIMESTAMP '2019-01-01' r_returned_at",
        customer_value=", TIMESTAMP '2019-01-01'",
    )
    sql, rows = _sql_and_rows(executor)
    assert "WITH" in sql, sql
    assert rows == [(1, True, "ann"), (3, False, "bob")]


def test_raw_column_reference_keeps_its_cte_on_the_optional_side():
    # `~order_id` makes returns the partial side, so orders drives and every
    # order without a return row is padded.
    sql, rows = _sql_and_rows(_executor(key="~"))
    assert "WITH" in sql, sql
    assert "LEFT OUTER JOIN" in sql, sql
    assert rows == [(1, True, "ann"), (2, None, "ann"), (3, False, "bob")]


def test_raw_column_reference_keeps_its_cte_when_the_column_is_undeclared():
    sql, rows = _sql_and_rows(_executor(returned_at=""))
    assert "WITH" in sql, sql
    assert rows == [(1, True, "ann"), (3, False, "bob")]


def _probe_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(
        TABLES.format(
            customer_column=", TIMESTAMP '2019-01-01' r_returned_at",
            customer_value=", TIMESTAMP '2019-01-01'",
        )
    )
    return connection


def test_probe_ambiguous():
    """The shape the sibling-name test refuses, written out by hand."""
    connection = _probe_connection()
    with raises(duckdb.BinderException, match="Ambiguous reference"):
        connection.execute(
            'SELECT "orders"."o_id", r_returned_at IS NOT NULL '
            'FROM "returns" '
            'INNER JOIN "orders" on "returns"."r_order" = "orders"."o_id" '
            'INNER JOIN "customers" on "orders"."o_cust" = "customers"."c_id"'
        )


def test_probe_outer_join():
    """The folded text reads FALSE on a padded row where the scan reads NULL."""
    connection = _probe_connection()
    assert connection.execute(
        'SELECT "orders"."o_id", r_returned_at IS NOT NULL '
        'FROM "orders" '
        'LEFT OUTER JOIN "returns" on "orders"."o_id" = "returns"."r_order" '
        "ORDER BY 1"
    ).fetchall() == [(1, True), (2, False), (3, False)]
