"""An outer-join padding NULL is absence, not a key value, so it must never
pair null-safely with a real NULL group on the other side.

The q30 shape: a customer side preserved against a complete address domain
(so unmatched addresses carry an all-NULL customer), merged with an aggregate
grouped by a genuinely nullable customer key (so the "unknown customer" rows
form a real NULL group). `is not distinct from` treated both NULLs as the same
value and cross-joined them -- 311 padded address rows x 20 unknown-customer
groups = 6,220 duplicates of 20 distinct rows.

Contract: null-safe equality needs NULLs that are VALUES on BOTH sides. The
control cell pins that half -- two aggregates over the same nullable key still
pair their NULL groups exactly once.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# (addr_sk, state) -- three GA addresses hold no customer, so the preserved
# customer side pads three rows; a single unmatched row could not distinguish
# a cross join from an ordinary one.
ADDRESS_ROWS: list[tuple[int, str]] = [
    (1, "GA"),
    (2, "GA"),
    (3, "GA"),
    (4, "CA"),
    (5, "GA"),
]
# (cust_sk, cust_name, addr_sk)
CUSTOMER_ROWS: list[tuple[int, str, int]] = [
    (10, "ann", 1),
    (11, "bob", 4),
]
# (ret_id, cust_sk, region, amt) -- two NULL-key groups, one per region, so a
# fan-out multiplies by 2 rather than merely duplicating.
RETURN_ROWS: list[tuple[int, int | None, str, int]] = [
    (100, 10, "e", 5),
    (101, None, "e", 7),
    (102, None, "w", 8),
    (103, 11, "w", 9),
]
# (pay_id, cust_sk, pay) -- control side, same nullable key
PAYMENT_ROWS: list[tuple[int, int | None, int]] = [
    (200, 10, 100),
    (201, None, 50),
    (202, 11, 70),
]

MODEL = """
key addr_sk int;
property addr_sk.state string;
datasource addresses (addr_sk: addr_sk, state: state) grain (addr_sk)
query '''{addresses}''';

key cust_sk int;
property cust_sk.cust_name string;
datasource customers (cust_sk: cust_sk, cust_name: cust_name, addr_sk: ~addr_sk)
grain (cust_sk)
query '''{customers}''';

key ret_id int;
property ret_id.amt int;
property ret_id.region string;
datasource returns (ret_id: ret_id, cust_sk: ~?cust_sk, region: region, amt: amt)
grain (ret_id)
query '''{returns}''';

key pay_id int;
property pay_id.pay int;
datasource payments (pay_id: pay_id, cust_sk: ~?cust_sk, pay: pay) grain (pay_id)
query '''{payments}''';
"""

PADDING_QUERY = """
auto cust_amt <- sum(amt) by cust_sk, region;
auto region_avg <- avg(cust_amt) by region;

where cust_amt > 0.5 * region_avg and state = 'GA'
select cust_name, cust_amt
order by cust_name asc nulls first, cust_amt asc nulls first;
"""

VALUE_NULL_QUERY = """
auto total_amt <- sum(amt) by cust_sk;
auto total_pay <- sum(pay) by cust_sk;

select cust_sk, total_amt, total_pay
order by cust_sk asc nulls first;
"""


def _rows_sql(rows: list[tuple], names: list[str]) -> str:
    def cell(value) -> str:
        if value is None:
            return "cast(null as int)"
        return f"'{value}'" if isinstance(value, str) else str(value)

    return " union all ".join(
        "select " + ", ".join(f"{cell(v)} {n}" for v, n in zip(row, names))
        for row in rows
    )


def _model() -> str:
    return MODEL.format(
        addresses=_rows_sql(ADDRESS_ROWS, ["addr_sk", "state"]),
        customers=_rows_sql(CUSTOMER_ROWS, ["cust_sk", "cust_name", "addr_sk"]),
        returns=_rows_sql(RETURN_ROWS, ["ret_id", "cust_sk", "region", "amt"]),
        payments=_rows_sql(PAYMENT_ROWS, ["pay_id", "cust_sk", "pay"]),
    )


def _run(tmp_path: Path, query: str) -> tuple[list[tuple], str]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(_model() + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]), sql


def _amount_by_customer_region() -> dict[tuple[int | None, str], int]:
    out: dict[tuple[int | None, str], int] = {}
    for _, cust, region, amt in RETURN_ROWS:
        out[(cust, region)] = out.get((cust, region), 0) + amt
    return out


def _expected_padding_rows() -> list[tuple]:
    amounts = _amount_by_customer_region()
    per_region: dict[str, list[int]] = {}
    for (_, region), amt in amounts.items():
        per_region.setdefault(region, []).append(amt)
    ga_states = {addr for addr, state in ADDRESS_ROWS if state == "GA"}
    # A NULL return key names no customer, so it reaches no customer row -- and
    # a GA address with no customer contributes no customer attributes.
    ga_customers = {
        cust: name for cust, name, addr in CUSTOMER_ROWS if addr in ga_states
    }
    rows = []
    for (cust, region), amt in amounts.items():
        avg = sum(per_region[region]) / len(per_region[region])
        if amt > 0.5 * avg and cust in ga_customers:
            rows.append((ga_customers[cust], amt))
    return sort_rows(rows)


def _expected_value_null_rows() -> list[tuple]:
    amt: dict[int | None, int] = {}
    for _, cust, _, value in RETURN_ROWS:
        amt[cust] = amt.get(cust, 0) + value
    pay: dict[int | None, int] = {}
    for _, cust, value in PAYMENT_ROWS:
        pay[cust] = pay.get(cust, 0) + value
    return sort_rows([(c, amt[c], pay[c]) for c in amt.keys() & pay.keys()])


def test_padding_null_does_not_pair_with_a_real_null_group(tmp_path: Path):
    rows, sql = _run(tmp_path, PADDING_QUERY)
    assert rows == _expected_padding_rows(), f"{rows}\n{sql}"


def test_value_nulls_still_pair_exactly_once(tmp_path: Path):
    rows, sql = _run(tmp_path, VALUE_NULL_QUERY)
    assert rows == _expected_value_null_rows(), f"{rows}\n{sql}"
