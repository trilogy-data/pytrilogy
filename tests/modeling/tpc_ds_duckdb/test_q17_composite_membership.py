"""Cross-model tuple membership `(a, b) in (m.a, m.b)` must plan the same way
wherever it is hosted. The right side is by definition the UNFILTERED set, so
moving the membership from a plain `where` into an inline-filtered aggregate
changes which rows are tested, never what the set is -- but the v4 group-graph
route used to source the tuple per address, landing the pair on two independent
dimension groups and rejecting it (q17).

Both spellings must feed off ONE co-occurrence source scanned from the fact.
Sourcing the components separately would test a dimension cross product instead
of pairs the fact actually carries, so the row counts below are the real guard;
the `on 1=1` assertion catches the cross product at generation time.

A pair whose components live on two different facts genuinely has no single
source and is still rejected, naming the logical concepts and the remedy."""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL_DIR = Path(__file__).parent

HEADER = """
import store_sales as ss;
import catalog_sales as cs;
"""

FILTERED_AGGREGATE = HEADER + """
auto cat_in_st <- count(grain(ss.ticket_number, ss.item.sk)
    ? (ss.customer.sk, ss.item.sk) in (cs.billing_customer.sk, cs.item.sk)) by *;

select cat_in_st as match_out;
"""

PLAIN_WHERE = HEADER + """
select count(grain(ss.ticket_number, ss.item.sk)) as match_out
where (ss.customer.sk, ss.item.sk) in (cs.billing_customer.sk, cs.item.sk);
"""

ANCHORED_ROWSET = HEADER + """
with pairs as
select cs.billing_customer.sk as pc, cs.item.sk as pi, count(cs.order_number) as _anchor;

auto cat_in_st <- count(grain(ss.ticket_number, ss.item.sk)
    ? (ss.customer.sk, ss.item.sk) in (pairs.pc, pairs.pi)) by *;

select cat_in_st as match_out;
"""

SPLIT_FACTS = """
import store_sales as ss;
import catalog_sales as cs;
import web_sales as ws;

auto cat_in_st <- count(grain(ss.ticket_number, ss.item.sk)
    ? (ss.customer.sk, ss.item.sk) in (cs.billing_customer.sk, ws.item.sk)) by *;

select cat_in_st as match_out;
"""

ORACLE = """
SELECT count(*) FROM memory.store_sales s
WHERE EXISTS (
    SELECT 1 FROM memory.catalog_sales c
    WHERE c.cs_bill_customer_sk IS NOT DISTINCT FROM s.ss_customer_sk
      AND c.cs_item_sk IS NOT DISTINCT FROM s.ss_item_sk)
"""


def _executor():
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR)
    )


def test_membership_in_filtered_aggregate_reads_one_fact_anchored_source():
    sql = _executor().generate_sql(FILTERED_AGGREGATE)[-1]
    assert "INVALID_REFERENCE" not in sql
    assert "catalog_sales" in sql
    assert "on 1=1" not in sql


def test_split_fact_pair_error_names_concepts_and_remedy():
    with pytest.raises(ValueError) as err:
        _executor().generate_sql(SPLIT_FACTS)
    message = str(err.value)
    assert "cs.billing_customer.sk" in message
    assert "ws.item.sk" in message
    assert "ONE model or rowset" in message
    assert "rowset" in message
    assert "INVALID_REFERENCE_BUG" not in message
    assert "cs_item_items" not in message


def test_anchored_rowset_remedy_generates():
    sql = _executor().generate_sql(ANCHORED_ROWSET)[-1]
    assert "INVALID_REFERENCE" not in sql
    assert "catalog_sales" in sql
    assert "on 1=1" not in sql


@pytest.mark.parametrize(
    "query",
    [FILTERED_AGGREGATE, PLAIN_WHERE, ANCHORED_ROWSET],
    ids=["agg", "where", "rowset"],
)
def test_membership_row_counts_match_oracle(engine_sf001, query):
    expected = engine_sf001.execute_raw_sql(ORACLE).fetchall()[0][0]
    assert expected > 0
    assert engine_sf001.execute_text(query)[-1].fetchall()[0][0] == expected
