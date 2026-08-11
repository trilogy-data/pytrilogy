"""Cross-model tuple membership inside an inline-filtered aggregate is not
supported (the right-side pair plans through separate dimension CTEs, so no
single existence source exists). The q17 probe surfaced that limit as an
"Unexpected error" leaking an INVALID_REFERENCE_BUG placeholder and a physical
table alias. The error must instead name the logical concepts and the
validated remedy: stage the pair through a fact-anchored rowset."""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL_DIR = Path(__file__).parent

PROBE = """
import store_sales as ss;
import catalog_sales as cs;

auto cat_in_st <- count(grain(ss.ticket_number, ss.item.sk)
    ? ss.sale_date.year = 2001
      and (ss.customer.sk, ss.item.sk) in (cs.billing_customer.sk, cs.item.sk)
      and cs.sale_date.year in (2001, 2002)
      and cs.billing_customer.sk is not null) by *;

select cat_in_st as match_out;
"""

REMEDY = """
import store_sales as ss;
import catalog_sales as cs;

with pairs as
select cs.billing_customer.sk as pc, cs.item.sk as pi, count(cs.order_number) as _anchor;

auto cat_in_st <- count(grain(ss.ticket_number, ss.item.sk)
    ? (ss.customer.sk, ss.item.sk) in (pairs.pc, pairs.pi)) by *;

select cat_in_st as match_out;
"""


def _executor():
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR)
    )


def test_cross_model_membership_in_filtered_aggregate_error_is_clean():
    with pytest.raises(ValueError) as err:
        _executor().generate_sql(PROBE)
    message = str(err.value)
    assert "cs.billing_customer.sk" in message
    assert "ONE model or rowset" in message
    assert "rowset" in message
    assert "INVALID_REFERENCE_BUG" not in message
    assert "cs_item_items" not in message


def test_fact_anchored_rowset_remedy_generates():
    sql = _executor().generate_sql(REMEDY)[-1]
    assert "INVALID_REFERENCE" not in sql
    # the pair set must come from the fact, not a dim cross product
    assert "catalog_sales" in sql
    assert "on 1=1" not in sql
