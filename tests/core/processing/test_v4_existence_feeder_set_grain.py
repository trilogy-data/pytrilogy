"""A semijoin feeder is a SET: the group graph's provider, sliced to the
subselect's columns and emitted one row per value, wired once.

Distilled from tpc-ds q54 and q64. Both need only the planner, so no database.
"""

from pathlib import Path
from unittest.mock import patch

from trilogy import Dialects, Environment
from trilogy.core.models.execute import CTE
from trilogy.core.processing.v4_helper import strategy_builder
from trilogy.executor import Executor

_WORKING = Path(__file__).resolve().parents[3] / "tests" / "modeling" / "tpc_ds_duckdb"

# q54's shape: the set is a rowset handle whose body reads the multi-channel
# union, so the body's own grain keys (channel, item) sit hidden on the node the
# boundary projects the handle off.
_ROWSET_HANDLE_SET = """import all_sales as sales;
import store_sales as ss;

rowset my_customers <- where
    sales.channel in ('CATALOG', 'WEB')
    and sales.item.category = 'Women'
select sales.billing_customer.sk as my_cust_id;

select ss.customer.sk
where ss.customer.sk in my_customers.my_cust_id;"""

# q64's shape: the membership is hosted inside a rowset body, so the outer
# plan's build loop walks the inner plan's already-wired tree.
_NESTED_HOST = """import store_sales as ss;
import catalog_sales as cs;

auto cs_sale <- sum(cs.ext_list_price) by cs.item.sk;

rowset cs_ui <- where cs_sale > 0 select cs.item.sk as cs_ui_item_id;

rowset ss_rows <- where ss.item.sk in cs_ui.cs_ui_item_id
select ss.ticket_number, ss.item.sk;

select ss_rows.ss.item.sk, count(ss_rows.ss.ticket_number) as tickets;"""


def _executor() -> Executor:
    env = Environment(working_path=_WORKING)
    return Dialects.DUCK_DB.default_executor(environment=env)


def test_feeder_drops_the_body_grain_keys_the_set_does_not_need():
    executor = _executor()
    sql = executor.generate_sql(_ROWSET_HANDLE_SET)[-1]
    assert "sales_channel" not in sql, sql
    assert "sales_item_sk" not in sql, sql
    query = executor.parse_text(_ROWSET_HANDLE_SET)[-1]
    grouped = [
        [c.address for c in cte.output_columns]
        for cte in query.ctes
        if isinstance(cte, CTE) and cte.group_to_grain
    ]
    assert grouped == [["my_customers.my_cust_id"]]


def test_a_membership_inside_a_rowset_body_is_planned_once():
    """The inner plan wires the provider; the outer loop must leave it alone
    rather than reach the standalone fallback for a set it holds no group for."""
    with patch.object(
        strategy_builder._CleanFeederCache,
        "_build",
        autospec=True,
        side_effect=strategy_builder._CleanFeederCache._build,
    ) as build:
        _executor().generate_sql(_NESTED_HOST)
    build.assert_not_called()
