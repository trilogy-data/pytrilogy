"""Terminal BASICs over one upstream, each at its own output key, plan as one
projection: the keys are statement outputs FINAL relates anyway."""

from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement

MODEL = """
key item_sk int;
property item_sk.item_name string;
datasource items (i_sk: item_sk, i_name: item_name)
grain (item_sk)
address items;

key store_sk int;
property store_sk.store_name string;
datasource stores (s_sk: store_sk, s_name: store_name)
grain (store_sk)
address stores;

key ticket int;
datasource sales (t: ticket, i: item_sk, s: store_sk)
grain (ticket)
address sales;
"""


def _built_basic_groups(query: str) -> list[str]:
    env, statements = Environment().parse(MODEL + query)
    statement = statements[-1]
    assert isinstance(statement, SelectStatement)
    renderer = Dialects.DUCK_DB.default_renderer()
    trace = plan_trace.start(MODEL + query, renderer, None)
    try:
        process_query(env, statement)
    finally:
        plan_trace.stop()
    return [
        step["title"]
        for step in trace.to_dict()["steps"]
        if step["title"].startswith("built grp:basic")
    ]


def test_terminal_sibling_basics_share_a_group():
    groups = _built_basic_groups(
        "select item_sk, store_sk, item_name as p_name, store_name as s_name;"
    )
    assert len(groups) == 1, groups


def test_sibling_basic_off_the_output_grain_stays_apart():
    groups = _built_basic_groups(
        "select store_sk, item_name as p_name, store_name as s_name;"
    )
    assert len(groups) == 2, groups
