"""An aggregate whose group key a consumer INNER-joins against a *filtered*
relation gets that key set mirrored into its scan as `k in (select k from ...)`.

TPC-H q21 is the motivating shape: two stacked group-bys over the whole fact
table, of which the final join keeps a few thousand orders. Guard the two ways
this turns into a pessimization — an unrestricted feeder, and an aggregate that
already filters — since both measured as regressions on the TPC-H corpus.
"""

import re
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import CTE

TPCH = Path(__file__).parent.parent / "modeling" / "tpc_h"
TPCDS = Path(__file__).parent.parent / "modeling" / "tpc_ds_duckdb"

MODEL = """
key order_id int;
key line_no int;
key supp_id int;
key nation_id int;
property supp_id.supp_name string;
property nation_id.nation_name string;
property <order_id, line_no>.late bool;

datasource lines (order_id, line_no, supp_id, late)
grain (order_id, line_no)
query '''
select 1 order_id, 1 line_no, 10 supp_id, true late
union all select 1, 2, 11, false
union all select 2, 3, 10, false
union all select 2, 4, 12, false
union all select 3, 5, 20, true
union all select 3, 6, 21, true
''';

datasource supps (supp_id, nation_id, supp_name)
grain (supp_id)
query '''
select 10 supp_id, 1 nation_id, 'S10' supp_name
union all select 11, 1, 'S11'
union all select 12, 2, 'S12'
union all select 20, 2, 'S20'
union all select 21, 2, 'S21'
''';

datasource nations (nation_id, nation_name)
grain (nation_id)
query '''select 1 nation_id, 'SAUDI' nation_name union all select 2, 'FRANCE' ''';

auto supp_count <- count(supp_id) by order_id;
"""

# The q21 shape: `supp_count` groups every row in `lines`, while the consumer
# keeps only SAUDI suppliers — so its key set is strictly narrower.
RESTRICTED = (
    MODEL + "where nation_name = 'SAUDI' and supp_count > 1 "
    "select supp_name, count(line_no) -> hits order by supp_name asc;"
)


def _with_flag(enabled: bool, fn):
    previous = CONFIG.optimizations.push_semi_join_into_aggregate
    CONFIG.optimizations.push_semi_join_into_aggregate = enabled
    try:
        return fn(Dialects.DUCK_DB.default_executor())
    finally:
        CONFIG.optimizations.push_semi_join_into_aggregate = previous


def generate(text: str, enabled: bool = True) -> str:
    return _with_flag(enabled, lambda e: e.generate_sql(text)[-1])


def _ctes(text: str) -> list:
    return _with_flag(True, lambda e: e.parse_text(text)[-1].ctes)


def _semi_join_hosts(text: str) -> list[CTE]:
    return [
        cte for cte in _ctes(text) if isinstance(cte, CTE) and cte.semi_join_filters
    ]


def _tpch_sql(query: str) -> str:
    return _model_sql(TPCH, query)


def _model_sql(root: Path, query: str) -> str:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=root)
    )
    return executor.generate_sql((root / f"{query}.preql").read_text())[-1]


def execute(text: str, enabled: bool = True) -> list:
    return _with_flag(enabled, lambda e: e.execute_text(text)[-1].fetchall())


def test_filtered_consumer_key_set_is_mirrored_into_the_aggregate():
    assert "in (select" in generate(RESTRICTED)


def test_mirror_does_not_change_results():
    assert execute(RESTRICTED, enabled=True) == execute(RESTRICTED, enabled=False)


def test_mirror_descends_past_the_joined_aggregate():
    """The probe must reach the group-by nearest the scan, not stop at the one
    the consumer joins; stopping short measured 1.7x against 6.0x on q21."""
    hosts = _semi_join_hosts(RESTRICTED)
    assert len(hosts) == 1
    host = hosts[0]
    # the host is itself consumed by a further aggregate -- i.e. we walked down
    consumers = [
        cte
        for cte in _ctes(RESTRICTED)
        if any(p.name == host.name for p in cte.dependency_nodes())
    ]
    assert consumers, f"{host.name} is the shallowest aggregate; no descent happened"
    assert any(c.group_to_grain for c in consumers)


def test_unrestricted_feeder_is_not_mirrored():
    """A join whose other side filters nothing (TPC-H q18/q10: a plain FK
    lookup) would probe an unrestricted relation — all cost, no rows removed."""
    text = (
        MODEL + "where supp_count > 1 "
        "select supp_name, count(line_no) -> hits order by supp_name asc;"
    )
    assert "in (select" not in generate(text)


@pytest.mark.parametrize("query", ["query02", "query20"])
def test_already_filtered_tpch_aggregates_are_not_mirrored(query: str):
    """When the aggregate's own scan is filtered its group set is already
    narrow, and the feeder's hash build costs more than it saves: both of these
    measured ~15% SLOWER at SF=1 before the gate was added. Pinned against the
    real models, since the shape needs a genuine pushed-down WHERE on the
    aggregate's scan that a toy model does not reproduce."""
    assert "in (select" not in _tpch_sql(query)


def test_tpch_q21_is_mirrored():
    """The measured 5x case, and the only TPC-H query the rule fires on."""
    assert "in (select" in _tpch_sql("query21")


def test_feeder_is_declared_before_the_cte_that_probes_it():
    """A WITH list is sequential, so the probed CTE has to be ordered ahead of
    its host even though it is not one of the host's row sources."""
    sql = generate(RESTRICTED)
    probe = sql.index("in (select")
    feeder = sql[probe : sql.index(")", probe)].rsplit("from ", 1)[1].strip().strip('"')
    assert sql.index(f"{feeder} as (") < sql.rindex("as (", 0, probe)


def _tpcds_ctes(query: str) -> list:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=TPCDS)
    )
    return executor.parse_text((TPCDS / f"{query}.preql").read_text())[-1].ctes


def _cte_body(sql: str, name: str) -> str:
    """The rendered text of one CTE, up to the start of the next."""
    start = sql.index("\n" + name + " as (")
    following = re.search(r"\n\w+ as \(", sql[start + 1 :])
    return sql[start : start + 1 + following.start()] if following else sql[start:]


def test_probe_reaches_the_scan_below_a_projected_join_target():
    """TPC-DS q64's consumer joins a projection over a full-join enrichment, so
    the aggregate scanning store_sales sits two nodes below the join target and
    every key it exposes is nullable. Both used to skip the mirror, leaving the
    enrichment to group the whole fact table for a two row answer."""
    hosts = [
        cte
        for cte in _tpcds_ctes("query64")
        if isinstance(cte, CTE) and cte.semi_join_filters
    ]
    assert len(hosts) == 1, [cte.name for cte in hosts]
    body = _cte_body(_model_sql(TPCDS, "query64"), hosts[0].name)
    assert '"memory"."store_sales"' in body, body
    assert "in (select" in body, body
