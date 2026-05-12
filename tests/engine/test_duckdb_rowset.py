
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import networkx as nx
from pytest import mark, raises

from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.enums import Derivation, FunctionType, Granularity, Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.author import Concept, Grain
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import ShowStatement
from trilogy.dialect.mock import DEFAULT_SCALE_FACTOR
from trilogy.executor import Executor
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse_text

_ROWSET_DEDUP_FIXTURE = """
key row_id int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;
property row_id.return_val1 int?;
property row_id.return_val2 int?;
property row_id.is_returned bool;

datasource src (
    row_id,
    group_key,
    val1,
    val2,
)
grain (row_id)
query '''
select 1 as row_id, 100 as group_key, 10 as val1, 100 as val2 union all
select 2 as row_id, 100 as group_key, 10 as val1, 200 as val2 union all
select 3 as row_id, 100 as group_key, 20 as val1, 100 as val2 union all
select 4 as row_id, 100 as group_key, 20 as val1, 200 as val2
''';

datasource src_returns (
    row_id: ~row_id,
    return_val1,
    return_val2,
    raw(''' True '''): is_returned,
)
grain (row_id)
complete where is_returned is True
query '''
select 0 as row_id, 0 as return_val1, 0 as return_val2 where false
''';
"""

# Source has 4 rows differing on (val1, val2) but sharing val1 ∈ {10, 20}
# and val2 ∈ {100, 200}. A full-tuple SELECT DISTINCT over (group_key,
# net_val1, net_val2) keeps all 4 rows; a per-column dedup collapses 4→2.


def test_rowset_full_tuple_dedup_plain_select():
    # Plain SELECT of the rowset's columns — no aggregates downstream. The
    # rowset's declared grain is the full projection; there should be one
    # materialization with GROUP BY over all 3 cols. This is the baseline:
    # if this fails the planner can't even get the dedup right when nothing
    # downstream consumes it.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_ROWSET_DEDUP_FIXTURE)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

SELECT group_key, net_val1, net_val2;
""")[0].fetchall()
    assert len(results) == 4, f"expected 4 distinct (gk, val1, val2) rows, got {len(results)}: {results}"
    assert {(r[0], r[1], r[2]) for r in results} == {
        (100, 10, 100),
        (100, 10, 200),
        (100, 20, 100),
        (100, 20, 200),
    }


def test_rowset_full_tuple_dedup_with_aggregates():
    # Same rowset shape, but with two SUM aggregates over the rowset's
    # row-level columns. This is the q75-shape bug: each aggregate plans
    # its own rowset materialization pruned to its own column, silently
    # splitting the rowset's declared grain across consumers. Both
    # aggregates should resolve via one shared rowset materialization at
    # the rowset's full grain.
    executor = Dialects.DUCK_DB.default_executor()
    DebuggingHook()
    executor.execute_text(_ROWSET_DEDUP_FIXTURE)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1) as total_val1,
    sum(deduped.net_val2) as total_val2,
;
""")[0].fetchall()
    assert len(results) == 1
    row = results[0]
    assert row.gk == 100
    # 4 distinct (group_key, net_val1, net_val2) rows: sums should be 10+10+20+20=60 and 100+200+100+200=600
    assert row.total_val1 == 60, f"expected 60, got {row.total_val1} (per-column dedup of net_val1 collapsed 4 rows to 2)"
    assert row.total_val2 == 600, f"expected 600, got {row.total_val2} (per-column dedup of net_val2 collapsed 4 rows to 2)"


# Variant: source has a 'year' column distinguishing rows. The q75 shape
# adds inline `?` filters per year (`sum(x ? year=YYYY)`), which trilogy
# implements by generating `_virt_filter_*` BASIC autos that wrap the
# rowset value via `CASE WHEN year=YYYY THEN x ELSE NULL END`. Those
# BASIC wrappers are an extra layer between the SUM and the rowset item;
# the planner must still see them as sourcing from the rowset and
# materialize the rowset *once* with its full declared grain.
_ROWSET_DEDUP_YEAR_FIXTURE = """
key row_id int;
property row_id.year int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;
property row_id.return_val1 int?;
property row_id.return_val2 int?;
property row_id.is_returned bool;

datasource src (
    row_id,
    year,
    group_key,
    val1,
    val2,
)
grain (row_id)
query '''
-- Two years × four (val1, val2) tuples that won't survive per-col dedup:
-- val1 ∈ {10, 20}, val2 ∈ {100, 200} per year.
select 1 as row_id, 2001 as year, 100 as group_key, 10 as val1, 100 as val2 union all
select 2 as row_id, 2001 as year, 100 as group_key, 10 as val1, 200 as val2 union all
select 3 as row_id, 2001 as year, 100 as group_key, 20 as val1, 100 as val2 union all
select 4 as row_id, 2001 as year, 100 as group_key, 20 as val1, 200 as val2 union all
select 5 as row_id, 2002 as year, 100 as group_key, 11 as val1, 110 as val2 union all
select 6 as row_id, 2002 as year, 100 as group_key, 11 as val1, 220 as val2 union all
select 7 as row_id, 2002 as year, 100 as group_key, 22 as val1, 110 as val2 union all
select 8 as row_id, 2002 as year, 100 as group_key, 22 as val1, 220 as val2
''';

datasource src_returns (
    row_id: ~row_id,
    return_val1,
    return_val2,
    raw(''' True '''): is_returned,
)
grain (row_id)
complete where is_returned is True
query '''
select 0 as row_id, 0 as return_val1, 0 as return_val2 where false
''';
"""


def test_rowset_full_tuple_dedup_with_filtered_aggregates():
    # The q75 shape: aggregates use inline `?` filters (e.g.,
    # `sum(deduped.x ? year = 2001)`). Each filtered SUM produces a
    # `_virt_filter_*` BASIC auto wrapping the rowset value, so the
    # SUM's direct argument is a BASIC concept — not a rowset item
    # directly. The planner must still recognize the underlying rowset
    # source and share one materialization across the four filtered
    # aggregates (val1 ∈ {2001, 2002} × val2 ∈ {2001, 2002}).
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_ROWSET_DEDUP_YEAR_FIXTURE)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT year, group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1 ? deduped.year = 2001) as v1_2001,
    sum(deduped.net_val1 ? deduped.year = 2002) as v1_2002,
    sum(deduped.net_val2 ? deduped.year = 2001) as v2_2001,
    sum(deduped.net_val2 ? deduped.year = 2002) as v2_2002,
;
""")[0].fetchall()
    assert len(results) == 1
    row = results[0]
    assert row.gk == 100
    # 4 distinct (year=2001, gk, v1, v2) rows: v1 sum = 10+10+20+20 = 60, v2 sum = 100+200+100+200 = 600
    # 4 distinct (year=2002, gk, v1, v2) rows: v1 sum = 11+11+22+22 = 66, v2 sum = 110+220+110+220 = 660
    assert row.v1_2001 == 60, f"expected 60, got {row.v1_2001}"
    assert row.v2_2001 == 600, f"expected 600, got {row.v2_2001}"
    assert row.v1_2002 == 66, f"expected 66, got {row.v1_2002}"
    assert row.v2_2002 == 660, f"expected 660, got {row.v2_2002}"