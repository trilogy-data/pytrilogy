"""Predicate placement across GROUP BY boundaries: the optimizer must never
relocate a filter into a grouping CTE unless it only drops whole groups, and
discovery must place pre-aggregation WHERE filters itself.

Regression for TPC-DS q81: `having home_state = 'GA' and total > 1.2 * state_avg`
where `state_avg <- avg(totals.total) by totals.return_state`. The HAVING
finer-dim rewrite turns the GA predicate into a grain-key membership semijoin
(`entity in (entity ? segment = 'keep')`) — safe for the select-grain aggregate,
but PredicatePushdown then pushed that membership into the avg's GROUP BY CTE,
whose group key is the partition (not the entity). A WHERE there filters rows
*within* each partition group, silently changing the average.

The fix (``_predicate_safe_past_grouping``) blocks every such push regardless
of atom provenance: pushdown is an optimization pass, so a push that changes
what a group computes is never legal. That exposed TPC-DS q44, which had
relied on the illegal push: an aggregate referenced in a WHERE is evaluated
over its own unfiltered scope (test_where_aggregate_input_not_filtered_by_
where), so `where store = 1 and item_avg > X` reads the all-store average —
restricting it requires binding the filter inside (`avg(x ? store = 1) by
item`), which query44.preql now does.
"""

import re

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# part A: keep rows 100+100, other rows 0+0.
# correct part_avg (all rows) = 50 -> threshold 60 -> both keep rows qualify.
# leaked part_avg (keep rows only) = 100 -> threshold 120 -> nothing qualifies.
MODEL = """
key entity int;
property entity.segment string;
property entity.part string;
property entity.amount float;

datasource rows (e: entity, s: segment, p: part, a: amount) grain (entity)
query '''
select 1 e, 'keep' s, 'A' p, 100.0 a union all
select 2 e, 'keep' s, 'A' p, 100.0 a union all
select 3 e, 'other' s, 'A' p, 0.0 a union all
select 4 e, 'other' s, 'A' p, 0.0 a''';

with totals as
select
    entity,
    segment,
    part,
    sum(amount) as total;
"""

QUERY = MODEL + """
auto part_avg <- avg(totals.total) by totals.part;

select
    totals.entity,
    totals.total
having
    totals.segment = 'keep'
    and totals.total > 1.2 * part_avg
order by totals.entity asc;
"""

# control: the segment filter authored INSIDE the aggregate's population is
# intentionally restrictive -> avg over keep rows = 100, threshold 120, no rows.
CONTROL = MODEL + """
auto part_avg <- avg(totals.total ? totals.segment = 'keep') by totals.part;

select
    totals.entity,
    totals.total
having
    totals.segment = 'keep'
    and totals.total > 1.2 * part_avg
order by totals.entity asc;
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def _cte_bodies(sql: str) -> dict[str, str]:
    ctes_only = sql.rsplit(")\nSELECT", 1)[0]
    parts = re.split(r"(?:WITH\s+|\),?\s*\n)(\w+) as \(", ctes_only)
    return {parts[i]: parts[i + 1] for i in range(1, len(parts), 2)}


def test_outer_filter_not_pushed_into_partitioned_average(executor: Executor):
    sql = executor.generate_sql(QUERY)[-1]
    avg_ctes = [body for body in _cte_bodies(sql).values() if "avg(" in body]
    assert avg_ctes, sql
    for body in avg_ctes:
        assert "_virt_filter" not in body, sql
    rows = [tuple(r) for r in executor.execute_text(QUERY)[0].fetchall()]
    assert rows == [(1, 100.0), (2, 100.0)]


def test_filter_authored_inside_average_still_applies(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(CONTROL)[0].fetchall()]
    assert rows == []


# q44 shape: the WHERE both row-filters and references the by-aggregate. An
# aggregate referenced in a WHERE is evaluated over its own unfiltered scope:
# part_avg = 50 over all rows, so `part_avg > 60` fails and no rows return —
# even though the keep-only average (100) would pass. The optimizer must not
# resurrect the old behavior by pushing `segment = 'keep'` into the avg group.
BASE = """
key entity int;
property entity.segment string;
property entity.part string;
property entity.amount float;

datasource rows (e: entity, s: segment, p: part, a: amount) grain (entity)
query '''
select 1 e, 'keep' s, 'A' p, 100.0 a union all
select 2 e, 'keep' s, 'A' p, 100.0 a union all
select 3 e, 'other' s, 'A' p, 0.0 a union all
select 4 e, 'other' s, 'A' p, 0.0 a''';
"""

WHERE_AND_AGG_REF = BASE + """
auto part_avg <- avg(amount) by part;

where segment = 'keep' and part_avg > 60
select
    entity,
    rank(entity) over (order by part_avg asc, entity asc) as rnk
order by entity asc;
"""

# the q44 idiom: restricting the aggregate's population is authored INSIDE it
WHERE_AND_BOUND_AGG = BASE + """
auto part_avg <- avg(amount ? segment = 'keep') by part;

where segment = 'keep' and part_avg > 60
select
    entity,
    rank(entity) over (order by part_avg asc, entity asc) as rnk
order by entity asc;
"""


def test_where_referenced_aggregate_scope_unfiltered(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(WHERE_AND_AGG_REF)[0].fetchall()]
    assert rows == []


def test_where_referenced_aggregate_filter_bound_inside(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(WHERE_AND_BOUND_AGG)[0].fetchall()]
    assert rows == [(1, 1), (2, 2)]
