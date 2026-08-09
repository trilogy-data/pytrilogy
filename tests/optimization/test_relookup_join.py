"""The planner must not hand a consumer both a derived node and the relation
that node was built on.

Merging those two joins a node back to its own source; once the consumer binds
the shared columns to the source side, the source becomes load-bearing and the
plan pays a full extra pass over the row-grain relation to re-read columns the
derived node already carries. `_drop_ancestor_parents` removes the ancestor at
construction (TPC-DS q74: 0.30s -> 0.072s, and 5306 -> 3117 chars).

This is deliberately a PLANNER test, not an optimizer one: every fact the
decision needs (which node derives from which, and that the descendant already
exposes the ancestor's columns) is known where the merge is built, so no
optimizer rule should have to reconstruct it. An earlier optimizer-side repair
of this same shape landed at 0.082s / 4474 chars — strictly worse, because by
then the redundant node already exists and can only be joined around, not
un-built.
"""

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import CTE, Join

TPCDS = Path(__file__).parent.parent / "modeling" / "tpc_ds_duckdb"


def _processed(query: str):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=TPCDS)
    )
    return executor.parse_text((TPCDS / f"{query}.preql").read_text())[-1]


def _ancestors(cte: CTE | None) -> set[str]:
    if cte is None:
        return set()
    seen: set[str] = set()
    stack = list(cte.parent_ctes)
    while stack:
        node = stack.pop()
        if node.name in seen:
            continue
        seen.add(node.name)
        stack.extend(node.parent_ctes)
    return seen


def test_no_cte_joins_a_relation_its_own_base_derives_from():
    """A join whose target is already in the base's ancestry is a self-lookup:
    the rows on both sides came from the same relation."""
    processed = _processed("query74")
    by_name = {c.name: c for c in processed.ctes}
    offenders = []
    for cte in processed.ctes:
        if not isinstance(cte, CTE):
            continue
        base = by_name.get(cte.base_alias)
        if base is None or base.name == cte.name:
            continue
        ancestry = _ancestors(base)
        for join in cte.joins:
            if isinstance(join, Join) and join.right_cte.name in ancestry:
                offenders.append((cte.name, join.right_cte.name))
    assert not offenders, f"row-identity re-lookup joins remain: {offenders}"


def test_q74_stays_join_lean():
    """Guards the win: the redundant relation must not creep back as an extra
    join or the CTE it would need."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=TPCDS)
    )
    sql = executor.generate_sql((TPCDS / "query74.preql").read_text())[-1]
    assert sql.count("JOIN") <= 2, sql
    assert len(sql) < 4092, "relookup drop should keep this plan small"
