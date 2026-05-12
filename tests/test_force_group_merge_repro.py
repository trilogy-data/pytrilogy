"""Minimal reproduction of "can only merge two datasources if the force_group
flag is the same" error during multi-select merge resolution.

Hit by q77 when wrapping the catalog branch in its own `with … as` rowset.
The catalog branch cross-joins a scalar rowset (cr_totals) to broadcast a
count + totals, which sets force_group differently from the plain per-store
aggregation rowsets. Trying to MERGE the two rowsets in a multi-select with
`align` fails in merge_node._resolve at:

    merged[source.identifier] = merged[source.identifier] + source

raising SyntaxError("can only merge two datasources if the force_group flag
is the same") from BuildDatasource.__add__.

Workaround used in query77.preql: inline the SELECTs directly inside the
multi-select branches rather than wrapping each branch in `with … as`.
"""

from textwrap import dedent

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment


REPRO_PREQL = dedent(
    """
    key fact_id int;
    properties <fact_id> (
        grp int?,
        val int,
    );

    datasource fact_a (
        id: fact_id,
        grp: grp,
        val: val,
    )
    grain (fact_id)
    address memory.fact_a;

    # Scalar rowset (1 row): cross-join broadcast source.
    with totals as
    SELECT
        sum(val) as total_value,
        count(fact_id) as total_count,
    ;

    # Branch A wraps a SELECT that cross-joins `totals`. The scalar
    # broadcast sets force_group=True on the resulting datasource.
    with branch_a as
    SELECT
        'A' as branch_a_name,
        grp as branch_a_grp,
        sum(val) * totals.total_count as branch_a_val,
    ;

    # Branch B is a plain per-grp aggregation, force_group=False.
    with branch_b as
    SELECT
        'B' as branch_b_name,
        grp as branch_b_grp,
        sum(val) as branch_b_val,
    ;

    # Multi-select with align hits the force_group mismatch when merging
    # branch_a and branch_b. Inlining the SELECTs directly inside the
    # multi-select (rather than referencing the `with branch_a/b as`
    # rowsets) avoids the error — that's the workaround used in q77.
    SELECT
        branch_a.branch_a_name as u_name_a,
        branch_a.branch_a_grp as u_grp_a,
        branch_a.branch_a_val as u_val_a,
    MERGE
    SELECT
        branch_b.branch_b_name as u_name_b,
        branch_b.branch_b_grp as u_grp_b,
        branch_b.branch_b_val as u_val_b,
    align
        u_name: u_name_a, u_name_b
        AND u_grp: u_grp_a, u_grp_b
        AND u_val: u_val_a, u_val_b
    ;
    """
).strip()


@pytest.mark.xfail(
    strict=True,
    reason="Planner bug: multi-select MERGE of two rowsets with mismatched "
    "force_group flags raises 'can only merge two datasources if the "
    "force_group flag is the same' during BuildDatasource.__add__. See q77 "
    "workaround in tests/modeling/tpc_ds_duckdb/query77.preql.",
)
def test_force_group_merge_bug():
    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.generate_sql(REPRO_PREQL)
