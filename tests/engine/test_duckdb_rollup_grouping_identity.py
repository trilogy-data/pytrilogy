"""Grouping-set identity is part of a ROLLUP/CUBE row's grain.

A window (or any sibling output) over a `by rollup` measure, alongside a
`grouping()`-derived level column, used to split the single grouping pass into
two ROLLUP CTEs and stitch them back with a join on the (nullable) grouping
dims. That join collides rows across grouping sets — a rolled-up subtotal
(dim -> NULL) and a leaf carrying a data NULL are indistinguishable — so
subtotal/grand-total rows dropped or doubled.

The per-dim `grouping()` identity is now auto-injected as a hidden output on
every rollup select and kept in the grain, so all measures co-source in one
ROLLUP CTE (no stitch join) and the output matches a hand-written
single-statement ROLLUP even when the source data itself carries NULL dims.
"""

import pytest

from trilogy import Dialects

FIXTURE = """
key sale_id int;
property sale_id.cat string;
property sale_id.cls string;
property sale_id.np float;
property sale_id.esp float;
datasource sales (sale_id: sale_id, cat: cat, cls: cls, np: np, esp: esp)
grain (sale_id)
query '''
select 1 as sale_id, 'Books' as cat, 'Fic' as cls, 10.0 as np, 100.0 as esp union all
select 2, 'Books', 'Non', -5.0, 50.0 union all
select 3, 'Elec', 'TV', 3.0, 30.0 union all
select 4, 'Elec', 'Radio', -2.0, 20.0 union all
select 5, 'Elec', 'TV', 8.0, 80.0 union all
select 6, NULL, 'Fic', 4.0, 40.0 union all
select 7, NULL, NULL, 9.0, 90.0
''';
"""

QUERY = """
auto tnp <- sum(sale_id.np);
auto tesp <- sum(sale_id.esp);
auto gross_margin <- tnp / nullif(tesp, 0);
select
    grouping(sale_id.cat) + grouping(sale_id.cls) as hierarchy_level,
    sale_id.cat,
    sale_id.cls,
    gross_margin,
    rank() over (
        partition by
            grouping(sale_id.cat) + grouping(sale_id.cls),
            case when grouping(sale_id.cls) = 0 then sale_id.cat else 'X' end
        order by gross_margin asc
    ) as within_parent_rank
by rollup (sale_id.cat, sale_id.cls)
order by
    hierarchy_level desc nulls first,
    sale_id.cat nulls first,
    within_parent_rank nulls first;
"""

# Hand-written single-statement ROLLUP + window — the oracle.
REFERENCE_SQL = """
WITH base AS (
  select 1 as sale_id, 'Books' as cat, 'Fic' as cls, 10.0 as np, 100.0 as esp union all
  select 2, 'Books', 'Non', -5.0, 50.0 union all
  select 3, 'Elec', 'TV', 3.0, 30.0 union all
  select 4, 'Elec', 'Radio', -2.0, 20.0 union all
  select 5, 'Elec', 'TV', 8.0, 80.0 union all
  select 6, NULL, 'Fic', 4.0, 40.0 union all
  select 7, NULL, NULL, 9.0, 90.0
)
SELECT
    grouping(cat) + grouping(cls) as hierarchy_level,
    cat, cls,
    sum(np) / nullif(sum(esp), 0) as gross_margin,
    rank() over (
        partition by grouping(cat) + grouping(cls),
            case when grouping(cls) = 0 then cat else 'X' end
        order by sum(np) / nullif(sum(esp), 0) asc
    ) as within_parent_rank
FROM base
GROUP BY ROLLUP (cat, cls)
ORDER BY hierarchy_level desc nulls first, cat nulls first, within_parent_rank nulls first;
"""


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def _norm(rows):
    # Sort tie-insensitively: rows tied on the ORDER BY keys (hierarchy_level,
    # cat, rank) can still differ in cls, which is not an ordering key.
    return sorted(
        (
            tuple(round(float(v), 6) if isinstance(v, float) else v for v in row)
            for row in rows
        ),
        key=lambda t: tuple((x is not None, str(x)) for x in t),
    )


def test_window_over_rollup_with_grouping_level_no_stitch_join(executor):
    sql = executor.generate_sql(QUERY)[-1]
    # One grouping pass, no join-back onto the rollup.
    assert sql.upper().count("ROLLUP") == 1, sql
    assert "JOIN" not in sql.upper(), sql


def test_window_over_rollup_matches_single_statement_oracle(executor):
    got = _norm(executor.execute_text(QUERY)[0].fetchall())
    expected = _norm(executor.execute_raw_sql(REFERENCE_SQL).fetchall())
    assert got == expected
    # exactly one grand-total row (was duplicated pre-fix)
    assert len([r for r in got if r[0] == 2]) == 1
