"""Lock for a renamed ROLLUP dimension over a row-preserving inline aggregate
(tpc-ds q80 / q97 regression, 2026-06-26).

A query that aggregates an *inline* expression whose args are row-preserving
(`sum(profit - coalesce(loss, 0))`) grouped `by rollup chan_label`, while
renaming the grouping dimension in the output (`chan_label as channel`), must
keep the SELECT dimension and the GROUP-BY ROLLUP key on the SAME column.

The regression (`_row_preserving_function_inputs` recursing into a
`BuildConceptArgs` arg) classified the inline aggregate's inputs as
row-preserving and skipped input-grain normalization, which materialized the
rename `channel` as its own CASE column separate from the rollup key
`chan_label`. The final then SELECTed `channel` while GROUPing on `chan_label`
-> "column channel must appear in the GROUP BY clause" (invalid SQL). Only a
top-level `BuildConcept` arg is row-preserving; an inline function arg forces
normalization, so the dimension is rendered once and the rename rides the
grouped key. Guards correctness (rows) and validity (no dangling SELECT dim)."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key txn int;
property txn.chan string;
property txn.profit float;
property txn.loss float;

datasource facts (
    t: txn,
    c: chan,
    p: profit,
    l: loss,
)
grain (txn)
query '''
select 1 t, 'STORE' c, 10.0 p, 1.0 l union all
select 2 t, 'STORE' c, 20.0 p, 2.0 l union all
select 3 t, 'WEB' c, 40.0 p, 4.0 l
''';
"""

_QUERY = """
auto chan_label <- case
    when chan = 'STORE' then 'store channel'
    when chan = 'WEB' then 'web channel'
    else null
end;
select
    chan_label as channel,
    sum(profit - coalesce(loss, 0)) as total,
by rollup (chan_label)
order by channel asc nulls first;
"""

_EXPECTED_ROWS = [
    (None, 63.0),
    ("store channel", 27.0),
    ("web channel", 36.0),
]


def _run(v4: bool):
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = v4
    try:
        env, _ = Environment().parse(_MODEL)
        engine = Dialects.DUCK_DB.default_executor(environment=env)
        rows = engine.execute_text(_QUERY)[-1].fetchall()
        return [(r[0], float(r[1])) for r in rows]
    finally:
        CONFIG.use_v4_discovery = prior


def test_rollup_rename_alias_rows_match_baseline():
    assert _run(v4=False) == _EXPECTED_ROWS
    assert _run(v4=True) == _EXPECTED_ROWS
