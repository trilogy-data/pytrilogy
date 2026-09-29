# Filtered COUNT pushed into a LEFT JOIN nulls a grouping key

A correctness bug: wrong rows, silently. Found 2026-09-29 while probing for
`docs/handoff_open_optimization_items.md`; it predates that work (same rows on
`ba73ef34f`).

## Symptom

On the model in `tests/core/processing/test_v4_dim_peel_not_built.py`
(`_MODEL`: `order_items` with `uid: ~user_id`, three users, user 3 has no
orders, order 11 is user 2's only order and its one line costs 3.0):

```
select order_id, state, count(line_id ? sale_price > 4) as big_lines
order by order_id asc;
```

| | rows |
|---|---|
| returned | `(10, 'ca', 2)`, `(None, 'ny', 0)`, `(None, 'wa', 0)` |
| expected | `(10, 'ca', 2)`, `(11, 'ny', 0)`, `(None, 'wa', 0)` |

Order 11 exists and has no line over 4, so its count is 0. It comes back with
its key NULL, indistinguishable from the extension row of a user with no
orders.

## Cause

`PushFilteredCountIntoJoin`
(`trilogy/core/optimizations/filtered_count_join.py`) rewrites

```
count(CASE WHEN r.sale_price > 4 THEN r.line_id END) ... LEFT JOIN r ON <keys>
```

into

```
count(r.line_id) ... LEFT JOIN r ON <keys> AND r.sale_price > 4
```

The count is the same either way. Every other column read from `r` is not: a
row of `r` that fails the filter no longer matches, so the left row is
NULL-extended and `r.order_id` renders NULL. Here `order_id` is a grouping key
read from the right side:

```
SELECT
    "wakeful"."order_id" as "order_id",
    "highfalutin"."state" as "state",
    count("wakeful"."line_id") as "big_lines"
FROM
    "highfalutin"
    LEFT OUTER JOIN "wakeful" on "highfalutin"."user_id" = "wakeful"."user_id"
        and "wakeful"."sale_price" > 4
GROUP BY 1, 2
```

The rule checks that the filter's arguments come from the right side. It
never checks that nothing else does.

## Evidence

Same model, rule on unless noted:

| query | rows | right? |
|---|---|---|
| `order_id, state, count(line_id ? sale_price > 4)` | `(None, 'ny', 0)` for order 11 | no |
| same, `CONFIG.optimizations.push_filtered_count_into_join = False` | `(11, 'ny', 0)` | yes |
| same, FK declared solid (`uid: user_id`): join is INNER, rule does not fire | `(11, 'ny', 0)` | yes |
| `order_id, count(line_id ? sale_price > 4)`: no join | `(11, 0)` | yes |
| `order_id, state, sum(sale_price ? sale_price > 4)`: not a COUNT | `(11, 'ny', None)` | yes |
| `order_id, state, count(line_id), count(line_id ? sale_price > 4)`: two aggregates | `(11, 'ny', 1, 0)` | yes |
| `auto big_line <- line_id ? sale_price > 4; select order_id, state, count(big_line)` | `(None, 'ny', 0)` | no |

Turning off `predicate_pushdown` or `push_filtered_aggregate_input` does not
change the wrong rows; only this rule does.

Over tpc_h, tpc_ds_duckdb and thelook_duckdb the rule fires once: tpc_h q13
(customers LEFT JOIN orders, filter on the order comment). There no
non-aggregate output reads the right side, so q13 is the legitimate case and
its rows are right.

## Fix

The rewrite holds only when the aggregate is the sole reader of the right
side. Veto it when any other column the CTE renders is sourced from
`right_source`: outputs, grouping columns, and anything the WHERE, HAVING or
ORDER BY reads.

Points to settle while fixing:

- `cte.output_columns` may not be the whole read set. Check hidden outputs,
  the grouping columns and `cte.condition` too, or a hidden right-side
  grouping key reproduces this with a clean output list.
- A column rendered `coalesce(left.k, right.k)` reads the right side but is
  unchanged by the rewrite when the left side always supplies it. Treat it as
  a read unless the distinction is needed to keep q13 (it is not: q13 has
  none).
- The join key itself (`user_id` here) is safe when rendered off the left
  side, which is how a plan-time LEFT renders it.

Do not fix this by changing the join type or the planner. The plan before
optimization is right.

## Reproduce

From the repo root:

```python
import sys
sys.path.insert(0, ".")
from tests.core.processing.test_v4_dim_peel_not_built import _MODEL
from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

q = """select order_id, state, count(line_id ? sale_price > 4) as big_lines
order by order_id asc;"""
for on in (True, False):
    CONFIG.optimizations.push_filtered_count_into_join = on
    env, _ = Environment().parse(_MODEL)
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    print(on, [tuple(r) for r in ex.execute_text(q)[-1].fetchall()])
    print(ex.generate_sql(q)[-1])
```

Restore the flag afterwards: `CONFIG` is process-wide and a leaked `False`
hides the bug from every later test.

## Acceptance

- The query above returns `(11, 'ny', 0)`, asserted by rows in a test. The
  named-filter form (`count(big_line)`) too.
- A unit test on the rule: it does not fire when a non-aggregate column reads
  the right side, and still fires on the q13 shape. The rule has no test of
  its own today; `tests/optimization/test_optimization_pipeline.py` only
  lists its name.
- tpc_h q13 SQL is unchanged (`tests/modeling/tpc_h/zquery13.log`). No other
  `zquery<N>.log` should move, since the rule fires nowhere else in the
  corpora.
- Planner suites pass (tests/modeling, tests/engine, tests/optimization).
- `ruff check . --fix`, `mypy trilogy`, `black .`

## Not checked

- Dialects other than DuckDB. The rule is dialect-independent, so the rows
  should be wrong everywhere it fires.
- Whether downstream projects have queries of this shape. Any filtered count
  grouped by a fact-side column beside a `~` dimension attribute is exposed.
- Whether `PushFilteredAggregateInput` or the HAVING push have the same gap.
  They did not change these rows, which is not proof they are sound.
