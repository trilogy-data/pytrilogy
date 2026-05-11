# Multi-select rowset + outer alias: SQL generation hangs

## Summary

`engine.generate_sql(text)` time scales superlinearly with the number of
unaligned ("carry-through") columns in a multi-select rowset, but only
when the outer SELECT applies an alias (`as foo`) to a rowset-derived
concept. With ~12 carry-through columns and a single outer alias,
generation does not complete in 60s (treated as a hang). Removing only
the alias drops the same shape to <1s.

This is the same family as the existing q80 note in
[STATUS.md](STATUS.md):

> Final `SELECT alias` of rowset-derived concepts (e.g.
> `q80_results.sales_total as sales`) causes the planner to hang
> indefinitely. Bare references plan in <1s.

…but the new evidence below shows the blowup is graded — it scales with
the number of carried columns — and the trigger is in `generate_sql()`
alone, no execution needed.

## Reproduction

The trigger is in [`query64_repro.preql`](query64_repro.preql). It uses
the standard TPC-DS `store_sales` model and the existing `memory_sf001/`
sf=0.01 parquet data (loaded via `IMPORT DATABASE 'memory_sf001';`).

```bash
cd tests/modeling/tpc_ds_duckdb
.venv/Scripts/python -c "
import sys, time
sys.path.insert(0, '../../..')
from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig
from pathlib import Path
env = Environment(working_path=Path('.'))
ex = Dialects.DUCK_DB.default_executor(environment=env, conf=DuckDBConfig())
ex.execute_raw_sql(\"IMPORT DATABASE 'memory_sf001';\")
text = open('query64_repro.preql').read()
t = time.time(); sql = ex.generate_sql(text)[-1]; print(f'WITH alias: {time.time()-t:.1f}s')
text2 = text.replace('q64_results.item_sk as ik', 'q64_results.item_sk')
t = time.time(); sql = ex.generate_sql(text2)[-1]; print(f'WITHOUT alias: {time.time()-t:.1f}s')
"
```

Expected output (on a recent commit):

```
WITH alias: 11.7s
WITHOUT alias: 0.3s
```

## Trigger conditions (all required)

1. A multi-select rowset (`with X as ... MERGE ... align ...;`).
2. Each branch has unaligned "carry-through" columns (selected but not in
   the `align` list).
3. The outer SELECT applies an alias to one of the rowset-derived
   concepts: `X.field as renamed_field` or `X.aligned_key as ak`.

The slowdown is not all-or-nothing; SQL generation time grows with the
number of carry-through columns.

## Measurements

Single multi-select (`q64_results`), 3 align keys, 1 outer alias on the
aligned key. `n` = unaligned carry-through columns in the left branch
(right branch has only the align keys + 1 metric in all cases). Right
branch metrics + carry-through are not part of the trigger — the
trigger appears to grow with the left branch's carried column count.

| n  | WITH alias | WITHOUT alias |
|----|------------|---------------|
| 1  | 0.3 s      | 0.2 s         |
| 2  | 0.7 s      | 0.2 s         |
| 3  | 1.1 s      | 0.2 s         |
| 4  | 11.2 s     | 0.2 s         |
| 12 | >60 s (hang) | 0.4 s       |

The "WITHOUT alias" column is essentially flat; the "WITH alias" column
explodes between n=3 and n=4. Suggests combinatorial/exponential search
through some path that is gated on the presence of the alias.

## Why this matters

This blocks expressing query shapes that naturally want one rowset with
a self-join via `align` (e.g. TPC-DS q64: "compare 1999 vs 2000 sales
per (item, store, zip)"). The workaround — two separate rowsets +
cross-rowset `merge a.k into b.k;` — works, but doubles the preql size
and makes the cross-branch join harder to read. See
[`query64.preql`](query64.preql) for the workaround in use.

## Workaround

Either:

- **Drop the outer alias.** If your downstream code can refer to the
  rowset-qualified name (`q64_results.item_sk`), the hang vanishes.
- **Split into two rowsets** and use cross-rowset
  `merge cross_99.k into cross_00.k;` instead of one multi-select
  with `align`.

## Where to start digging

- The path that emits SQL for a rowset's "aliased-outer-reference"
  output. Without an alias, the outer reference is satisfied by
  passing through the rowset's CTE column directly; with an alias the
  outer SELECT needs to render a fresh column expression, and that
  appears to be where the search explodes.
- `query51_repro.preql` documents a different planner bug also tied to
  outer-SELECT rendering of rowset-derived concepts (this one emits an
  `INVALID_REFERENCE_BUG_<...>` marker into the SQL). Worth checking
  whether the two paths share code, since both are sensitive to how the
  outer SELECT projects rowset columns.
- Try gating the SQL generation entry point with a recursion-depth
  counter to confirm the path is recursing/expanding.

## Spike findings (2026-05-11)

### Root cause

`QueryDatasource` (`trilogy/core/models/execute.py`) is a `@dataclass`
with a custom `__hash__` that returns `hash(self.identifier)` but **no
custom `__eq__`**. The dataclass-generated `__eq__` recursively compares
every field — `input_concepts`, `output_concepts`, `datasources`,
`source_map`, `grain`, `joins`, …  `source_map` is a
`dict[str, set[QueryDatasource | BuildDatasource | UnnestJoin]]`, so the
auto-eq cascades through nested datasources whenever Python falls back
from a hash match to value-equality (every `set.union`, every dict
insert).

This violates the hash/eq invariant in a particularly bad way: two QDS
with the same identifier *always* hash-collide, so the recursive
field-by-field eq runs on essentially every set lookup.

`QueryDatasource.__add__` in `merge_node._resolve` does
`set.union` for each entry in `source_map`. With ≥4 carry-through
columns + outer alias, the planner walks deeper merge trees, multiplying
the recursive cost. Profile showed:

```
ncalls    tottime  cumtime  function
31_517_733  11.48s   14.92s  BuildConcept.__eq__         (build.py:1014)
29_167_048   8.06s   22.01s  <auto>.__eq__ (small dc, line 9)
33_054_409   3.55s    3.55s  BuildGrain.__eq__           (build.py:512)
   75_016/256  0.95s  29.47s  QueryDatasource.__eq__ (line 25, recursive)
   7_222 set.union calls inside QueryDatasource.__add__ → 29.5s total
```

### Fix

Add an identifier-based `__eq__` to `QueryDatasource` that mirrors the
existing `__hash__`. The class is already designed around identifier
identity — the same-identifier-different-fields case in `__add__` calls
the merge path that consolidates outputs, so identity-based eq is the
existing contract.

```python
def __eq__(self, other):
    if type(other) is not QueryDatasource:
        return NotImplemented
    return self.identifier == other.identifier
```

### Results

Same repro on the same data:

| case                 | before  | after  |
|----------------------|---------|--------|
| query64_repro WITH   | 11.45 s | 0.60 s |
| query64_repro WITHOUT| 0.32 s  | 0.32 s |

Full test suite: 2413 passed / 187 skipped / 0 failed across all
non-adventureworks tests, plus 102 passing / 1 xfailed in the
`tpc_ds_duckdb` modeling suite. No regressions.

### Caveats / follow-ups

- The originally-documented q80 hang (`q80_results.sales_total as
  sales`) no longer hangs, but reaches a *different* planner failure
  (`ValueError: Cannot resolve query. No remaining priority concepts…`).
  That's a preexisting, distinct planner gap — the hang was masking it.
  File as a separate ticket if/when someone needs the aliased q80 shape.
- `BaseJoin` and `UnnestJoin` are also `@dataclass` with custom
  `__hash__` and auto-eq — the same pattern. They show up far smaller in
  the profile (downstream of `QueryDatasource.__eq__` recursion), and
  pruning `QueryDatasource.__eq__` already cuts off the recursion. Could
  be revisited if a future repro shows them dominating.

