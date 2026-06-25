# Bug: renaming a `union(...)` output column drops UNION ALL arm duplicates

**Status:** OPEN — confirmed, deterministic repro on the current tree (2026-06-25).
**Surfaced by:** writing tests for the named-union self-alias fix
(`q05_union_output_self_alias_recursion_bug.md`); the self-alias passthrough
returned fewer rows than the no-rename projection.
**Severity:** MEDIUM — silently changes result cardinality, so a query can drop
rows just by adding an `as` rename. No crash, no error — a correctness/surprise
issue.

## Summary

Projecting a relational `union(...)` TVF's output columns **directly** preserves the
UNION ALL stack (duplicate rows from different arms are kept). Projecting the **same**
columns with an `as` rename groups them to the select grain and **drops the
duplicates**. A pure rename should not change row multiplicity.

## Repro (isolated)

Fixture — note item `2` has `val = 5` in **both** the 2001 and the 2002 arm:

```trilogy
key line_id int;
property line_id.item_id int;
property line_id.yr int;
property line_id.val int;

datasource lines (line_id: line_id, item_id: item_id, yr: yr, val: val)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 2001 as yr, 10 as val union all
select 2 as line_id, 1 as item_id, 2002 as yr, 30 as val union all
select 3 as line_id, 2 as item_id, 2001 as yr,  5 as val union all
select 4 as line_id, 2 as item_id, 2002 as yr,  5 as val
''';

with combined as union(
  (where yr = 2001 select item_id -> k, val -> v),
  (where yr = 2002 select item_id -> k, val -> v)
) -> (k, v);
```

| Projection | Rows |
|---|---|
| `select combined.k, combined.v` (no rename) | `(1,10) (1,30) (2,5) (2,5)` — **4 rows, dup kept** |
| `select combined.k as kk, combined.v as vv` | `(1,10) (1,30) (2,5)` — **3 rows, dup dropped** |
| `select combined.k as k, combined.v as v` (self-name) | same as `as kk` — 3 rows |

The dup loss is driven by the **rename**, not the self-name (`as kk` and `as k`
behave identically). The no-rename form (`test_tvf_union_named` in
`tests/engine/test_duckdb_rowset.py`, asserts all 4 rows) is the canonical correct
behavior.

## Likely cause (hypothesis — for the next agent to confirm)

The union output columns are `Purpose.KEY` with an abstract/stack grain, so a
direct projection carries the rowset/union grain and preserves arm duplicates.
The `as` rename mints a **new derived (BASIC/alias) concept** whose grain resolves
to the select grain `{k, v}`; the planner then emits a `GROUP BY k, v` and the
two `(2,5)` rows collapse to one. So the rename re-grains the projection from
"stacked rows" to "distinct (k, v)".

Where to look:
- `union_item_to_concept` / `function_to_concept` (ALIAS branch) in
  `trilogy/parsing/common.py` — what grain/keys the renamed alias concept inherits
  from a `TVF_UNION` source.
- The aggregate/group decision in the planner for a select whose only outputs are
  key-purpose union columns wrapped in an alias.

## Expected

A bare rename (`combined.k as kk`) is a 1:1 relabel and must preserve row
multiplicity — both projections should return all 4 rows. (Equivalently: if the
intent is ever to dedup, that should require an explicit aggregate/`distinct`, and
should apply to the no-rename form too — but the two forms must not silently
disagree.)

## Repro harness

```python
from trilogy import Dialects
ex = Dialects.DUCK_DB.default_executor()
ex.execute_text(_FIXTURE)  # the datasource above
def rows(proj):
    q = f"with combined as union((where yr=2001 select item_id->k, val->v),(where yr=2002 select item_id->k, val->v)) -> (k,v); select {proj} order by 1,2;"
    return [tuple(r) for r in ex.execute_text(q)[0].fetchall()]
rows("combined.k, combined.v")          # [(1,10),(1,30),(2,5),(2,5)]
rows("combined.k as kk, combined.v as vv")  # [(1,10),(1,30),(2,5)]  <-- dup dropped
```

## Relationship to other work

Orthogonal to the named-union self-alias recursion fix
(`q05_union_output_self_alias_recursion_bug.md`, FIXED 2026-06-25) — that fix only
made the renamed/self-aliased form *build* instead of stack-overflowing; it did not
change rename grain semantics. This report is the newly-visible behavior the fix
exposed.
