# Regression: v4 union-concept branches render invalid SQL (union_concepts)

Status: OPEN, **correctness regression** (invalid SQL / `BinderException`). Introduced
by commit `350dcab60 more_fixes` (the q81 dimension-rejoin + q02 commit; strategy_builder
+97). Passes through `921032cd5`; first fails at `350dcab60`.

`test_v4_parity[union_concepts]` (a curated `cases/` parity case — these are supposed
to pass) crashes under v4.

## Symptom

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python -m pytest \
  "tests/core/processing/test_v4_parity_cases.py::test_v4_parity" -k union_concepts -q
```

```
_duckdb.BinderException: Binder Error: Column "name" referenced that exists in the
SELECT clause - but this column cannot be referenced before it is defined
```

## Root cause

Case: two datasources `sone(x:space_one, y:one_name)` and `stwo(x:space_two,
y:two_name)`; `space_all <- union(space_one, space_two)`, `name <- union(one_name,
two_name)`; `select space_all, name`.

v4 renders each UNION branch selecting the **union OUTPUT concept names** directly,
but the branch's source only exposes that branch's own columns:

```sql
SELECT name as "name", space_all as "space_all"          -- ❌ name/space_all not in sone
FROM ( select 2 x,'test' y union all select 1,'fun' ) as "sone"
UNION ALL
SELECT name as "name", space_all as "space_all"          -- ❌ not in stwo
FROM ( select 4 x,'bravo' y union all select 5,'alpha' ) as "stwo"
ORDER BY "space_all" asc
```

Each branch should map ITS contributing column to the union output, e.g. for `sone`:

```sql
SELECT "sone".x as "space_all", "sone".y as "name" FROM ... as "sone"
```

i.e. branch 1 contributes `space_one→space_all`, `one_name→name`; branch 2 contributes
`space_two→space_all`, `two_name→name`. Instead both branches emit the bare union
output names (`name`, `space_all`), which exist in neither branch's source map → the
binder rejects them (and DuckDB reads `space_all` in ORDER BY as a forward reference).

The `350dcab60` strategy_builder changes (cover-group / `needed` / dimension-projection
work) appear to have changed how a UNION group's per-branch outputs are assembled, so
the branch SELECT now projects the union's output addresses rather than each branch's
local source columns aliased to those outputs.

## Where to look

- UNION node assembly in `strategy_builder.py` (changed in `350dcab60`): how each
  branch's output concepts are chosen / aliased. Compare the union-branch output
  mapping against `921032cd5` (last good).
  `git diff 921032cd5 350dcab60 -- trilogy/core/processing/v4_helper/strategy_builder.py`
- The UNION generator (`v4_node_generators`, `gen_union`) and how branch contributors
  declare which local column feeds each union output.
- Likely interaction with the `needed`/cover changes: a union branch's `needed` set may
  now resolve to the union output address instead of the branch's contributing concept.

## Reproduce / verify

- Generation-only: `engine.generate_sql(local_scripts/v4_evals/cases/union_concepts.preql)`
  and check each UNION branch SELECTs its own source columns aliased to `space_all`/`name`.
- Expected rows: `space_all ∈ {1,2,4,5}` with matching `name`.

## Acceptance

- `test_v4_parity[union_concepts]` passes under v4 (valid SQL, rows = v3 oracle).
- No regression to the `350dcab60` size wins (q81/q30.alt/q76 still pass) — bisect the
  strategy_builder change so the union path is fixed without reverting the dimension
  work.
- Full v4 sweep: only the 3 `TestAggregateInputGrain` stale tests remain (those are a
  separate, benign `minimize_build_grain` test-update — see below), and they should be
  updated, not this.
