# Bug: aggregate used as an `align` target → invalid GROUP-BY SQL

**Status:** FIXED 2026-06-07 (found 2026-06-07, enriched eval q76). Took fix option 1
("make it just work"): an aligned multiselect column now inherits the group-ness of its
underlying per-arm concept, so aligning an aggregate keeps it OUT of the arm's GROUP BY
while aligning a dimension still groups. Fix in `CTE.group_concepts.check_is_not_in_group`
(trilogy/core/models/execute.py): added a `Derivation.MULTISELECT` branch that resolves via
`find_source` (mirrors the ROWSET branch); derive items always group-exempt. Regression test
`test_multi_select_align_aggregate` in tests/engine/test_duckdb.py.
**Severity:** medium-high — agents naturally align the per-arm aggregate (so the combined
measure lines up across channels). Trilogy emits the aggregate into the outer `GROUP BY`,
producing SQL DuckDB rejects. `generate_sql` succeeds; the failure is at execution. ~16 calls /
~1M tokens on q76 before the agent stumbled onto the `derive`-side workaround.

## Symptom

```
(_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
```
The aligned aggregate is emitted twice in each arm's SELECT and referenced by GROUP-BY ordinal —
e.g. `count(...) as "cnt1", count(...) as "lc" ... GROUP BY 2, ..., N` where ordinal N *is* the
aggregate column.

## Deterministic reproduction (inline model — executes)

```python
from trilogy import Dialects
from trilogy.core.models.environment import Environment
eng = Dialects.DUCK_DB.default_executor(environment=Environment())
sql = eng.generate_sql(BODY)[-1]                      # succeeds
eng.execute_raw_sql("CREATE TABLE s(sid int, cat varchar)")
eng.execute_raw_sql("CREATE TABLE w(wid int, cat2 varchar)")
eng.execute_raw_sql(sql).fetchall()                   # BinderException
```

`BODY`:
```trilogy
key sid int; property sid.cat string;
datasource s (sid:sid, cat:cat) grain (sid) address s;
key wid int; property wid.cat2 string;
datasource w (wid:wid, cat2:cat2) grain (wid) address w;

SELECT cat as g1, count(sid) as cnt1,
MERGE
SELECT cat2 as g2, count(wid) as cnt2,
ALIGN grp: g1, g2 and lc: cnt1, cnt2          -- aligning the AGGREGATE is the trigger
ORDER BY grp;
```

Control (align only the dimensions, combine the aggregate in `derive`) → valid:
```trilogy
... ALIGN grp: g1, g2
DERIVE coalesce(cnt1, cnt2) as lc
ORDER BY grp;
```

## Suggested fix (either)

1. **Don't duplicate the aligned aggregate into the outer GROUP BY** — pass it through as a
   grouped output (the align identity, not a group key). Then aligning an aggregate "just works."
2. **Reject cleanly at parse/build:** "an `align` target must be a dimension, not an aggregate;
   project per-arm aggregates and combine them in `derive` (e.g. `coalesce(cnt1, cnt2)`)." Plus an
   agent-info GOTCHA on the `aligned-multi-select` example.

## Provenance

Enriched eval q76 (per-channel null-FK counts). The agent built a 3-arm channel union and aligned
the per-channel `line_count` aggregate — the intuitive way to make the counts line up — and hit
this on every permutation (msgs 37/41/49) until the docs' "combine aggregates in derive" hint
landed (msg 50). The companion same-name multiselect bug
(`bug_multiselect_duplicate_arm_alias_codegen.md`) is now fixed; this is the next residual
multiselect codegen gap.
