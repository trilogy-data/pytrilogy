# Bug: multi-select arms sharing an output name → broken SQL (INVALID_REFERENCE_BUG / _virt_func_alias)

**Status:** FIXED 2026-06-06 (clean-error route, suggested fix #2). Found 2026-06-07 [sic],
enriched eval q76; same family seen in q75, q77.

## Fix

`multi_select_statement` (`trilogy/parsing/v2/rules/multiselect_rules.py`) now scans arm
`output_components` after finalize and raises `InvalidSyntaxException` when an output address
appears in more than one arm, instead of letting the collapsed graph node reach codegen as
`INVALID_REFERENCE_BUG` / a dropped `_virt_func_alias`. Message points at the fix:
"Multi-select arms must use distinct output names; '<name>' appears in more than one arm …
`align grp: <name>1, <name>2`". This is the clean-error path (suggested fix #2); internal
per-arm disambiguation (fix #1) was not attempted. Test:
`tests/test_parse_engine_v2.py::test_parse_text_v2_multiselect_duplicate_arm_outputs_raise`.

---

**Original report (OPEN, found 2026-06-07, enriched eval q76; same family seen in q75, q77).**
**Severity:** high — the natural way to write a 3-channel UNION (`store`/`catalog`/`web`) in a
`merge…align…derive` multi-select is to give each arm the SAME output column names. That emits
SQL containing a literal `INVALID_REFERENCE_BUG`, an `UndefinedConceptException` on a
`_virt_func_alias_…`, or an unbound `:label` bind-param — never a clean error. This single bug
strands the entire channel-union cluster: q76 (4.78M tokens, exhausted), and contributes to q75 /
q77. The agent cycled ~8 wholesale rewrites trying to escape it.

## Root cause (hypothesis)

The planner keys concepts by address (`local.<name>`). When two merge arms both project the same
alias (`… as g` in both arms, `count(...) as cnt` in both arms), the two arm outputs collapse to a
single `local.g` / `local.cnt` graph node *before* `align` can tie them. Codegen then can't resolve
which arm's CTE a reference points at and substitutes `INVALID_REFERENCE_BUG`, or loses the
aggregate's virtual alias (`_virt_func_alias_…`). Same family as the q64 invalid-GROUP-BY name
collision (`bug_query_scoped_join_conflicting_filter.md`, derive-output-reuses-arm-name section).

**Either** make same-named arm outputs work (disambiguate by arm), **or** reject with a clean error
("multi-select arms must use distinct output names; `align` ties them") — never emit
`INVALID_REFERENCE_BUG`.

## Deterministic reproduction (no checked-in model needed — inline model)

Driver:
```python
from trilogy import Dialects
from trilogy.core.models.environment import Environment
Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(BODY)[-1]
```

Inline model shared by all variants:
```trilogy
key sid int; property sid.cat string; property sid.amt int;
datasource s (sid:sid, cat:cat, amt:amt) grain (sid) address s;
key wid int; property wid.cat2 string; property wid.amt2 int;
datasource w (wid:wid, cat2:cat2, amt2:amt2) grain (wid) address w;
```

### Variant A — same aggregate alias in both arms → `INVALID_REFERENCE_BUG`
```trilogy
SELECT cat as g, count(sid) as cnt,
MERGE
SELECT cat2 as g, count(wid) as cnt,
ALIGN grp: g, g
DERIVE coalesce(cnt, 0) as t
ORDER BY grp;
```
→ `ValueError: Invalid reference string found in query: SELECT INVALID_REFERENCE_BUG as "grp" …`

### Variant B — constant channel label per arm + aggregate → `_virt_func_alias` lost
```trilogy
SELECT 'store' as ch, cat as g, count(sid) as cnt,
MERGE
SELECT 'web' as ch2, cat2 as g, count(wid) as cnt,
ALIGN c: ch, ch2 and grp: g, g
DERIVE coalesce(cnt, 0) as t
ORDER BY c;
```
→ `UndefinedConceptException: Concept '_virt_func_alias_…' not found in environment.`

### Control — DISTINCT names per arm → compiles OK
```trilogy
SELECT cat as g1, count(sid) as cnt1,
MERGE
SELECT cat2 as g2, count(wid) as cnt2,
ALIGN grp: g1, g2
DERIVE coalesce(cnt1, 0) as a, coalesce(cnt2, 0) as b
ORDER BY grp;
```
→ generates valid SQL.

## Minimization findings

- Trigger = **two arms emit the same output alias** (`g`/`g` and/or `cnt`/`cnt`). Distinct names per
  arm → clean. The `align` clause is *supposed* to tie distinct-named columns, but agents
  intuitively reuse one name set across arms (the SQL `UNION ALL` mental model).
- A constant label per arm (`'store' as ch`) shifts the failure from `INVALID_REFERENCE_BUG` to the
  `_virt_func_alias` drop, but both are the same underlying address-collision.

## Provenance

Enriched eval q76 (count rows per channel where an FK is null). The agent needs a 3-channel union;
Trilogy has no `UNION`, so it reached for `merge…align…derive` with identical arm column names and
got broken SQL every time (M32 `_virt_func_alias`, M45 `INVALID_REFERENCE_BUG`). The
agent-info `aligned-multi-select` example uses distinct names per arm, but nothing warns that
same-named arms break — and the error gives no hint. See `token_burn_inventory` / the q76 deep-dive.

## Suggested fixes (either)

1. Disambiguate same-named arm outputs internally (qualify by arm index before address-keying), so
   the intuitive form works.
2. Detect duplicate arm-output addresses at parse/build and raise: "multi-select arms must use
   distinct output names; use `align` to tie them (e.g. `cat as g1` / `cat2 as g2`,
   `align grp: g1, g2`)." Plus an agent-info GOTCHA line.
