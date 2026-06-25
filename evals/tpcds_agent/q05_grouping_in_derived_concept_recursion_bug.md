# Bug: `grouping()` inside a named derived concept (no rollup) → recursion (q05)

**Status:** FIXED (2026-06-25). `_fix_projection_grouping_mode` (select_finalize.py) now
raises a clean `InvalidSyntaxException` when a `grouping()`/`grouping_id()` is reached from the
SELECT but no rollup/cube/grouping-sets aggregate anchors it — instead of letting the
abstract-grain concept recurse. Tests: `test_grouping_without_rollup_raises_clean_error` (V1+V3)
and `test_grouping_in_named_concept_with_rollup_builds` in tests/engine/test_duckdb.py. **Separate
bug** from `q05_union_derived_output_recursion_bug.md` (different trigger, different path).
**Surfaced by:** TPC-DS q05 enriched eval, agent attempt using `grouping()` in a `case`.
**Severity:** Medium. The input is malformed (`grouping()` with no rollup/cube), but the planner
should reject it with a clean error, not recurse / stack-overflow.

## Symptom

The agent wrapped `grouping()` in a derived concept and selected it without any `by rollup`:

```trilogy
auto channel_type <- case
    when grouping(combined.channel) = 0 and grouping(combined.entity_id) = 1 then ...
    when grouping(combined.channel) = 1 and grouping(combined.entity_id) = 1 then 'grand total'
    else ... end;
select channel_type, sum(...) ... limit 100;
```

In the eval trace it raised a guard message:

```
Recursion error building concept local.channel_type with grain Grain<Abstract> and lineage
case(WHEN grouping(ref:combined.channel)<abstract> = 0 ... ). This is likely due to a circular reference.
```

On the current tree the minimal form stack-overflows (`RecursionError`) in `generate_sql`.

## Minimal repro / trigger isolation

All three ingredients are required — drop any one and it builds:

```trilogy
import raw.all_sales as all_sales;
auto ct <- case when grouping(all_sales.channel) = 1 then 'tot' else all_sales.channel end;
select ct, sum(all_sales.ext_sales_price) as g;
```

| Variant | Result |
|---|---|
| V1: `grouping()` in a **named derived concept** (`auto ct <- ...`), **no `by rollup`** | **RECURSE** |
| V2: same, but the select adds `... by rollup all_sales.channel` | OK |
| V3: `grouping()` **inline** in a SELECT `case` (not a named concept), no rollup | OK |

So the trigger is specifically **`grouping()` captured in a named/derived concept whose grain is
unresolved because no rollup spec exists** to anchor it. Inline `grouping()` (V3) is fine, and a
rollup (V2) gives it a grain.

## Root cause (hypothesis)

`grouping()` is only meaningful under a ROLLUP/CUBE/GROUPING SETS. When it is wrapped in a named
concept and there is no rollup spec, the concept gets `Grain<Abstract>`, and grain resolution for
that concept loops (the concept's grain can't be pinned, and resolving it re-enters). This is the
same "grain can't be anchored → recursion" family as the union cyclic-grain bug, but reached via
the `grouping()`-without-rollup path rather than a union output. The two fixes are independent:
the union fix (`get_select_grain_and_keys` keeps a RowsetItem's own grain) does not cover this.

## Suggested fix

Reject `grouping()`/`grouping_id()` that is not under a rollup/cube/grouping-sets query with a
clean author-time error, e.g. *"grouping() requires a `by rollup`/`cube`/grouping-sets clause in
the enclosing select; it has no meaning without a grouping set."* That both prevents the recursion
and tells the agent the actual fix (add `by rollup`, as V2). Defense in depth: a visiting guard in
the grain-resolution walk so an unanchorable grain never stack-overflows.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-155234/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # RecursionError
```
