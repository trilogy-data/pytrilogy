# Bug: a `by rollup` aggregate wrapped in an expression (`coalesce(...)`) hides the rollup spec, so `grouping()` is falsely rejected (q05)

**Status:** FIXED (2026-06-26). `_select_rollup_spec` now walks into the item lineage via a new
`_collect_rollup_wrappers` traversal (mirrors `_collect_standard_grouping_wrappers`) to detect a
non-STANDARD rollup/cube/grouping-sets aggregate nested in `coalesce(...)`/arithmetic/`case`/etc.
Regression test: `tests/engine/test_duckdb.py::test_grouping_over_rollup_wrapped_in_expression_colocates`.
**Surfaced by:** TPC-DS q05 enriched eval (run `20260626-125555`) — the natural zero-filled-rollup +
grouping()-ordering pattern is blocked.
**Severity:** Medium-High. Blocks the canonical rollup report idiom: zero-fill the measures
(`coalesce(sum(x) by rollup …, 0)`) AND order subtotals with `grouping()`.

## Symptom

```
Syntax error: grouping()/grouping_id() requires a `by rollup`/`by cube`/grouping-sets aggregate
in the enclosing select; it has no meaning without a grouping set
```

…raised even though the select **does** contain `by rollup` aggregates — they're just wrapped in
`coalesce(...)`.

## Minimal repro

```trilogy
import raw.all_sales as s;
-- A: rollup NOT wrapped + grouping()                         -> OK
select s.channel, sum(s.ext_sales_price) by rollup s.channel as m, --grouping(s.channel) as g
order by grouping(s.channel), s.channel limit 5;

-- B: rollup WRAPPED in coalesce + grouping()  (q05's form)   -> ERR "grouping() requires a rollup"
select s.channel, coalesce(sum(s.ext_sales_price) by rollup s.channel, 0) as m, --grouping(s.channel) as g
order by grouping(s.channel), s.channel limit 5;

-- C: wrapped rollup, NO grouping()                           -> OK (rollup is valid + recognized)
select s.channel, coalesce(sum(s.ext_sales_price) by rollup s.channel, 0) as m order by s.channel limit 5;
```

A and C pass; **only B fails.** So the wrapped rollup is a perfectly valid rollup (C executes it);
the failure is *specifically* that `grouping()` can't find the rollup spec when it's nested.

## Root cause

`_select_rollup_spec` (`trilogy/parsing/v2/select_finalize.py:730`) only recognizes a rollup when
the select item's lineage **is itself** an `AggregateWrapper`:

```python
for item in select.selection:
    lineage = _item_lineage(item, context)
    if (
        isinstance(lineage, AggregateWrapper)
        and lineage.grouping != AggregateGroupingMode.STANDARD
    ):
        return (lineage.grouping, list(lineage.by), ...)
return None
```

For `coalesce(sum(x) by rollup s.channel, 0)` the lineage is a `Function` (coalesce) that *wraps*
the rollup `AggregateWrapper`, so the top-level `isinstance` check fails → returns `None`. With no
spec found, `_fix_projection_grouping_mode` then raises the (correct-for-no-rollup, wrong-here)
"grouping() requires a rollup" error.

## Suggested fix

Make `_select_rollup_spec` **walk into the item lineage** to find a nested non-STANDARD
`AggregateWrapper` — mirroring `_collect_standard_grouping_wrappers` (`:759`), which already
recursively descends `Function`/`Parenthetical`/`CaseWhen`/`Between`/arithmetic to find grouping
wrappers. Reuse that traversal (or a sibling that matches rollup/cube/grouping-sets aggregates)
so a rollup nested in `coalesce(...)`/`round(...)`/arithmetic is detected. Once the spec is found,
the existing grouping-mode alignment already handles the rest (A proves the path works unwrapped).

This is the same family as the prior grouping/rollup spec work
(`project_grouping_in_derived_concept_recursion_bug` added the "no rollup" guard;
`project_b3_grouping_in_orderby_rollup`); the guard just needs the rollup detector to see through
expression wrappers.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-125555/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # variant B -> InvalidSyntaxException
```
